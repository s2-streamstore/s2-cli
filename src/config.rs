use std::path::{Path, PathBuf};

use config::{Config, FileFormat};
use miette::Diagnostic;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::error::S2CliError;

use serde::de;

#[derive(Debug, Serialize)]
pub struct S2Config {
    pub access_token: String,
}

/// Note: Custom deserialization to support both old and new token formats.
impl<'de> Deserialize<'de> for S2Config {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum TokenField {
            New { access_token: String },
            Old { auth_token: String },
        }

        let token = TokenField::deserialize(deserializer)?;

        Ok(S2Config {
            access_token: match token {
                TokenField::New { access_token } => access_token,
                TokenField::Old { auth_token } => auth_token,
            },
        })
    }
}

#[cfg(target_os = "windows")]
pub fn config_path() -> Result<PathBuf, S2CliError> {
    let mut path = dirs::config_dir().ok_or(S2ConfigError::DirNotFound)?;
    path.push("s2");
    path.push("config.toml");
    Ok(path)
}

#[cfg(not(target_os = "windows"))]
pub fn config_path() -> Result<PathBuf, S2CliError> {
    let mut path = dirs::home_dir().ok_or(S2ConfigError::DirNotFound)?;
    path.push(".config");
    path.push("s2");
    path.push("config.toml");
    Ok(path)
}

pub fn load_config(path: &Path) -> Result<S2Config, S2ConfigError> {
    let mut builder = Config::builder();
    if path.exists() {
        builder = builder.add_source(config::File::new(
            path.to_str().expect("config path is valid utf8"),
            FileFormat::Toml,
        ));
    }
    builder = builder.add_source(config::Environment::with_prefix("S2"));
    Ok(builder.build()?.try_deserialize::<S2Config>()?)
}

pub fn create_config(config_path: &PathBuf, access_token: String) -> Result<(), S2ConfigError> {
    let cfg = S2Config { access_token };

    if let Some(parent) = config_path.parent() {
        std::fs::create_dir_all(parent).map_err(S2ConfigError::Write)?;
    }

    let toml = toml::to_string(&cfg).unwrap();
    std::fs::write(config_path, toml).map_err(S2ConfigError::Write)?;

    Ok(())
}

#[derive(Error, Debug, Diagnostic)]
pub enum S2ConfigError {
    #[error("Failed to find a home for config directory")]
    DirNotFound,

    #[error("Failed to load config file")]
    #[diagnostic(help(
        "Did you run `s2 config set`? or use `S2_ACCESS_TOKEN` environment variable."
    ))]
    Load(#[from] config::ConfigError),

    #[error("Failed to write config file")]
    Write(#[source] std::io::Error),
}
