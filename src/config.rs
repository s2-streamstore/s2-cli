use std::path::{Path, PathBuf};

use config::{Config, FileFormat};
use miette::Diagnostic;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::error::S2CliError;

#[derive(Debug, Deserialize, Serialize)]
pub struct S2Config {
    pub auth_token: String,
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

pub fn create_config(config_path: &PathBuf, auth_token: String) -> Result<(), S2ConfigError> {
    let cfg = S2Config { auth_token };

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
        "Did you run `s2 config set`? or use `S2_AUTH_TOKEN` environment variable."
    ))]
    Load(#[from] config::ConfigError),

    #[error("Failed to write config file")]
    Write(#[source] std::io::Error),
}
