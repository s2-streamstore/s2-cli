//! Types for Basin configuration that directly map to streamstore::types.

use clap::{Parser, ValueEnum};
use serde::Serialize;
use std::{str::FromStr, time::Duration};
use streamstore::types::{BasinName, ConvertError};

pub const STORAGE_CLASS_PATH: &str = "default_stream_config.storage_class";
pub const RETENTION_POLICY_PATH: &str = "default_stream_config.retention_policy";

#[derive(Debug, Clone)]
pub struct BasinNameOrUri<S> {
    pub basin: BasinName,
    pub stream: S,
}

impl<S> From<BasinNameOrUri<S>> for BasinName {
    fn from(value: BasinNameOrUri<S>) -> Self {
        value.basin
    }
}

fn parse_maybe_basin_or_uri(s: &str) -> Result<(BasinName, Option<String>), ConvertError> {
    match BasinName::from_str(s) {
        Ok(basin) => {
            // Definitely a basin name since a valid basin name cannot have `:`
            // which is required for the URI.
            Ok((basin, None))
        }
        Err(parse_basin_err) => {
            // Should definitely be a URI else error.
            let uri = http::Uri::from_str(s).map_err(|_| parse_basin_err)?;

            match uri.scheme_str() {
                Some("s2") => (),
                Some(other) => return Err(format!("Invalid S2 URL scheme: '{other}'").into()),
                None => return Err("S2 URL scheme empty".into()),
            };

            let basin = uri.host().ok_or("Basin name missing in S2 URL")?;
            let basin = BasinName::from_str(basin)
                .map_err(|e| format!("Invalid basin name in S2 URL: {e}"))?;

            let stream = uri.path().trim_start_matches('/');
            let stream = if stream.is_empty() {
                None
            } else {
                Some(stream.to_string())
            };

            Ok((basin, stream))
        }
    }
}

pub type BasinNameOnlyUri = BasinNameOrUri<()>;

impl FromStr for BasinNameOnlyUri {
    type Err = ConvertError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (basin, stream) = parse_maybe_basin_or_uri(s)?;
        if stream.is_none() {
            Ok(Self { basin, stream: () })
        } else {
            Err("Expected S2 URL with only basin name".into())
        }
    }
}

pub type BasinNameAndMaybeStreamUri = BasinNameOrUri<Option<String>>;

impl FromStr for BasinNameAndMaybeStreamUri {
    type Err = ConvertError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (basin, stream) = parse_maybe_basin_or_uri(s)?;
        Ok(Self { basin, stream })
    }
}

#[derive(Parser, Debug, Clone)]
pub struct BasinNameAndStreamArgs {
    /// Name of the basin to manage or S2 URL with basin and stream.
    #[arg(value_name = "BASIN/S2_URL")]
    url: BasinNameAndMaybeStreamUri,
    /// Name of the stream.
    stream: Option<String>,
}

impl BasinNameAndStreamArgs {
    pub fn try_into_parts(self) -> Result<(BasinName, String), ConvertError> {
        let stream = match (self.stream, self.url.stream) {
            (Some(_), Some(_)) => return Err("Multiple stream names provided".into()),
            (None, None) => return Err("Stream name required".into()),
            (Some(s), None) | (None, Some(s)) => s,
        };
        Ok((self.url.basin, stream))
    }
}

#[derive(Parser, Debug, Clone, Serialize)]
pub struct BasinConfig {
    #[clap(flatten)]
    pub default_stream_config: Option<StreamConfig>,
}

#[derive(Parser, Debug, Clone, Serialize)]
pub struct StreamConfig {
    #[arg(short = 's', long)]
    /// Storage class for a stream.
    pub storage_class: Option<StorageClass>,
    #[arg(short = 'r', long, help("Example: 1d, 1w, 1y"))]
    /// Retention policy for a stream.
    pub retention_policy: Option<RetentionPolicy>,
}

#[derive(ValueEnum, Debug, Clone, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum StorageClass {
    Unspecified,
    Standard,
    Express,
}

#[derive(Clone, Debug, Serialize)]
pub enum RetentionPolicy {
    #[allow(dead_code)]
    Age(Duration),
}

impl From<&str> for RetentionPolicy {
    fn from(s: &str) -> Self {
        match humantime::parse_duration(s) {
            Ok(d) => RetentionPolicy::Age(d),
            Err(_) => RetentionPolicy::Age(Duration::from_secs(0)),
        }
    }
}

impl From<BasinConfig> for streamstore::types::BasinConfig {
    fn from(config: BasinConfig) -> Self {
        if let Some(default_stream_config) = config.default_stream_config.map(|c| c.into()) {
            streamstore::types::BasinConfig::with_default_stream_config(default_stream_config)
        } else {
            streamstore::types::BasinConfig::default()
        }
    }
}

impl From<StreamConfig> for streamstore::types::StreamConfig {
    fn from(config: StreamConfig) -> Self {
        let storage_class = config
            .storage_class
            .map(streamstore::types::StorageClass::from)
            .unwrap_or(streamstore::types::StorageClass::Unspecified);
        let retention_policy = config.retention_policy.map(|r| r.into());
        let stream_config =
            streamstore::types::StreamConfig::new().with_storage_class(storage_class);

        if let Some(retention_policy) = retention_policy {
            stream_config.with_retention_policy(retention_policy)
        } else {
            stream_config
        }
    }
}

impl From<StorageClass> for streamstore::types::StorageClass {
    fn from(class: StorageClass) -> Self {
        match class {
            StorageClass::Unspecified => streamstore::types::StorageClass::Unspecified,
            StorageClass::Standard => streamstore::types::StorageClass::Standard,
            StorageClass::Express => streamstore::types::StorageClass::Express,
        }
    }
}

impl From<streamstore::types::StorageClass> for StorageClass {
    fn from(class: streamstore::types::StorageClass) -> Self {
        match class {
            streamstore::types::StorageClass::Unspecified => StorageClass::Unspecified,
            streamstore::types::StorageClass::Standard => StorageClass::Standard,
            streamstore::types::StorageClass::Express => StorageClass::Express,
        }
    }
}

impl From<RetentionPolicy> for streamstore::types::RetentionPolicy {
    fn from(policy: RetentionPolicy) -> Self {
        match policy {
            RetentionPolicy::Age(d) => streamstore::types::RetentionPolicy::Age(d),
        }
    }
}

impl From<streamstore::types::RetentionPolicy> for RetentionPolicy {
    fn from(policy: streamstore::types::RetentionPolicy) -> Self {
        match policy {
            streamstore::types::RetentionPolicy::Age(d) => RetentionPolicy::Age(d),
        }
    }
}

impl From<streamstore::types::BasinConfig> for BasinConfig {
    fn from(config: streamstore::types::BasinConfig) -> Self {
        BasinConfig {
            default_stream_config: config.default_stream_config.map(Into::into),
        }
    }
}

impl From<streamstore::types::StreamConfig> for StreamConfig {
    fn from(config: streamstore::types::StreamConfig) -> Self {
        StreamConfig {
            storage_class: Some(config.storage_class.into()),
            retention_policy: config.retention_policy.map(|r| r.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::types::BasinNameOnlyUri;

    use super::BasinNameAndMaybeStreamUri;

    #[test]
    fn test_basin_name_or_url_parse() {
        let test_cases = vec![
            ("valid-basin", Some(("valid-basin", None))),
            ("s2://valid-basin", Some(("valid-basin", None))),
            ("s2://valid-basin/", Some(("valid-basin", None))),
            (
                "s2://valid-basin/stream/name",
                Some(("valid-basin", Some("stream/name"))),
            ),
            ("-invalid-basin", None),
            ("http://valid-basin", None),
            ("s2://-invalid-basin", None),
            ("s2:///stream/name", None),
            ("random:::string", None),
        ];

        for (s, expected) in test_cases {
            let b = BasinNameAndMaybeStreamUri::from_str(s);
            if let Some((expected_basin, expected_stream)) = expected {
                let b = b.unwrap();
                assert_eq!(b.basin.as_ref(), expected_basin);
                assert_eq!(b.stream.as_deref(), expected_stream);
                assert_eq!(
                    expected_stream.is_some(),
                    BasinNameOnlyUri::from_str(s).is_err()
                );
            } else {
                assert!(b.is_err());
            }
        }
    }
}
