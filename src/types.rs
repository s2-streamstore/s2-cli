//! Types for Basin configuration that directly map to s2::types.

use clap::{Parser, ValueEnum};
use s2::types::BasinName;
use serde::Serialize;
use std::{str::FromStr, time::Duration};

use crate::error::S2UriParseError;

#[derive(Debug, Clone)]
struct S2Uri {
    basin: BasinName,
    stream: Option<String>,
}

#[cfg(test)]
impl PartialEq for S2Uri {
    fn eq(&self, other: &Self) -> bool {
        self.basin.as_ref().eq(other.basin.as_ref()) && self.stream.eq(&other.stream)
    }
}

impl FromStr for S2Uri {
    type Err = S2UriParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (scheme, s) = s
            .split_once("://")
            .ok_or(S2UriParseError::MissingUriScheme)?;
        if scheme != "s2" {
            return Err(S2UriParseError::InvalidUriScheme(scheme.to_owned()));
        }

        let (basin, stream) = if let Some((basin, stream)) = s.split_once("/") {
            let stream = if stream.is_empty() {
                None
            } else {
                Some(stream.to_owned())
            };
            (basin, stream)
        } else {
            (s, None)
        };

        Ok(S2Uri {
            basin: basin.parse().map_err(S2UriParseError::InvalidBasinName)?,
            stream,
        })
    }
}

#[derive(Debug, Clone)]
pub struct S2BasinUri(pub BasinName);

impl From<S2BasinUri> for BasinName {
    fn from(value: S2BasinUri) -> Self {
        value.0
    }
}

#[cfg(test)]
impl PartialEq for S2BasinUri {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_ref().eq(other.0.as_ref())
    }
}

impl FromStr for S2BasinUri {
    type Err = S2UriParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match S2Uri::from_str(s) {
            Ok(S2Uri {
                basin,
                stream: None,
            }) => Ok(Self(
                basin.parse().map_err(S2UriParseError::InvalidBasinName)?,
            )),
            Ok(S2Uri {
                basin: _,
                stream: Some(_),
            }) => Err(S2UriParseError::UnexpectedStreamName),
            Err(S2UriParseError::MissingUriScheme) => {
                Ok(Self(s.parse().map_err(S2UriParseError::InvalidBasinName)?))
            }
            Err(other) => Err(other),
        }
    }
}

#[derive(Debug, Clone)]
pub struct S2BasinAndMaybeStreamUri {
    pub basin: BasinName,
    pub stream: Option<String>,
}

#[cfg(test)]
impl PartialEq for S2BasinAndMaybeStreamUri {
    fn eq(&self, other: &Self) -> bool {
        self.basin.as_ref().eq(other.basin.as_ref()) && self.stream.eq(&other.stream)
    }
}

impl FromStr for S2BasinAndMaybeStreamUri {
    type Err = S2UriParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match S2Uri::from_str(s) {
            Ok(S2Uri { basin, stream }) => Ok(Self { basin, stream }),
            Err(S2UriParseError::MissingUriScheme) => Ok(Self {
                basin: s.parse().map_err(S2UriParseError::InvalidBasinName)?,
                stream: None,
            }),
            Err(other) => Err(other),
        }
    }
}

#[derive(Debug, Clone)]
pub struct S2BasinAndStreamUri {
    pub basin: BasinName,
    pub stream: String,
}

#[cfg(test)]
impl PartialEq for S2BasinAndStreamUri {
    fn eq(&self, other: &Self) -> bool {
        self.basin.as_ref().eq(other.basin.as_ref()) && self.stream == other.stream
    }
}

impl FromStr for S2BasinAndStreamUri {
    type Err = S2UriParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let S2Uri { basin, stream } = s.parse()?;
        let stream = stream.ok_or(S2UriParseError::MissingStreamName)?;
        Ok(Self { basin, stream })
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

impl From<BasinConfig> for s2::types::BasinConfig {
    fn from(config: BasinConfig) -> Self {
        let BasinConfig {
            default_stream_config,
        } = config;
        s2::types::BasinConfig {
            default_stream_config: default_stream_config.map(Into::into),
        }
    }
}

impl From<StreamConfig> for s2::types::StreamConfig {
    fn from(config: StreamConfig) -> Self {
        let storage_class = config
            .storage_class
            .map(s2::types::StorageClass::from)
            .unwrap_or(s2::types::StorageClass::Unspecified);
        let retention_policy = config.retention_policy.map(|r| r.into());
        let stream_config = s2::types::StreamConfig::new().with_storage_class(storage_class);

        if let Some(retention_policy) = retention_policy {
            stream_config.with_retention_policy(retention_policy)
        } else {
            stream_config
        }
    }
}

impl From<StorageClass> for s2::types::StorageClass {
    fn from(class: StorageClass) -> Self {
        match class {
            StorageClass::Unspecified => s2::types::StorageClass::Unspecified,
            StorageClass::Standard => s2::types::StorageClass::Standard,
            StorageClass::Express => s2::types::StorageClass::Express,
        }
    }
}

impl From<s2::types::StorageClass> for StorageClass {
    fn from(class: s2::types::StorageClass) -> Self {
        match class {
            s2::types::StorageClass::Unspecified => StorageClass::Unspecified,
            s2::types::StorageClass::Standard => StorageClass::Standard,
            s2::types::StorageClass::Express => StorageClass::Express,
        }
    }
}

impl From<RetentionPolicy> for s2::types::RetentionPolicy {
    fn from(policy: RetentionPolicy) -> Self {
        match policy {
            RetentionPolicy::Age(d) => s2::types::RetentionPolicy::Age(d),
        }
    }
}

impl From<s2::types::RetentionPolicy> for RetentionPolicy {
    fn from(policy: s2::types::RetentionPolicy) -> Self {
        match policy {
            s2::types::RetentionPolicy::Age(d) => RetentionPolicy::Age(d),
        }
    }
}

impl From<s2::types::BasinConfig> for BasinConfig {
    fn from(config: s2::types::BasinConfig) -> Self {
        BasinConfig {
            default_stream_config: config.default_stream_config.map(Into::into),
        }
    }
}

impl From<s2::types::StreamConfig> for StreamConfig {
    fn from(config: s2::types::StreamConfig) -> Self {
        StreamConfig {
            storage_class: Some(config.storage_class.into()),
            retention_policy: config.retention_policy.map(|r| r.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{error::S2UriParseError, types::S2BasinAndStreamUri};

    use super::{S2BasinAndMaybeStreamUri, S2BasinUri, S2Uri};

    #[test]
    fn test_s2_uri_parse() {
        let test_cases = vec![
            (
                "valid-basin",
                Err(S2UriParseError::MissingUriScheme),
                Ok(S2BasinUri("valid-basin".parse().unwrap())),
                Err(S2UriParseError::MissingUriScheme),
                Ok(S2BasinAndMaybeStreamUri {
                    basin: "valid-basin".parse().unwrap(),
                    stream: None,
                }),
            ),
            (
                "s2://valid-basin",
                Ok(S2Uri {
                    basin: "valid-basin".parse().unwrap(),
                    stream: None,
                }),
                Ok(S2BasinUri("valid-basin".parse().unwrap())),
                Err(S2UriParseError::MissingStreamName),
                Ok(S2BasinAndMaybeStreamUri {
                    basin: "valid-basin".parse().unwrap(),
                    stream: None,
                }),
            ),
            (
                "s2://valid-basin/",
                Ok(S2Uri {
                    basin: "valid-basin".parse().unwrap(),
                    stream: None,
                }),
                Ok(S2BasinUri("valid-basin".parse().unwrap())),
                Err(S2UriParseError::MissingStreamName),
                Ok(S2BasinAndMaybeStreamUri {
                    basin: "valid-basin".parse().unwrap(),
                    stream: None,
                }),
            ),
            (
                "s2://valid-basin/stream/name",
                Ok(S2Uri {
                    basin: "valid-basin".parse().unwrap(),
                    stream: Some("stream/name".to_owned()),
                }),
                Err(S2UriParseError::UnexpectedStreamName),
                Ok(S2BasinAndStreamUri {
                    basin: "valid-basin".parse().unwrap(),
                    stream: "stream/name".to_owned(),
                }),
                Ok(S2BasinAndMaybeStreamUri {
                    basin: "valid-basin".parse().unwrap(),
                    stream: Some("stream/name".to_owned()),
                }),
            ),
            (
                "-invalid-basin",
                Err(S2UriParseError::MissingUriScheme),
                Err(S2UriParseError::InvalidBasinName("".into())),
                Err(S2UriParseError::MissingUriScheme),
                Err(S2UriParseError::InvalidBasinName("".into())),
            ),
            (
                "http://valid-basin",
                Err(S2UriParseError::InvalidUriScheme("http".to_owned())),
                Err(S2UriParseError::InvalidUriScheme("http".to_owned())),
                Err(S2UriParseError::InvalidUriScheme("http".to_owned())),
                Err(S2UriParseError::InvalidUriScheme("http".to_owned())),
            ),
            (
                "s2://-invalid-basin",
                Err(S2UriParseError::InvalidBasinName("".into())),
                Err(S2UriParseError::InvalidBasinName("".into())),
                Err(S2UriParseError::InvalidBasinName("".into())),
                Err(S2UriParseError::InvalidBasinName("".into())),
            ),
            (
                "s2:///stream/name",
                Err(S2UriParseError::InvalidBasinName("".into())),
                Err(S2UriParseError::InvalidBasinName("".into())),
                Err(S2UriParseError::InvalidBasinName("".into())),
                Err(S2UriParseError::InvalidBasinName("".into())),
            ),
            (
                "random:::string",
                Err(S2UriParseError::MissingUriScheme),
                Err(S2UriParseError::InvalidBasinName("".into())),
                Err(S2UriParseError::MissingUriScheme),
                Err(S2UriParseError::InvalidBasinName("".into())),
            ),
        ];

        for (
            s,
            expected_uri,
            expected_basin_uri,
            expected_basin_and_stream_uri,
            expected_basin_and_maybe_stream_uri,
        ) in test_cases
        {
            assert_eq!(s.parse(), expected_uri, "S2Uri: {s}");
            assert_eq!(s.parse(), expected_basin_uri, "S2BasinUri: {s}");
            assert_eq!(
                s.parse(),
                expected_basin_and_stream_uri,
                "S2BasinAndStreamUri: {s}"
            );
            assert_eq!(
                s.parse(),
                expected_basin_and_maybe_stream_uri,
                "S2BasinAndMaybeStreamUri: {s}"
            );
        }
    }
}
