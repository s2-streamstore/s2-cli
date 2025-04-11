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

/// String Format: s2://{basin}/{stream}
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
    /// Create stream on append with basin defaults if it doesn't exist.
    #[arg(short = 'c', long)]
    pub create_stream_on_append: Option<bool>,
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
            create_stream_on_append,
        } = config;
        s2::types::BasinConfig {
            default_stream_config: default_stream_config.map(Into::into),
            create_stream_on_append: create_stream_on_append.unwrap_or_default(),
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
            create_stream_on_append: Some(config.create_stream_on_append),
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

#[derive(Debug, Clone)]
pub enum ResourceSet<const MIN: usize, const MAX: usize> {
    Exact(String),
    Prefix(String),
}

impl<const MIN: usize, const MAX: usize> From<ResourceSet<MIN, MAX>> for s2::types::ResourceSet {
    fn from(value: ResourceSet<MIN, MAX>) -> Self {
        match value {
            ResourceSet::Exact(s) => s2::types::ResourceSet::Exact(s),
            ResourceSet::Prefix(s) => s2::types::ResourceSet::Prefix(s),
        }
    }
}

impl<const MIN: usize, const MAX: usize> FromStr for ResourceSet<MIN, MAX> {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let len = s.len();

        if s.starts_with('=') {
            if len <= 1 {
                return Err("Exact resource must have a value after '='".to_string());
            }
            let value = &s[1..];
            let value_len = value.len();
            if value_len > MAX || value_len < MIN {
                return Err(format!(
                    "Exact value '{}' is too long: length {} must be between {} and {}",
                    value, value_len, MIN, MAX
                ));
            }
            Ok(ResourceSet::Exact(value.to_string()))
        } else {
            if len > MAX {
                return Err(format!(
                    "Prefix value '{}' is too long: length {} exceeds maximum {}",
                    s, len, MAX
                ));
            }
            Ok(ResourceSet::Prefix(s.to_string()))
        }
    }
}

#[derive(Debug, Clone)]
pub struct PermittedOperationGroups {
    pub account: Option<ReadWritePermissions>,
    pub basin: Option<ReadWritePermissions>,
    pub stream: Option<ReadWritePermissions>,
}

impl From<PermittedOperationGroups> for s2::types::PermittedOperationGroups {
    fn from(groups: PermittedOperationGroups) -> Self {
        s2::types::PermittedOperationGroups {
            account: groups.account.map(Into::into),
            basin: groups.basin.map(Into::into),
            stream: groups.stream.map(Into::into),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReadWritePermissions {
    pub read: bool,
    pub write: bool,
}

impl From<ReadWritePermissions> for s2::types::ReadWritePermissions {
    fn from(permissions: ReadWritePermissions) -> Self {
        s2::types::ReadWritePermissions {
            read: permissions.read,
            write: permissions.write,
        }
    }
}

pub fn parse_op_groups(s: &str) -> Result<PermittedOperationGroups, String> {
    let mut account = None;
    let mut basin = None;
    let mut stream = None;

    if s.is_empty() {
        return Ok(PermittedOperationGroups {
            account,
            basin,
            stream,
        });
    }

    for part in s.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (key, value) = part
            .split_once('=')
            .ok_or_else(|| format!("Invalid op_group format: '{}'. Expected 'key=value'", part))?;
        let perms = parse_permissions(value)?;
        match key {
            "account" => account = Some(perms),
            "basin" => basin = Some(perms),
            "stream" => stream = Some(perms),
            _ => {
                return Err(format!(
                    "Invalid op_group key: '{}'. Expected 'account', 'basin', or 'stream'",
                    key
                ));
            }
        }
    }

    Ok(PermittedOperationGroups {
        account,
        basin,
        stream,
    })
}

fn parse_permissions(s: &str) -> Result<ReadWritePermissions, String> {
    let mut read = false;
    let mut write = false;
    for c in s.chars() {
        match c {
            'r' => read = true,
            'w' => write = true,
            _ => return Err(format!("Invalid permission character: {}", c)),
        }
    }
    if !read && !write {
        return Err("At least one permission ('r' or 'w') must be specified".to_string());
    }
    Ok(ReadWritePermissions { read, write })
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
