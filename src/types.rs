//! Types for Basin configuration that directly map to s2::types.

use clap::{Parser, ValueEnum};
use s2::types::BasinName;
use serde::Serialize;
use std::{str::FromStr, time::Duration};
use thiserror::Error;

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
    #[arg(long)]
    pub create_stream_on_append: Option<bool>,
    /// Create stream on read with basin defaults if it doesn't exist.
    #[arg(long)]
    pub create_stream_on_read: Option<bool>,
}

#[derive(Parser, Debug, Clone, Serialize)]
pub struct StreamConfig {
    #[arg(long)]
    /// Storage class for a stream.
    pub storage_class: Option<StorageClass>,
    #[arg(long, help("Example: 1d, 1w, 1y"))]
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
            create_stream_on_read,
        } = config;
        s2::types::BasinConfig {
            default_stream_config: default_stream_config.map(Into::into),
            create_stream_on_append: create_stream_on_append.unwrap_or_default(),
            create_stream_on_read: create_stream_on_read.unwrap_or_default(),
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
            create_stream_on_read: Some(config.create_stream_on_read),
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

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
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

impl<const MIN: usize, const MAX: usize> From<s2::types::ResourceSet> for ResourceSet<MIN, MAX> {
    fn from(value: s2::types::ResourceSet) -> Self {
        match value {
            s2::types::ResourceSet::Exact(s) => ResourceSet::Exact(s),
            s2::types::ResourceSet::Prefix(s) => ResourceSet::Prefix(s),
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ResourceSetParseError {
    #[error("Exact value '{value}' length {length} must be between {min} and {max}")]
    ExactValueLengthInvalid {
        value: String,
        length: usize,
        min: usize,
        max: usize,
    },

    #[error("Prefix '{value}' length {length} exceeds maximum {max}")]
    PrefixTooLong {
        value: String,
        length: usize,
        max: usize,
    },
}

impl<const MIN: usize, const MAX: usize> FromStr for ResourceSet<MIN, MAX> {
    type Err = ResourceSetParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Ok(ResourceSet::Prefix(String::new()));
        }

        if let Some(value) = s.strip_prefix('=') {
            if value.is_empty() {
                return Ok(ResourceSet::Exact(String::new()));
            }
            let len = value.len();
            if len > MAX || len < MIN {
                return Err(ResourceSetParseError::ExactValueLengthInvalid {
                    value: value.to_owned(),
                    length: len,
                    min: MIN,
                    max: MAX,
                });
            }
            Ok(ResourceSet::Exact(value.to_owned()))
        } else {
            let len = s.len();
            if len > MAX {
                return Err(ResourceSetParseError::PrefixTooLong {
                    value: s.to_owned(),
                    length: len,
                    max: MAX,
                });
            }
            Ok(ResourceSet::Prefix(s.to_owned()))
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
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

impl From<s2::types::PermittedOperationGroups> for PermittedOperationGroups {
    fn from(groups: s2::types::PermittedOperationGroups) -> Self {
        PermittedOperationGroups {
            account: groups.account.map(Into::into),
            basin: groups.basin.map(Into::into),
            stream: groups.stream.map(Into::into),
        }
    }
}

impl FromStr for PermittedOperationGroups {
    type Err = OpGroupsParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
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
            let (key, value) =
                part.split_once('=')
                    .ok_or_else(|| OpGroupsParseError::InvalidFormat {
                        value: part.to_owned(),
                    })?;
            let perms = value.parse::<ReadWritePermissions>()?;
            match key {
                "account" => account = Some(perms),
                "basin" => basin = Some(perms),
                "stream" => stream = Some(perms),
                _ => {
                    return Err(OpGroupsParseError::InvalidKey {
                        key: key.to_owned(),
                    });
                }
            }
        }

        Ok(PermittedOperationGroups {
            account,
            basin,
            stream,
        })
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ReadWritePermissions {
    pub read: bool,
    pub write: bool,
}

impl FromStr for ReadWritePermissions {
    type Err = OpGroupsParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut read = false;
        let mut write = false;
        for c in s.chars() {
            match c {
                'r' => read = true,
                'w' => write = true,
                _ => return Err(OpGroupsParseError::InvalidPermissionChar(c)),
            }
        }
        if !read && !write {
            return Err(OpGroupsParseError::MissingPermission);
        }
        Ok(ReadWritePermissions { read, write })
    }
}

impl From<ReadWritePermissions> for s2::types::ReadWritePermissions {
    fn from(permissions: ReadWritePermissions) -> Self {
        s2::types::ReadWritePermissions {
            read: permissions.read,
            write: permissions.write,
        }
    }
}

impl From<s2::types::ReadWritePermissions> for ReadWritePermissions {
    fn from(permissions: s2::types::ReadWritePermissions) -> Self {
        ReadWritePermissions {
            read: permissions.read,
            write: permissions.write,
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum OpGroupsParseError {
    #[error("Invalid op_group format: '{value}'. Expected 'key=value'")]
    InvalidFormat { value: String },

    #[error("Invalid op_group key: '{key}'. Expected 'account', 'basin', or 'stream'")]
    InvalidKey { key: String },

    #[error("At least one permission ('r' or 'w') must be specified")]
    MissingPermission,

    #[error("Invalid permission character: {0}")]
    InvalidPermissionChar(char),
}

#[derive(Debug, Serialize)]
pub struct AccessTokenInfo {
    pub id: String,
    pub expires_at: Option<u32>,
    pub auto_prefix_streams: bool,
    pub scope: Option<AccessTokenScope>,
}

impl From<s2::types::AccessTokenInfo> for AccessTokenInfo {
    fn from(info: s2::types::AccessTokenInfo) -> Self {
        AccessTokenInfo {
            id: info.id.to_string(),
            expires_at: info.expires_at,
            auto_prefix_streams: info.auto_prefix_streams,
            scope: info.scope.map(Into::into),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct AccessTokenScope {
    pub basins: Option<ResourceSet<8, 48>>,
    pub streams: Option<ResourceSet<1, 512>>,
    pub access_tokens: Option<ResourceSet<1, 96>>,
    pub op_groups: Option<PermittedOperationGroups>,
    pub ops: Vec<Operation>,
}

impl From<s2::types::AccessTokenScope> for AccessTokenScope {
    fn from(scope: s2::types::AccessTokenScope) -> Self {
        AccessTokenScope {
            basins: scope.basins.map(Into::into),
            streams: scope.streams.map(Into::into),
            access_tokens: scope.access_tokens.map(Into::into),
            op_groups: scope.op_groups.map(Into::into),
            ops: scope.ops.into_iter().map(Operation::from).collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum Operation {
    Unspecified,
    ListBasins,
    CreateBasin,
    DeleteBasin,
    ReconfigureBasin,
    GetBasinConfig,
    IssueAccessToken,
    RevokeAccessToken,
    ListAccessTokens,
    ListStreams,
    CreateStream,
    DeleteStream,
    GetStreamConfig,
    ReconfigureStream,
    CheckTail,
    Append,
    Read,
    Tail,
    Trim,
    Fence,
}

impl From<Operation> for s2::types::Operation {
    fn from(op: Operation) -> Self {
        match op {
            Operation::Unspecified => s2::types::Operation::Unspecified,
            Operation::ListBasins => s2::types::Operation::ListBasins,
            Operation::CreateBasin => s2::types::Operation::CreateBasin,
            Operation::DeleteBasin => s2::types::Operation::DeleteBasin,
            Operation::ReconfigureBasin => s2::types::Operation::ReconfigureBasin,
            Operation::GetBasinConfig => s2::types::Operation::GetBasinConfig,
            Operation::IssueAccessToken => s2::types::Operation::IssueAccessToken,
            Operation::RevokeAccessToken => s2::types::Operation::RevokeAccessToken,
            Operation::ListAccessTokens => s2::types::Operation::ListAccessTokens,
            Operation::ListStreams => s2::types::Operation::ListStreams,
            Operation::CreateStream => s2::types::Operation::CreateStream,
            Operation::DeleteStream => s2::types::Operation::DeleteStream,
            Operation::GetStreamConfig => s2::types::Operation::GetStreamConfig,
            Operation::ReconfigureStream => s2::types::Operation::ReconfigureStream,
            Operation::CheckTail => s2::types::Operation::CheckTail,
            Operation::Append => s2::types::Operation::Append,
            Operation::Read => s2::types::Operation::Read,
            Operation::Tail => s2::types::Operation::Read,
            Operation::Trim => s2::types::Operation::Trim,
            Operation::Fence => s2::types::Operation::Fence,
        }
    }
}

impl From<s2::types::Operation> for Operation {
    fn from(op: s2::types::Operation) -> Self {
        match op {
            s2::types::Operation::Unspecified => Operation::Unspecified,
            s2::types::Operation::ListBasins => Operation::ListBasins,
            s2::types::Operation::CreateBasin => Operation::CreateBasin,
            s2::types::Operation::DeleteBasin => Operation::DeleteBasin,
            s2::types::Operation::ReconfigureBasin => Operation::ReconfigureBasin,
            s2::types::Operation::GetBasinConfig => Operation::GetBasinConfig,
            s2::types::Operation::IssueAccessToken => Operation::IssueAccessToken,
            s2::types::Operation::RevokeAccessToken => Operation::RevokeAccessToken,
            s2::types::Operation::ListAccessTokens => Operation::ListAccessTokens,
            s2::types::Operation::ListStreams => Operation::ListStreams,
            s2::types::Operation::CreateStream => Operation::CreateStream,
            s2::types::Operation::DeleteStream => Operation::DeleteStream,
            s2::types::Operation::GetStreamConfig => Operation::GetStreamConfig,
            s2::types::Operation::ReconfigureStream => Operation::ReconfigureStream,
            s2::types::Operation::CheckTail => Operation::CheckTail,
            s2::types::Operation::Append => Operation::Append,
            s2::types::Operation::Read => Operation::Read,
            s2::types::Operation::Trim => Operation::Trim,
            s2::types::Operation::Fence => Operation::Fence,
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum OperationParseError {
    #[error("Invalid operation: '{0}'")]
    InvalidOperation(String),
}

impl FromStr for Operation {
    type Err = OperationParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "list-basins" => Ok(Self::ListBasins),
            "create-basin" => Ok(Self::CreateBasin),
            "delete-basin" => Ok(Self::DeleteBasin),
            "reconfigure-basin" => Ok(Self::ReconfigureBasin),
            "get-basin-config" => Ok(Self::GetBasinConfig),
            "issue-access-token" => Ok(Self::IssueAccessToken),
            "revoke-access-token" => Ok(Self::RevokeAccessToken),
            "list-access-tokens" => Ok(Self::ListAccessTokens),
            "list-streams" => Ok(Self::ListStreams),
            "create-stream" => Ok(Self::CreateStream),
            "delete-stream" => Ok(Self::DeleteStream),
            "get-stream-config" => Ok(Self::GetStreamConfig),
            "reconfigure-stream" => Ok(Self::ReconfigureStream),
            "check-tail" => Ok(Self::CheckTail),
            "append" => Ok(Self::Append),
            "read" => Ok(Self::Read),
            "tail" => Ok(Self::Tail),
            "trim" => Ok(Self::Trim),
            "fence" => Ok(Self::Fence),
            _ => Err(OperationParseError::InvalidOperation(s.to_owned())),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{error::S2UriParseError, types::S2BasinAndStreamUri};
    use rstest::rstest;

    use super::{
        OpGroupsParseError, PermittedOperationGroups, ReadWritePermissions, ResourceSet,
        ResourceSetParseError, S2BasinAndMaybeStreamUri, S2BasinUri, S2Uri,
    };

    #[rstest]
    #[case("", Ok(PermittedOperationGroups {
        account: None,
        basin: None,
        stream: None,
    }))]
    #[case("account=r", Ok(PermittedOperationGroups {
        account: Some(ReadWritePermissions {
            read: true,
            write: false,
        }),
        basin: None,
        stream: None,
    }))]
    #[case("account=w", Ok(PermittedOperationGroups {
        account: Some(ReadWritePermissions {
            read: false,
            write: true,
        }),
        basin: None,
        stream: None,
    }))]
    #[case("account=rw", Ok(PermittedOperationGroups {
        account: Some(ReadWritePermissions {
            read: true,
            write: true,
        }),
        basin: None,
        stream: None,
    }))]
    #[case("basin=r,stream=w", Ok(PermittedOperationGroups {
        account: None,
        basin: Some(ReadWritePermissions {
            read: true,
            write: false,
        }),
        stream: Some(ReadWritePermissions {
            read: false,
            write: true,
        }),
    }))]
    #[case("account=rw,basin=rw,stream=rw", Ok(PermittedOperationGroups {
        account: Some(ReadWritePermissions {
            read: true,
            write: true,
        }),
        basin: Some(ReadWritePermissions {
            read: true,
            write: true,
        }),
        stream: Some(ReadWritePermissions {
            read: true,
            write: true,
        }),
    }))]
    #[case("invalid", Err(OpGroupsParseError::InvalidFormat { value: "invalid".to_owned() }))]
    #[case("unknown=rw", Err(OpGroupsParseError::InvalidKey { key: "unknown".to_owned() }))]
    #[case("account=", Err(OpGroupsParseError::MissingPermission))]
    #[case("account=x", Err(OpGroupsParseError::InvalidPermissionChar('x')))]
    fn test_parse_op_groups(
        #[case] input: &str,
        #[case] expected: Result<PermittedOperationGroups, OpGroupsParseError>,
    ) {
        assert_eq!(
            input.parse::<PermittedOperationGroups>(),
            expected,
            "Testing input: {}",
            input
        );
    }

    #[rstest]
    // Valid empty string case
    #[case("", Ok(ResourceSet::<8, 48>::Prefix(String::new())))]
    // Valid exact values
    #[case("=exact-value", Ok(ResourceSet::<8, 48>::Exact("exact-value".to_string())))]
    #[case("=mybasintestingvalue", Ok(ResourceSet::<8, 48>::Exact("mybasintestingvalue".to_string())))]
    // Valid prefix values
    #[case("prefix", Ok(ResourceSet::<8, 48>::Prefix("prefix".to_string())))]
    #[case("my-prefix", Ok(ResourceSet::<8, 48>::Prefix("my-prefix".to_string())))]
    // Error cases for exact values - too short or too long
    #[case("=short", Err(ResourceSetParseError::ExactValueLengthInvalid {
        value: "short".to_owned(), length: 5, min: 8, max: 48
    }))]
    #[case("=waytoolongvaluethatshouldexceedthemaximumlengthallowed",
           Err(ResourceSetParseError::ExactValueLengthInvalid {
               value: "waytoolongvaluethatshouldexceedthemaximumlengthallowed".to_owned(),
               length: 54, min: 8, max: 48
           }))]
    // Error case for prefix - too long
    #[case("waytoolongvaluethatshouldexceedthemaximumlengthallowed",
           Err(ResourceSetParseError::PrefixTooLong {
               value: "waytoolongvaluethatshouldexceedthemaximumlengthallowed".to_owned(),
               length: 54, max: 48
           }))]
    fn test_resource_set_parsing(
        #[case] input: &str,
        #[case] expected: Result<ResourceSet<8, 48>, ResourceSetParseError>,
    ) {
        assert_eq!(
            input.parse::<ResourceSet<8, 48>>(),
            expected,
            "Testing input: {}",
            input
        );
    }

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
