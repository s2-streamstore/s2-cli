//! Types for Basin configuration that directly map to streamstore::types.

use clap::{Parser, ValueEnum};
use serde::Serialize;
use std::time::Duration;

pub const STORAGE_CLASS_PATH: &str = "default_stream_config.storage_class";
pub const RETENTION_POLICY_PATH: &str = "default_stream_config.retention_policy";

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
