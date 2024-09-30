use clap::{Parser, ValueEnum};
use std::time::Duration;

#[derive(Parser, Debug, Clone)]
pub struct BasinConfig {
    #[clap(flatten)]
    pub default_stream_config: Option<StreamConfig>,
}

#[derive(Parser, Debug, Clone)]
pub struct StreamConfig {
    #[arg(short, long)]
    /// Storage class for a stream.
    pub storage_class: Option<StorageClass>,
    #[arg(short, long, help("Example: 1d, 1w, 1y"))]
    /// Retention policy for a stream.
    pub retention_policy: Option<RetentionPolicy>,
}

#[derive(ValueEnum, Debug, Clone)]
pub enum StorageClass {
    Unspecified,
    Standard,
    Express,
}

#[derive(Clone, Debug)]
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
        let default_stream_config = config.default_stream_config.map(|c| c.into());
        s2::types::BasinConfig::builder()
            .default_stream_config(default_stream_config)
            .build()
    }
}

impl From<StreamConfig> for s2::types::StreamConfig {
    fn from(config: StreamConfig) -> Self {
        let storage_class = config
            .storage_class
            .map(s2::types::StorageClass::from)
            .unwrap_or(s2::types::StorageClass::Unspecified);
        let retention_policy = config.retention_policy.map(|r| r.into());
        s2::types::StreamConfig::builder()
            .storage_class(storage_class)
            .retention_policy(retention_policy)
            .build()
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

impl From<RetentionPolicy> for s2::types::RetentionPolicy {
    fn from(policy: RetentionPolicy) -> Self {
        match policy {
            RetentionPolicy::Age(d) => s2::types::RetentionPolicy::Age(d),
        }
    }
}
