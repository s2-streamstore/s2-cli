use s2_sdk::types::{BasinInfo, SequencedRecord, StreamInfo, StreamPosition};

use crate::error::CliError;
use crate::types::{StorageClass, StreamConfig, TimestampingMode};

/// Basin config info for reconfiguration
#[derive(Debug, Clone)]
pub struct BasinConfigInfo {
    pub create_stream_on_append: bool,
    pub create_stream_on_read: bool,
    // Default stream config
    pub storage_class: Option<StorageClass>,
    pub retention_age_secs: Option<u64>,  // None = infinite
    pub timestamping_mode: Option<TimestampingMode>,
    pub timestamping_uncapped: bool,
}

/// Stream config info for reconfiguration
#[derive(Debug, Clone)]
pub struct StreamConfigInfo {
    pub storage_class: Option<StorageClass>,
    pub retention_age_secs: Option<u64>,  // None = infinite
    pub timestamping_mode: Option<TimestampingMode>,
    pub timestamping_uncapped: bool,
}

/// Events that can occur in the TUI
#[derive(Debug)]
pub enum Event {
    /// Basins have been loaded from the API
    BasinsLoaded(Result<Vec<BasinInfo>, CliError>),

    /// Streams have been loaded from the API
    StreamsLoaded(Result<Vec<StreamInfo>, CliError>),

    /// Stream configuration loaded
    StreamConfigLoaded(Result<StreamConfig, CliError>),

    /// Tail position loaded
    TailPositionLoaded(Result<StreamPosition, CliError>),

    /// A record was received during read/tail
    RecordReceived(Result<SequencedRecord, CliError>),

    /// Read stream ended
    ReadEnded,

    /// Basin created successfully
    BasinCreated(Result<BasinInfo, CliError>),

    /// Basin deleted successfully
    BasinDeleted(Result<String, CliError>),

    /// Stream created successfully
    StreamCreated(Result<StreamInfo, CliError>),

    /// Stream deleted successfully
    StreamDeleted(Result<String, CliError>),

    /// Basin config loaded for reconfiguration
    BasinConfigLoaded(Result<BasinConfigInfo, CliError>),

    /// Stream config loaded for reconfiguration
    StreamConfigForReconfigLoaded(Result<StreamConfigInfo, CliError>),

    /// Basin reconfigured successfully
    BasinReconfigured(Result<(), CliError>),

    /// Stream reconfigured successfully
    StreamReconfigured(Result<(), CliError>),

    /// An error occurred in a background task
    Error(CliError),
}
