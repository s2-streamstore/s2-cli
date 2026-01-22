use s2_sdk::types::{BasinInfo, SequencedRecord, StreamInfo, StreamPosition};

use crate::error::CliError;
use crate::types::StreamConfig;

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

    /// An error occurred in a background task
    Error(CliError),
}
