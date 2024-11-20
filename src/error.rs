use miette::Diagnostic;
use streamstore::{
    client::{ClientError, ParseError},
    types::ConvertError,
};
use thiserror::Error;

use crate::config::S2ConfigError;

const HELP: &str = color_print::cstr!(
    "\n<cyan><bold>Notice something wrong?</bold></cyan>\n\n\
     <green> > Open an issue:</green>\n\
     <bold>https://github.com/s2-cli/issues</bold>\n\n\
     <green> > Reach out to us:</green>\n\
     <bold>hi@s2.dev</bold>"
);

const BUG_HELP: &str = color_print::cstr!(
    "\n<cyan><bold>Looks like you may have encountered a bug!</bold></cyan>\n\n\
     <green> > Report this issue here: </green>\n\
     <bold>https://github.com/s2-cli/issues</bold>
"
);

#[derive(Error, Debug, Diagnostic)]
pub enum S2CliError {
    #[error(transparent)]
    #[diagnostic(transparent)]
    Config(#[from] S2ConfigError),

    #[error(transparent)]
    #[diagnostic(help("Are you trying to operate on an invalid basin?"))]
    ConvertError(#[from] ConvertError),

    #[error(transparent)]
    #[diagnostic(help("Are you overriding `S2_CLOUD`, `S2_CELL`, or `S2_BASIN_ZONE`?"))]
    HostEndpoints(#[from] ParseError),

    #[error(transparent)]
    #[diagnostic(help("{}", BUG_HELP))]
    InvalidConfig(#[from] serde_json::Error),

    #[error("Failed to initialize a `Record Reader`! {0}")]
    RecordReaderInit(String),

    #[error("Failed to write records: {0}")]
    RecordWrite(String),

    #[error(transparent)]
    #[diagnostic(help("{}", HELP))]
    Service(#[from] ServiceError),
}

/// Error for holding relevant info from `tonic::Status`
#[derive(thiserror::Error, Debug, Default)]
#[error("{status}: \n{message}")]
pub struct ServiceStatus {
    pub message: String,
    pub status: String,
}

impl From<ClientError> for ServiceStatus {
    fn from(error: ClientError) -> Self {
        match error {
            ClientError::Service(status) => Self {
                message: status.message().to_string(),
                status: status.code().to_string(),
            },
            _ => Self {
                message: error.to_string(),
                ..Default::default()
            },
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ErrorKind {
    #[error("Failed to list basins")]
    ListBasins,
    #[error("Failed to create basin")]
    CreateBasin,
    #[error("Failed to delete basin")]
    DeleteBasin,
    #[error("Failed to get basin config")]
    GetBasinConfig,
    #[error("Failed to reconfigure basin")]
    ReconfigureBasin,
    #[error("Failed to list streams")]
    ListStreams,
    #[error("Failed to create stream")]
    CreateStream,
    #[error("Failed to delete stream")]
    DeleteStream,
    #[error("Failed to get stream config")]
    GetStreamConfig,
    #[error("Failed to check tail")]
    CheckTail,
    #[error("Failed to append session")]
    AppendSession,
    #[error("Failed to read session")]
    ReadSession,
    #[error("Failed to write session")]
    ReconfigureStream,
}

#[derive(Debug, thiserror::Error)]
#[error("{kind}:\n {status}")]
pub struct ServiceError {
    kind: ErrorKind,
    status: ServiceStatus,
}

impl ServiceError {
    pub fn new(kind: ErrorKind, status: impl Into<ServiceStatus>) -> Self {
        Self {
            kind,
            status: status.into(),
        }
    }
}
