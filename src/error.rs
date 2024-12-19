use miette::Diagnostic;
use s2::{client::ClientError, types::ConvertError};
use thiserror::Error;

use crate::config::S2ConfigError;

const HELP: &str = color_print::cstr!(
    "\n<cyan><bold>Notice something wrong?</bold></cyan>\n\n\
     <green> > Open an issue:</green>\n\
     <bold>https://github.com/s2-streamstore/s2-cli/issues</bold>\n\n\
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

    #[error("Invalid CLI arguments: {0}")]
    #[diagnostic(transparent)]
    InvalidArgs(miette::Report),

    #[error("Unable to load S2 endpoints from environment")]
    #[diagnostic(help(
        "Are you overriding `S2_CLOUD`, `S2_ACCOUNT_ENDPOINT` or `S2_BASIN_ENDPOINT`?
            Make sure the values are in the expected format."
    ))]
    EndpointsFromEnv(String),

    #[error(transparent)]
    #[diagnostic(help("{}", BUG_HELP))]
    InvalidConfig(#[from] serde_json::Error),

    #[error("Failed to initialize a `Record Reader`! {0}")]
    RecordReaderInit(String),

    #[error("Stream mutated concurrently during ping")]
    PingStreamMutated,

    #[error("Failed to write records: {0}")]
    RecordWrite(String),

    #[error(transparent)]
    #[diagnostic(help("{}", HELP))]
    Service(#[from] ServiceError),
}

#[derive(Debug, Clone, Copy)]
pub enum ServiceErrorContext {
    ListBasins,
    CreateBasin,
    DeleteBasin,
    GetBasinConfig,
    ReconfigureBasin,
    ListStreams,
    CreateStream,
    DeleteStream,
    GetStreamConfig,
    CheckTail,
    Trim,
    Fence,
    AppendSession,
    ReadSession,
    ReconfigureStream,
}

impl std::fmt::Display for ServiceErrorContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ListBasins => write!(f, "Failed to list basins"),
            Self::CreateBasin => write!(f, "Failed to create basin"),
            Self::DeleteBasin => write!(f, "Failed to delete basin"),
            Self::GetBasinConfig => write!(f, "Failed to get basin config"),
            Self::ReconfigureBasin => write!(f, "Failed to reconfigure basin"),
            Self::ListStreams => write!(f, "Failed to list streams"),
            Self::CreateStream => write!(f, "Failed to create stream"),
            Self::DeleteStream => write!(f, "Failed to delete stream"),
            Self::GetStreamConfig => write!(f, "Failed to get stream config"),
            Self::CheckTail => write!(f, "Failed to check tail"),
            Self::Trim => write!(f, "Failed to trim"),
            Self::Fence => write!(f, "Failed to set fencing token"),
            Self::AppendSession => write!(f, "Failed to append session"),
            Self::ReadSession => write!(f, "Failed to read session"),
            Self::ReconfigureStream => write!(f, "Failed to reconfigure stream"),
        }
    }
}

/// Error for holding relevant info from `tonic::Status`
#[derive(thiserror::Error, Debug, Default)]
#[error("{status}:\n {message}")]
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
            ClientError::Conversion(conv) => Self {
                message: conv.to_string(),
                status: "Failed to convert SDK type".to_string(),
            },
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{context}:\n {status}")]
pub struct ServiceError {
    context: ServiceErrorContext,
    status: ServiceStatus,
}

impl ServiceError {
    pub fn new(context: ServiceErrorContext, status: impl Into<ServiceStatus>) -> Self {
        Self {
            context,
            status: status.into(),
        }
    }
}

#[derive(Debug, Error, Diagnostic)]
pub enum BasinNameOrUriParseError {
    #[error(transparent)]
    #[diagnostic(help("Are you trying to operate on an invalid basin?"))]
    BasinName(#[from] ConvertError),

    #[error("Invalid S2 URI: {0}")]
    #[diagnostic(transparent)]
    InvalidUri(miette::Report),
}
