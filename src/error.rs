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
    #[diagnostic(help("{}", HELP))]
    ServiceError(Box<ServiceError>),

    #[error(transparent)]
    #[diagnostic(help("{}", BUG_HELP))]
    InvalidConfig(#[from] serde_json::Error),

    #[error("Failed to initialize a `Record Reader`! {0}")]
    RecordReaderInit(String),

    #[error("Failed to write records: {0}")]
    RecordWrite(String),
}

// Error for holding relevant info from `tonic::Status`
#[derive(Error, Debug, Default)]
#[error("{status}: \n{message}")]
pub struct Status {
    pub message: String,
    pub status: String,
}

impl From<ClientError> for Status {
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
#[error("Failed to {operation} {entity}{plural} {context} \n{status}", plural = plural.map_or("", |p| p))]
pub struct ServiceError {
    entity: String,
    operation: String,
    status: Status,
    context: String,
    plural: Option<&'static str>,
}

impl From<ServiceError> for S2CliError {
    fn from(error: ServiceError) -> Self {
        S2CliError::ServiceError(Box::new(error))
    }
}

impl ServiceError {
    pub fn new(
        entity: impl Into<String>,
        operation: impl Into<String>,
        status: impl Into<Status>,
    ) -> Self {
        Self {
            entity: entity.into(),
            operation: operation.into(),
            status: status.into(),
            context: String::new(),
            plural: None,
        }
    }

    pub fn with_context(self, context: impl Into<String>) -> Self {
        Self {
            context: context.into(),
            ..self
        }
    }

    pub fn with_plural(self) -> Self {
        let plural = if self.operation.ends_with('s') {
            "es"
        } else {
            "s"
        };
        Self {
            plural: Some(plural),
            ..self
        }
    }
}
