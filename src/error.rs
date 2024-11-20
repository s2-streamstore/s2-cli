use miette::Diagnostic;
use streamstore::{
    client::{ClientError, ParseError},
    types::ConvertError,
};
use thiserror::Error;

use crate::{
    account::AccountServiceError, basin::BasinServiceError, config::S2ConfigError,
    stream::StreamServiceError,
};

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
    AccountService(#[from] AccountServiceError),

    #[error(transparent)]
    #[diagnostic(help("{}", HELP))]
    BasinService(#[from] BasinServiceError),

    #[error(transparent)]
    #[diagnostic(help("{}", HELP))]
    StreamService(#[from] StreamServiceError),

    #[error(transparent)]
    #[diagnostic(help("{}", BUG_HELP))]
    InvalidConfig(#[from] serde_json::Error),

    #[error("Failed to initialize a `Record Reader`! {0}")]
    RecordReaderInit(String),

    #[error("Failed to write records: {0}")]
    RecordWrite(String),
}

pub fn s2_status(error: &ClientError) -> String {
    match error {
        ClientError::Service(status) => status.code().to_string(),
        _ => error.to_string(),
    }
}
