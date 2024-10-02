use miette::Diagnostic;
use s2::client::ClientError;
use thiserror::Error;

use crate::{account::AccountServiceError, basin::BasinServiceError, config::S2ConfigError};

const HELP: &str = color_print::cstr!(
    "\n<cyan><bold>Notice something wrong?</bold></cyan>\n\n\
     <green> ► Open an issue:</green>\n\
     <bold>https://github.com/s2-cli/issues</bold>\n\n\
     <green> ► Reach out to us:</green>\n\
     <bold>hi@s2.dev</bold>\n\n\
     <green> ► Join our community:</green>\n\
     <bold>Discord: https://discord.gg/s2</bold>"
);

#[derive(Error, Debug, Diagnostic)]
pub enum S2CliError {
    #[error(transparent)]
    #[diagnostic(transparent)]
    Config(#[from] S2ConfigError),

    #[error("Failed to connect to s2: {0}")]
    #[diagnostic(help("Are you connected to the internet?"))]
    Connection(#[from] ClientError),

    #[error(transparent)]
    #[diagnostic(help("{}", HELP))]
    AccountService(#[from] AccountServiceError),

    #[error(transparent)]
    #[diagnostic(help("{}", HELP))]
    BasinService(#[from] BasinServiceError),

    #[error(transparent)]
    InvalidConfig(#[from] serde_json::Error),
}
