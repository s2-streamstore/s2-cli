use colored::*;
use miette::Diagnostic;
use s2::client::ClientError;
use thiserror::Error;

use crate::{account::AccountServiceError, config::S2ConfigError};

#[derive(Error, Debug, Diagnostic)]
#[diagnostic(help(r#"
{}

► {}
   {}

► {}
   {}

► {}
   {}
"#,
"Notice something wrong?".cyan().bold(),
"Open an issue:".green(),
"https://github.com/foo/issues".bold(),
"Reach out to us:".green(),
"hi@s2.dev".bold(),
"Join our community:".green(),
"Discord: https://discord.gg/s2".bold(),
))]
pub enum S2CliError {
    #[error(transparent)]
    Config(#[from] S2ConfigError),

    #[error("Failed to connect to s2: {0}")]
    Connection(#[from] ClientError),

    #[error(transparent)]
    AccountService(#[from] AccountServiceError),
}
