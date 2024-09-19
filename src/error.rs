use std::sync::OnceLock;

use colored::*;
use miette::Diagnostic;
use s2::client::ClientError;
use thiserror::Error;

use crate::{account::AccountServiceError, config::S2ConfigError};

static HELP: OnceLock<String> = OnceLock::new();

fn get_help() -> &'static str {
    HELP.get_or_init(|| {
        format!(
            "\n{}\n\n ► {}\n{}\n\n ► {}\n{}\n\n ► {}\n{}",
            "Notice something wrong?".cyan().bold(),
            "Open an issue:".green(),
            "https://github.com/s2-cli/issues".bold(),
            "Reach out to us:".green(),
            "hi@s2.dev".bold(),
            "Join our community:".green(),
            "Discord: https://discord.gg/s2".bold(),
        )
    })
}

#[derive(Error, Debug, Diagnostic)]
pub enum S2CliError {
    #[error(transparent)]
    #[diagnostic(transparent)]
    Config(#[from] S2ConfigError),

    #[error("Failed to connect to s2: {0}")]
    #[diagnostic(help("Are you connected to the internet?"))]
    Connection(#[from] ClientError),

    #[error(transparent)]
    #[diagnostic(help("{}", get_help()))]
    AccountService(#[from] AccountServiceError),
}
