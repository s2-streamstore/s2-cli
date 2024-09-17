use thiserror::Error;

use crate::config::S2ConfigError;

#[derive(Error, Debug)]
pub enum S2CliError {
    #[error(transparent)]
    ConfigError(#[from] S2ConfigError),
}
