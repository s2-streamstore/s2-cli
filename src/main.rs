use std::{error::Error, time::Duration};

use account::AccountService;
use basin::BasinService;
use clap::{builder::styling, Parser, Subcommand};
use colored::*;
use config::{config_path, create_config};
use dialoguer::Confirm;
use error::S2CliError;
use json_dotpath::DotPaths;
use s2::{
    client::{Client, ClientConfig, HostCloud},
    types::{BasinConfig, BasinMetadata, RetentionPolicy, StorageClass, StreamConfig},
};
use serde_json::Value;
use tracing_subscriber::{fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt};

mod account;
mod basin;
mod config;
mod error;

const STYLES: styling::Styles = styling::Styles::styled()
    .header(styling::AnsiColor::Green.on_default().bold())
    .usage(styling::AnsiColor::Green.on_default().bold())
    .literal(styling::AnsiColor::Blue.on_default().bold())
    .placeholder(styling::AnsiColor::Cyan.on_default());

const USAGE: &str = color_print::cstr!(
    r#"          
    <dim>$</dim> <bold>s2-cli config set --token ...</bold>
    <dim>$</dim> <bold>s2-cli account list-basins --prefix "bar" --start-after "foo" --limit 100</bold>        
    "#
);

#[derive(Parser, Debug)]
#[command(version, about, override_usage = USAGE, styles = STYLES)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Manage s2-cli configuration
    Config {
        #[command(subcommand)]
        action: ConfigActions,
    },

    /// Manage s2 account
    Account {
        #[command(subcommand)]
        action: AccountActions,
    },

    /// Manage s2 basins
    Basins {
        #[command(subcommand)]
        action: BasinActions,
    },
}

#[derive(Subcommand, Debug)]
enum ConfigActions {
    /// Set the authentication token
    Set {
        #[arg(short, long)]
        auth_token: String,
    },
}

#[deny(missing_docs)]
#[derive(Subcommand, Debug)]
enum AccountActions {
    /// List basins
    ListBasins {
        /// List basin names that begin with this prefix.
        #[arg(short, long)]
        prefix: Option<String>,

        /// List basins names that lexicographically start after this name.        
        #[arg(short, long)]
        start_after: Option<String>,

        /// Number of results, upto a maximum of 1000.
        #[arg(short, long)]
        limit: Option<usize>,
    },

    /// Create a basin
    CreateBasin {
        /// Basin name, which must be globally unique.        
        basin: String,

        /// Storage class for recent writes.
        #[arg(short, long)]
        storage_class: Option<StorageClass>,

        /// Age threshold of oldest records in the stream, which can be automatically trimmed.
        #[arg(short, long)]
        retention_policy: Option<humantime::Duration>,
    },

    /// Delete a basin
    DeleteBasin {
        /// Basin name to delete.        
        basin: String,
    },

    /// Get basin config
    GetBasinConfig {
        /// Basin name to get config for.
        basin: String,
    },

    /// Reconfigure a basin
    ReconfigureBasin {
        /// Basin name to reconfigure.
        basin: String,

        /// Configuration to apply.
        #[arg(short, long, value_parser = parse_key_val::<String, String>, num_args = 1..)]
        config: Vec<(String, String)>,
    },
}

#[derive(Subcommand, Debug)]
enum BasinActions {
    /// List Streams
    ListStreams {
        /// Name of the basin to list streams from.
        basin: String,

        /// List stream names that begin with this prefix.
        #[arg(short, long)]
        prefix: String,

        /// List stream names that lexicographically start after this name.        
        #[arg(short, long)]
        start_after: String,

        /// Number of results, upto a maximum of 1000.
        #[arg(short, long)]
        limit: u32,
    },

    /// Create a stream
    CreateStream {},
}

async fn s2_client(auth_token: String) -> Result<Client, S2CliError> {
    let config = ClientConfig::builder()
        .host_uri(HostCloud::Local)
        .token(auth_token.to_string())
        .connection_timeout(std::time::Duration::from_secs(5))
        .build();

    Ok(Client::connect(config).await?)
}

fn parse_key_val<T, U>(s: &str) -> Result<(T, U), Box<dyn Error + Send + Sync + 'static>>
where
    T: std::str::FromStr,
    T::Err: Error + Send + Sync + 'static,
    U: std::str::FromStr,
    U::Err: Error + Send + Sync + 'static,
{
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{s}`"))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}

#[tokio::main]
async fn main() -> miette::Result<()> {
    run().await?;
    Ok(())
}

async fn run() -> Result<(), S2CliError> {
    let commands = Cli::parse();
    let config_path = config_path()?;

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .pretty()
                .with_span_events(FmtSpan::NEW)
                .compact()
                .with_writer(std::io::stderr),
        )
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    match commands.command {
        Commands::Config { action } => match action {
            ConfigActions::Set { auth_token } => {
                create_config(&config_path, auth_token)?;
                println!("{}", "✓ Token set successfully".green().bold());
                println!(
                    "  Configuration saved to: {}",
                    config_path.display().to_string().cyan()
                );
            }
        },

        Commands::Account { action } => {
            let cfg = config::load_config(&config_path)?;
            let account_service = AccountService::new(s2_client(cfg.auth_token).await?);
            match action {
                AccountActions::ListBasins {
                    prefix,
                    start_after,
                    limit,
                } => {
                    let response = account_service
                        .list_basins(
                            prefix.unwrap_or_default(),
                            start_after.unwrap_or_default(),
                            limit.unwrap_or_default(),
                        )
                        .await?;

                    for basin_metadata in response.basins {
                        let BasinMetadata { name, state, .. } = basin_metadata;

                        let state = match state {
                            s2::types::BasinState::Active => state.to_string().green(),
                            s2::types::BasinState::Deleting => state.to_string().red(),
                            _ => state.to_string().yellow(),
                        };
                        println!("{} {}", name, state);
                    }
                }

                AccountActions::CreateBasin {
                    basin,
                    storage_class,
                    retention_policy,
                } => {
                    account_service
                        .create_basin(basin, storage_class, retention_policy)
                        .await?;

                    println!("{}", "✓ Basin created successfully".green().bold());
                }
                AccountActions::DeleteBasin { basin } => {
                    account_service.delete_basin(basin).await?;
                    println!("{}", "✓ Basin deleted successfully".green().bold());
                }

                AccountActions::GetBasinConfig { basin } => {
                    let basin_config = account_service.get_basin_config(basin).await?;
                    println!("{:?}", basin_config);
                }
                AccountActions::ReconfigureBasin { basin, config } => {
                    // dummy basin config for full path matching
                    let basin_config = BasinConfig::builder()
                        .default_stream_config(Some(
                            StreamConfig::builder()
                                .storage_class(StorageClass::Unspecified)
                                .retention_policy(RetentionPolicy::Age(Duration::from_secs(60)))
                                .build(),
                        ))
                        .build();

                    let mut json_config: Value = serde_json::to_value(basin_config)
                        .expect("Failed to convert basin_config to Value");

                    for (key, value) in &config {
                        match value.as_str() {
                            "null" | "" => {
                                json_config.dot_remove(key)?;
                            }
                            _ => {
                                let parsed_value = match humantime::parse_duration(value) {
                                    Ok(duration) => serde_json::json!({
                                        "secs": duration.as_secs(),
                                        "nanos": duration.subsec_nanos()
                                    }),
                                    Err(_) => Value::String(value.clone()),
                                };

                                match json_config.dot_has_checked(key) {
                                    Ok(true) => {
                                        json_config.dot_set(key, parsed_value)?;
                                    }
                                    _ => {
                                        Err(S2CliError::PathKeyNotFound(key.clone()))?;
                                    }
                                }
                            }
                        }
                    }

                    let basin_config: BasinConfig = serde_json::from_value(json_config)?;

                    let confirmation = Confirm::new()
                        .with_prompt(color_print::cformat!(
                            "Are you sure you want to reconfigure basin <red>{}</red>?",
                            basin,
                        ))
                        .interact()?;

                    match confirmation {
                        true => {
                            account_service
                                .reconfigure_basin(
                                    basin,
                                    basin_config,
                                    config.iter().map(|(k, _)| k.clone()).collect(),
                                )
                                .await?;
                            println!("{}", "✓ Basin reconfigured successfully".green().bold());
                        }
                        false => {
                            println!("{}", "✗ Reconfigure cancelled".red().bold());
                        }
                    }
                }
            }
        }
        Commands::Basins { action } => {
            let cfg = config::load_config(&config_path)?;
            let client = s2_client(cfg.auth_token).await?;
            match action {
                BasinActions::ListStreams {
                    basin,
                    prefix,
                    start_after,
                    limit,
                } => {
                    let basin_client = client.basin_client(basin).await?;
                    let response = BasinService::new(basin_client)
                        .list_streams(prefix, start_after, limit as usize)
                        .await?;
                    for stream in response.streams {
                        println!("{}", stream);
                    }
                }
                BasinActions::CreateStream {} => todo!(),
            }
        }
    }

    Ok(())
}
