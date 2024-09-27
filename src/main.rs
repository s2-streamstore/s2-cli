use account::AccountService;
use basin::BasinService;
use clap::{builder::styling, Parser, Subcommand};
use colored::*;
use config::{config_path, create_config};
use error::S2CliError;
use s2::{
    client::{Client, ClientConfig, HostCloud},
    types::{BasinMetadata, StorageClass},
};
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
}

async fn s2_client(auth_token: String) -> Result<Client, S2CliError> {
    let config = ClientConfig::builder()
        .host_uri(HostCloud::Local)
        .token(auth_token.to_string())
        .connection_timeout(std::time::Duration::from_secs(5))
        .build();

    Ok(Client::connect(config).await?)
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
            println!("cfg: {:?}", cfg);
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
            }
        }
    }

    Ok(())
}
