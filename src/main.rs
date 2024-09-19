use account::AccountService;
use clap::{Parser, Subcommand};
use colored::*;
use config::{config_path, create_config};
use error::S2CliError;
use s2::{
    client::{Client, ClientConfig, HostCloud},
    types::StorageClass,
};

mod account;
mod config;
mod error;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Manage s2 configuration
    Config {
        #[command(subcommand)]
        action: ConfigActions,
    },

    /// Manage s2 account
    Account {
        #[command(subcommand)]
        action: AccountActions,
    },
}

#[derive(Subcommand, Debug)]
enum ConfigActions {
    /// Set the authentication token
    Set {
        #[arg(short, long)]
        token: String,
    },
}

#[derive(Subcommand, Debug)]
enum AccountActions {
    /// List basins
    ListBasins {
        /// List basin names that begin with this prefix.
        #[arg(short, long)]
        prefix: String,

        /// List basins names that lexicographically start after this name.        
        #[arg(short, long)]
        start_after: String,

        /// Number of results, upto a maximum of 1000.
        #[arg(short, long)]
        limit: u32,
    },

    /// Create a basin
    CreateBasin {
        /// Basin name, which must be globally unique.        
        basin: String,

        /// Storage class for recent writes.
        #[arg(short, long, requires_all = ["retention_policy"])]
        storage_class: Option<StorageClass>,

        /// Age threshold of oldest records in the stream, which can be automatically trimmed.
        #[arg(short, long, requires_all = ["storage_class"])]
        retention_policy: Option<humantime::Duration>,
    },

    /// Delete a basin
    DeleteBasin {
        /// Basin name to delete.        
        basin: String,
    },
}

async fn s2_client(token: String) -> Result<Client, S2CliError> {
    let config = ClientConfig::builder()
        .host_uri(HostCloud::Local)
        .token(token.to_string())
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

    match commands.command {
        Commands::Config { action } => match action {
            ConfigActions::Set { token } => {
                create_config(&config_path, token)?;
                println!("{}", "✓ Token set successfully".green().bold());
                println!(
                    "  Configuration saved to: {}",
                    config_path.display().to_string().cyan()
                );
            }
        },

        Commands::Account { action } => {
            let cfg = config::load_config(&config_path)?;
            let account_service = AccountService::new(s2_client(cfg.token).await?);
            match action {
                AccountActions::ListBasins {
                    prefix,
                    start_after,
                    limit,
                } => {
                    let response = account_service
                        .list_basins(prefix, start_after, limit)
                        .await?;

                    for basin_metadata in response.basins {
                        println!("{}", basin_metadata.name);
                    }
                }

                AccountActions::CreateBasin {
                    basin,
                    storage_class,
                    retention_policy,
                } => {
                    let response = account_service
                        .create_basin(basin, storage_class, retention_policy)
                        .await?;
                    println!("{:?}", response);
                }
                AccountActions::DeleteBasin { basin } => {
                    account_service.delete_basin(basin).await?;
                    println!("{}", "✓ Basin deleted successfully".green().bold());
                }
            }
        }
    }

    Ok(())
}
