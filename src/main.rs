use clap::{Parser, Subcommand};
use colored::*;
use config::{config_path, create_config};
use error::S2CliError;

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
}

#[derive(Subcommand, Debug)]
enum ConfigActions {
    /// Set the authentication token
    Set {
        #[arg(short, long)]
        token: String,
    },
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{}", format!("✗ {}", err).red());
        std::process::exit(1);
    }
}

fn run() -> Result<(), S2CliError> {
    let commands = Cli::parse();
    let config_path = config_path()?;

    match commands.command {
        Commands::Config { action } => match action {
            ConfigActions::Set { token } => {
                create_config(&config_path, &token)?;
                println!("{}", "✓ Token set successfully".green().bold());
                println!(
                    "  Configuration saved to: {}",
                    config_path.display().to_string().cyan()
                );
            }
        },
    }

    Ok(())
}
