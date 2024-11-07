use std::path::PathBuf;

use account::AccountService;
use basin::BasinService;
use clap::{builder::styling, Parser, Subcommand};
use colored::*;
use config::{config_path, create_config};
use error::S2CliError;
use stream::{RecordStream, StreamService, StreamServiceError};
use streamstore::{
    client::{BasinClient, Client, ClientConfig, HostEndpoints, InvalidHostError, StreamClient},
    types::{BasinMetadata, ReadOutput},
};
use tokio::{
    fs::{File, OpenOptions},
    io::{self, AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter},
};
use tokio_stream::StreamExt;
use tracing::trace;
use tracing_subscriber::{fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt};
use types::{BasinConfig, StreamConfig, RETENTION_POLICY_PATH, STORAGE_CLASS_PATH};

mod account;
mod basin;
mod stream;

mod config;
mod error;
mod types;

const STYLES: styling::Styles = styling::Styles::styled()
    .header(styling::AnsiColor::Green.on_default().bold())
    .usage(styling::AnsiColor::Green.on_default().bold())
    .literal(styling::AnsiColor::Blue.on_default().bold())
    .placeholder(styling::AnsiColor::Cyan.on_default());

const GENERAL_USAGE: &str = color_print::cstr!(
    r#"
    <dim>$</dim> <bold>s2 config set --auth-token ...</bold>
    <dim>$</dim> <bold>s2 account list-basins --prefix "bar" --start-after "foo" --limit 100</bold>
    "#
);

#[derive(Parser, Debug)]
#[command(version, about, override_usage = GENERAL_USAGE, styles = STYLES)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Manage CLI configuration.
    Config {
        #[command(subcommand)]
        action: ConfigActions,
    },

    /// Operate on an S2 account.
    Account {
        #[command(subcommand)]
        action: AccountActions,
    },

    /// Operate on an S2 basin.
    Basin {
        /// Name of the basin to manage.
        basin: String,

        #[command(subcommand)]
        action: BasinActions,
    },

    /// Operate on an S2 stream.
    Stream {
        /// Name of the basin.
        basin: String,

        /// Name of the stream.
        stream: String,

        #[command(subcommand)]
        action: StreamActions,
    },
}

#[derive(Subcommand, Debug)]
enum ConfigActions {
    /// Set the authentication token to be reused in subsequent commands.
    /// Alternatively, use the S2_AUTH_TOKEN environment variable.
    Set {
        #[arg(short, long)]
        auth_token: String,
    },
}

#[deny(missing_docs)]
#[derive(Subcommand, Debug)]
enum AccountActions {
    /// List basins.
    ListBasins {
        /// Filter to basin names that begin with this prefix.
        #[arg(short, long, default_value = "")]
        prefix: Option<String>,

        /// Filter to basin names that lexicographically start after this name.
        #[arg(short, long, default_value = "")]
        start_after: Option<String>,

        /// Number of results, upto a maximum of 1000.
        #[arg(short, long, default_value = "0")]
        limit: Option<usize>,
    },

    /// Create a basin.
    CreateBasin {
        /// Name of the basin to create.
        basin: String,

        #[command(flatten)]
        config: BasinConfig,
    },

    /// Delete a basin.
    DeleteBasin {
        /// Name of the basin to delete.
        basin: String,
    },

    /// Get basin config.
    GetBasinConfig {
        /// Basin name to get config for.
        basin: String,
    },

    /// Reconfigure a basin.
    ReconfigureBasin {
        /// Name of the basin to reconfigure.
        basin: String,

        /// Configuration to apply.
        #[command(flatten)]
        config: BasinConfig,
    },
}

#[derive(Subcommand, Debug)]
enum BasinActions {
    /// List streams.
    ListStreams {
        /// Filter to stream names that begin with this prefix.
        #[arg(short, long)]
        prefix: Option<String>,

        /// Filter to stream names that lexicographically start after this name.
        #[arg(short, long)]
        start_after: Option<String>,

        /// Number of results, upto a maximum of 1000.
        #[arg(short, long)]
        limit: Option<usize>,
    },

    /// Create a stream.
    CreateStream {
        /// Name of the stream to create.
        stream: String,

        /// Configuration to apply.
        #[command(flatten)]
        config: Option<StreamConfig>,
    },

    /// Delete a stream.
    DeleteStream {
        /// Name of the stream to delete.
        stream: String,
    },

    /// Get stream config.
    GetStreamConfig {
        /// Name of the stream to get config for.
        stream: String,
    },

    /// Reconfigure a stream.
    ReconfigureStream {
        /// Name of the stream to reconfigure.
        stream: String,

        /// Configuration to apply.
        #[command(flatten)]
        config: StreamConfig,
    },
}

#[derive(Subcommand, Debug)]
enum StreamActions {
    /// Get the next sequence number that will be assigned by a stream.
    CheckTail,

    /// Append records to a stream. Currently, only newline delimited records are supported.
    Append {
        /// Newline delimited records to append from a file or stdin (all records are treated as plain text).
        /// Use "-" to read from stdin.
        #[arg(value_parser = parse_records_input_source)]
        records: RecordsIO,
    },

    Read {
        /// Starting sequence number (inclusive). If not specified, the latest record.
        start_seq_num: Option<u64>,

        /// Output records to a file or stdout.
        /// Use "-" to write to stdout.
        #[arg(value_parser = parse_records_output_source)]
        output: Option<RecordsIO>,
    },
}

/// Source of records for an append session.
#[derive(Debug, Clone)]
pub enum RecordsIO {
    File(PathBuf),
    Stdin,
    Stdout,
}

impl RecordsIO {
    pub async fn into_reader(&self) -> std::io::Result<Box<dyn AsyncBufRead + Send + Unpin>> {
        match self {
            RecordsIO::File(path) => Ok(Box::new(BufReader::new(File::open(path).await?))),
            RecordsIO::Stdin => Ok(Box::new(BufReader::new(tokio::io::stdin()))),
            _ => panic!("unsupported record source"),
        }
    }

    pub async fn into_writer(&self) -> io::Result<Box<dyn AsyncWrite + Send + Unpin>> {
        match self {
            RecordsIO::File(path) => {
                trace!(?path, "opening file writer");
                let file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(path)
                    .await?;

                Ok(Box::new(BufWriter::new(file)))
            }
            RecordsIO::Stdout => {
                trace!("stdout writer");
                Ok(Box::new(BufWriter::new(tokio::io::stdout())))
            },
            RecordsIO::Stdin => panic!("unsupported record source"),
        }
    }
}

fn parse_records_input_source(s: &str) -> Result<RecordsIO, std::io::Error> {
    match s {
        "-" => Ok(RecordsIO::Stdin),
        _ => Ok(RecordsIO::File(PathBuf::from(s))),
    }
}

fn parse_records_output_source(s: &str) -> Result<RecordsIO, std::io::Error> {
    match s {
        "-" => Ok(RecordsIO::Stdout),
        _ => Ok(RecordsIO::File(PathBuf::from(s))),
    }
}

fn client_config(auth_token: String) -> Result<ClientConfig, InvalidHostError> {
    Ok(ClientConfig::new(auth_token.to_string())
        .with_host_endpoint(HostEndpoints::from_env()?)
        .with_connection_timeout(std::time::Duration::from_secs(5)))
}

#[tokio::main]
async fn main() -> miette::Result<()> {
    miette::set_panic_hook();
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
                eprintln!("{}", "✓ Token set".green().bold());
                eprintln!(
                    "  Configuration saved to: {}",
                    config_path.display().to_string().cyan()
                );
            }
        },

        Commands::Account { action } => {
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.auth_token)?;
            let account_service = AccountService::new(Client::connect(client_config).await?);
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
                            streamstore::types::BasinState::Active => state.to_string().green(),
                            streamstore::types::BasinState::Deleting => state.to_string().red(),
                            _ => state.to_string().yellow(),
                        };
                        println!("{} {}", name, state);
                    }
                }

                AccountActions::CreateBasin { basin, config } => {
                    let (storage_class, retention_policy) = match &config.default_stream_config {
                        Some(config) => {
                            let storage_class = config.storage_class.clone();
                            let retention_policy = config.retention_policy.clone();
                            (storage_class, retention_policy)
                        }
                        None => (None, None),
                    };
                    account_service
                        .create_basin(basin, storage_class, retention_policy)
                        .await?;

                    eprintln!("{}", "✓ Basin created".green().bold());
                }

                AccountActions::DeleteBasin { basin } => {
                    account_service.delete_basin(basin).await?;
                    eprintln!("{}", "✓ Basin deletion requested".green().bold());
                }

                AccountActions::GetBasinConfig { basin } => {
                    let basin_config = account_service.get_basin_config(basin).await?;
                    let basin_config: BasinConfig = basin_config.into();
                    println!("{}", serde_json::to_string_pretty(&basin_config)?);
                }

                AccountActions::ReconfigureBasin { basin, config } => {
                    let mut mask = Vec::new();
                    if let Some(config) = &config.default_stream_config {
                        if config.storage_class.is_some() {
                            mask.push(STORAGE_CLASS_PATH.to_string());
                        }
                        if config.retention_policy.is_some() {
                            mask.push(RETENTION_POLICY_PATH.to_string());
                        }
                    }

                    account_service
                        .reconfigure_basin(basin, config.into(), mask)
                        .await?;
                }
            }
        }

        Commands::Basin { basin, action } => {
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.auth_token)?;
            match action {
                BasinActions::ListStreams {
                    prefix,
                    start_after,
                    limit,
                } => {
                    let basin_client = BasinClient::connect(client_config, basin).await?;
                    let streams = BasinService::new(basin_client)
                        .list_streams(
                            prefix.unwrap_or_default(),
                            start_after.unwrap_or_default(),
                            limit.unwrap_or_default(),
                        )
                        .await?;
                    for stream in streams {
                        println!("{}", stream);
                    }
                }

                BasinActions::CreateStream { stream, config } => {
                    let basin_client = BasinClient::connect(client_config, basin).await?;
                    BasinService::new(basin_client)
                        .create_stream(stream, config.map(Into::into))
                        .await?;
                    eprintln!("{}", "✓ Stream created".green().bold());
                }

                BasinActions::DeleteStream { stream } => {
                    let basin_client = BasinClient::connect(client_config, basin).await?;
                    BasinService::new(basin_client)
                        .delete_stream(stream)
                        .await?;
                    eprintln!("{}", "✓ Stream deleted".green().bold());
                }

                BasinActions::GetStreamConfig { stream } => {
                    let basin_client = BasinClient::connect(client_config, basin).await?;
                    let config = BasinService::new(basin_client)
                        .get_stream_config(stream)
                        .await?;
                    let config: StreamConfig = config.into();
                    println!("{}", serde_json::to_string_pretty(&config)?);
                }

                BasinActions::ReconfigureStream { stream, config } => {
                    let basin_client = BasinClient::connect(client_config, basin).await?;
                    let mut mask = Vec::new();

                    if config.storage_class.is_some() {
                        mask.push("storage_class".to_string());
                    };

                    if config.retention_policy.is_some() {
                        mask.push("retention_policy".to_string());
                    };

                    BasinService::new(basin_client)
                        .reconfigure_stream(stream, config.into(), mask)
                        .await?;

                    eprintln!("{}", "✓ Stream reconfigured".green().bold());
                }
            }
        }
        Commands::Stream {
            basin,
            stream,
            action,
        } => {
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.auth_token)?;
            match action {
                StreamActions::CheckTail => {
                    let stream_client = StreamClient::connect(client_config, basin, stream).await?;
                    let next_seq_num = StreamService::new(stream_client).check_tail().await?;
                    println!("{}", next_seq_num);
                }
                StreamActions::Append { records } => {
                    let stream_client = StreamClient::connect(client_config, basin, stream).await?;
                    let append_input_stream = RecordStream::new(
                        records
                            .into_reader()
                            .await
                            .map_err(|_| S2CliError::RecordReaderInit)?
                            .lines(),
                    );

                    let mut append_output_stream = StreamService::new(stream_client)
                        .append_session(append_input_stream)
                        .await?;
                    while let Some(append_result) = append_output_stream.next().await {
                        append_result
                            .map(|append_result| {
                                eprintln!(
                                    "{}",
                                    format!(
                                        "✓ [APPENDED] start: {}, end: {}, next: {}",
                                        append_result.start_seq_num,
                                        append_result.end_seq_num,
                                        append_result.next_seq_num
                                    )
                                    .green()
                                    .bold()
                                );
                            })
                            .map_err(StreamServiceError::AppendSession)?;
                    }
                }
                StreamActions::Read {
                    start_seq_num,
                    output,
                } => {
                    let stream_client = StreamClient::connect(client_config, basin, stream).await?;
                    let mut read_output_stream = StreamService::new(stream_client)
                        .read_session(start_seq_num)
                        .await?;
                    let mut writer = match output {
                        Some(output) => Some(output.into_writer().await.unwrap()),
                        None => None,
                    };
                    while let Some(read_result) = read_output_stream.next().await {
                        let read_result = read_result.map_err(StreamServiceError::ReadSession)?;
                        match read_result.output {
                            ReadOutput::Batch(sequenced_record_batch) => {
                                for sequenced_record in sequenced_record_batch.records {
                                    eprintln!(
                                        "{}",
                                        format!(
                                            "✓ [READ] got record batch: seq_num: {}",
                                            sequenced_record.seq_num,
                                        )
                                        .green()
                                        .bold()
                                    );
                                    if let Some(ref mut writer) = writer {
                                        writer.write_all(&sequenced_record.body).await.unwrap();
                                        writer.write_all(b"\n").await.unwrap();
                                    }
                                }
                            }
                            // TODO: better message for these cases
                            ReadOutput::FirstSeqNum(seq_num) => {
                                eprintln!(
                                    "{}",
                                    format!("✓ [READ] first_seq_num: {}", seq_num).blue().bold()
                                );
                            }
                            ReadOutput::NextSeqNum(seq_num) => {
                                eprintln!(
                                    "{}",
                                    format!("✓ [READ] next_seq_num: {}", seq_num).blue().bold()
                                );
                            }
                        }
                        if let Some(ref mut writer) = writer {
                            writer.flush().await.expect("writer flush");
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
