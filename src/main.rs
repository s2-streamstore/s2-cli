use std::{
    io::BufRead,
    path::PathBuf,
    pin::Pin,
    time::{Duration, UNIX_EPOCH},
};

use account::AccountService;
use basin::BasinService;
use clap::{builder::styling, Parser, Subcommand};
use colored::*;
use config::{config_path, create_config};
use error::{S2CliError, ServiceError, ServiceErrorContext};
use ping::{LatencyStats, PingResult, Pinger};
use rand::Rng;
use stream::{RecordStream, StreamService};
use streamstore::{
    batching::AppendRecordsBatchingOpts,
    client::{BasinClient, Client, ClientConfig, S2Endpoints, StreamClient},
    types::{
        AppendRecordBatch, BasinInfo, CommandRecord, ConvertError, FencingToken, MeteredBytes as _,
        ReadOutput, StreamInfo,
    },
    HeaderValue,
};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufWriter},
    select,
    time::Instant,
};
use tokio::{signal, sync::mpsc};
use tokio_stream::{
    wrappers::{LinesStream, ReceiverStream},
    Stream, StreamExt,
};
use tracing::trace;
use tracing_subscriber::{fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt};
use types::{
    BasinConfig, BasinNameAndMaybeStreamUri, BasinNameAndStreamArgs, BasinNameOnlyUri,
    StreamConfig, RETENTION_POLICY_PATH, STORAGE_CLASS_PATH,
};

mod account;
mod basin;
mod stream;

mod config;
mod error;
mod ping;
mod types;

const STYLES: styling::Styles = styling::Styles::styled()
    .header(styling::AnsiColor::Green.on_default().bold())
    .usage(styling::AnsiColor::Green.on_default().bold())
    .literal(styling::AnsiColor::Blue.on_default().bold())
    .placeholder(styling::AnsiColor::Cyan.on_default());

const GENERAL_USAGE: &str = color_print::cstr!(
    r#"
    <dim>$</dim> <bold>s2 config set --auth-token ...</bold>
    <dim>$</dim> <bold>s2 list-basins --prefix "foo" --limit 100</bold>
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

    /// List basins.
    ListBasins {
        /// Filter to basin names that begin with this prefix.
        #[arg(short = 'p', long, default_value = "")]
        prefix: Option<String>,

        /// Filter to basin names that lexicographically start after this name.
        #[arg(short = 's', long, default_value = "")]
        start_after: Option<String>,

        /// Number of results, upto a maximum of 1000.
        #[arg(short = 'n', long, default_value = "0")]
        limit: Option<usize>,
    },

    /// Create a basin.
    CreateBasin {
        /// Name of the basin to create.
        basin: BasinNameOnlyUri,

        #[command(flatten)]
        config: BasinConfig,
    },

    /// Delete a basin.
    DeleteBasin {
        /// Name of the basin to delete.
        basin: BasinNameOnlyUri,
    },

    /// Get basin config.
    GetBasinConfig {
        /// Basin name to get config for.
        basin: BasinNameOnlyUri,
    },

    /// Reconfigure a basin.
    ReconfigureBasin {
        /// Name of the basin to reconfigure.
        basin: BasinNameOnlyUri,

        /// Configuration to apply.
        #[command(flatten)]
        config: BasinConfig,
    },

    /// List streams.
    #[command(alias = "ls")]
    ListStreams {
        /// Name of the basin to manage or S2 URL with basin and prefix.
        #[arg(value_name = "BASIN/S2_URL")]
        basin: BasinNameAndMaybeStreamUri,

        /// Filter to stream names that begin with this prefix.
        #[arg(short = 'p', long)]
        prefix: Option<String>,

        /// Filter to stream names that lexicographically start after this name.
        #[arg(short = 's', long)]
        start_after: Option<String>,

        /// Number of results, upto a maximum of 1000.
        #[arg(short = 'n', long)]
        limit: Option<usize>,
    },

    /// Create a stream.
    CreateStream {
        #[command(flatten)]
        args: BasinNameAndStreamArgs,

        /// Configuration to apply.
        #[command(flatten)]
        config: Option<StreamConfig>,
    },

    /// Delete a stream.
    #[command(alias = "rm")]
    DeleteStream {
        #[command(flatten)]
        args: BasinNameAndStreamArgs,
    },

    /// Get stream config.
    GetStreamConfig {
        #[command(flatten)]
        args: BasinNameAndStreamArgs,
    },

    /// Reconfigure a stream.
    ReconfigureStream {
        #[command(flatten)]
        args: BasinNameAndStreamArgs,

        /// Configuration to apply.
        #[command(flatten)]
        config: StreamConfig,
    },

    /// Get the next sequence number that will be assigned by a stream.
    CheckTail {
        #[command(flatten)]
        args: BasinNameAndStreamArgs,
    },

    /// Set the trim point for the stream.
    ///
    /// Trimming is eventually consistent, and trimmed records may be visible
    /// for a brief period.
    Trim {
        #[command(flatten)]
        args: BasinNameAndStreamArgs,

        /// Earliest sequence number that should be retained.
        /// This sequence number is only allowed to advance,
        /// and any regression will be ignored.
        trim_point: u64,

        /// Enforce fencing token specified in hex.
        #[arg(short = 'f', long, value_parser = parse_fencing_token)]
        fencing_token: Option<FencingToken>,

        /// Enforce that the sequence number issued to the first record matches.
        #[arg(short = 'm', long)]
        match_seq_num: Option<u64>,
    },

    /// Set a fencing token for the stream.
    ///
    /// Fencing is strongly consistent, and subsequent appends that specify a
    /// token will be rejected if it does not match.
    ///
    /// Note that fencing is a cooperative mechanism,
    /// and it is only enforced when a token is provided.
    Fence {
        #[command(flatten)]
        args: BasinNameAndStreamArgs,

        /// New fencing token specified in hex.
        /// It may be upto 16 bytes, and can be empty.
        #[arg(value_parser = parse_fencing_token)]
        new_fencing_token: FencingToken,

        /// Enforce existing fencing token, specified in hex.
        #[arg(short = 'f', long, value_parser = parse_fencing_token)]
        fencing_token: Option<FencingToken>,

        /// Enforce that the sequence number issued to this command matches.
        #[arg(short = 'm', long)]
        match_seq_num: Option<u64>,
    },

    /// Append records to a stream.
    ///
    /// Currently, only newline delimited records are supported.
    Append {
        #[command(flatten)]
        args: BasinNameAndStreamArgs,

        /// Enforce fencing token specified in hex.
        #[arg(short = 'f', long, value_parser = parse_fencing_token)]
        fencing_token: Option<FencingToken>,

        /// Enforce that the sequence number issued to the first record matches.
        #[arg(short = 'm', long)]
        match_seq_num: Option<u64>,

        /// Input newline delimited records to append from a file or stdin.
        /// All records are treated as plain text.
        /// Use "-" to read from stdin.
        #[arg(short = 'i', long, value_parser = parse_records_input_source, default_value = "-")]
        input: RecordsIn,
    },

    /// Read records from a stream.
    ///
    /// If a limit if specified, reading will stop when the limit is reached or there are no more records on the stream.
    /// If a limit is not specified, the reader will keep tailing and wait for new records.
    Read {
        #[command(flatten)]
        args: BasinNameAndStreamArgs,

        /// Starting sequence number (inclusive).
        #[arg(short = 's', long, default_value_t = 0)]
        start_seq_num: u64,

        /// Output records to a file or stdout.
        /// Use "-" to write to stdout.
        #[arg(short = 'o', long, value_parser = parse_records_output_source, default_value = "-")]
        output: RecordsOut,

        /// Limit the number of records returned.
        #[arg(short = 'n', long)]
        limit_count: Option<u64>,

        /// Limit the number of bytes returned.
        #[arg(short = 'b', long)]
        limit_bytes: Option<u64>,
    },

    /// Ping the stream to get append acknowledgement and end-to-end latencies.
    Ping {
        #[command(flatten)]
        args: BasinNameAndStreamArgs,

        /// Send a batch after this interval.
        ///
        /// Will be set to a minimum of 100ms.
        #[arg(short = 'i', long, default_value = "500ms")]
        interval: humantime::Duration,

        /// Batch size in bytes. A jitter (+/- 25%) will be added.
        ///
        /// Truncated to a maximum of 128 KiB.
        #[arg(short = 'b', long, default_value_t = 32 * 1024)]
        batch_bytes: u64,

        /// Stop after sending this number of batches.
        #[arg(short = 'n', long)]
        num_batches: Option<usize>,
    },
}

#[derive(Subcommand, Debug)]
enum ConfigActions {
    /// Set the authentication token to be reused in subsequent commands.
    /// Alternatively, use the S2_AUTH_TOKEN environment variable.
    Set {
        #[arg(short = 'a', long)]
        auth_token: String,
    },
}

#[derive(Debug, Clone)]
pub enum RecordsIn {
    File(PathBuf),
    Stdin,
}

/// Sink for records in a read session.
#[derive(Debug, Clone)]
pub enum RecordsOut {
    File(PathBuf),
    Stdout,
}

impl RecordsIn {
    pub async fn into_reader(
        &self,
    ) -> std::io::Result<Pin<Box<dyn Stream<Item = std::io::Result<String>> + Send>>> {
        match self {
            RecordsIn::File(path) => {
                let file = File::open(path).await?;
                Ok(Box::pin(LinesStream::new(
                    tokio::io::BufReader::new(file).lines(),
                )))
            }
            RecordsIn::Stdin => Ok(Box::pin(stdio_lines_stream(std::io::stdin()))),
        }
    }
}

fn stdio_lines_stream<F>(f: F) -> ReceiverStream<std::io::Result<String>>
where
    F: std::io::Read + Send + 'static,
{
    let lines = std::io::BufReader::new(f).lines();
    let (tx, rx) = mpsc::channel(AppendRecordBatch::MAX_CAPACITY);
    let _handle = std::thread::spawn(move || {
        for line in lines {
            if tx.blocking_send(line).is_err() {
                return;
            }
        }
    });
    ReceiverStream::new(rx)
}

impl RecordsOut {
    pub async fn into_writer(&self) -> std::io::Result<Box<dyn AsyncWrite + Send + Unpin>> {
        match self {
            RecordsOut::File(path) => {
                trace!(?path, "opening file writer");
                let file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(path)
                    .await?;

                Ok(Box::new(BufWriter::new(file)))
            }
            RecordsOut::Stdout => {
                trace!("stdout writer");
                Ok(Box::new(BufWriter::new(tokio::io::stdout())))
            }
        }
    }
}

fn parse_records_input_source(s: &str) -> Result<RecordsIn, std::io::Error> {
    match s {
        "" | "-" => Ok(RecordsIn::Stdin),
        _ => Ok(RecordsIn::File(PathBuf::from(s))),
    }
}

fn parse_records_output_source(s: &str) -> Result<RecordsOut, std::io::Error> {
    match s {
        "" | "-" => Ok(RecordsOut::Stdout),
        _ => Ok(RecordsOut::File(PathBuf::from(s))),
    }
}

fn parse_fencing_token(s: &str) -> Result<FencingToken, ConvertError> {
    base16ct::mixed::decode_vec(s)
        .map_err(|_| "invalid hex")?
        .try_into()
}

fn client_config(auth_token: String) -> Result<ClientConfig, S2CliError> {
    let endpoints = S2Endpoints::from_env().map_err(S2CliError::EndpointsFromEnv)?;
    let client_config = ClientConfig::new(auth_token.to_string())
        .with_user_agent("s2-cli".parse::<HeaderValue>().expect("valid user agent"))
        .with_endpoints(endpoints)
        .with_request_timeout(Duration::from_secs(30));
    Ok(client_config)
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

        Commands::ListBasins {
            prefix,
            start_after,
            limit,
        } => {
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.auth_token)?;
            let account_service = AccountService::new(Client::new(client_config));
            let response = account_service
                .list_basins(
                    prefix.unwrap_or_default(),
                    start_after.unwrap_or_default(),
                    limit.unwrap_or_default(),
                )
                .await?;

            for basin_info in response.basins {
                let BasinInfo { name, state, .. } = basin_info;

                let state = match state {
                    streamstore::types::BasinState::Active => state.to_string().green(),
                    streamstore::types::BasinState::Deleting => state.to_string().red(),
                    _ => state.to_string().yellow(),
                };
                println!("{} {}", name, state);
            }
        }

        Commands::CreateBasin { basin, config } => {
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.auth_token)?;
            let account_service = AccountService::new(Client::new(client_config));
            let (storage_class, retention_policy) = match &config.default_stream_config {
                Some(config) => {
                    let storage_class = config.storage_class.clone();
                    let retention_policy = config.retention_policy.clone();
                    (storage_class, retention_policy)
                }
                None => (None, None),
            };
            account_service
                .create_basin(basin.into(), storage_class, retention_policy)
                .await?;

            eprintln!("{}", "✓ Basin created".green().bold());
        }

        Commands::DeleteBasin { basin } => {
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.auth_token)?;
            let account_service = AccountService::new(Client::new(client_config));
            account_service.delete_basin(basin.into()).await?;
            eprintln!("{}", "✓ Basin deletion requested".green().bold());
        }

        Commands::GetBasinConfig { basin } => {
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.auth_token)?;
            let account_service = AccountService::new(Client::new(client_config));
            let basin_config = account_service.get_basin_config(basin.into()).await?;
            let basin_config: BasinConfig = basin_config.into();
            println!("{}", serde_json::to_string_pretty(&basin_config)?);
        }

        Commands::ReconfigureBasin { basin, config } => {
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.auth_token)?;
            let account_service = AccountService::new(Client::new(client_config));
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
                .reconfigure_basin(basin.into(), config.into(), mask)
                .await?;
        }

        Commands::ListStreams {
            basin,
            prefix,
            start_after,
            limit,
        } => {
            let BasinNameAndMaybeStreamUri {
                basin,
                stream: maybe_prefix,
            } = basin;
            let prefix = match (maybe_prefix, prefix) {
                (Some(_), Some(_)) => {
                    return Err(ConvertError::from("Multiple prefix specified").into())
                }
                (Some(s), None) | (None, Some(s)) => Some(s),
                (None, None) => None,
            };
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.auth_token)?;
            let basin_client = BasinClient::new(client_config, basin);
            let streams = BasinService::new(basin_client)
                .list_streams(
                    prefix.unwrap_or_default(),
                    start_after.unwrap_or_default(),
                    limit.unwrap_or_default(),
                )
                .await?;
            for StreamInfo {
                name,
                created_at,
                deleted_at,
            } in streams
            {
                let date_time = |time: u32| {
                    humantime::format_rfc3339_seconds(UNIX_EPOCH + Duration::from_secs(time as u64))
                };

                println!(
                    "{} {} {}",
                    name,
                    date_time(created_at).to_string().green(),
                    deleted_at
                        .map(|d| date_time(d).to_string().red())
                        .unwrap_or_default()
                );
            }
        }

        Commands::CreateStream { args, config } => {
            let (basin, stream) = args.try_into_parts()?;
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.auth_token)?;
            let basin_client = BasinClient::new(client_config, basin);
            BasinService::new(basin_client)
                .create_stream(stream, config.map(Into::into))
                .await?;
            eprintln!("{}", "✓ Stream created".green().bold());
        }

        Commands::DeleteStream { args } => {
            let (basin, stream) = args.try_into_parts()?;
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.auth_token)?;
            let basin_client = BasinClient::new(client_config, basin);
            BasinService::new(basin_client)
                .delete_stream(stream)
                .await?;
            eprintln!("{}", "✓ Stream deletion requested".green().bold());
        }

        Commands::GetStreamConfig { args } => {
            let (basin, stream) = args.try_into_parts()?;
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.auth_token)?;
            let basin_client = BasinClient::new(client_config, basin);
            let config: StreamConfig = BasinService::new(basin_client)
                .get_stream_config(stream)
                .await?
                .into();
            println!("{}", serde_json::to_string_pretty(&config)?);
        }

        Commands::ReconfigureStream { args, config } => {
            let (basin, stream) = args.try_into_parts()?;
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.auth_token)?;
            let basin_client = BasinClient::new(client_config, basin);
            let mut mask = Vec::new();

            if config.storage_class.is_some() {
                mask.push("storage_class".to_string());
            };

            if config.retention_policy.is_some() {
                mask.push("retention_policy".to_string());
            };

            let config: StreamConfig = BasinService::new(basin_client)
                .reconfigure_stream(stream, config.into(), mask)
                .await?
                .into();

            eprintln!("{}", "✓ Stream reconfigured".green().bold());
            println!("{}", serde_json::to_string_pretty(&config)?);
        }

        Commands::CheckTail { args } => {
            let (basin, stream) = args.try_into_parts()?;
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.auth_token)?;
            let stream_client = StreamClient::new(client_config, basin, stream);
            let next_seq_num = StreamService::new(stream_client).check_tail().await?;
            println!("{}", next_seq_num);
        }

        Commands::Trim {
            args,
            trim_point,
            fencing_token,
            match_seq_num,
        } => {
            let (basin, stream) = args.try_into_parts()?;
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.auth_token)?;
            let stream_client = StreamClient::new(client_config, basin, stream);
            let out = StreamService::new(stream_client)
                .append_command_record(
                    CommandRecord::trim(trim_point),
                    fencing_token,
                    match_seq_num,
                )
                .await?;
            eprintln!(
                "{}",
                format!("✓ Trim requested at seq_num={}", out.start_seq_num)
                    .green()
                    .bold()
            );
        }

        Commands::Fence {
            args,
            new_fencing_token,
            fencing_token,
            match_seq_num,
        } => {
            let (basin, stream) = args.try_into_parts()?;
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.auth_token)?;
            let stream_client = StreamClient::new(client_config, basin, stream);
            let out = StreamService::new(stream_client)
                .append_command_record(
                    CommandRecord::fence(new_fencing_token),
                    fencing_token,
                    match_seq_num,
                )
                .await?;
            eprintln!(
                "{}",
                format!("✓ Fencing token set at seq_num: {}", out.start_seq_num)
                    .green()
                    .bold()
            );
        }

        Commands::Append {
            args,
            input,
            fencing_token,
            match_seq_num,
        } => {
            let (basin, stream) = args.try_into_parts()?;
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.auth_token)?;
            let stream_client = StreamClient::new(client_config, basin, stream);
            let append_input_stream = RecordStream::new(
                input
                    .into_reader()
                    .await
                    .map_err(|e| S2CliError::RecordReaderInit(e.to_string()))?,
            );

            let mut append_output_stream = StreamService::new(stream_client)
                .append_session(
                    append_input_stream,
                    AppendRecordsBatchingOpts::new()
                        .with_fencing_token(fencing_token)
                        .with_match_seq_num(match_seq_num),
                )
                .await?;

            loop {
                select! {
                    maybe_append_result = append_output_stream.next() => {
                        match maybe_append_result {
                            Some(append_result) => {
                                match append_result {
                                    Ok(append_result) => {
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
                                    },
                                    Err(e) => {
                                        return Err(ServiceError::new(ServiceErrorContext::AppendSession, e).into());
                                    }
                                }
                            }
                            None => break,
                        }
                    }

                    _ = signal::ctrl_c() => {
                        drop(append_output_stream);
                        eprintln!("{}", "■ [ABORTED]".red().bold());
                        break;
                    }
                }
            }
        }

        Commands::Read {
            args,
            start_seq_num,
            output,
            limit_count,
            limit_bytes,
        } => {
            let (basin, stream) = args.try_into_parts()?;
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.auth_token)?;
            let stream_client = StreamClient::new(client_config, basin, stream);
            let mut read_output_stream = StreamService::new(stream_client)
                .read_session(start_seq_num, limit_count, limit_bytes)
                .await?;
            let mut writer = output.into_writer().await.unwrap();

            let mut start = None;
            let mut total_data_len = 0;

            loop {
                select! {
                    maybe_read_result = read_output_stream.next() => {
                        match maybe_read_result {
                            Some(read_result) => {
                                if start.is_none() {
                                    start = Some(Instant::now());
                                }
                                match read_result {
                                    Ok(ReadOutput::Batch(sequenced_record_batch)) => {
                                        let num_records = sequenced_record_batch.records.len();
                                        let mut batch_len = 0;

                                        let seq_range = match (
                                            sequenced_record_batch.records.first(),
                                            sequenced_record_batch.records.last(),
                                        ) {
                                            (Some(first), Some(last)) => first.seq_num..=last.seq_num,
                                            _ => panic!("empty batch"),
                                        };
                                        for sequenced_record in sequenced_record_batch.records {
                                            batch_len += sequenced_record.metered_bytes();

                                            if let Some(command_record) = sequenced_record.as_command_record() {
                                                let (cmd, description) = match command_record {
                                                    CommandRecord::Fence { fencing_token } => (
                                                        "fence",
                                                        format!("FencingToken({})", base16ct::lower::encode_string(fencing_token.as_ref())),
                                                    ),
                                                    CommandRecord::Trim { seq_num } => (
                                                        "trim",
                                                        format!("TrimPoint({seq_num})"),
                                                    ),
                                                };
                                                eprintln!("{} with {}", cmd.bold(), description.green().bold());
                                            } else {
                                                let data = &sequenced_record.body;
                                                writer
                                                    .write_all(data)
                                                    .await
                                                    .map_err(|e| S2CliError::RecordWrite(e.to_string()))?;
                                                writer
                                                    .write_all(b"\n")
                                                    .await
                                                    .map_err(|e| S2CliError::RecordWrite(e.to_string()))?;
                                            }
                                        }
                                        total_data_len += batch_len;

                                        let throughput_mibps = (total_data_len as f64
                                            / start.unwrap().elapsed().as_secs_f64())
                                            / 1024.0
                                            / 1024.0;

                                        eprintln!(
                                            "{}",
                                            format!(
                                                "⦿ {throughput_mibps:.2} MiB/s \
                                                    ({num_records} records in range {seq_range:?})",
                                            )
                                            .blue()
                                            .bold()
                                        );
                                    }

                                    Ok(ReadOutput::FirstSeqNum(seq_num)) => {
                                        eprintln!("{}", format!("first_seq_num: {seq_num}").blue().bold());
                                    }

                                    Ok(ReadOutput::NextSeqNum(seq_num)) => {
                                        eprintln!("{}", format!("next_seq_num: {seq_num}").blue().bold());
                                    }

                                    Err(e) => {
                                        return Err(ServiceError::new(ServiceErrorContext::ReadSession, e).into());
                                    }
                                }
                            }
                            None => break,
                        }
                    },
                    _ = signal::ctrl_c() => {
                        drop(read_output_stream);
                        eprintln!("{}", "■ [ABORTED]".red().bold());
                        break;
                    }
                }
                let total_elapsed_time = start.unwrap().elapsed().as_secs_f64();

                let total_throughput_mibps =
                    (total_data_len as f64 / total_elapsed_time) / 1024.0 / 1024.0;

                eprintln!(
                    "{}",
                    format!(
                        "{total_data_len} metered bytes in \
                                {total_elapsed_time} seconds \
                                at {total_throughput_mibps:.2} MiB/s"
                    )
                    .yellow()
                    .bold()
                );

                writer.flush().await.expect("writer flush");
            }
        }

        Commands::Ping {
            args,
            interval,
            batch_bytes,
            num_batches,
        } => {
            let (basin, stream) = args.try_into_parts()?;
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.auth_token)?;
            let stream_client = StreamService::new(StreamClient::new(client_config, basin, stream));

            let interval = interval.max(Duration::from_millis(100));
            let batch_bytes = batch_bytes.min(128 * 1024);

            eprintln!("Preparing...");

            let mut pinger = Pinger::init(&stream_client).await?;

            let mut pings = Vec::new();

            async fn ping_next(
                pinger: &mut Pinger,
                pings: &mut Vec<PingResult>,
                interval: Duration,
                batch_bytes: u64,
            ) -> Result<(), S2CliError> {
                let jitter_op = if rand::random() {
                    u64::saturating_add
                } else {
                    u64::saturating_sub
                };

                let max_jitter = batch_bytes / 4;

                let record_bytes =
                    jitter_op(batch_bytes, rand::thread_rng().gen_range(0..=max_jitter));

                let Some(res) = pinger.ping(record_bytes).await? else {
                    return Ok(());
                };

                eprintln!(
                    "{:<5} bytes:  ack = {:<7} e2e = {:<7}",
                    res.bytes.to_string().blue(),
                    format!("{} ms", res.ack.as_millis()).blue(),
                    format!("{} ms", res.e2e.as_millis()).blue(),
                );

                pings.push(res);

                tokio::time::sleep(interval).await;
                Ok(())
            }

            while Some(pings.len()) != num_batches {
                select! {
                    _ = ping_next(&mut pinger, &mut pings, interval, batch_bytes) => (),
                    _ = signal::ctrl_c() => break,
                }
            }

            // Close the pinger.
            std::mem::drop(pinger);

            let total_batches = pings.len();
            let (bytes, (acks, e2es)): (Vec<_>, (Vec<_>, Vec<_>)) = pings
                .into_iter()
                .map(|PingResult { bytes, ack, e2e }| (bytes, (ack, e2e)))
                .unzip();
            let total_bytes = bytes.into_iter().sum::<u64>();

            eprintln!(/* Empty line */);
            eprintln!("Round-tripped {total_bytes} bytes in {total_batches} batches");

            pub fn print_stats(stats: LatencyStats, name: &str) {
                eprintln!(
                    "{:-^60}",
                    format!(" {name} Latency Statistics ").yellow().bold()
                );

                fn stat(key: &str, val: String) {
                    eprintln!("{:>9} {}", key, val.green());
                }

                fn stat_duration(key: &str, val: Duration) {
                    stat(key, format!("{} ms", val.as_millis()));
                }

                let LatencyStats {
                    mean,
                    median,
                    p95,
                    p99,
                    max,
                    min,
                    stddev,
                } = stats;

                stat_duration("Mean", mean);
                stat_duration("Median", median);
                stat_duration("P95", p95);
                stat_duration("P99", p99);
                stat_duration("Max", max);
                stat_duration("Min", min);
                stat_duration("Std Dev", stddev);
            }

            eprintln!(/* Empty line */);
            print_stats(LatencyStats::generate(acks), "Append Acknowledgement");
            eprintln!(/* Empty line */);
            print_stats(LatencyStats::generate(e2es), "End-to-End");
        }
    };

    Ok(())
}
