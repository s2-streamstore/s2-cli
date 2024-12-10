use std::{
    path::PathBuf,
    time::{Duration, UNIX_EPOCH},
};

use account::AccountService;
use basin::BasinService;
use clap::{builder::styling, Parser, Subcommand};
use colored::*;
use config::{config_path, create_config};
use error::{S2CliError, ServiceError, ServiceErrorContext};
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use rand::{
    distributions::{Alphanumeric, Uniform},
    Rng,
};
use scopeguard::defer;
use stream::{RecordStream, StreamService};
use streamstore::{
    batching::AppendRecordsBatchingOpts,
    client::{BasinClient, Client, ClientConfig, S2Endpoints, StreamClient},
    types::{
        AppendOutput, AppendRecord, BasinInfo, BasinName, CommandRecord, ConvertError,
        FencingToken, Header, MeteredBytes as _, ReadOutput, SequencedRecordBatch, StreamInfo,
    },
    HeaderValue,
};
use tokio::{
    fs::{File, OpenOptions},
    io::{self, AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter},
    select,
    time::Instant,
};
use tokio::{signal, sync::mpsc};
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
        basin: BasinName,

        #[command(flatten)]
        config: BasinConfig,
    },

    /// Delete a basin.
    DeleteBasin {
        /// Name of the basin to delete.
        basin: BasinName,
    },

    /// Get basin config.
    GetBasinConfig {
        /// Basin name to get config for.
        basin: BasinName,
    },

    /// Reconfigure a basin.
    ReconfigureBasin {
        /// Name of the basin to reconfigure.
        basin: BasinName,

        /// Configuration to apply.
        #[command(flatten)]
        config: BasinConfig,
    },

    /// List streams.
    ListStreams {
        /// Name of the basin to manage.
        basin: BasinName,

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
        /// Name of the basin to manage.
        basin: BasinName,

        /// Name of the stream to create.
        stream: String,

        /// Configuration to apply.
        #[command(flatten)]
        config: Option<StreamConfig>,
    },

    /// Delete a stream.
    DeleteStream {
        /// Name of the basin to manage.
        basin: BasinName,

        /// Name of the stream to delete.
        stream: String,
    },

    /// Get stream config.
    GetStreamConfig {
        /// Name of the basin to manage.
        basin: BasinName,

        /// Name of the stream to get config for.
        stream: String,
    },

    /// Reconfigure a stream.
    ReconfigureStream {
        /// Name of the basin to manage.
        basin: BasinName,

        /// Name of the stream to reconfigure.
        stream: String,

        /// Configuration to apply.
        #[command(flatten)]
        config: StreamConfig,
    },

    /// Get the next sequence number that will be assigned by a stream.
    CheckTail {
        /// Name of the basin.
        basin: BasinName,

        /// Name of the stream.
        stream: String,
    },

    /// Set the trim point for the stream.
    ///
    /// Trimming is eventually consistent, and trimmed records may be visible
    /// for a brief period.
    Trim {
        /// Name of the basin.
        basin: BasinName,

        /// Name of the stream.
        stream: String,

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
        /// Name of the basin.
        basin: BasinName,

        /// Name of the stream.
        stream: String,

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
        /// Name of the basin.
        basin: BasinName,

        /// Name of the stream.
        stream: String,

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
        /// Name of the basin.
        basin: BasinName,

        /// Name of the stream.
        stream: String,

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

    /// Run a speed test.
    Speedtest {
        /// Name of the basin.
        basin: BasinName,

        /// Name of the stream.
        stream: String,

        /// Bytes to send.
        ///
        /// Will be truncated to a maximum of 100 MiB.
        #[arg(short = 'b', long, default_value_t = 100 * 1024 * 1024)]
        total_bytes: u64,
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
    pub async fn into_reader(&self) -> std::io::Result<Box<dyn AsyncBufRead + Send + Unpin>> {
        match self {
            RecordsIn::File(path) => Ok(Box::new(BufReader::new(File::open(path).await?))),
            RecordsIn::Stdin => Ok(Box::new(BufReader::new(tokio::io::stdin()))),
        }
    }
}

impl RecordsOut {
    pub async fn into_writer(&self) -> io::Result<Box<dyn AsyncWrite + Send + Unpin>> {
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
                .create_basin(basin, storage_class, retention_policy)
                .await?;

            eprintln!("{}", "✓ Basin created".green().bold());
        }

        Commands::DeleteBasin { basin } => {
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.auth_token)?;
            let account_service = AccountService::new(Client::new(client_config));
            account_service.delete_basin(basin).await?;
            eprintln!("{}", "✓ Basin deletion requested".green().bold());
        }

        Commands::GetBasinConfig { basin } => {
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.auth_token)?;
            let account_service = AccountService::new(Client::new(client_config));
            let basin_config = account_service.get_basin_config(basin).await?;
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
                .reconfigure_basin(basin, config.into(), mask)
                .await?;
        }

        Commands::ListStreams {
            basin,
            prefix,
            start_after,
            limit,
        } => {
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

        Commands::CreateStream {
            basin,
            stream,
            config,
        } => {
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.auth_token)?;
            let basin_client = BasinClient::new(client_config, basin);
            BasinService::new(basin_client)
                .create_stream(stream, config.map(Into::into))
                .await?;
            eprintln!("{}", "✓ Stream created".green().bold());
        }

        Commands::DeleteStream { basin, stream } => {
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.auth_token)?;
            let basin_client = BasinClient::new(client_config, basin);
            BasinService::new(basin_client)
                .delete_stream(stream)
                .await?;
            eprintln!("{}", "✓ Stream deletion requested".green().bold());
        }

        Commands::GetStreamConfig { basin, stream } => {
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.auth_token)?;
            let basin_client = BasinClient::new(client_config, basin);
            let config: StreamConfig = BasinService::new(basin_client)
                .get_stream_config(stream)
                .await?
                .into();
            println!("{}", serde_json::to_string_pretty(&config)?);
        }

        Commands::ReconfigureStream {
            basin,
            stream,
            config,
        } => {
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

        Commands::CheckTail { basin, stream } => {
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.auth_token)?;
            let stream_client = StreamClient::new(client_config, basin, stream);
            let next_seq_num = StreamService::new(stream_client).check_tail().await?;
            println!("{}", next_seq_num);
        }

        Commands::Trim {
            basin,
            stream,
            trim_point,
            fencing_token,
            match_seq_num,
        } => {
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
            basin,
            stream,
            new_fencing_token,
            fencing_token,
            match_seq_num,
        } => {
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
            basin,
            stream,
            input,
            fencing_token,
            match_seq_num,
        } => {
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.auth_token)?;
            let stream_client = StreamClient::new(client_config, basin, stream);
            let append_input_stream = RecordStream::new(
                input
                    .into_reader()
                    .await
                    .map_err(|e| S2CliError::RecordReaderInit(e.to_string()))?
                    .lines(),
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
            basin,
            stream,
            start_seq_num,
            output,
            limit_count,
            limit_bytes,
        } => {
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

        Commands::Speedtest {
            basin,
            stream,
            total_bytes,
        } => {
            const RECORD_BYTES: u64 = 2 * 1024;
            const RECORD_COUNT_IN_BATCH: usize = 10;

            const WARM_UP_BATCH_BODY: &str = "warm up";
            const RECORD_ID_HEADER: &[u8] = b"record-id";

            let total_bytes = total_bytes.min(100 * 1024 * 1024);

            let record_count = (total_bytes as f64 / RECORD_BYTES as f64).ceil() as usize;

            let total_bytes = record_count as u64 * RECORD_BYTES;

            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.auth_token)?;
            let stream_client = StreamService::new(StreamClient::new(client_config, basin, stream));

            let progress_bar = MultiProgress::with_draw_target(ProgressDrawTarget::stderr());

            let progress_bar_style = ProgressStyle::default_bar()
                .template("{prefix:>10.bold} [{bar:<72.cyan/blue}] {percent:>3}%")
                .expect("valid template")
                .progress_chars("#>-");

            let add_progress_bar = |prefix: &'static str| {
                progress_bar.add(
                    ProgressBar::new(record_count as u64)
                        .with_style(progress_bar_style.clone())
                        .with_prefix(prefix),
                )
            };

            let prepare_progress_bar = add_progress_bar("Prepare");
            let sends_progress_bar = add_progress_bar("Sends");
            let appends_progress_bar = add_progress_bar("Appends");
            let reads_progress_bar = add_progress_bar("Reads");

            let (records, record_ids): (Vec<_>, Vec<_>) = (0..record_count)
                .map(|_| {
                    let jitter_op = if rand::random() {
                        u64::saturating_add
                    } else {
                        u64::saturating_sub
                    };

                    let record_bytes =
                        jitter_op(RECORD_BYTES, rand::thread_rng().gen_range(0..=10));

                    let body = rand::thread_rng()
                        .sample_iter(&Uniform::new_inclusive(0, u8::MAX))
                        .take(record_bytes as usize)
                        .collect::<Vec<_>>();

                    let rec_id = rand::thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(16)
                        .collect::<Vec<_>>();

                    let rec = AppendRecord::new(body)
                        .expect("pre validated append record bytes")
                        .with_headers(vec![Header::new(RECORD_ID_HEADER, rec_id.clone())])
                        .expect("pre validated append record header");

                    prepare_progress_bar.inc(1);

                    (rec, rec_id)
                })
                .unzip();

            let tail = stream_client.check_tail().await?;

            prepare_progress_bar.finish();

            let mut read_stream = stream_client.read_session(tail, None, None).await?;

            let reads_handle = tokio::spawn(async move {
                defer!(reads_progress_bar.finish());

                let mut reads = Vec::with_capacity(record_count);

                let mut received_warmup = false;

                let mut reads_start = Instant::now();
                let mut record_ids = record_ids.into_iter();

                while let Some(next) = read_stream.next().await {
                    match next {
                        Err(e) => {
                            return Err(
                                ServiceError::new(ServiceErrorContext::ReadSession, e).into()
                            );
                        }
                        Ok(output) => {
                            if let ReadOutput::Batch(SequencedRecordBatch { records }) = output {
                                let recv = Instant::now();
                                let records = if !received_warmup {
                                    // First batch should be "warm up"
                                    let first = records.first().expect("empty batch");
                                    if first.body.as_ref() != WARM_UP_BATCH_BODY.as_bytes()
                                        || first.seq_num != tail
                                    {
                                        return Err(S2CliError::SpeedtestStreamMutated);
                                    }
                                    received_warmup = true;
                                    // Start read now.
                                    reads_start = recv;
                                    &records[1..]
                                } else {
                                    &records
                                };
                                // Validate records
                                for rec in records {
                                    if rec.headers.len() != 1 {
                                        return Err(S2CliError::SpeedtestStreamMutated);
                                    }
                                    let header = rec.headers.first().expect("validated length");
                                    if header.name.as_ref() != RECORD_ID_HEADER {
                                        return Err(S2CliError::SpeedtestStreamMutated);
                                    }
                                    let rec_id = record_ids
                                        .next()
                                        .ok_or(S2CliError::SpeedtestStreamMutated)?;
                                    if header.value.as_ref() != rec_id.as_slice() {
                                        return Err(S2CliError::SpeedtestStreamMutated);
                                    }
                                }
                                reads.extend(std::iter::repeat_n(recv, records.len()));
                                reads_progress_bar.inc(records.len() as u64);
                            } else {
                                return Err(S2CliError::SpeedtestStreamMutated);
                            }
                        }
                    }

                    if reads.len() >= record_count {
                        break;
                    }
                }

                Ok((reads_start, reads))
            });

            let (tx, rx) = mpsc::channel(RECORD_COUNT_IN_BATCH);

            let append_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

            let mut append_stream = stream_client
                .append_session(
                    append_stream,
                    AppendRecordsBatchingOpts::new()
                        .with_max_batch_records(RECORD_COUNT_IN_BATCH)
                        .with_linger(Duration::from_millis(1))
                        .with_match_seq_num(Some(tail)),
                )
                .await?;

            // Send in a "warm up" which we're going to ignore.
            tx.send(AppendRecord::new(WARM_UP_BATCH_BODY).expect("valid record"))
                .await
                .expect("channel open");

            match append_stream.next().await.expect("stream should receive") {
                Ok(AppendOutput { start_seq_num, .. }) if start_seq_num == tail => (),
                Ok(_) => return Err(S2CliError::SpeedtestStreamMutated),
                Err(e) => {
                    return Err(ServiceError::new(ServiceErrorContext::AppendSession, e).into())
                }
            };

            let appends_handle = tokio::spawn(async move {
                defer!(appends_progress_bar.finish());

                let mut appends = Vec::with_capacity(record_count);

                let appends_start = Instant::now();

                while let Some(next) = append_stream.next().await {
                    match next {
                        Ok(AppendOutput {
                            start_seq_num,
                            end_seq_num,
                            ..
                        }) => {
                            let append = Instant::now();
                            let records = end_seq_num - start_seq_num;
                            appends.extend(std::iter::repeat_n(append, records as usize));
                            appends_progress_bar.inc(records);
                        }
                        Err(e) => {
                            return Err(ServiceError::new(ServiceErrorContext::AppendSession, e))
                        }
                    }
                }

                Ok((appends_start, appends))
            });

            let mut sends = Vec::with_capacity(record_count);

            // Send in an extra batch for warm-up, which is going to be ignored.
            for rec in records {
                if tx.send(rec).await.is_ok() {
                    sends.push(Instant::now());
                    sends_progress_bar.inc(1);
                } else {
                    // Receiver closed.
                    break;
                }
            }

            sends_progress_bar.finish();

            // Close the stream.
            std::mem::drop(tx);

            let (reads_start, reads) = reads_handle.await.expect("reads task panic")?;
            let (appends_start, appends) = appends_handle.await.expect("appends task panic")?;

            eprintln!("\n");

            StatsReport::generate(&sends, appends_start, &appends, total_bytes)
                .print("Append acknowledgement");

            eprintln!(/* Empty line */);

            StatsReport::generate(&sends, reads_start, &reads, total_bytes).print("End to end");
        }
    };

    std::process::exit(0);
}

struct StatsReport {
    pub mean: Duration,
    pub median: Duration,
    pub p95: Duration,
    pub p99: Duration,
    pub max: Duration,
    pub min: Duration,
    pub stddev: Duration,
    // In bytes per second
    pub throughput: u128,
}

impl StatsReport {
    pub fn generate(
        sends: &[Instant],
        op_start: Instant,
        op: &[Instant],
        total_bytes: u64,
    ) -> Self {
        let op_duration = *op.last().unwrap() - op_start;
        let throughput = total_bytes as u128 * 1_000_000 / op_duration.as_micros();

        let mut data = op
            .iter()
            .zip(sends.iter())
            .map(|(o, s)| *o - *s)
            .collect::<Vec<_>>();
        data.sort_unstable();

        let n = data.len();

        let mean = data.iter().sum::<Duration>() / n as u32;

        let median = if n % 2 == 0 {
            (data[n / 2 - 1] + data[n / 2]) / 2
        } else {
            data[n / 2]
        };

        let p_idx = |p: f64| ((n as f64) * p).ceil() as usize - 1;

        let variance = data
            .iter()
            .map(|d| (d.as_secs_f64() - mean.as_secs_f64()).powi(2))
            .sum::<f64>()
            / n as f64;
        let stddev = Duration::from_secs_f64(variance.sqrt());

        Self {
            mean,
            median,
            p95: data[p_idx(0.95)],
            p99: data[p_idx(0.99)],
            max: data[n - 1],
            min: data[0],
            stddev,
            throughput,
        }
    }

    pub fn print(self, name: &str) {
        eprintln!("{}", format!("{name} report").yellow().bold());

        fn stat(key: &str, val: String) {
            eprintln!("{:>12} {}", key, val.green());
        }

        fn stat_duration(key: &str, val: Duration) {
            stat(key, format!("{} ms", val.as_millis()));
        }

        let StatsReport {
            mean,
            median,
            p95,
            p99,
            max,
            min,
            stddev,
            throughput,
        } = self;

        stat_duration("Mean", mean);
        stat_duration("Median", median);
        stat_duration("P95", p95);
        stat_duration("P99", p99);
        stat_duration("Max", max);
        stat_duration("Min", min);
        stat_duration("Std Dev", stddev);

        let mibps = throughput as f64 / (1024.0 * 1024.0);
        stat("Throughput", format!("{mibps:.2} MiB/s"));
    }
}
