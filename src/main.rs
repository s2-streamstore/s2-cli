use account::AccountService;
use basin::BasinService;
use clap::{Parser, Subcommand, ValueEnum, builder::styling};
use colored::Colorize;
use config::{config_path, create_config};
use error::{S2CliError, ServiceError, ServiceErrorContext};
use formats::{Base64JsonFormatter, RawBodyFormatter, RawJsonFormatter, RecordWriter};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use json_to_table::json_to_table;
use ping::{LatencyStats, PingResult, Pinger};
use rand::Rng;
use s2::{
    Streaming,
    batching::AppendRecordsBatchingOpts,
    client::{BasinClient, Client, ClientConfig, S2Endpoints, StreamClient},
    types::{
        AccessTokenId, AppendRecord, AppendRecordBatch, BasinInfo, Command, CommandRecord,
        FencingToken, MeteredBytes as _, ReadLimit, ReadOutput, ReadStart, StreamInfo,
    },
};
use std::{
    io::BufRead,
    path::PathBuf,
    pin::Pin,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use stream::{RecordStream, StreamService};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufWriter},
    select,
};
use tokio::{signal, sync::mpsc};
use tokio_stream::{
    Stream, StreamExt,
    wrappers::{LinesStream, ReceiverStream},
};
use tracing::trace;
use tracing_subscriber::{fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt};
use types::{
    AccessTokenInfo, BasinConfig, DeleteOnEmpty, DeleteOnEmptyConfig, Operation,
    PermittedOperationGroups, ResourceSet, RetentionPolicy, S2BasinAndMaybeStreamUri,
    S2BasinAndStreamUri, S2BasinUri, StorageClass, StreamConfig, TimestampingConfig,
    TimestampingMode,
};

mod account;
mod basin;
mod stream;

mod config;
mod error;
mod formats;
mod ping;
mod types;

const STYLES: styling::Styles = styling::Styles::styled()
    .header(styling::AnsiColor::Green.on_default().bold())
    .usage(styling::AnsiColor::Green.on_default().bold())
    .literal(styling::AnsiColor::Blue.on_default().bold())
    .placeholder(styling::AnsiColor::Cyan.on_default());

const GENERAL_USAGE: &str = color_print::cstr!(
    r#"
    <dim>$</dim> <bold>s2 config set --access-token ...</bold>
    <dim>$</dim> <bold>s2 list-basins --prefix "foo" --limit 100</bold>
    "#
);

#[derive(Parser, Debug)]
#[command(name = "s2", version, override_usage = GENERAL_USAGE, styles = STYLES)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Parser, Debug)]
struct S2BasinAndStreamUriArgs {
    /// S2 URI of the format: s2://{basin}/{stream}
    #[arg(value_name = "S2_URI")]
    uri: S2BasinAndStreamUri,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Manage CLI configuration.
    Config {
        #[command(subcommand)]
        action: ConfigActions,
    },

    /// List basins or streams in a basin.
    ///
    /// List basins if basin name is not provided otherwise lists streams in
    /// the basin.
    Ls {
        /// Name of the basin to manage or S2 URI with basin and optionally prefix.
        ///
        /// S2 URI is of the format: s2://{basin}/{prefix}
        #[arg(value_name = "BASIN|S2_URI")]
        uri: Option<S2BasinAndMaybeStreamUri>,

        /// Filter to names that begin with this prefix.
        #[arg(short = 'p', long)]
        prefix: Option<String>,

        /// Filter to names that lexicographically start after this name.
        #[arg(short = 's', long)]
        start_after: Option<String>,

        /// Number of results, upto a maximum of 1000.
        #[arg(short = 'n', long)]
        limit: Option<usize>,

        /// Disable automatic following of pagination responses, which can make multiple RPC calls.
        #[arg(long, default_value_t = false)]
        no_auto_paginate: bool,
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
        #[arg(short = 'n', long)]
        limit: Option<usize>,

        /// Disable automatic following of pagination responses, which can make multiple RPC calls.
        #[arg(long, default_value_t = false)]
        no_auto_paginate: bool,
    },

    /// Create a basin.
    CreateBasin {
        /// Name of the basin to create.
        basin: S2BasinUri,

        #[command(flatten)]
        config: BasinConfig,
    },

    /// Delete a basin.
    DeleteBasin {
        /// Name of the basin to delete.
        basin: S2BasinUri,
    },

    /// Get basin config.
    GetBasinConfig {
        /// Basin name to get config for.
        basin: S2BasinUri,
    },

    /// Reconfigure a basin.
    ReconfigureBasin {
        /// Name of the basin to reconfigure.
        basin: S2BasinUri,

        /// Storage class for the default stream config.
        #[arg(long)]
        storage_class: Option<StorageClass>,

        /// Retention policy for the default stream config (e.g., "1d", "1w", "1y").
        #[arg(long)]
        retention_policy: Option<RetentionPolicy>,

        /// Timestamping mode for the default stream config.
        #[arg(long)]
        timestamping_mode: Option<TimestampingMode>,

        /// Uncapped timestamps for the default stream config.
        #[arg(long)]
        timestamping_uncapped: Option<bool>,

        /// Create stream on append with basin defaults if it doesn't exist.
        #[arg(long)]
        create_stream_on_append: Option<bool>,

        /// Create stream on read with basin defaults if it doesn't exist.
        #[arg(long)]
        create_stream_on_read: Option<bool>,

        /// Delete-on-empty configuration for the default stream config.
        #[arg(long)]
        delete_on_empty_min_age: Option<DeleteOnEmpty>,
    },

    /// Issue an access token.
    IssueAccessToken {
        /// Access token ID.
        #[arg(long)]
        id: AccessTokenId,

        /// Expiration time in seconds since Unix epoch.
        #[arg(long)]
        expires_at: Option<u32>,

        /// Namespace streams based on the configured stream-level scope, which must be a prefix.
        /// Stream name arguments will be automatically prefixed, and the prefix will be stripped
        /// when listing streams.
        #[arg(long, default_value_t = false)]
        auto_prefix_streams: bool,

        /// Basin names allowed.
        /// Matches exact value if it starts with `=`, otherwise treats it as a prefix.
        #[arg(long)]
        basins: Option<ResourceSet<8, 48>>,

        /// Stream names allowed.
        /// Matches exact value if it starts with `=`, otherwise treats it as a prefix.
        #[arg(long)]
        streams: Option<ResourceSet<1, 512>>,

        /// Token IDs allowed.
        /// Matches exact value if it starts with `=`, otherwise treats it as a prefix.
        #[arg(long)]
        access_tokens: Option<ResourceSet<1, 96>>,

        /// Access permissions at the group level.
        /// The format is: "account=rw,basin=r,stream=w"
        /// where 'r' indicates read permission and 'w' indicates write permission.
        #[arg(long)]
        op_groups: Option<PermittedOperationGroups>,

        /// Operations allowed for the token.
        /// A union of allowed operations and groups is used as an effective set of allowed operations.
        #[arg(long, value_delimiter = ',')]
        ops: Vec<Operation>,
    },

    /// Revoke an access token.
    RevokeAccessToken {
        /// ID of the access token to revoke.
        #[arg(long)]
        id: AccessTokenId,
    },

    /// List access tokens.
    ListAccessTokens {
        /// List access tokens that begin with this prefix.
        #[arg(short = 'p', long, default_value = "")]
        prefix: Option<String>,

        /// Only return access tokens that lexicographically start after this token ID.
        #[arg(short = 's', long, default_value = "")]
        start_after: Option<String>,

        /// Number of results, upto a maximum of 1000.
        #[arg(short = 'n', long)]
        limit: Option<usize>,

        /// Disable automatic following of pagination responses, which can make multiple RPC calls.
        #[arg(long, default_value_t = false)]
        no_auto_paginate: bool,
    },

    /// List streams.
    ListStreams {
        /// Name of the basin to manage or S2 URI with basin and optionally prefix.
        ///
        /// S2 URI is of the format: s2://{basin}/{prefix}
        #[arg(value_name = "BASIN|S2_URI")]
        uri: S2BasinAndMaybeStreamUri,

        /// Filter to stream names that begin with this prefix.
        #[arg(short = 'p', long)]
        prefix: Option<String>,

        /// Filter to stream names that lexicographically start after this name.
        #[arg(short = 's', long)]
        start_after: Option<String>,

        /// Number of results, upto a maximum of 1000.
        #[arg(short = 'n', long)]
        limit: Option<usize>,

        /// Disable automatic following of pagination responses, which can make multiple RPC calls.
        #[arg(long, default_value_t = false)]
        no_auto_paginate: bool,
    },

    /// Create a stream.
    CreateStream {
        #[command(flatten)]
        uri: S2BasinAndStreamUriArgs,

        /// Configuration to apply.
        #[command(flatten)]
        config: StreamConfig,
    },

    /// Delete a stream.
    DeleteStream {
        #[command(flatten)]
        uri: S2BasinAndStreamUriArgs,
    },

    /// Get stream config.
    GetStreamConfig {
        #[command(flatten)]
        uri: S2BasinAndStreamUriArgs,
    },

    /// Reconfigure a stream.
    ReconfigureStream {
        #[command(flatten)]
        uri: S2BasinAndStreamUriArgs,

        /// Storage class for the stream.
        #[arg(long)]
        storage_class: Option<StorageClass>,

        /// Retention policy for the stream (e.g., "1d", "1w", "1y").
        #[arg(long)]
        retention_policy: Option<RetentionPolicy>,

        /// Timestamping mode for the stream.
        #[arg(long)]
        timestamping_mode: Option<TimestampingMode>,

        /// Uncapped timestamps for the stream.
        #[arg(long)]
        timestamping_uncapped: Option<bool>,

        /// Delete-on-empty configuration for the stream.
        #[arg(long)]
        delete_on_empty_min_age: Option<DeleteOnEmpty>,
    },

    /// Get the next sequence number that will be assigned by a stream.
    CheckTail {
        #[command(flatten)]
        uri: S2BasinAndStreamUriArgs,
    },

    /// Set the trim point for the stream.
    ///
    /// Trimming is eventually consistent, and trimmed records may be visible
    /// for a brief period.
    Trim {
        #[command(flatten)]
        uri: S2BasinAndStreamUriArgs,

        /// Earliest sequence number that should be retained.
        /// This sequence number is only allowed to advance,
        /// and any regression will be ignored.
        trim_point: u64,

        /// Enforce fencing token.
        #[arg(short = 'f', long)]
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
        uri: S2BasinAndStreamUriArgs,

        /// New fencing token.
        /// It may be upto 36 characters, and can be empty.
        #[arg()]
        new_fencing_token: FencingToken,

        /// Enforce existing fencing token.
        #[arg(short = 'f', long)]
        fencing_token: Option<FencingToken>,

        /// Enforce that the sequence number issued to this command matches.
        #[arg(short = 'm', long)]
        match_seq_num: Option<u64>,
    },

    /// Append records to a stream.
    Append {
        #[command(flatten)]
        uri: S2BasinAndStreamUriArgs,

        /// Enforce fencing token.
        #[arg(short = 'f', long)]
        fencing_token: Option<FencingToken>,

        /// Enforce that the sequence number issued to the first record matches.
        #[arg(short = 'm', long)]
        match_seq_num: Option<u64>,

        /// Input format.
        #[arg(long, value_enum, default_value_t)]
        format: Format,

        /// Input newline delimited records to append from a file or stdin.
        /// All records are treated as plain text.
        /// Use "-" to read from stdin.
        #[arg(short = 'i', long, value_parser = parse_records_input_source, default_value = "-")]
        input: RecordsIn,

        /// How long to wait while accumulating records before emitting a batch.
        #[arg(long, default_value = "5ms")]
        linger: humantime::Duration,
    },

    /// Read records from a stream.
    ///
    /// If a limit if specified, reading will stop when the limit is reached or there are no more records on the stream.
    /// If a limit is not specified, the reader will keep tailing and wait for new records.
    Read {
        #[command(flatten)]
        uri: S2BasinAndStreamUriArgs,

        /// Starting sequence number (inclusive).
        #[arg(short = 's', long, group = "start")]
        seq_num: Option<u64>,

        /// Starting timestamp (inclusive).
        #[arg(long, group = "start")]
        timestamp: Option<u64>,

        /// Starting timestamp as a human-friendly delta from current time e.g. "1h",
        /// which will be converted to milliseconds since Unix epoch.
        #[arg(long, group = "start")]
        ago: Option<humantime::Duration>,

        /// Start from N records before the tail of the stream.
        #[arg(long, group = "start")]
        tail_offset: Option<u64>,

        /// Output format.
        #[arg(long, value_enum, default_value_t)]
        format: Format,

        /// Limit the number of records returned.
        #[arg(short = 'n', long)]
        count: Option<u64>,

        /// Limit the number of bytes returned.
        #[arg(short = 'b', long)]
        bytes: Option<u64>,

        /// Clamp the start position at the tail position.
        #[arg(long, default_value_t = false)]
        clamp: bool,

        /// Output records to a file or stdout.
        /// Use "-" to write to stdout.
        #[arg(short = 'o', long, value_parser = parse_records_output_source, default_value = "-")]
        output: RecordsOut,

        /// Exclusive end-timestamp.
        /// If provided, results will be limited such that all records returned
        /// will have a timestamp < the one provided via `until`.
        #[arg(long)]
        until: Option<u64>,
    },

    /// Tail a stream, showing the last N records.
    Tail {
        #[command(flatten)]
        uri: S2BasinAndStreamUriArgs,

        /// Output the last N records instead of the default (10).
        #[arg(short = 'n', long = "lines", default_value_t = 10)]
        lines: u64,

        /// Follow the stream, waiting for new records to be appended.
        #[arg(short = 'f', long, default_value_t = false)]
        follow: bool,

        /// Output format.
        #[arg(long, value_enum, default_value_t)]
        format: Format,

        /// Output records to a file or stdout.
        /// Use "-" to write to stdout.
        #[arg(short = 'o', long, value_parser = parse_records_output_source, default_value = "-")]
        output: RecordsOut,
    },

    /// Ping the stream to get append acknowledgement and end-to-end latencies.
    Ping {
        #[command(flatten)]
        uri: S2BasinAndStreamUriArgs,

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
    /// Alternatively, use the S2_ACCESS_TOKEN environment variable.
    Set {
        #[arg(short = 'a', long, group = "auth")]
        access_token: Option<String>,
        #[arg(long = "auth-token", group = "auth", hide = true)]
        auth_token: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, Default, ValueEnum)]
pub enum Format {
    /// Plaintext record body as UTF-8.
    /// If the body is not valid UTF-8, this will be a lossy decoding.
    /// Headers cannot be represented, so command records are sent to stderr when reading.
    #[default]
    #[clap(name = "")]
    BodyRaw,
    /// JSON format with UTF-8 headers and body.
    /// If the data is not valid UTF-8, this will be a lossy decoding.
    #[clap(name = "raw", alias = "json")]
    JsonRaw,
    /// JSON format with headers and body encoded as Base64.
    #[clap(name = "base64", alias = "json-binsafe")]
    JsonBase64,
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

fn client_config(access_token: String) -> Result<ClientConfig, S2CliError> {
    let endpoints = S2Endpoints::from_env().map_err(S2CliError::EndpointsFromEnv)?;
    let disable_compression = std::env::var("S2_DISABLE_COMPRESSION")
        .ok()
        .and_then(|val| val.to_lowercase().parse::<bool>().ok())
        .unwrap_or(false);
    let disable_tls = std::env::var("S2_DISABLE_TLS")
        .ok()
        .and_then(|val| val.to_lowercase().parse::<bool>().ok())
        .unwrap_or(false);
    let client_config = ClientConfig::new(access_token.to_string())
        .with_user_agent("s2-cli".parse().expect("valid user agent"))
        .with_endpoints(endpoints)
        .with_compression(!disable_compression)
        .with_uri_scheme(if disable_tls {
            http::uri::Scheme::HTTP
        } else {
            http::uri::Scheme::HTTPS
        })
        .with_request_timeout(Duration::from_secs(30));
    trace!(?client_config);
    Ok(client_config)
}

fn build_basin_reconfig(
    storage_class: Option<&StorageClass>,
    retention_policy: Option<&RetentionPolicy>,
    timestamping_mode: Option<&TimestampingMode>,
    timestamping_uncapped: Option<&bool>,
    create_stream_on_append: Option<&bool>,
    create_stream_on_read: Option<&bool>,
    delete_on_empty: Option<&DeleteOnEmpty>,
) -> (Option<StreamConfig>, Vec<String>) {
    let mut mask = Vec::new();
    let has_stream_args = storage_class.is_some()
        || retention_policy.is_some()
        || timestamping_mode.is_some()
        || timestamping_uncapped.is_some()
        || delete_on_empty.is_some();

    let default_stream_config = if has_stream_args {
        let timestamping = if timestamping_mode.is_some() || timestamping_uncapped.is_some() {
            Some(TimestampingConfig {
                timestamping_mode: timestamping_mode.cloned(),
                timestamping_uncapped: timestamping_uncapped.copied(),
            })
        } else {
            None
        };

        let delete_on_empty_config = delete_on_empty.map(|doe| DeleteOnEmptyConfig {
            min_age: Some(doe.clone()),
        });

        Some(StreamConfig {
            storage_class: storage_class.cloned(),
            retention_policy: retention_policy.cloned(),
            timestamping,
            delete_on_empty_config,
        })
    } else {
        None
    };

    if storage_class.is_some() {
        mask.push("default_stream_config.storage_class".to_owned());
    }
    if retention_policy.is_some() {
        mask.push("default_stream_config.retention_policy".to_owned());
    }
    if timestamping_mode.is_some() {
        mask.push("default_stream_config.timestamping.mode".to_owned());
    }
    if timestamping_uncapped.is_some() {
        mask.push("default_stream_config.timestamping.uncapped".to_owned());
    }
    if delete_on_empty.is_some() {
        mask.push("default_stream_config.delete_on_empty".to_owned());
    }
    if create_stream_on_append.is_some() {
        mask.push("create_stream_on_append".to_owned());
    }
    if create_stream_on_read.is_some() {
        mask.push("create_stream_on_read".to_owned());
    }

    (default_stream_config, mask)
}

fn build_stream_reconfig(
    storage_class: Option<&StorageClass>,
    retention_policy: Option<&RetentionPolicy>,
    timestamping_mode: Option<&TimestampingMode>,
    timestamping_uncapped: Option<&bool>,
    delete_on_empty: Option<&DeleteOnEmpty>,
) -> (StreamConfig, Vec<String>) {
    let mut mask = Vec::new();

    let timestamping = if timestamping_mode.is_some() || timestamping_uncapped.is_some() {
        Some(TimestampingConfig {
            timestamping_mode: timestamping_mode.cloned(),
            timestamping_uncapped: timestamping_uncapped.copied(),
        })
    } else {
        None
    };

    let delete_on_empty_config = delete_on_empty.map(|doe| DeleteOnEmptyConfig {
        min_age: Some(doe.clone()),
    });

    let stream_config = StreamConfig {
        storage_class: storage_class.cloned(),
        retention_policy: retention_policy.cloned(),
        timestamping,
        delete_on_empty_config,
    };

    if storage_class.is_some() {
        mask.push("storage_class".to_string());
    }
    if retention_policy.is_some() {
        mask.push("retention_policy".to_string());
    }
    if timestamping_mode.is_some() {
        mask.push("timestamping.mode".to_string());
    }
    if timestamping_uncapped.is_some() {
        mask.push("timestamping.uncapped".to_string());
    }
    if delete_on_empty.is_some() {
        mask.push("delete_on_empty".to_string());
    }

    (stream_config, mask)
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

    async fn list_basins(
        client_config: ClientConfig,
        prefix: Option<String>,
        start_after: Option<String>,
        limit: Option<usize>,
        no_auto_paginate: bool,
    ) -> Result<(), S2CliError> {
        let account_service = AccountService::new(Client::new(client_config));
        let basin_response_stream = account_service.list_basins(
            prefix.unwrap_or_default(),
            start_after.unwrap_or_default(),
            limit,
            no_auto_paginate,
        );

        tokio::pin!(basin_response_stream);

        while let Some(response) = basin_response_stream.next().await {
            for basin_info in response?.basins {
                let BasinInfo { name, state, .. } = basin_info;
                let state = if let Some(state) = state {
                    match state {
                        s2::types::BasinState::Active => state.to_string().green(),
                        s2::types::BasinState::Creating => state.to_string().yellow(),
                        s2::types::BasinState::Deleting => state.to_string().red(),
                    }
                } else {
                    "unknown".to_owned().blue()
                };
                println!("{name} {state}");
            }
        }

        Ok(())
    }

    async fn list_streams(
        client_config: ClientConfig,
        uri: S2BasinAndMaybeStreamUri,
        prefix: Option<String>,
        start_after: Option<String>,
        limit: Option<usize>,
        no_auto_paginate: bool,
    ) -> Result<(), S2CliError> {
        let S2BasinAndMaybeStreamUri {
            basin,
            stream: maybe_prefix,
        } = uri;
        let prefix = match (maybe_prefix, prefix) {
            (Some(_), Some(_)) => {
                return Err(S2CliError::InvalidArgs(miette::miette!(
                    help = "Make sure to provide the prefix once either using '--prefix' opt or in URI like 's2://basin-name/prefix'",
                    "Multiple prefixes provided"
                )));
            }
            (Some(s), None) | (None, Some(s)) => Some(s),
            (None, None) => None,
        };

        let basin_service = BasinService::new(BasinClient::new(client_config, basin.clone()));
        let streams = basin_service.list_streams(
            prefix.unwrap_or_default(),
            start_after.unwrap_or_default(),
            limit,
            no_auto_paginate,
        );

        tokio::pin!(streams);

        while let Some(stream) = streams.next().await {
            for StreamInfo {
                name,
                created_at,
                deleted_at,
            } in stream?.streams
            {
                let date_time = |time: u32| {
                    humantime::format_rfc3339_seconds(UNIX_EPOCH + Duration::from_secs(time as u64))
                };

                println!(
                    "s2://{}/{} {} {}",
                    basin,
                    name,
                    date_time(created_at).to_string().green(),
                    deleted_at
                        .map(|d| date_time(d).to_string().red())
                        .unwrap_or_default()
                );
            }
        }

        Ok(())
    }

    async fn list_tokens(
        client_config: ClientConfig,
        prefix: Option<String>,
        start_after: Option<String>,
        limit: Option<usize>,
        no_auto_paginate: bool,
    ) -> Result<(), S2CliError> {
        let account_service = AccountService::new(Client::new(client_config));
        let tokens = account_service.list_access_tokens(
            prefix.unwrap_or_default(),
            start_after.unwrap_or_default(),
            limit,
            no_auto_paginate,
        );

        tokio::pin!(tokens);

        while let Some(token) = tokens.next().await {
            for token_info in token?.access_tokens {
                let exp_date = token_info
                    .expires_at
                    .map(|exp| {
                        humantime::format_rfc3339_seconds(
                            UNIX_EPOCH + Duration::from_secs(exp as u64),
                        )
                        .to_string()
                        .green()
                    })
                    .expect("expires_at");

                println!(
                    "{} {}",
                    token_info.id.parse::<String>().expect("id"),
                    exp_date
                );
            }
        }

        Ok(())
    }

    match commands.command {
        Commands::Config { action } => match action {
            ConfigActions::Set {
                access_token,
                auth_token,
            } => {
                let access_token = access_token.or(auth_token).ok_or_else(|| {
                    S2CliError::InvalidArgs(miette::miette!(
                        "Access token not provided. Use --access-token to set it."
                    ))
                })?;
                create_config(&config_path, access_token)?;
                eprintln!("{}", "✓ Token set".green().bold());
                eprintln!(
                    "  Configuration saved to: {}",
                    config_path.display().to_string().cyan()
                );
            }
        },
        Commands::Ls {
            uri,
            prefix,
            start_after,
            limit,
            no_auto_paginate,
        } => {
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.access_token)?;
            if let Some(uri) = uri {
                list_streams(
                    client_config,
                    uri,
                    prefix,
                    start_after,
                    limit,
                    no_auto_paginate,
                )
                .await?;
            } else {
                list_basins(client_config, prefix, start_after, limit, no_auto_paginate).await?;
            }
        }
        Commands::ListBasins {
            prefix,
            start_after,
            limit,
            no_auto_paginate,
        } => {
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.access_token)?;
            list_basins(client_config, prefix, start_after, limit, no_auto_paginate).await?;
        }
        Commands::CreateBasin { basin, config } => {
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.access_token)?;
            let account_service = AccountService::new(Client::new(client_config));
            let (storage_class, retention_policy) = match &config.default_stream_config {
                Some(config) => {
                    let storage_class = config.storage_class.clone();
                    let retention_policy = config.retention_policy.clone();
                    (storage_class, retention_policy)
                }
                None => (None, None),
            };
            let BasinInfo { state, .. } = account_service
                .create_basin(
                    basin.into(),
                    storage_class,
                    retention_policy,
                    config.create_stream_on_append.unwrap_or_default(),
                    config.create_stream_on_read.unwrap_or_default(),
                )
                .await?;

            let message = match state {
                Some(s2::types::BasinState::Creating) => {
                    "✓ Basin creation requested".yellow().bold()
                }
                Some(s2::types::BasinState::Active) => "✓ Basin created".green().bold(),
                s => format!("Unexpected state: {s:?}").red().bold(),
            };
            eprintln!("{message}");
        }
        Commands::DeleteBasin { basin } => {
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.access_token)?;
            let account_service = AccountService::new(Client::new(client_config));
            account_service.delete_basin(basin.into()).await?;
            eprintln!("{}", "✓ Basin deletion requested".green().bold());
        }
        Commands::GetBasinConfig { basin } => {
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.access_token)?;
            let account_service = AccountService::new(Client::new(client_config));
            let basin_config = account_service.get_basin_config(basin.into()).await?;
            let basin_config: BasinConfig = basin_config.into();
            println!("{}", json_to_table(&serde_json::to_value(&basin_config)?));
        }
        Commands::ReconfigureBasin {
            basin,
            storage_class,
            retention_policy,
            timestamping_mode,
            timestamping_uncapped,
            create_stream_on_append,
            create_stream_on_read,
            delete_on_empty_min_age,
        } => {
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.access_token)?;
            let account_service = AccountService::new(Client::new(client_config));

            let (default_stream_config, mask) = build_basin_reconfig(
                storage_class.as_ref(),
                retention_policy.as_ref(),
                timestamping_mode.as_ref(),
                timestamping_uncapped.as_ref(),
                create_stream_on_append.as_ref(),
                create_stream_on_read.as_ref(),
                delete_on_empty_min_age.as_ref(),
            );

            let basin_config = BasinConfig {
                default_stream_config,
                create_stream_on_append,
                create_stream_on_read,
            };

            let config: BasinConfig = account_service
                .reconfigure_basin(basin.into(), basin_config.into(), mask)
                .await?
                .into();
            eprintln!("{}", "✓ Basin reconfigured".green().bold());
            println!("{}", json_to_table(&serde_json::to_value(&config)?));
        }
        Commands::ListStreams {
            uri,
            prefix,
            start_after,
            limit,
            no_auto_paginate,
        } => {
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.access_token)?;
            list_streams(
                client_config,
                uri,
                prefix,
                start_after,
                limit,
                no_auto_paginate,
            )
            .await?;
        }
        Commands::CreateStream { uri, config } => {
            let S2BasinAndStreamUri { basin, stream } = uri.uri;
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.access_token)?;
            let basin_client = BasinClient::new(client_config, basin);
            BasinService::new(basin_client)
                .create_stream(stream, config.into())
                .await?;
            eprintln!("{}", "✓ Stream created".green().bold());
        }
        Commands::DeleteStream { uri } => {
            let S2BasinAndStreamUri { basin, stream } = uri.uri;
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.access_token)?;
            let basin_client = BasinClient::new(client_config, basin);
            BasinService::new(basin_client)
                .delete_stream(stream)
                .await?;
            eprintln!("{}", "✓ Stream deletion requested".green().bold());
        }
        Commands::GetStreamConfig { uri } => {
            let S2BasinAndStreamUri { basin, stream } = uri.uri;
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.access_token)?;
            let basin_client = BasinClient::new(client_config, basin);
            let config: StreamConfig = BasinService::new(basin_client)
                .get_stream_config(stream)
                .await?
                .into();
            println!("{}", json_to_table(&serde_json::to_value(&config)?));
        }
        Commands::ReconfigureStream {
            uri,
            storage_class,
            retention_policy,
            timestamping_mode,
            timestamping_uncapped,
            delete_on_empty_min_age,
        } => {
            let S2BasinAndStreamUri { basin, stream } = uri.uri;
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.access_token)?;
            let basin_client = BasinClient::new(client_config, basin);

            let (stream_config, mask) = build_stream_reconfig(
                storage_class.as_ref(),
                retention_policy.as_ref(),
                timestamping_mode.as_ref(),
                timestamping_uncapped.as_ref(),
                delete_on_empty_min_age.as_ref(),
            );

            let config: StreamConfig = BasinService::new(basin_client)
                .reconfigure_stream(stream, stream_config.into(), mask)
                .await?
                .into();

            eprintln!("{}", "✓ Stream reconfigured".green().bold());
            println!("{}", json_to_table(&serde_json::to_value(&config)?));
        }
        Commands::CheckTail { uri } => {
            let S2BasinAndStreamUri { basin, stream } = uri.uri;
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.access_token)?;
            let stream_client = StreamClient::new(client_config, basin, stream);
            let tail = StreamService::new(stream_client).check_tail().await?;
            println!("{}\t{}", tail.seq_num, tail.timestamp);
        }
        Commands::Trim {
            uri,
            trim_point,
            fencing_token,
            match_seq_num,
        } => {
            let S2BasinAndStreamUri { basin, stream } = uri.uri;
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.access_token)?;
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
                format!(
                    "✓ Trim request for trim point {trim_point} appended at: {:?}",
                    out.start
                )
                .green()
                .bold()
            );
        }
        Commands::Fence {
            uri,
            new_fencing_token,
            fencing_token,
            match_seq_num,
        } => {
            let S2BasinAndStreamUri { basin, stream } = uri.uri;
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.access_token)?;
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
                format!("✓ Fencing token appended at seq_num: {:?}", out.start)
                    .green()
                    .bold()
            );
        }
        Commands::Append {
            uri,
            input,
            fencing_token,
            match_seq_num,
            format,
            linger,
        } => {
            let S2BasinAndStreamUri { basin, stream } = uri.uri;
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.access_token)?;
            let stream_client = StreamClient::new(client_config, basin, stream);

            let records_in = input
                .into_reader()
                .await
                .map_err(|e| S2CliError::RecordReaderInit(e.to_string()))?;

            let append_input_stream: Box<dyn Stream<Item = AppendRecord> + Send + Unpin> =
                match format {
                    Format::BodyRaw => {
                        Box::new(RecordStream::<_, RawBodyFormatter>::new(records_in))
                    }
                    Format::JsonRaw => {
                        Box::new(RecordStream::<_, RawJsonFormatter>::new(records_in))
                    }
                    Format::JsonBase64 => {
                        Box::new(RecordStream::<_, Base64JsonFormatter>::new(records_in))
                    }
                };

            let mut append_output_stream = StreamService::new(stream_client)
                .append_session(
                    append_input_stream,
                    AppendRecordsBatchingOpts::new()
                        .with_fencing_token(fencing_token)
                        .with_match_seq_num(match_seq_num)
                        .with_linger(linger),
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
                                                "✓ [APPENDED] {}..{} // tail: {} @ {}",
                                                append_result.start.seq_num,
                                                append_result.end.seq_num,
                                                append_result.tail.seq_num,
                                                append_result.tail.timestamp
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
        Commands::Tail {
            uri,
            lines,
            follow,
            output,
            format,
        } => {
            let read_start = ReadStart::TailOffset(lines);
            let read_limit = if follow {
                ReadLimit::default()
            } else {
                ReadLimit::new().with_count(lines)
            };
            let S2BasinAndStreamUri { basin, stream } = uri.uri;
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.access_token)?;
            let stream_client = StreamClient::new(client_config, basin, stream);
            let mut read_outputs = StreamService::new(stream_client)
                .read_session(read_start, read_limit, None, false)
                .await?;
            let mut writer = output.into_writer().await.unwrap();
            while handle_read_outputs(&mut read_outputs, &mut writer, format).await? {}
        }
        Commands::Read {
            uri,
            seq_num,
            timestamp,
            tail_offset,
            ago,
            output,
            count,
            bytes,
            clamp,
            format,
            until,
        } => {
            let S2BasinAndStreamUri { basin, stream } = uri.uri;
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.access_token)?;
            let stream_client = StreamClient::new(client_config, basin, stream);
            let read_start = match (seq_num, timestamp, tail_offset, ago) {
                (Some(seq_num), None, None, None) => ReadStart::SeqNum(seq_num),
                (None, Some(timestamp), None, None) => ReadStart::Timestamp(timestamp),
                (None, None, Some(offset), None) => ReadStart::TailOffset(offset),
                (None, None, None, Some(ago)) => {
                    let timestamp = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_millis()
                        .saturating_sub(ago.as_millis()) as u64;
                    ReadStart::Timestamp(timestamp)
                }
                (None, None, None, None) => s2::types::ReadStart::TailOffset(0),
                _ => unreachable!("clap ensures only one start option is provided"),
            };
            let read_limit = ReadLimit { count, bytes };
            let mut read_outputs = StreamService::new(stream_client)
                .read_session(read_start, read_limit, until.map(|end| ..end), clamp)
                .await?;
            let mut writer = output.into_writer().await.unwrap();
            while handle_read_outputs(&mut read_outputs, &mut writer, format).await? {}
        }
        Commands::Ping {
            uri,
            interval,
            batch_bytes,
            num_batches,
        } => {
            let S2BasinAndStreamUri { basin, stream } = uri.uri;
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.access_token)?;
            let stream_client = StreamService::new(StreamClient::new(client_config, basin, stream));

            let interval = Duration::from(interval).max(Duration::from_millis(100));
            let batch_bytes = batch_bytes.min(128 * 1024);

            let prepare_loader = ProgressBar::new_spinner()
                .with_prefix("Preparing...")
                .with_style(
                    ProgressStyle::default_spinner()
                        .template("{spinner} {prefix}")
                        .expect("valid template"),
                );
            prepare_loader.enable_steady_tick(Duration::from_millis(50));

            let mut pinger = Pinger::init(&stream_client).await?;

            prepare_loader.finish_and_clear();

            let mut pings = Vec::new();

            let stat_bars = MultiProgress::new();

            let bytes_bar = ProgressBar::no_length().with_prefix("bytes").with_style(
                ProgressStyle::default_bar()
                    .template("{pos:.bold} {prefix:.bold}")
                    .expect("valid template"),
            );

            let mut max_ack = 500;
            let ack_bar = ProgressBar::new(max_ack).with_prefix("ack").with_style(
                ProgressStyle::default_bar()
                    .template("{prefix:.bold} [{bar:40.blue/blue}] {pos:>4}/{len:<4} ms")
                    .expect("valid template"),
            );

            let mut max_e2e = 500;
            let e2e_bar = ProgressBar::new(max_e2e).with_prefix("e2e").with_style(
                ProgressStyle::default_bar()
                    .template("{prefix:.bold} [{bar:40.red/red}] {pos:>4}/{len:<4} ms")
                    .expect("valid template"),
            );

            // HACK: This bar basically has no purpose. It's just to clear all
            // other bars since the very first bar in the set doesn't clear when
            // `^C` signal is received.
            let empty_line_bar = {
                let bar = stat_bars.add(
                    ProgressBar::no_length().with_style(
                        ProgressStyle::default_bar()
                            .template("\n")
                            .expect("valid template"),
                    ),
                );
                // Force render the bar.
                bar.inc(1);
                bar
            };
            let bytes_bar = stat_bars.add(bytes_bar);
            let ack_bar = stat_bars.add(ack_bar);
            let e2e_bar = stat_bars.add(e2e_bar);

            async fn ping_next(
                pinger: &mut Pinger,
                pings: &mut Vec<PingResult>,
                interval: Duration,
                batch_bytes: u64,
                bytes_bar: &ProgressBar,
                ack_meter: (&ProgressBar, /* max_ack */ &mut u64),
                e2e_meter: (&ProgressBar, /* max_e2e */ &mut u64),
            ) -> Result<(), S2CliError> {
                let jitter_op = if rand::random() {
                    u64::saturating_add
                } else {
                    u64::saturating_sub
                };

                let max_jitter = batch_bytes / 4;

                let record_bytes = jitter_op(batch_bytes, rand::rng().random_range(0..=max_jitter));

                let Some(res) = pinger.ping(record_bytes).await? else {
                    return Ok(());
                };

                bytes_bar.set_position(record_bytes);

                let (ack_bar, max_ack) = ack_meter;

                let ack = res.ack.as_millis() as u64;
                *max_ack = std::cmp::max(*max_ack, ack);
                ack_bar.set_length(*max_ack);
                ack_bar.set_position(ack);

                let (e2e_bar, max_e2e) = e2e_meter;

                let e2e = res.e2e.as_millis() as u64;
                *max_e2e = std::cmp::max(*max_e2e, e2e);
                e2e_bar.set_length(*max_e2e);
                e2e_bar.set_position(e2e);

                pings.push(res);

                tokio::time::sleep(interval).await;
                Ok(())
            }

            while Some(pings.len()) != num_batches {
                select! {
                    res = ping_next(
                        &mut pinger,
                        &mut pings,
                        interval,
                        batch_bytes,
                        &bytes_bar,
                        (&ack_bar, &mut max_ack),
                        (&e2e_bar, &mut max_e2e),
                    ) => res?,
                    _ = signal::ctrl_c() => break,
                }
            }

            // Close the pinger.
            std::mem::drop(pinger);

            bytes_bar.finish_and_clear();
            ack_bar.finish_and_clear();
            e2e_bar.finish_and_clear();
            empty_line_bar.finish_and_clear();

            let total_batches = pings.len();
            let (bytes, (acks, e2es)): (Vec<_>, (Vec<_>, Vec<_>)) = pings
                .into_iter()
                .map(|PingResult { bytes, ack, e2e }| (bytes, (ack, e2e)))
                .unzip();
            let total_bytes = bytes.into_iter().sum::<u64>();

            eprintln!("Round-tripped {total_bytes} bytes in {total_batches} batches");

            pub fn print_stats(stats: LatencyStats, name: &str) {
                eprintln!("{}", format!("{name} Latency Statistics ").yellow().bold());

                fn stat_duration(key: &str, val: Duration, scale: f64) {
                    let bar = "⠸".repeat((val.as_millis() as f64 * scale).round() as usize);
                    eprintln!(
                        "{:7}: {:>7} │ {}",
                        key,
                        format!("{} ms", val.as_millis()).green().bold(),
                        bar
                    );
                }

                let stats = stats.into_vec();
                let max_val = stats
                    .iter()
                    .map(|(_, val)| val)
                    .max()
                    .unwrap_or(&Duration::ZERO);

                let max_bar_len = 50;
                let scale = if max_val.as_millis() > max_bar_len {
                    max_bar_len as f64 / max_val.as_millis() as f64
                } else {
                    1.0
                };

                for (name, val) in stats {
                    stat_duration(&name, val, scale);
                }
            }

            eprintln!(/* Empty line */);
            print_stats(LatencyStats::generate(acks), "Append Acknowledgement");
            eprintln!(/* Empty line */);
            print_stats(LatencyStats::generate(e2es), "End-to-End");
        }
        Commands::IssueAccessToken {
            id,
            expires_at,
            auto_prefix_streams,
            basins,
            streams,
            access_tokens,
            op_groups,
            ops,
        } => {
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.access_token)?;
            let account_service = AccountService::new(Client::new(client_config));
            let access_token = account_service
                .issue_access_token(
                    id,
                    expires_at,
                    auto_prefix_streams,
                    basins.map(Into::into),
                    streams.map(Into::into),
                    access_tokens.map(Into::into),
                    op_groups.map(Into::into),
                    ops.into_iter().map(Into::into).collect(),
                )
                .await?;
            println!("{access_token}");
        }
        Commands::RevokeAccessToken { id } => {
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.access_token)?;
            let account_service = AccountService::new(Client::new(client_config));
            let info = account_service.revoke_access_token(id).await?;
            let info: AccessTokenInfo = info.into();
            eprintln!("{}", "✓ Access token revoked".green().bold());
            println!("{}", json_to_table(&serde_json::to_value(&info)?));
        }
        Commands::ListAccessTokens {
            prefix,
            start_after,
            limit,
            no_auto_paginate,
        } => {
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.access_token)?;
            list_tokens(client_config, prefix, start_after, limit, no_auto_paginate).await?;
        }
    }

    Ok(())
}

async fn handle_read_outputs(
    read_outputs: &mut Streaming<ReadOutput>,
    writer: &mut Box<dyn AsyncWrite + Send + Unpin>,
    format: Format,
) -> Result<bool, S2CliError> {
    select! {
        maybe_read_result = read_outputs.next() => {
            match maybe_read_result {
                Some(read_result) => {
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
                                match format {
                                    Format::BodyRaw => {
                                        if let Some(command_record) = sequenced_record.as_command_record() {
                                            let (cmd, description) = match command_record.command {
                                                Command::Fence { fencing_token } => (
                                                    "fence",
                                                    format!("FencingToken({fencing_token})")
                                                ),
                                                Command::Trim { seq_num } => (
                                                    "trim",
                                                    format!("TrimPoint({seq_num})"),
                                                ),
                                            };
                                            eprintln!("{} with {} // {} @ {}",
                                                cmd.bold(),
                                                description.green().bold(),
                                                sequenced_record.seq_num,
                                                sequenced_record.timestamp
                                            );
                                            Ok(())
                                        } else {
                                            RawBodyFormatter::write_record(
                                                &sequenced_record,
                                                writer,
                                            ).await
                                        }
                                    },
                                    Format::JsonRaw => {
                                        RawJsonFormatter::write_record(
                                            &sequenced_record,
                                            writer,
                                        ).await
                                    },
                                    Format::JsonBase64 => {
                                        Base64JsonFormatter::write_record(
                                            &sequenced_record,
                                            writer,
                                        ).await
                                    },
                                }
                                .map_err(|e| S2CliError::RecordWrite(e.to_string()))?;
                                writer
                                    .write_all(b"\n")
                                    .await
                                    .map_err(|e| S2CliError::RecordWrite(e.to_string()))?;
                            }

                            eprintln!(
                                "{}",
                                format!("⦿ {batch_len} bytes ({num_records} records in range {seq_range:?})")
                                .blue()
                                .bold()
                            );
                        }

                        Ok(ReadOutput::NextSeqNum(seq_num)) => {
                            eprintln!("{}", format!("next_seq_num: {seq_num}").blue().bold());
                            return Ok(false);
                        }

                        Err(e) => {
                            return Err(ServiceError::new(ServiceErrorContext::ReadSession, e).into());
                        }
                    }
                }
                None => return Ok(false),
            }
        },
        _ = signal::ctrl_c() => {
            eprintln!("{}", "■ [ABORTED]".red().bold());
            return Ok(false);
        }
    }
    writer
        .flush()
        .await
        .map_err(|e| S2CliError::RecordWrite(e.to_string()))?;
    Ok(true)
}
