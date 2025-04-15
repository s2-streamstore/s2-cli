use json_to_table::json_to_table;
use std::{
    fmt::Display,
    io::BufRead,
    path::PathBuf,
    pin::Pin,
    str::FromStr,
    time::{Duration, UNIX_EPOCH},
};

use account::AccountService;
use base64ct::{Base64, Encoding};
use basin::BasinService;
use clap::{Parser, Subcommand, ValueEnum, builder::styling};
use colored::*;
use config::{config_path, create_config};
use error::{S2CliError, ServiceError, ServiceErrorContext};
use formats::{JsonBinsafeFormatter, JsonFormatter, RecordWriter, TextFormatter};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use ping::{LatencyStats, PingResult, Pinger};
use rand::Rng;
use s2::{
    batching::AppendRecordsBatchingOpts,
    client::{BasinClient, Client, ClientConfig, S2Endpoints, StreamClient},
    types::{
        AccessTokenId, AppendRecord, AppendRecordBatch, BasinInfo, Command, CommandRecord,
        ConvertError, FencingToken, MeteredBytes as _, ReadOutput, StreamInfo,
    },
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
    AccessTokenInfo, BasinConfig, Operation, PermittedOperationGroups, ResourceSet,
    S2BasinAndMaybeStreamUri, S2BasinAndStreamUri, S2BasinUri, StreamConfig, parse_op_groups,
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

        /// Configuration to apply.
        #[command(flatten)]
        config: BasinConfig,
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

        /// Basin name restrictions.
        #[arg(long)]
        basins: Option<ResourceSet<8, 48>>,

        /// Stream name restrictions.
        #[arg(long)]
        streams: Option<ResourceSet<1, 512>>,

        /// Token ID restrictions.
        #[arg(long)]
        tokens: Option<ResourceSet<1, 50>>,

        /// Access permissions at the group level.
        /// Format: "account=rw,basin=r,stream=w"
        /// where 'r' indicates read permission and 'w' indicates write permission.        
        #[arg(long, value_parser = parse_op_groups, required_unless_present = "ops")]
        op_groups: Option<PermittedOperationGroups>,

        /// Operations allowed for the token.
        /// A union of allowed operations and groups is used as an effective set of allowed operations.
        #[arg(long, value_delimiter = ',', required_unless_present = "op_groups")]
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
        config: Option<StreamConfig>,
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

        /// Configuration to apply.
        #[command(flatten)]
        config: StreamConfig,
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

        /// Enforce fencing token specified in base64.
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
        uri: S2BasinAndStreamUriArgs,

        /// New fencing token specified in base64.
        /// It may be upto 16 bytes, and can be empty.
        #[arg(value_parser = parse_fencing_token)]
        new_fencing_token: FencingToken,

        /// Enforce existing fencing token, specified in base64.
        #[arg(short = 'f', long, value_parser = parse_fencing_token)]
        fencing_token: Option<FencingToken>,

        /// Enforce that the sequence number issued to this command matches.
        #[arg(short = 'm', long)]
        match_seq_num: Option<u64>,
    },

    /// Append records to a stream.
    Append {
        #[command(flatten)]
        uri: S2BasinAndStreamUriArgs,

        /// Enforce fencing token specified in base64.
        #[arg(short = 'f', long, value_parser = parse_fencing_token)]
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
    },

    /// Read records from a stream.
    ///
    /// If a limit if specified, reading will stop when the limit is reached or there are no more records on the stream.
    /// If a limit is not specified, the reader will keep tailing and wait for new records.
    Read {
        #[command(flatten)]
        uri: S2BasinAndStreamUriArgs,

        /// Starting sequence number (inclusive).
        #[arg(short = 's', long, default_value_t = 0)]
        start_seq_num: u64,

        /// Output format.
        #[arg(long, value_enum, default_value_t)]
        format: Format,

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
        #[arg(short = 'a', long)]
        access_token: String,
    },
}

#[derive(Debug, Clone, Copy, Default, ValueEnum)]
pub enum Format {
    /// Newline delimited records with UTF-8 bodies.
    #[default]
    Text,
    /// Newline delimited records in JSON format with UTF-8 headers and body.
    Json,
    /// Newline delimited records in JSON format with base64 encoded headers
    /// and body.
    JsonBinsafe,
}

impl Format {
    const TEXT: &str = "text";
    const JSON: &str = "json";
    const JSON_BINSAFE: &str = "json-binsafe";

    fn as_str(&self) -> &str {
        match self {
            Self::Text => Self::TEXT,
            Self::Json => Self::JSON,
            Self::JsonBinsafe => Self::JSON_BINSAFE,
        }
    }
}

impl Display for Format {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Format {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case(Self::TEXT) {
            Ok(Self::Text)
        } else if s.eq_ignore_ascii_case(Self::JSON) {
            Ok(Self::Json)
        } else if s.eq_ignore_ascii_case(Self::JSON_BINSAFE) {
            Ok(Self::JsonBinsafe)
        } else {
            Err("Unsupported format".to_owned())
        }
    }
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
    Base64::decode_vec(s)
        .map_err(|_| "invalid base64")?
        .try_into()
}

fn client_config(access_token: String) -> Result<ClientConfig, S2CliError> {
    let endpoints = S2Endpoints::from_env().map_err(S2CliError::EndpointsFromEnv)?;
    let client_config = ClientConfig::new(access_token.to_string())
        .with_user_agent("s2-cli".parse().expect("valid user agent"))
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

                let state = match state {
                    s2::types::BasinState::Active => state.to_string().green(),
                    s2::types::BasinState::Deleting => state.to_string().red(),
                    _ => state.to_string().yellow(),
                };
                println!("{} {}", name, state);
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
            for token_info in token?.tokens {
                let exp_date = token_info
                    .expires_at
                    .map(|exp| {
                        humantime::format_rfc3339_seconds(
                            UNIX_EPOCH + Duration::from_secs(exp as u64),
                        )
                        .to_string()
                        .red()
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
            ConfigActions::Set { access_token } => {
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
                s2::types::BasinState::Creating => "✓ Basin creation requested".yellow().bold(),
                _ => "✓ Basin created".green().bold(),
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
        Commands::ReconfigureBasin { basin, config } => {
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.access_token)?;
            let account_service = AccountService::new(Client::new(client_config));
            let mut mask = Vec::new();
            if let Some(config) = &config.default_stream_config {
                if config.storage_class.is_some() {
                    mask.push("default_stream_config.storage_class".to_owned());
                }
                if config.retention_policy.is_some() {
                    mask.push("default_stream_config.retention_policy".to_owned());
                }
            }
            if config.create_stream_on_append.is_some() {
                mask.push("create_stream_on_append".to_owned());
            }
            let config: BasinConfig = account_service
                .reconfigure_basin(basin.into(), config.into(), mask)
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
                .create_stream(stream, config.map(Into::into))
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
        Commands::ReconfigureStream { uri, config } => {
            let S2BasinAndStreamUri { basin, stream } = uri.uri;
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.access_token)?;
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
            println!("{}", json_to_table(&serde_json::to_value(&config)?));
        }
        Commands::CheckTail { uri } => {
            let S2BasinAndStreamUri { basin, stream } = uri.uri;
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.access_token)?;
            let stream_client = StreamClient::new(client_config, basin, stream);
            let next_seq_num = StreamService::new(stream_client).check_tail().await?;
            println!("{}", next_seq_num);
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
                    "✓ Trim request for trim point {} appended at seq_num: {}",
                    trim_point, out.start_seq_num,
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
                format!("✓ Fencing token appended at seq_num: {}", out.start_seq_num)
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
                    Format::Text => Box::new(RecordStream::<_, TextFormatter>::new(records_in)),
                    Format::Json => Box::new(RecordStream::<_, JsonFormatter>::new(records_in)),
                    Format::JsonBinsafe => {
                        Box::new(RecordStream::<_, JsonBinsafeFormatter>::new(records_in))
                    }
                };

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
            uri,
            start_seq_num,
            output,
            limit_count,
            limit_bytes,
            format,
        } => {
            let S2BasinAndStreamUri { basin, stream } = uri.uri;
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.access_token)?;
            let stream_client = StreamClient::new(client_config, basin, stream);
            let mut read_output_stream = StreamService::new(stream_client)
                .read_session(start_seq_num, limit_count, limit_bytes)
                .await?;
            let mut writer = output.into_writer().await.unwrap();

            loop {
                select! {
                    maybe_read_result = read_output_stream.next() => {
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

                                            if let Some(command_record) = sequenced_record.as_command_record() {
                                                let (cmd, description) = match command_record.command {
                                                    Command::Fence { fencing_token } => (
                                                        "fence",
                                                        format!(
                                                            "FencingToken({})",
                                                            Base64::encode_string(fencing_token.as_ref()),
                                                        ),
                                                    ),
                                                    Command::Trim { seq_num } => (
                                                        "trim",
                                                        format!("TrimPoint({seq_num})"),
                                                    ),
                                                };
                                                eprintln!("{} with {}", cmd.bold(), description.green().bold());
                                            } else {
                                                match format {
                                                    Format::Text => {
                                                        TextFormatter::write_record(
                                                            &sequenced_record,
                                                            &mut writer,
                                                        ).await
                                                    },
                                                    Format::Json => {
                                                        JsonFormatter::write_record(
                                                            &sequenced_record,
                                                            &mut writer,
                                                        ).await
                                                    },
                                                    Format::JsonBinsafe => {
                                                        JsonBinsafeFormatter::write_record(
                                                            &sequenced_record,
                                                            &mut writer,
                                                        ).await
                                                    },
                                                }
                                                .map_err(|e| S2CliError::RecordWrite(e.to_string()))?;
                                                writer
                                                    .write_all(b"\n")
                                                    .await
                                                    .map_err(|e| S2CliError::RecordWrite(e.to_string()))?;
                                            }
                                        }

                                        eprintln!(
                                            "{}",
                                            format!(
                                                "⦿ {batch_len} bytes \
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

                writer.flush().await.expect("writer flush");
            }
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

            let interval = interval.max(Duration::from_millis(100));
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

                let record_bytes =
                    jitter_op(batch_bytes, rand::thread_rng().gen_range(0..=max_jitter));

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
                    )
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
            tokens,
            op_groups,
            ops,
        } => {
            let cfg = config::load_config(&config_path)?;
            let client_config = client_config(cfg.access_token)?;
            let account_service = AccountService::new(Client::new(client_config));
            let token = account_service
                .issue_access_token(
                    id,
                    expires_at,
                    auto_prefix_streams,
                    basins.map(Into::into),
                    streams.map(Into::into),
                    tokens.map(Into::into),
                    op_groups.map(Into::into),
                    ops.into_iter().map(Into::into).collect(),
                )
                .await?;
            println!("{token}");
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
    };

    Ok(())
}
