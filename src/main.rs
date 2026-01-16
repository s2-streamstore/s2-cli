mod cli;
mod config;
mod error;
mod ops;
mod record_format;
mod types;

use std::pin::Pin;
use std::time::Duration;

use clap::Parser;
use cli::ConfigCommand;
use cli::{Cli, Command, ListBasinsArgs, ListStreamsArgs};
use colored::Colorize;
use config::{
    ConfigKey, load_cli_config, load_config_file, sdk_config, set_config_value, unset_config_value,
};
use error::{CliError, OpKind};
use futures::{Stream, StreamExt, TryStreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use json_to_table::json_to_table;
use record_format::Format;
use record_format::{
    Base64JsonFormatter, RawBodyFormatter, RawJsonFormatter, RecordParser, RecordWriter,
};
use s2_sdk::{
    S2,
    types::{BasinState, MeteredBytes, Metric},
};
use strum::VariantNames;
use tabled::{Table, Tabled};
use tokio::io::AsyncWriteExt;
use tokio::select;
use tracing_subscriber::{fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt};
use types::{
    AccessTokenInfo, BasinConfig, LatencyStats, Pong, S2BasinAndMaybeStreamUri, StreamConfig,
};

#[tokio::main]
async fn main() -> miette::Result<()> {
    miette::set_panic_hook();
    run().await?;
    Ok(())
}

async fn run() -> Result<(), CliError> {
    let commands = Cli::try_parse().unwrap_or_else(|e| {
        // Customize error message for metric commands to say "metric" instead of "subcommand"
        let msg = e.to_string();
        if msg.contains("requires a subcommand") && msg.contains("get-") && msg.contains("-metrics")
        {
            let msg = msg
                .replace("requires a subcommand", "requires a metric")
                .replace("[subcommands:", "[metrics:");
            eprintln!("{msg}");
            std::process::exit(2);
        }
        e.exit()
    });

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

    if let Command::Config(config_cmd) = &commands.command {
        match config_cmd {
            ConfigCommand::List => {
                let config = load_config_file()?;
                for k in ConfigKey::VARIANTS {
                    if let Ok(key) = k.parse::<ConfigKey>()
                        && let Some(v) = config.get(key)
                    {
                        println!("{} = {}", k, v);
                    }
                }
            }
            ConfigCommand::Get { key } => {
                let config = load_config_file()?;
                if let Some(v) = config.get(*key) {
                    println!("{}", v);
                }
            }
            ConfigCommand::Set { key, value } => {
                let saved_path = set_config_value(*key, value.clone())?;
                eprintln!("{}", format!("✓ {} set", key).green().bold());
                eprintln!(
                    "  Configuration saved to: {}",
                    saved_path.display().to_string().cyan()
                );
            }
            ConfigCommand::Unset { key } => {
                let saved_path = unset_config_value(*key)?;
                eprintln!("{}", format!("✓ {} unset", key).green().bold());
                eprintln!(
                    "  Configuration saved to: {}",
                    saved_path.display().to_string().cyan()
                );
            }
        }
        return Ok(());
    }

    let cli_config = load_cli_config()?;
    let sdk_config = sdk_config(&cli_config)?;
    let s2 = S2::new(sdk_config).map_err(CliError::SdkInit)?;

    match commands.command {
        Command::Config(..) => unreachable!(),

        Command::Ls(args) => {
            if let Some(ref uri) = args.uri {
                // List streams
                let S2BasinAndMaybeStreamUri {
                    basin,
                    stream: uri_prefix,
                } = uri.clone();

                if uri_prefix.is_some() && args.prefix.is_some() {
                    return Err(CliError::InvalidArgs(miette::miette!(
                        help = "Make sure to provide the prefix once either using '--prefix' opt or in URI like 's2://basin-name/prefix'",
                        "Multiple prefixes provided"
                    )));
                }

                let list_streams_args = ListStreamsArgs {
                    uri: S2BasinAndMaybeStreamUri {
                        basin: basin.clone(),
                        stream: uri_prefix,
                    },
                    prefix: args
                        .prefix
                        .clone()
                        .map(|s| s.parse())
                        .transpose()
                        .map_err(|e| CliError::InvalidArgs(miette::miette!("{e}")))?,
                    start_after: args
                        .start_after
                        .clone()
                        .map(|s| s.parse())
                        .transpose()
                        .map_err(|e| CliError::InvalidArgs(miette::miette!("{e}")))?,
                    limit: args.limit,
                    no_auto_paginate: args.no_auto_paginate,
                };

                let mut streams = ops::list_streams(&s2, list_streams_args).await?;
                while let Some(stream_info) = streams.try_next().await? {
                    println!(
                        "s2://{}/{} {}",
                        basin,
                        stream_info.name,
                        stream_info.created_at.to_string().green(),
                    );
                }
            } else {
                // List basins
                let list_basins_args = ListBasinsArgs {
                    prefix: args
                        .prefix
                        .clone()
                        .map(|s| s.parse())
                        .transpose()
                        .map_err(|e| CliError::InvalidArgs(miette::miette!("{e}")))?,
                    start_after: args
                        .start_after
                        .clone()
                        .map(|s| s.parse())
                        .transpose()
                        .map_err(|e| CliError::InvalidArgs(miette::miette!("{e}")))?,
                    limit: args.limit,
                    no_auto_paginate: args.no_auto_paginate,
                };

                let mut basins = ops::list_basins(&s2, list_basins_args).await?;
                while let Some(basin_info) = basins.try_next().await? {
                    println!(
                        "{} {}",
                        basin_info.name,
                        format_basin_state(basin_info.state)
                    );
                }
            }
        }

        Command::ListBasins(args) => {
            let mut basins = ops::list_basins(&s2, args).await?;
            while let Some(basin_info) = basins.try_next().await? {
                println!(
                    "{} {}",
                    basin_info.name,
                    format_basin_state(basin_info.state)
                );
            }
        }

        Command::CreateBasin(args) => {
            let info = ops::create_basin(&s2, args).await?;

            let message = match info.state {
                BasinState::Creating => "✓ Basin creation requested".yellow().bold(),
                BasinState::Active => "✓ Basin created".green().bold(),
                BasinState::Deleting => "Basin is being deleted".red().bold(),
            };
            eprintln!("{message}");
        }

        Command::DeleteBasin { basin } => {
            ops::delete_basin(&s2, &basin.into()).await?;
            eprintln!("{}", "✓ Basin deletion requested".green().bold());
        }

        Command::GetBasinConfig { basin } => {
            let basin_config: BasinConfig = ops::get_basin_config(&s2, &basin.into()).await?.into();
            println!("{}", json_to_table(&serde_json::to_value(&basin_config)?));
        }

        Command::ReconfigureBasin(args) => {
            let config = ops::reconfigure_basin(&s2, args).await?;

            eprintln!("{}", "✓ Basin reconfigured".green().bold());
            println!("{}", json_to_table(&serde_json::to_value(&config)?));
        }

        Command::ListAccessTokens(args) => {
            let mut tokens = ops::list_access_tokens(&s2, args).await?;
            while let Some(token_info) = tokens.try_next().await? {
                let info = AccessTokenInfo::from(token_info);
                println!("{}", json_to_table(&serde_json::to_value(&info)?));
            }
        }

        Command::IssueAccessToken(args) => {
            let token = ops::issue_access_token(&s2, args).await?;
            println!("{}", token);
        }

        Command::RevokeAccessToken { id } => {
            ops::revoke_access_token(&s2, id.clone()).await?;
            eprintln!(
                "{}",
                format!("✓ Access token '{}' revoked", id).green().bold()
            );
        }

        Command::GetAccountMetrics(args) => {
            let metrics = ops::get_account_metrics(&s2, args).await?;
            print_metrics(&metrics);
        }

        Command::GetBasinMetrics(args) => {
            let metrics = ops::get_basin_metrics(&s2, args).await?;
            print_metrics(&metrics);
        }

        Command::GetStreamMetrics(args) => {
            let metrics = ops::get_stream_metrics(&s2, args).await?;
            print_metrics(&metrics);
        }

        Command::ListStreams(args) => {
            let basin_name = args.uri.basin.clone();
            let mut streams = ops::list_streams(&s2, args).await?;
            while let Some(stream_info) = streams.try_next().await? {
                println!("s2://{}/{}", basin_name, stream_info.name);
            }
        }

        Command::CreateStream(args) => {
            ops::create_stream(&s2, args).await?;
            eprintln!("{}", "✓ Stream created".green().bold());
        }

        Command::DeleteStream { uri } => {
            ops::delete_stream(&s2, uri).await?;
            eprintln!("{}", "✓ Stream deletion requested".green().bold());
        }

        Command::GetStreamConfig { uri } => {
            let stream_config = ops::get_stream_config(&s2, uri).await?;
            let stream_config: StreamConfig = stream_config.into();
            println!("{}", json_to_table(&serde_json::to_value(&stream_config)?));
        }

        Command::ReconfigureStream(args) => {
            let config = ops::reconfigure_stream(&s2, args).await?;

            eprintln!("{}", "✓ Stream reconfigured".green().bold());
            println!("{}", json_to_table(&serde_json::to_value(&config)?));
        }

        Command::CheckTail { uri } => {
            let tail = ops::check_tail(&s2, uri).await?;
            println!("{}\t{}", tail.seq_num, tail.timestamp);
        }

        Command::Trim(args) => {
            let trim_point = args.trim_point;
            let out = ops::trim(&s2, args).await?;
            eprintln!(
                "{}",
                format!(
                    "✓ Trim request for trim point {} appended at: {:?}",
                    trim_point, out.start
                )
                .green()
                .bold()
            );
        }

        Command::Fence(args) => {
            let out = ops::fence(&s2, args).await?;
            eprintln!(
                "{}",
                format!("✓ Fencing token appended at seq_num: {:?}", out.start)
                    .green()
                    .bold()
            );
        }

        Command::Append(args) => {
            let records_in = args
                .input
                .reader()
                .await
                .map_err(|e| CliError::RecordReaderInit(e.to_string()))?;

            let record_stream: Pin<Box<dyn Stream<Item = _> + Send + Unpin>> = match args.format {
                Format::BodyRaw => Box::pin(RawBodyFormatter::parse_records(records_in)),
                Format::JsonRaw => Box::pin(RawJsonFormatter::parse_records(records_in)),
                Format::JsonBase64 => Box::pin(Base64JsonFormatter::parse_records(records_in)),
            };

            let acks = ops::append(
                &s2,
                record_stream,
                args.uri,
                args.fencing_token,
                args.match_seq_num,
                *args.linger,
            );
            let mut acks = std::pin::pin!(acks);
            let mut last_printed_batch_end: Option<u64> = None;

            loop {
                select! {
                    ack = acks.next() => {
                        match ack {
                            Some(Ok(ack)) => {
                                if last_printed_batch_end.is_none_or(|end| end != ack.batch.end.seq_num) {
                                    last_printed_batch_end = Some(ack.batch.end.seq_num);
                                    eprintln!(
                                        "{}",
                                        format!(
                                            "✓ [APPENDED] {}..{} // tail: {} @ {}",
                                            ack.batch.start.seq_num,
                                            ack.batch.end.seq_num,
                                            ack.batch.tail.seq_num,
                                            ack.batch.tail.timestamp
                                        )
                                        .green()
                                        .bold()
                                    );
                                }
                            }
                            Some(Err(e)) => {
                                return Err(e);
                            }
                            None => break, // Stream exhausted, all done
                        }
                    }
                    _ = tokio::signal::ctrl_c() => {
                        eprintln!("{}", "■ [ABORTED]".red().bold());
                        break;
                    }
                }
            }
        }

        Command::Read(args) => {
            let mut batches = ops::read(&s2, &args).await?;
            let mut writer = args
                .output
                .writer()
                .await
                .map_err(|e| CliError::RecordWrite(e.to_string()))?;

            loop {
                select! {
                    batch = batches.next() => {
                        match batch {
                            Some(Ok(batch)) => {
                                let num_records = batch.records.len();
                                let mut batch_len = 0;

                                let seq_range = match (batch.records.first(), batch.records.last()) {
                                    (Some(first), Some(last)) => first.seq_num..=last.seq_num,
                                    _ => continue,
                                };

                                for record in &batch.records {
                                    batch_len += record.metered_bytes();
                                    write_record(record, &mut writer, args.format).await?;
                                    writer
                                        .write_all(b"\n")
                                        .await
                                        .map_err(|e| CliError::RecordWrite(e.to_string()))?;
                                }

                                eprintln!(
                                    "{}",
                                    format!(
                                        "⦿ {batch_len} bytes ({num_records} records in range {seq_range:?})"
                                    )
                                    .blue()
                                    .bold()
                                );

                                writer
                                    .flush()
                                    .await
                                    .map_err(|e| CliError::RecordWrite(e.to_string()))?;
                            }
                            Some(Err(e)) => {
                                return Err(CliError::op(OpKind::Read, e));
                            }
                            None => break,
                        }
                    }
                    _ = tokio::signal::ctrl_c() => {
                        eprintln!("{}", "■ [ABORTED]".red().bold());
                        break;
                    }
                }
            }
        }

        Command::Tail(args) => {
            let mut records = ops::tail(&s2, &args).await?;
            let mut writer = args
                .output
                .writer()
                .await
                .map_err(|e| CliError::RecordWrite(e.to_string()))?;

            loop {
                select! {
                    record = records.next() => {
                        match record {
                            Some(Ok(record)) => {
                                write_record(&record, &mut writer, args.format).await?;
                                writer
                                    .write_all(b"\n")
                                    .await
                                    .map_err(|e| CliError::RecordWrite(e.to_string()))?;
                                writer
                                    .flush()
                                    .await
                                    .map_err(|e| CliError::RecordWrite(e.to_string()))?;
                            }
                            Some(Err(e)) => {
                                return Err(e);
                            }
                            None => break,
                        }
                    }
                    _ = tokio::signal::ctrl_c() => {
                        eprintln!("{}", "■ [ABORTED]".red().bold());
                        break;
                    }
                }
            }
        }

        Command::Ping(args) => {
            let interval = std::cmp::max(*args.interval, Duration::from_millis(100));
            let batch_bytes = std::cmp::min(args.batch_bytes, 128 * 1024);
            let num_batches = args.num_batches;

            let prepare_spinner = ProgressBar::new_spinner()
                .with_prefix("Preparing...")
                .with_style(
                    ProgressStyle::default_spinner()
                        .template("{spinner} {prefix}")
                        .expect("valid template"),
                );
            prepare_spinner.enable_steady_tick(Duration::from_millis(50));

            let pong_stream = ops::ping(&s2, args, batch_bytes);
            let mut pong_stream = std::pin::pin!(pong_stream);

            let first_pong = pong_stream.next().await;
            prepare_spinner.finish_and_clear();

            let mut pongs = Vec::new();

            let stat_bars = MultiProgress::new();

            let bytes_bar = ProgressBar::no_length().with_prefix("bytes").with_style(
                ProgressStyle::default_bar()
                    .template("{pos:.bold} {prefix:.bold}")
                    .expect("valid template"),
            );

            let mut max_ack: u64 = 500;
            let ack_bar = ProgressBar::new(max_ack).with_prefix("ack").with_style(
                ProgressStyle::default_bar()
                    .template("{prefix:.bold} [{bar:40.blue/blue}] {pos:>4}/{len:<4} ms")
                    .expect("valid template"),
            );

            let mut max_e2e: u64 = 500;
            let e2e_bar = ProgressBar::new(max_e2e).with_prefix("e2e").with_style(
                ProgressStyle::default_bar()
                    .template("{prefix:.bold} [{bar:40.red/red}] {pos:>4}/{len:<4} ms")
                    .expect("valid template"),
            );

            let empty_line_bar = {
                let bar = stat_bars.add(
                    ProgressBar::no_length().with_style(
                        ProgressStyle::default_bar()
                            .template("\n")
                            .expect("valid template"),
                    ),
                );
                bar.inc(1);
                bar
            };
            let bytes_bar = stat_bars.add(bytes_bar);
            let ack_bar = stat_bars.add(ack_bar);
            let e2e_bar = stat_bars.add(e2e_bar);

            let mut update_bars = |pong: &Pong| {
                bytes_bar.set_position(pong.bytes);

                let ack_ms = pong.ack.as_millis() as u64;
                max_ack = std::cmp::max(max_ack, ack_ms);
                ack_bar.set_length(max_ack);
                ack_bar.set_position(ack_ms);

                let e2e_ms = pong.e2e.as_millis() as u64;
                max_e2e = std::cmp::max(max_e2e, e2e_ms);
                e2e_bar.set_length(max_e2e);
                e2e_bar.set_position(e2e_ms);
            };

            match first_pong {
                Some(Ok(pong)) => {
                    update_bars(&pong);
                    pongs.push(pong);
                }
                Some(Err(e)) => {
                    bytes_bar.finish_and_clear();
                    ack_bar.finish_and_clear();
                    e2e_bar.finish_and_clear();
                    empty_line_bar.finish_and_clear();
                    return Err(e);
                }
                None => {}
            }

            while num_batches.is_none_or(|max| pongs.len() < max) {
                select! {
                    _ = tokio::time::sleep(interval) => {

                        match pong_stream.next().await {
                            Some(Ok(pong)) => {
                                update_bars(&pong);
                                pongs.push(pong);
                            }
                            Some(Err(e)) => {
                                bytes_bar.finish_and_clear();
                                ack_bar.finish_and_clear();
                                e2e_bar.finish_and_clear();
                                empty_line_bar.finish_and_clear();
                                return Err(e);
                            }
                            None => break,
                        }
                    }
                    _ = tokio::signal::ctrl_c() => {
                        break;
                    }
                }
            }

            bytes_bar.finish_and_clear();
            ack_bar.finish_and_clear();
            e2e_bar.finish_and_clear();
            empty_line_bar.finish_and_clear();

            if !pongs.is_empty() {
                let total_batches = pongs.len();
                let (bytes, (acks, e2es)): (Vec<_>, (Vec<_>, Vec<_>)) =
                    pongs.into_iter().map(|p| (p.bytes, (p.ack, p.e2e))).unzip();
                let total_bytes: u64 = bytes.into_iter().sum();

                eprintln!(
                    "Round-tripped {} bytes in {} batches",
                    total_bytes, total_batches
                );

                eprintln!();
                print_latency_stats(LatencyStats::compute(acks), "Append Acknowledgement");
                eprintln!();
                print_latency_stats(LatencyStats::compute(e2es), "End-to-End");
            }
        }
    }

    Ok(())
}

fn format_basin_state(state: BasinState) -> colored::ColoredString {
    match state {
        BasinState::Active => "active".green(),
        BasinState::Creating => "creating".yellow(),
        BasinState::Deleting => "deleting".red(),
    }
}

async fn write_record(
    record: &s2_sdk::types::SequencedRecord,
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    format: Format,
) -> Result<(), CliError> {
    match format {
        Format::BodyRaw => {
            if record.is_command_record() {
                if let Some(header) = record.headers.first() {
                    let cmd_type = &header.value;
                    let (cmd, description) = if cmd_type.as_ref() == b"fence" {
                        let fencing_token = String::from_utf8_lossy(&record.body);
                        ("fence", format!("FencingToken({fencing_token})"))
                    } else if cmd_type.as_ref() == b"trim" {
                        let trim_point = if record.body.len() >= 8 {
                            u64::from_be_bytes(record.body[..8].try_into().unwrap_or_default())
                        } else {
                            0
                        };
                        ("trim", format!("TrimPoint({trim_point})"))
                    } else {
                        ("unknown", "UnknownCommand".to_string())
                    };
                    eprintln!(
                        "{} with {} // {} @ {}",
                        cmd.bold(),
                        description.green().bold(),
                        record.seq_num,
                        record.timestamp
                    );
                }
            } else {
                RawBodyFormatter::write_record(record, writer)
                    .await
                    .map_err(|e| CliError::RecordWrite(e.to_string()))?;
            }
        }
        Format::JsonRaw => {
            RawJsonFormatter::write_record(record, writer)
                .await
                .map_err(|e| CliError::RecordWrite(e.to_string()))?;
        }
        Format::JsonBase64 => {
            Base64JsonFormatter::write_record(record, writer)
                .await
                .map_err(|e| CliError::RecordWrite(e.to_string()))?;
        }
    }
    Ok(())
}

fn format_timestamp(ts: u32) -> String {
    use std::time::{Duration, UNIX_EPOCH};
    let time = UNIX_EPOCH + Duration::from_secs(ts as u64);
    humantime::format_rfc3339_seconds(time).to_string()
}

fn format_unit(unit: s2_sdk::types::MetricUnit) -> &'static str {
    match unit {
        s2_sdk::types::MetricUnit::Bytes => "bytes",
        s2_sdk::types::MetricUnit::Operations => "operations",
    }
}

fn print_metrics(metrics: &[Metric]) {
    #[derive(Tabled)]
    struct AccumulationRow {
        interval_start: String,
        count: String,
    }

    #[derive(Tabled)]
    struct GaugeRow {
        time: String,
        value: String,
    }

    for metric in metrics {
        match metric {
            Metric::Scalar(m) => {
                println!("{}: {} {}", m.name, m.value, format_unit(m.unit));
            }
            Metric::Accumulation(m) => {
                let rows: Vec<AccumulationRow> = m
                    .values
                    .iter()
                    .map(|(ts, value)| AccumulationRow {
                        interval_start: format_timestamp(*ts),
                        count: value.to_string(),
                    })
                    .collect();

                println!("{}", m.name);

                let mut table = Table::new(rows);
                table.modify(
                    tabled::settings::object::Columns::last(),
                    tabled::settings::Alignment::right(),
                );

                let interval_col = "interval start time".to_string();
                let count_col = format_unit(m.unit).to_string();
                table.with(
                    tabled::settings::Modify::new(tabled::settings::object::Cell::new(0, 0))
                        .with(tabled::settings::Format::content(|_| interval_col.clone())),
                );
                table.with(
                    tabled::settings::Modify::new(tabled::settings::object::Cell::new(0, 1))
                        .with(tabled::settings::Format::content(|_| count_col.clone())),
                );

                println!("{table}");
                println!();
            }
            Metric::Gauge(m) => {
                let rows: Vec<GaugeRow> = m
                    .values
                    .iter()
                    .map(|(ts, value)| GaugeRow {
                        time: format_timestamp(*ts),
                        value: value.to_string(),
                    })
                    .collect();

                let count_col = format_unit(m.unit).to_string();
                println!("{}\n", m.name);

                let mut table = Table::new(rows);
                table.modify(
                    tabled::settings::object::Columns::last(),
                    tabled::settings::Alignment::right(),
                );

                table.with(
                    tabled::settings::Modify::new(tabled::settings::object::Cell::new(0, 1))
                        .with(tabled::settings::Format::content(|_| count_col.clone())),
                );

                println!("{table}");
                println!();
            }
            Metric::Label(m) => {
                println!("{}:", m.name);
                for label in &m.values {
                    println!("  {}", label);
                }
            }
        }
    }
}

fn print_latency_stats(stats: LatencyStats, name: &str) {
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
