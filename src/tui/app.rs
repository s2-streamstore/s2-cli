use std::collections::VecDeque;
use std::time::Duration;

use base64ct::Encoding;
use crossterm::event::{self, Event as CrosstermEvent, KeyCode, KeyEvent, KeyModifiers};
use futures::StreamExt;
use ratatui::{Terminal, prelude::Backend};
use s2_sdk::types::{AccessTokenId, AccessTokenInfo, BasinInfo, BasinMetricSet, BasinName, StreamInfo, StreamMetricSet, StreamName, StreamPosition, TimeRange};
use tokio::sync::mpsc;

use crate::cli::{CreateStreamArgs, IssueAccessTokenArgs, ListAccessTokensArgs, ListBasinsArgs, ListStreamsArgs, ReadArgs, ReconfigureBasinArgs, ReconfigureStreamArgs};
use crate::error::CliError;
use crate::ops;
use crate::record_format::{RecordFormat, RecordsOut};
use crate::types::{BasinConfig, DeleteOnEmptyConfig, Operation, RetentionPolicy, S2BasinAndMaybeStreamUri, S2BasinAndStreamUri, S2BasinUri, StorageClass, StreamConfig, TimestampingConfig, TimestampingMode};

use super::event::{BasinConfigInfo, Event, StreamConfigInfo};
use super::ui;

/// Maximum records to keep in read view buffer
const MAX_RECORDS_BUFFER: usize = 1000;

/// Top-level navigation tabs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Tab {
    #[default]
    Basins,
    AccessTokens,
}

/// Current screen being displayed
#[derive(Debug, Clone)]
pub enum Screen {
    Splash,
    Basins(BasinsState),
    Streams(StreamsState),
    StreamDetail(StreamDetailState),
    ReadView(ReadViewState),
    AppendView(AppendViewState),
    AccessTokens(AccessTokensState),
    MetricsView(MetricsViewState),
}

/// State for the basins list screen
#[derive(Debug, Clone, Default)]
pub struct BasinsState {
    pub basins: Vec<BasinInfo>,
    pub selected: usize,
    pub loading: bool,
    pub filter: String,
    pub filter_active: bool,
}

/// State for the streams list screen
#[derive(Debug, Clone)]
pub struct StreamsState {
    pub basin_name: BasinName,
    pub streams: Vec<StreamInfo>,
    pub selected: usize,
    pub loading: bool,
    pub filter: String,
    pub filter_active: bool,
}

/// State for the stream detail screen
#[derive(Debug, Clone)]
pub struct StreamDetailState {
    pub basin_name: BasinName,
    pub stream_name: StreamName,
    pub config: Option<StreamConfig>,
    pub tail_position: Option<StreamPosition>,
    pub selected_action: usize,
    pub loading: bool,
}

/// State for the read/tail view
#[derive(Debug, Clone)]
pub struct ReadViewState {
    pub basin_name: BasinName,
    pub stream_name: StreamName,
    pub records: VecDeque<s2_sdk::types::SequencedRecord>,
    pub is_tailing: bool,
    pub selected: usize,
    pub paused: bool,
    pub loading: bool,
    pub show_detail: bool,
    pub hide_list: bool,
    pub output_file: Option<String>,
}

/// State for the append view
#[derive(Debug, Clone)]
pub struct AppendViewState {
    pub basin_name: BasinName,
    pub stream_name: StreamName,
    // Record fields
    pub body: String,
    pub headers: Vec<(String, String)>,  // List of (key, value) pairs
    pub match_seq_num: String,           // Empty = none
    pub fencing_token: String,           // Empty = none
    // UI state
    pub selected: usize,                 // 0=body, 1=headers, 2=match_seq, 3=fencing, 4=send
    pub editing: bool,
    pub header_key_input: String,        // For adding new header
    pub header_value_input: String,
    pub editing_header_key: bool,        // true = editing key, false = editing value
    // Results
    pub history: Vec<AppendResult>,
    pub appending: bool,
}

/// Result of an append operation
#[derive(Debug, Clone)]
pub struct AppendResult {
    pub seq_num: u64,
    pub body_preview: String,
    pub header_count: usize,
}

/// State for the access tokens list screen
#[derive(Debug, Clone, Default)]
pub struct AccessTokensState {
    pub tokens: Vec<AccessTokenInfo>,
    pub selected: usize,
    pub loading: bool,
    pub filter: String,
    pub filter_active: bool,
}

/// Type of metrics being viewed
#[derive(Debug, Clone)]
pub enum MetricsType {
    Account,
    Basin { basin_name: BasinName },
    Stream { basin_name: BasinName, stream_name: StreamName },
}

/// Which metric is currently selected (for basin/stream)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MetricCategory {
    #[default]
    Storage,
    AppendOps,
    ReadOps,
    AppendThroughput,
    ReadThroughput,
    // Account-level metrics
    ActiveBasins,
    AccountOps,
}

impl MetricCategory {
    pub fn next(&self) -> Self {
        match self {
            Self::Storage => Self::AppendOps,
            Self::AppendOps => Self::ReadOps,
            Self::ReadOps => Self::AppendThroughput,
            Self::AppendThroughput => Self::ReadThroughput,
            Self::ReadThroughput => Self::Storage,
            // Account metrics cycle
            Self::ActiveBasins => Self::AccountOps,
            Self::AccountOps => Self::ActiveBasins,
        }
    }

    pub fn prev(&self) -> Self {
        match self {
            Self::Storage => Self::ReadThroughput,
            Self::AppendOps => Self::Storage,
            Self::ReadOps => Self::AppendOps,
            Self::AppendThroughput => Self::ReadOps,
            Self::ReadThroughput => Self::AppendThroughput,
            // Account metrics cycle
            Self::ActiveBasins => Self::AccountOps,
            Self::AccountOps => Self::ActiveBasins,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Storage => "Storage",
            Self::AppendOps => "Append Ops",
            Self::ReadOps => "Read Ops",
            Self::AppendThroughput => "Append Throughput",
            Self::ReadThroughput => "Read Throughput",
            Self::ActiveBasins => "Active Basins",
            Self::AccountOps => "Account Ops",
        }
    }
}

/// State for the metrics view
#[derive(Debug, Clone)]
pub struct MetricsViewState {
    pub metrics_type: MetricsType,
    pub metrics: Vec<s2_sdk::types::Metric>,
    pub selected_category: MetricCategory,
    pub loading: bool,
    pub scroll: usize,
}

/// Status message level
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageLevel {
    Info,
    Success,
    Error,
}

/// Status message to display
#[derive(Debug, Clone)]
pub struct StatusMessage {
    pub text: String,
    pub level: MessageLevel,
}

/// Input mode for text input dialogs
#[derive(Debug, Clone)]
pub enum InputMode {
    /// Not in input mode
    Normal,
    /// Creating a new basin
    CreateBasin {
        name: String,
        // Basin scope (cloud provider/region)
        scope: BasinScopeOption,
        // Basin-level settings
        create_stream_on_append: bool,
        create_stream_on_read: bool,
        // Default stream config
        storage_class: Option<StorageClass>,
        retention_policy: RetentionPolicyOption,
        retention_age_input: String,
        timestamping_mode: Option<TimestampingMode>,
        timestamping_uncapped: bool,
        // Delete-on-empty config
        delete_on_empty_enabled: bool,
        delete_on_empty_min_age: String,
        // UI state
        selected: usize,
        editing: bool,
    },
    /// Creating a new stream
    CreateStream {
        basin: BasinName,
        name: String,
        // Stream config
        storage_class: Option<StorageClass>,
        retention_policy: RetentionPolicyOption,
        retention_age_input: String,
        timestamping_mode: Option<TimestampingMode>,
        timestamping_uncapped: bool,
        // Delete-on-empty config
        delete_on_empty_enabled: bool,
        delete_on_empty_min_age: String,
        // UI state
        selected: usize,
        editing: bool,
    },
    /// Confirming basin deletion
    ConfirmDeleteBasin { basin: BasinName },
    /// Confirming stream deletion
    ConfirmDeleteStream { basin: BasinName, stream: StreamName },
    /// Reconfiguring a basin
    ReconfigureBasin {
        basin: BasinName,
        // Basin-level settings
        create_stream_on_append: Option<bool>,
        create_stream_on_read: Option<bool>,
        // Default stream config
        storage_class: Option<StorageClass>,
        retention_policy: RetentionPolicyOption,
        retention_age_secs: u64,
        timestamping_mode: Option<TimestampingMode>,
        timestamping_uncapped: Option<bool>,
        // UI state
        selected: usize,
        editing_age: bool,
        age_input: String,
    },
    /// Reconfiguring a stream
    ReconfigureStream {
        basin: BasinName,
        stream: StreamName,
        storage_class: Option<StorageClass>,
        retention_policy: RetentionPolicyOption,
        retention_age_secs: u64,
        timestamping_mode: Option<TimestampingMode>,
        timestamping_uncapped: Option<bool>,
        // Delete-on-empty config
        delete_on_empty_enabled: bool,
        delete_on_empty_min_age: String,
        // UI state
        selected: usize,
        editing_age: bool,
        age_input: String,
    },
    /// Custom read configuration
    CustomRead {
        basin: BasinName,
        stream: StreamName,
        // Start position
        start_from: ReadStartFrom,
        seq_num_value: String,
        timestamp_value: String,
        ago_value: String,
        ago_unit: AgoUnit,
        tail_offset_value: String,
        // Limits
        count_limit: String,
        byte_limit: String,
        until_timestamp: String,
        // Options
        clamp: bool,
        format: ReadFormat,
        output_file: String,  // Empty = display only, path = write to file
        // UI state
        selected: usize,
        editing: bool,
    },
    /// Fence a stream (set new fencing token)
    Fence {
        basin: BasinName,
        stream: StreamName,
        new_token: String,
        current_token: String,  // Empty = no current token
        selected: usize,        // 0=new_token, 1=current_token, 2=submit
        editing: bool,
    },
    /// Trim a stream (delete records before seq num)
    Trim {
        basin: BasinName,
        stream: StreamName,
        trim_point: String,
        fencing_token: String,  // Empty = no fencing token
        selected: usize,        // 0=trim_point, 1=fencing_token, 2=submit
        editing: bool,
    },
    /// Issue a new access token
    IssueAccessToken {
        // Basic info
        id: String,
        expiry: ExpiryOption,
        expiry_custom: String,  // For custom duration input
        // Resource scopes
        basins_scope: ScopeOption,
        basins_value: String,
        streams_scope: ScopeOption,
        streams_value: String,
        tokens_scope: ScopeOption,
        tokens_value: String,
        // Operation permissions (Read/Write for each level)
        account_read: bool,
        account_write: bool,
        basin_read: bool,
        basin_write: bool,
        stream_read: bool,
        stream_write: bool,
        // Options
        auto_prefix_streams: bool,
        // UI state
        selected: usize,
        editing: bool,
    },
    /// Confirming access token revocation
    ConfirmRevokeToken { token_id: String },
    /// Show issued token (one-time display)
    ShowIssuedToken { token: String },
    /// View access token details
    ViewTokenDetail { token: AccessTokenInfo },
}

/// Retention policy option for UI
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RetentionPolicyOption {
    Infinite,
    Age,
}

impl Default for RetentionPolicyOption {
    fn default() -> Self {
        Self::Infinite
    }
}

/// Basin scope option for UI (cloud provider/region)
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum BasinScopeOption {
    #[default]
    AwsUsEast1,
}

/// Expiry options for access tokens
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum ExpiryOption {
    #[default]
    Never,
    OneDay,
    SevenDays,
    ThirtyDays,
    NinetyDays,
    OneYear,
    Custom,
}

impl ExpiryOption {
    pub fn next(&self) -> Self {
        match self {
            ExpiryOption::Never => ExpiryOption::OneDay,
            ExpiryOption::OneDay => ExpiryOption::SevenDays,
            ExpiryOption::SevenDays => ExpiryOption::ThirtyDays,
            ExpiryOption::ThirtyDays => ExpiryOption::NinetyDays,
            ExpiryOption::NinetyDays => ExpiryOption::OneYear,
            ExpiryOption::OneYear => ExpiryOption::Custom,
            ExpiryOption::Custom => ExpiryOption::Never,
        }
    }

    pub fn prev(&self) -> Self {
        match self {
            ExpiryOption::Never => ExpiryOption::Custom,
            ExpiryOption::OneDay => ExpiryOption::Never,
            ExpiryOption::SevenDays => ExpiryOption::OneDay,
            ExpiryOption::ThirtyDays => ExpiryOption::SevenDays,
            ExpiryOption::NinetyDays => ExpiryOption::ThirtyDays,
            ExpiryOption::OneYear => ExpiryOption::NinetyDays,
            ExpiryOption::Custom => ExpiryOption::OneYear,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ExpiryOption::Never => "Never (permanent)",
            ExpiryOption::OneDay => "1 day",
            ExpiryOption::SevenDays => "7 days",
            ExpiryOption::ThirtyDays => "30 days",
            ExpiryOption::NinetyDays => "90 days",
            ExpiryOption::OneYear => "1 year",
            ExpiryOption::Custom => "Custom",
        }
    }

    pub fn to_duration_str(&self) -> Option<&'static str> {
        match self {
            ExpiryOption::Never => None,
            ExpiryOption::OneDay => Some("1d"),
            ExpiryOption::SevenDays => Some("7d"),
            ExpiryOption::ThirtyDays => Some("30d"),
            ExpiryOption::NinetyDays => Some("90d"),
            ExpiryOption::OneYear => Some("365d"),
            ExpiryOption::Custom => None, // Use custom value
        }
    }
}

/// Scope options for resource access
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum ScopeOption {
    #[default]
    All,
    Prefix,
    Exact,
    None,
}

impl ScopeOption {
    pub fn next(&self) -> Self {
        match self {
            ScopeOption::All => ScopeOption::Prefix,
            ScopeOption::Prefix => ScopeOption::Exact,
            ScopeOption::Exact => ScopeOption::None,
            ScopeOption::None => ScopeOption::All,
        }
    }

    pub fn prev(&self) -> Self {
        match self {
            ScopeOption::All => ScopeOption::None,
            ScopeOption::Prefix => ScopeOption::All,
            ScopeOption::Exact => ScopeOption::Prefix,
            ScopeOption::None => ScopeOption::Exact,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ScopeOption::All => "All",
            ScopeOption::Prefix => "Prefix",
            ScopeOption::Exact => "Exact",
            ScopeOption::None => "None",
        }
    }
}

/// Start position for read operation
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReadStartFrom {
    /// From current tail (live follow, no historical)
    Tail,
    /// From specific sequence number
    SeqNum,
    /// From specific timestamp (ms)
    Timestamp,
    /// From N time ago
    Ago,
    /// From N records before tail
    TailOffset,
}

impl Default for ReadStartFrom {
    fn default() -> Self {
        Self::Tail
    }
}

/// Time unit for "ago" option
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AgoUnit {
    Seconds,
    Minutes,
    Hours,
    Days,
}

impl AgoUnit {
    pub fn to_seconds(&self, value: u64) -> u64 {
        match self {
            AgoUnit::Seconds => value,
            AgoUnit::Minutes => value * 60,
            AgoUnit::Hours => value * 3600,
            AgoUnit::Days => value * 86400,
        }
    }

    pub fn next(&self) -> Self {
        match self {
            AgoUnit::Seconds => AgoUnit::Minutes,
            AgoUnit::Minutes => AgoUnit::Hours,
            AgoUnit::Hours => AgoUnit::Days,
            AgoUnit::Days => AgoUnit::Seconds,
        }
    }
}

impl Default for AgoUnit {
    fn default() -> Self {
        Self::Minutes
    }
}

/// Output format for read operation
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum ReadFormat {
    #[default]
    Text,
    Json,
    JsonBase64,
}

impl ReadFormat {
    pub fn next(&self) -> Self {
        match self {
            ReadFormat::Text => ReadFormat::Json,
            ReadFormat::Json => ReadFormat::JsonBase64,
            ReadFormat::JsonBase64 => ReadFormat::Text,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ReadFormat::Text => "text",
            ReadFormat::Json => "json",
            ReadFormat::JsonBase64 => "json-base64",
        }
    }
}


/// Config for basin reconfiguration
#[derive(Debug, Clone)]
pub struct BasinReconfigureConfig {
    pub create_stream_on_append: Option<bool>,
    pub create_stream_on_read: Option<bool>,
    pub storage_class: Option<StorageClass>,
    pub retention_policy: RetentionPolicyOption,
    pub retention_age_secs: u64,
    pub timestamping_mode: Option<TimestampingMode>,
    pub timestamping_uncapped: Option<bool>,
}

/// Config for stream reconfiguration
#[derive(Debug, Clone)]
pub struct StreamReconfigureConfig {
    pub storage_class: Option<StorageClass>,
    pub retention_policy: RetentionPolicyOption,
    pub retention_age_secs: u64,
    pub timestamping_mode: Option<TimestampingMode>,
    pub timestamping_uncapped: Option<bool>,
    pub delete_on_empty_enabled: bool,
    pub delete_on_empty_min_age: String,
}

impl Default for InputMode {
    fn default() -> Self {
        Self::Normal
    }
}

/// Main application state
pub struct App {
    pub screen: Screen,
    pub tab: Tab,
    pub s2: s2_sdk::S2,
    pub message: Option<StatusMessage>,
    pub show_help: bool,
    pub input_mode: InputMode,
    should_quit: bool,
}

/// Build a basin config from form values
fn build_basin_config(
    create_stream_on_append: bool,
    create_stream_on_read: bool,
    storage_class: Option<StorageClass>,
    retention_policy: RetentionPolicyOption,
    retention_age_input: String,
    timestamping_mode: Option<TimestampingMode>,
    timestamping_uncapped: bool,
    delete_on_empty_enabled: bool,
    delete_on_empty_min_age: String,
) -> BasinConfig {
    // Parse retention policy
    let retention = match retention_policy {
        RetentionPolicyOption::Infinite => None,
        RetentionPolicyOption::Age => {
            humantime::parse_duration(&retention_age_input)
                .ok()
                .map(RetentionPolicy::Age)
        }
    };

    // Build timestamping config if specified
    let timestamping = if timestamping_mode.is_some() || timestamping_uncapped {
        Some(TimestampingConfig {
            timestamping_mode,
            timestamping_uncapped: if timestamping_uncapped { Some(true) } else { None },
        })
    } else {
        None
    };

    // Build delete-on-empty config if enabled
    let delete_on_empty = if delete_on_empty_enabled {
        humantime::parse_duration(&delete_on_empty_min_age)
            .ok()
            .map(|d| DeleteOnEmptyConfig { delete_on_empty_min_age: d })
    } else {
        None
    };

    BasinConfig {
        default_stream_config: StreamConfig {
            storage_class,
            retention_policy: retention,
            timestamping,
            delete_on_empty,
        },
        create_stream_on_append,
        create_stream_on_read,
    }
}

fn build_stream_config(
    storage_class: Option<StorageClass>,
    retention_policy: RetentionPolicyOption,
    retention_age_input: String,
    timestamping_mode: Option<TimestampingMode>,
    timestamping_uncapped: bool,
    delete_on_empty_enabled: bool,
    delete_on_empty_min_age: String,
) -> StreamConfig {
    // Parse retention policy
    let retention = match retention_policy {
        RetentionPolicyOption::Infinite => None,
        RetentionPolicyOption::Age => {
            humantime::parse_duration(&retention_age_input)
                .ok()
                .map(RetentionPolicy::Age)
        }
    };

    // Build timestamping config if specified
    let timestamping = if timestamping_mode.is_some() || timestamping_uncapped {
        Some(TimestampingConfig {
            timestamping_mode,
            timestamping_uncapped: if timestamping_uncapped { Some(true) } else { None },
        })
    } else {
        None
    };

    // Build delete-on-empty config if enabled
    let delete_on_empty = if delete_on_empty_enabled {
        humantime::parse_duration(&delete_on_empty_min_age)
            .ok()
            .map(|d| DeleteOnEmptyConfig { delete_on_empty_min_age: d })
    } else {
        None
    };

    StreamConfig {
        storage_class,
        retention_policy: retention,
        timestamping,
        delete_on_empty,
    }
}

impl App {
    pub fn new(s2: s2_sdk::S2) -> Self {
        Self {
            screen: Screen::Splash,
            tab: Tab::Basins,
            s2,
            message: None,
            show_help: false,
            input_mode: InputMode::Normal,
            should_quit: false,
        }
    }

    pub async fn run<B: Backend>(mut self, terminal: &mut Terminal<B>) -> Result<(), CliError> {
        let (tx, mut rx) = mpsc::unbounded_channel();

        // Show splash screen briefly
        let splash_start = std::time::Instant::now();
        let splash_duration = Duration::from_millis(1200);

        // Start loading basins in background
        self.load_basins(tx.clone());

        // Track loaded basins for transition from splash
        let mut pending_basins: Option<Result<Vec<BasinInfo>, CliError>> = None;

        loop {
            // Render
            terminal
                .draw(|f| ui::draw(f, &self))
                .map_err(|e| CliError::RecordWrite(format!("Failed to draw: {e}")))?;

            // Check if splash screen should end
            if matches!(self.screen, Screen::Splash) && splash_start.elapsed() >= splash_duration {
                // Transition to basins
                let mut basins_state = BasinsState {
                    loading: pending_basins.is_none(),
                    ..Default::default()
                };
                if let Some(result) = pending_basins.take() {
                    match result {
                        Ok(basins) => {
                            basins_state.basins = basins;
                            basins_state.loading = false;
                        }
                        Err(e) => {
                            basins_state.loading = false;
                            self.message = Some(StatusMessage {
                                text: format!("Failed to load basins: {e}"),
                                level: MessageLevel::Error,
                            });
                        }
                    }
                }
                self.screen = Screen::Basins(basins_state);
            }

            // Handle events
            tokio::select! {
                // Handle async events from background tasks
                Some(event) = rx.recv() => {
                    // If on splash screen, cache the basins result
                    if matches!(self.screen, Screen::Splash) {
                        if let Event::BasinsLoaded(result) = event {
                            pending_basins = Some(result);
                            continue;
                        }
                    }
                    self.handle_event(event);
                }

                // Handle keyboard input
                _ = tokio::time::sleep(Duration::from_millis(50)) => {
                    if event::poll(Duration::from_millis(0))
                        .map_err(|e| CliError::RecordWrite(format!("Failed to poll events: {e}")))?
                    {
                        if let CrosstermEvent::Key(key) = event::read()
                            .map_err(|e| CliError::RecordWrite(format!("Failed to read event: {e}")))?
                        {
                            // Skip to basins on any key during splash
                            if matches!(self.screen, Screen::Splash) {
                                let mut basins_state = BasinsState {
                                    loading: pending_basins.is_none(),
                                    ..Default::default()
                                };
                                if let Some(result) = pending_basins.take() {
                                    match result {
                                        Ok(basins) => {
                                            basins_state.basins = basins;
                                            basins_state.loading = false;
                                        }
                                        Err(e) => {
                                            basins_state.loading = false;
                                            self.message = Some(StatusMessage {
                                                text: format!("Failed to load basins: {e}"),
                                                level: MessageLevel::Error,
                                            });
                                        }
                                    }
                                }
                                self.screen = Screen::Basins(basins_state);
                                continue;
                            }
                            self.handle_key(key, tx.clone());
                        }
                    }
                }
            }

            if self.should_quit {
                break;
            }
        }

        Ok(())
    }

    fn handle_event(&mut self, event: Event) {
        match event {
            Event::BasinsLoaded(result) => {
                if let Screen::Basins(state) = &mut self.screen {
                    state.loading = false;
                    match result {
                        Ok(basins) => {
                            state.basins = basins;
                            self.message = Some(StatusMessage {
                                text: format!("Loaded {} basins", state.basins.len()),
                                level: MessageLevel::Success,
                            });
                        }
                        Err(e) => {
                            self.message = Some(StatusMessage {
                                text: format!("Failed to load basins: {e}"),
                                level: MessageLevel::Error,
                            });
                        }
                    }
                }
            }

            Event::StreamsLoaded(result) => {
                if let Screen::Streams(state) = &mut self.screen {
                    state.loading = false;
                    match result {
                        Ok(streams) => {
                            state.streams = streams;
                            self.message = Some(StatusMessage {
                                text: format!("Loaded {} streams", state.streams.len()),
                                level: MessageLevel::Success,
                            });
                        }
                        Err(e) => {
                            self.message = Some(StatusMessage {
                                text: format!("Failed to load streams: {e}"),
                                level: MessageLevel::Error,
                            });
                        }
                    }
                }
            }

            Event::StreamConfigLoaded(result) => {
                if let Screen::StreamDetail(state) = &mut self.screen {
                    state.loading = false;
                    match result {
                        Ok(config) => {
                            state.config = Some(config);
                        }
                        Err(e) => {
                            self.message = Some(StatusMessage {
                                text: format!("Failed to load config: {e}"),
                                level: MessageLevel::Error,
                            });
                        }
                    }
                }
            }

            Event::TailPositionLoaded(result) => {
                if let Screen::StreamDetail(state) = &mut self.screen {
                    match result {
                        Ok(pos) => {
                            state.tail_position = Some(pos);
                        }
                        Err(e) => {
                            self.message = Some(StatusMessage {
                                text: format!("Failed to load tail position: {e}"),
                                level: MessageLevel::Error,
                            });
                        }
                    }
                }
            }

            Event::RecordReceived(result) => {
                if let Screen::ReadView(state) = &mut self.screen {
                    state.loading = false;
                    match result {
                        Ok(record) => {
                            if !state.paused {
                                state.records.push_back(record);
                                // Keep buffer bounded
                                while state.records.len() > MAX_RECORDS_BUFFER {
                                    state.records.pop_front();
                                    // Adjust selected if we removed records from front
                                    if state.selected > 0 {
                                        state.selected = state.selected.saturating_sub(1);
                                    }
                                }
                                // Auto-follow: keep selected at latest when tailing
                                if state.is_tailing {
                                    state.selected = state.records.len().saturating_sub(1);
                                }
                            }
                        }
                        Err(e) => {
                            self.message = Some(StatusMessage {
                                text: format!("Read error: {e}"),
                                level: MessageLevel::Error,
                            });
                        }
                    }
                }
            }

            Event::ReadEnded => {
                if let Screen::ReadView(state) = &mut self.screen {
                    state.loading = false;
                    if !state.is_tailing {
                        self.message = Some(StatusMessage {
                            text: "Read complete".to_string(),
                            level: MessageLevel::Info,
                        });
                    }
                }
            }

            Event::BasinCreated(result) => {
                self.input_mode = InputMode::Normal;
                match result {
                    Ok(basin) => {
                        self.message = Some(StatusMessage {
                            text: format!("Created basin '{}'", basin.name),
                            level: MessageLevel::Success,
                        });
                        // Refresh basins list
                        if let Screen::Basins(state) = &mut self.screen {
                            state.loading = true;
                        }
                    }
                    Err(e) => {
                        self.message = Some(StatusMessage {
                            text: format!("Failed to create basin: {e}"),
                            level: MessageLevel::Error,
                        });
                    }
                }
            }

            Event::BasinDeleted(result) => {
                self.input_mode = InputMode::Normal;
                match result {
                    Ok(name) => {
                        self.message = Some(StatusMessage {
                            text: format!("Deleted basin '{}'", name),
                            level: MessageLevel::Success,
                        });
                        // Refresh basins list
                        if let Screen::Basins(state) = &mut self.screen {
                            state.loading = true;
                        }
                    }
                    Err(e) => {
                        self.message = Some(StatusMessage {
                            text: format!("Failed to delete basin: {e}"),
                            level: MessageLevel::Error,
                        });
                    }
                }
            }

            Event::StreamCreated(result) => {
                self.input_mode = InputMode::Normal;
                match result {
                    Ok(stream) => {
                        self.message = Some(StatusMessage {
                            text: format!("Created stream '{}'", stream.name),
                            level: MessageLevel::Success,
                        });
                        // Refresh streams list
                        if let Screen::Streams(state) = &mut self.screen {
                            state.loading = true;
                        }
                    }
                    Err(e) => {
                        self.message = Some(StatusMessage {
                            text: format!("Failed to create stream: {e}"),
                            level: MessageLevel::Error,
                        });
                    }
                }
            }

            Event::StreamDeleted(result) => {
                self.input_mode = InputMode::Normal;
                match result {
                    Ok(name) => {
                        self.message = Some(StatusMessage {
                            text: format!("Deleted stream '{}'", name),
                            level: MessageLevel::Success,
                        });
                        // Refresh streams list
                        if let Screen::Streams(state) = &mut self.screen {
                            state.loading = true;
                        }
                    }
                    Err(e) => {
                        self.message = Some(StatusMessage {
                            text: format!("Failed to delete stream: {e}"),
                            level: MessageLevel::Error,
                        });
                    }
                }
            }

            Event::BasinConfigLoaded(result) => {
                if let InputMode::ReconfigureBasin {
                    create_stream_on_append,
                    create_stream_on_read,
                    storage_class,
                    retention_policy,
                    retention_age_secs,
                    timestamping_mode,
                    timestamping_uncapped,
                    age_input,
                    ..
                } = &mut self.input_mode {
                    match result {
                        Ok(info) => {
                            *create_stream_on_append = Some(info.create_stream_on_append);
                            *create_stream_on_read = Some(info.create_stream_on_read);
                            *storage_class = info.storage_class;
                            if let Some(age) = info.retention_age_secs {
                                *retention_policy = RetentionPolicyOption::Age;
                                *retention_age_secs = age;
                                *age_input = age.to_string();
                            } else {
                                *retention_policy = RetentionPolicyOption::Infinite;
                            }
                            *timestamping_mode = info.timestamping_mode;
                            *timestamping_uncapped = Some(info.timestamping_uncapped);
                        }
                        Err(e) => {
                            self.input_mode = InputMode::Normal;
                            self.message = Some(StatusMessage {
                                text: format!("Failed to load basin config: {e}"),
                                level: MessageLevel::Error,
                            });
                        }
                    }
                }
            }

            Event::StreamConfigForReconfigLoaded(result) => {
                if let InputMode::ReconfigureStream {
                    storage_class,
                    retention_policy,
                    retention_age_secs,
                    timestamping_mode,
                    timestamping_uncapped,
                    delete_on_empty_enabled,
                    delete_on_empty_min_age,
                    age_input,
                    ..
                } = &mut self.input_mode {
                    match result {
                        Ok(info) => {
                            *storage_class = info.storage_class;
                            if let Some(age) = info.retention_age_secs {
                                *retention_policy = RetentionPolicyOption::Age;
                                *retention_age_secs = age;
                                *age_input = age.to_string();
                            } else {
                                *retention_policy = RetentionPolicyOption::Infinite;
                            }
                            *timestamping_mode = info.timestamping_mode;
                            *timestamping_uncapped = Some(info.timestamping_uncapped);
                            // Delete on empty
                            if let Some(min_age_secs) = info.delete_on_empty_min_age_secs {
                                *delete_on_empty_enabled = true;
                                *delete_on_empty_min_age = format!("{}s", min_age_secs);
                            } else {
                                *delete_on_empty_enabled = false;
                            }
                        }
                        Err(e) => {
                            self.input_mode = InputMode::Normal;
                            self.message = Some(StatusMessage {
                                text: format!("Failed to load stream config: {e}"),
                                level: MessageLevel::Error,
                            });
                        }
                    }
                }
            }

            Event::BasinReconfigured(result) => {
                self.input_mode = InputMode::Normal;
                match result {
                    Ok(()) => {
                        self.message = Some(StatusMessage {
                            text: "Basin reconfigured".to_string(),
                            level: MessageLevel::Success,
                        });
                    }
                    Err(e) => {
                        self.message = Some(StatusMessage {
                            text: format!("Failed to reconfigure basin: {e}"),
                            level: MessageLevel::Error,
                        });
                    }
                }
            }

            Event::StreamReconfigured(result) => {
                self.input_mode = InputMode::Normal;
                match result {
                    Ok(()) => {
                        self.message = Some(StatusMessage {
                            text: "Stream reconfigured".to_string(),
                            level: MessageLevel::Success,
                        });
                    }
                    Err(e) => {
                        self.message = Some(StatusMessage {
                            text: format!("Failed to reconfigure stream: {e}"),
                            level: MessageLevel::Error,
                        });
                    }
                }
            }

            Event::StreamFenced(result) => {
                self.input_mode = InputMode::Normal;
                match result {
                    Ok(token) => {
                        self.message = Some(StatusMessage {
                            text: format!("Stream fenced with token: {}", token),
                            level: MessageLevel::Success,
                        });
                    }
                    Err(e) => {
                        self.message = Some(StatusMessage {
                            text: format!("Failed to fence stream: {e}"),
                            level: MessageLevel::Error,
                        });
                    }
                }
            }

            Event::StreamTrimmed(result) => {
                self.input_mode = InputMode::Normal;
                match result {
                    Ok((trim_point, new_tail)) => {
                        self.message = Some(StatusMessage {
                            text: format!("Trimmed to {} (tail: {})", trim_point, new_tail),
                            level: MessageLevel::Success,
                        });
                    }
                    Err(e) => {
                        self.message = Some(StatusMessage {
                            text: format!("Failed to trim stream: {e}"),
                            level: MessageLevel::Error,
                        });
                    }
                }
            }

            Event::RecordAppended(result) => {
                if let Screen::AppendView(state) = &mut self.screen {
                    state.appending = false;
                    match result {
                        Ok((seq_num, body_preview, header_count)) => {
                            state.history.push(AppendResult { seq_num, body_preview, header_count });
                        }
                        Err(e) => {
                            self.message = Some(StatusMessage {
                                text: format!("Append failed: {e}"),
                                level: MessageLevel::Error,
                            });
                        }
                    }
                }
            }

            Event::AccessTokensLoaded(result) => {
                if let Screen::AccessTokens(state) = &mut self.screen {
                    state.loading = false;
                    match result {
                        Ok(tokens) => {
                            state.tokens = tokens;
                            self.message = Some(StatusMessage {
                                text: format!("Loaded {} access tokens", state.tokens.len()),
                                level: MessageLevel::Success,
                            });
                        }
                        Err(e) => {
                            self.message = Some(StatusMessage {
                                text: format!("Failed to load access tokens: {e}"),
                                level: MessageLevel::Error,
                            });
                        }
                    }
                }
            }

            Event::AccessTokenIssued(result) => {
                self.input_mode = InputMode::Normal;
                match result {
                    Ok(token) => {
                        // Show the token in a special dialog (one-time display)
                        self.input_mode = InputMode::ShowIssuedToken { token: token.clone() };
                        self.message = Some(StatusMessage {
                            text: "Access token issued - copy it now, it won't be shown again!".to_string(),
                            level: MessageLevel::Success,
                        });
                        // Refresh tokens list
                        if let Screen::AccessTokens(state) = &mut self.screen {
                            state.loading = true;
                        }
                    }
                    Err(e) => {
                        self.message = Some(StatusMessage {
                            text: format!("Failed to issue access token: {e}"),
                            level: MessageLevel::Error,
                        });
                    }
                }
            }

            Event::AccessTokenRevoked(result) => {
                self.input_mode = InputMode::Normal;
                match result {
                    Ok(id) => {
                        self.message = Some(StatusMessage {
                            text: format!("Revoked access token '{}'", id),
                            level: MessageLevel::Success,
                        });
                        // Refresh tokens list
                        if let Screen::AccessTokens(state) = &mut self.screen {
                            state.loading = true;
                        }
                    }
                    Err(e) => {
                        self.message = Some(StatusMessage {
                            text: format!("Failed to revoke access token: {e}"),
                            level: MessageLevel::Error,
                        });
                    }
                }
            }

            Event::AccountMetricsLoaded(result) => {
                if let Screen::MetricsView(state) = &mut self.screen {
                    state.loading = false;
                    match result {
                        Ok(metrics) => {
                            state.metrics = metrics;
                        }
                        Err(e) => {
                            self.message = Some(StatusMessage {
                                text: format!("Failed to load account metrics: {e}"),
                                level: MessageLevel::Error,
                            });
                        }
                    }
                }
            }

            Event::BasinMetricsLoaded(result) => {
                if let Screen::MetricsView(state) = &mut self.screen {
                    state.loading = false;
                    match result {
                        Ok(metrics) => {
                            state.metrics = metrics;
                        }
                        Err(e) => {
                            self.message = Some(StatusMessage {
                                text: format!("Failed to load basin metrics: {e}"),
                                level: MessageLevel::Error,
                            });
                        }
                    }
                }
            }

            Event::StreamMetricsLoaded(result) => {
                if let Screen::MetricsView(state) = &mut self.screen {
                    state.loading = false;
                    match result {
                        Ok(metrics) => {
                            state.metrics = metrics;
                        }
                        Err(e) => {
                            self.message = Some(StatusMessage {
                                text: format!("Failed to load stream metrics: {e}"),
                                level: MessageLevel::Error,
                            });
                        }
                    }
                }
            }

            Event::Error(e) => {
                self.message = Some(StatusMessage {
                    text: e.to_string(),
                    level: MessageLevel::Error,
                });
            }
        }
    }

    fn handle_key(&mut self, key: KeyEvent, tx: mpsc::UnboundedSender<Event>) {
        // Clear message on any keypress
        self.message = None;

        // Handle input mode first
        if !matches!(self.input_mode, InputMode::Normal) {
            self.handle_input_key(key, tx);
            return;
        }

        // Global keys
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc if self.show_help => {
                self.show_help = false;
                return;
            }
            KeyCode::Char('?') => {
                self.show_help = !self.show_help;
                return;
            }
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.should_quit = true;
                return;
            }
            KeyCode::Char('q') if !matches!(self.screen, Screen::Basins(_)) => {
                // q goes back except on basins screen where it quits
            }
            KeyCode::Char('q') => {
                self.should_quit = true;
                return;
            }
            _ => {}
        }

        if self.show_help {
            return;
        }

        // Tab key to switch between tabs (only on top-level screens)
        if key.code == KeyCode::Tab {
            match &self.screen {
                Screen::Basins(_) | Screen::AccessTokens(_) => {
                    self.switch_tab(tx.clone());
                    return;
                }
                _ => {}
            }
        }

        // Screen-specific keys - handle in place to avoid borrow issues
        match &self.screen {
            Screen::Splash => {} // Keys handled in run loop
            Screen::Basins(_) => self.handle_basins_key(key, tx),
            Screen::Streams(_) => self.handle_streams_key(key, tx),
            Screen::StreamDetail(_) => self.handle_stream_detail_key(key, tx),
            Screen::ReadView(_) => self.handle_read_view_key(key, tx),
            Screen::AppendView(_) => self.handle_append_view_key(key, tx),
            Screen::AccessTokens(_) => self.handle_access_tokens_key(key, tx),
            Screen::MetricsView(_) => self.handle_metrics_view_key(key, tx),
        }
    }

    fn handle_input_key(&mut self, key: KeyEvent, tx: mpsc::UnboundedSender<Event>) {
        // Handle IssueAccessToken submit separately to avoid borrow issues.
        // We need to extract values before calling the method since the match arm
        // holds borrows that conflict with the method call.
        if matches!(key.code, KeyCode::Char(' ') | KeyCode::Enter) {
            if let InputMode::IssueAccessToken {
                id,
                expiry,
                expiry_custom,
                basins_scope,
                basins_value,
                streams_scope,
                streams_value,
                tokens_scope,
                tokens_value,
                account_read,
                account_write,
                basin_read,
                basin_write,
                stream_read,
                stream_write,
                auto_prefix_streams,
                selected,
                editing,
            } = &self.input_mode
            {
                if *selected == 16 && !*editing && !id.is_empty() {
                    // Clone all values we need
                    let id = id.clone();
                    let expiry = *expiry;
                    let expiry_custom = expiry_custom.clone();
                    let basins_scope = *basins_scope;
                    let basins_value = basins_value.clone();
                    let streams_scope = *streams_scope;
                    let streams_value = streams_value.clone();
                    let tokens_scope = *tokens_scope;
                    let tokens_value = tokens_value.clone();
                    let account_read = *account_read;
                    let account_write = *account_write;
                    let basin_read = *basin_read;
                    let basin_write = *basin_write;
                    let stream_read = *stream_read;
                    let stream_write = *stream_write;
                    let auto_prefix_streams = *auto_prefix_streams;

                    // Now we can safely call the method
                    self.issue_access_token_v2(
                        id,
                        expiry,
                        expiry_custom,
                        basins_scope,
                        basins_value,
                        streams_scope,
                        streams_value,
                        tokens_scope,
                        tokens_value,
                        account_read,
                        account_write,
                        basin_read,
                        basin_write,
                        stream_read,
                        stream_write,
                        auto_prefix_streams,
                        tx,
                    );
                    return;
                }
            }
        }

        match &mut self.input_mode {
            InputMode::Normal => {}

            InputMode::CreateBasin {
                name,
                scope,
                create_stream_on_append,
                create_stream_on_read,
                storage_class,
                retention_policy,
                retention_age_input,
                timestamping_mode,
                timestamping_uncapped,
                delete_on_empty_enabled,
                delete_on_empty_min_age,
                selected,
                editing,
            } => {
                // Form fields:
                // 0: Name (text)
                // 1: Scope (cycle: AWS us-east-1)
                // 2: Storage Class (cycle: None/Standard/Express)
                // 3: Retention Policy (cycle: Infinite/Age)
                // 4: Retention Age (text, only if Age)
                // 5: Timestamping Mode (cycle: None/ClientPrefer/ClientRequire/Arrival)
                // 6: Timestamping Uncapped (toggle)
                // 7: Delete-on-empty (toggle)
                // 8: Delete-on-empty Min Age (text, only if enabled)
                // 9: Create Stream On Append (toggle)
                // 10: Create Stream On Read (toggle)
                // 11: Create button
                const FIELD_COUNT: usize = 12;

                if *editing {
                    // Text editing mode
                    match key.code {
                        KeyCode::Esc | KeyCode::Enter => {
                            *editing = false;
                        }
                        KeyCode::Backspace => {
                            if *selected == 0 {
                                name.pop();
                            } else if *selected == 4 {
                                retention_age_input.pop();
                            } else if *selected == 8 {
                                delete_on_empty_min_age.pop();
                            }
                        }
                        KeyCode::Char(c) => {
                            if *selected == 0 {
                                // Basin names: lowercase letters, numbers, hyphens
                                if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' {
                                    name.push(c);
                                }
                            } else if *selected == 4 {
                                // Retention age: alphanumeric for duration parsing
                                if c.is_ascii_alphanumeric() {
                                    retention_age_input.push(c);
                                }
                            } else if *selected == 8 {
                                // Delete-on-empty min age: alphanumeric for duration parsing
                                if c.is_ascii_alphanumeric() {
                                    delete_on_empty_min_age.push(c);
                                }
                            }
                        }
                        _ => {}
                    }
                } else {
                    match key.code {
                        KeyCode::Esc => {
                            self.input_mode = InputMode::Normal;
                        }
                        KeyCode::Up | KeyCode::Char('k') => {
                            if *selected > 0 {
                                *selected -= 1;
                                // Skip delete-on-empty min age if not enabled
                                if *selected == 8 && !*delete_on_empty_enabled {
                                    *selected = 7;
                                }
                                // Skip retention age if not using Age policy
                                if *selected == 4 && *retention_policy != RetentionPolicyOption::Age {
                                    *selected = 3;
                                }
                            }
                        }
                        KeyCode::Down | KeyCode::Char('j') => {
                            if *selected < FIELD_COUNT - 1 {
                                *selected += 1;
                                // Skip retention age if not using Age policy
                                if *selected == 4 && *retention_policy != RetentionPolicyOption::Age {
                                    *selected = 5;
                                }
                                // Skip delete-on-empty min age if not enabled
                                if *selected == 8 && !*delete_on_empty_enabled {
                                    *selected = 9;
                                }
                            }
                        }
                        KeyCode::Enter => {
                            match *selected {
                                0 => *editing = true, // Edit name
                                4 => {
                                    if *retention_policy == RetentionPolicyOption::Age {
                                        *editing = true; // Edit retention age
                                    }
                                }
                                8 => {
                                    if *delete_on_empty_enabled {
                                        *editing = true; // Edit delete-on-empty min age
                                    }
                                }
                                11 => {
                                    // Create button - validate and submit
                                    if name.len() >= 8 {
                                        // Extract all values to avoid borrow conflict
                                        let basin_name = name.clone();
                                        let basin_scope = *scope;
                                        let csoa = *create_stream_on_append;
                                        let csor = *create_stream_on_read;
                                        let sc = storage_class.clone();
                                        let rp = retention_policy.clone();
                                        let rai = retention_age_input.clone();
                                        let tm = timestamping_mode.clone();
                                        let tu = *timestamping_uncapped;
                                        let doe = *delete_on_empty_enabled;
                                        let doema = delete_on_empty_min_age.clone();

                                        let config = build_basin_config(csoa, csor, sc, rp, rai, tm, tu, doe, doema);
                                        self.create_basin_with_config(basin_name, basin_scope, config, tx.clone());
                                    }
                                }
                                _ => {}
                            }
                        }
                        KeyCode::Char(' ') => {
                            // Toggle for boolean fields
                            match *selected {
                                6 => *timestamping_uncapped = !*timestamping_uncapped,
                                9 => *create_stream_on_append = !*create_stream_on_append,
                                10 => *create_stream_on_read = !*create_stream_on_read,
                                _ => {}
                            }
                        }
                        KeyCode::Left | KeyCode::Char('h') => {
                            // Cycle left for enum fields
                            match *selected {
                                1 => {
                                    // Scope: currently only AWS us-east-1, so no cycling
                                }
                                2 => {
                                    *storage_class = match storage_class {
                                        None => Some(StorageClass::Express),
                                        Some(StorageClass::Standard) => None,
                                        Some(StorageClass::Express) => Some(StorageClass::Standard),
                                    };
                                }
                                3 => {
                                    *retention_policy = match retention_policy {
                                        RetentionPolicyOption::Infinite => RetentionPolicyOption::Age,
                                        RetentionPolicyOption::Age => RetentionPolicyOption::Infinite,
                                    };
                                }
                                5 => {
                                    *timestamping_mode = match timestamping_mode {
                                        None => Some(TimestampingMode::Arrival),
                                        Some(TimestampingMode::ClientPrefer) => None,
                                        Some(TimestampingMode::ClientRequire) => Some(TimestampingMode::ClientPrefer),
                                        Some(TimestampingMode::Arrival) => Some(TimestampingMode::ClientRequire),
                                    };
                                }
                                7 => {
                                    // Delete on empty: Never <-> After threshold
                                    *delete_on_empty_enabled = !*delete_on_empty_enabled;
                                }
                                _ => {}
                            }
                        }
                        KeyCode::Right | KeyCode::Char('l') => {
                            // Cycle right for enum fields
                            match *selected {
                                1 => {
                                    // Scope: currently only AWS us-east-1, so no cycling
                                }
                                2 => {
                                    *storage_class = match storage_class {
                                        None => Some(StorageClass::Standard),
                                        Some(StorageClass::Standard) => Some(StorageClass::Express),
                                        Some(StorageClass::Express) => None,
                                    };
                                }
                                3 => {
                                    *retention_policy = match retention_policy {
                                        RetentionPolicyOption::Infinite => RetentionPolicyOption::Age,
                                        RetentionPolicyOption::Age => RetentionPolicyOption::Infinite,
                                    };
                                }
                                5 => {
                                    *timestamping_mode = match timestamping_mode {
                                        None => Some(TimestampingMode::ClientPrefer),
                                        Some(TimestampingMode::ClientPrefer) => Some(TimestampingMode::ClientRequire),
                                        Some(TimestampingMode::ClientRequire) => Some(TimestampingMode::Arrival),
                                        Some(TimestampingMode::Arrival) => None,
                                    };
                                }
                                7 => {
                                    // Delete on empty: Never <-> After threshold
                                    *delete_on_empty_enabled = !*delete_on_empty_enabled;
                                }
                                _ => {}
                            }
                        }
                        _ => {}
                    }
                }
            }

            InputMode::CreateStream {
                basin,
                name,
                storage_class,
                retention_policy,
                retention_age_input,
                timestamping_mode,
                timestamping_uncapped,
                delete_on_empty_enabled,
                delete_on_empty_min_age,
                selected,
                editing,
            } => {
                // Form fields:
                // 0: Name (text)
                // 1: Storage Class (cycle: None/Standard/Express)
                // 2: Retention Policy (cycle: Infinite/Age)
                // 3: Retention Age (text, only if Age)
                // 4: Timestamping Mode (cycle: None/ClientPrefer/ClientRequire/Arrival)
                // 5: Timestamping Uncapped (toggle)
                // 6: Delete-on-empty (cycle: Never/After threshold)
                // 7: Delete-on-empty Min Age (text, only if enabled)
                // 8: Create button
                const FIELD_COUNT: usize = 9;

                if *editing {
                    // Text editing mode
                    match key.code {
                        KeyCode::Esc | KeyCode::Enter => {
                            *editing = false;
                        }
                        KeyCode::Backspace => {
                            if *selected == 0 {
                                name.pop();
                            } else if *selected == 3 {
                                retention_age_input.pop();
                            } else if *selected == 7 {
                                delete_on_empty_min_age.pop();
                            }
                        }
                        KeyCode::Char(c) => {
                            if *selected == 0 {
                                // Stream names: allow most characters
                                name.push(c);
                            } else if *selected == 3 {
                                // Retention age: alphanumeric for duration parsing
                                if c.is_ascii_alphanumeric() {
                                    retention_age_input.push(c);
                                }
                            } else if *selected == 7 {
                                // Delete-on-empty min age: alphanumeric for duration parsing
                                if c.is_ascii_alphanumeric() {
                                    delete_on_empty_min_age.push(c);
                                }
                            }
                        }
                        _ => {}
                    }
                } else {
                    match key.code {
                        KeyCode::Esc => {
                            self.input_mode = InputMode::Normal;
                        }
                        KeyCode::Up | KeyCode::Char('k') => {
                            if *selected > 0 {
                                *selected -= 1;
                                // Skip delete-on-empty min age if not enabled
                                if *selected == 7 && !*delete_on_empty_enabled {
                                    *selected = 6;
                                }
                                // Skip retention age if not using Age policy
                                if *selected == 3 && *retention_policy != RetentionPolicyOption::Age {
                                    *selected = 2;
                                }
                            }
                        }
                        KeyCode::Down | KeyCode::Char('j') => {
                            if *selected < FIELD_COUNT - 1 {
                                *selected += 1;
                                // Skip retention age if not using Age policy
                                if *selected == 3 && *retention_policy != RetentionPolicyOption::Age {
                                    *selected = 4;
                                }
                                // Skip delete-on-empty min age if not enabled
                                if *selected == 7 && !*delete_on_empty_enabled {
                                    *selected = 8;
                                }
                            }
                        }
                        KeyCode::Enter => {
                            match *selected {
                                0 => *editing = true, // Edit name
                                3 => {
                                    if *retention_policy == RetentionPolicyOption::Age {
                                        *editing = true; // Edit retention age
                                    }
                                }
                                7 => {
                                    if *delete_on_empty_enabled {
                                        *editing = true; // Edit delete-on-empty min age
                                    }
                                }
                                8 => {
                                    // Create button - validate and submit
                                    if !name.is_empty() {
                                        let basin_name = basin.clone();
                                        let stream_name = name.clone();
                                        let sc = storage_class.clone();
                                        let rp = retention_policy.clone();
                                        let rai = retention_age_input.clone();
                                        let tm = timestamping_mode.clone();
                                        let tu = *timestamping_uncapped;
                                        let doe = *delete_on_empty_enabled;
                                        let doema = delete_on_empty_min_age.clone();

                                        let config = build_stream_config(sc, rp, rai, tm, tu, doe, doema);
                                        self.create_stream_with_config(basin_name, stream_name, config, tx.clone());
                                    }
                                }
                                _ => {}
                            }
                        }
                        KeyCode::Char(' ') => {
                            // Toggle for boolean fields
                            if *selected == 5 {
                                *timestamping_uncapped = !*timestamping_uncapped;
                            }
                        }
                        KeyCode::Left | KeyCode::Char('h') => {
                            // Cycle left for enum fields
                            match *selected {
                                1 => {
                                    *storage_class = match storage_class {
                                        None => Some(StorageClass::Express),
                                        Some(StorageClass::Standard) => None,
                                        Some(StorageClass::Express) => Some(StorageClass::Standard),
                                    };
                                }
                                2 => {
                                    *retention_policy = match retention_policy {
                                        RetentionPolicyOption::Infinite => RetentionPolicyOption::Age,
                                        RetentionPolicyOption::Age => RetentionPolicyOption::Infinite,
                                    };
                                }
                                4 => {
                                    *timestamping_mode = match timestamping_mode {
                                        None => Some(TimestampingMode::Arrival),
                                        Some(TimestampingMode::ClientPrefer) => None,
                                        Some(TimestampingMode::ClientRequire) => Some(TimestampingMode::ClientPrefer),
                                        Some(TimestampingMode::Arrival) => Some(TimestampingMode::ClientRequire),
                                    };
                                }
                                6 => {
                                    // Delete on empty: Never <-> After threshold
                                    *delete_on_empty_enabled = !*delete_on_empty_enabled;
                                }
                                _ => {}
                            }
                        }
                        KeyCode::Right | KeyCode::Char('l') => {
                            // Cycle right for enum fields
                            match *selected {
                                1 => {
                                    *storage_class = match storage_class {
                                        None => Some(StorageClass::Standard),
                                        Some(StorageClass::Standard) => Some(StorageClass::Express),
                                        Some(StorageClass::Express) => None,
                                    };
                                }
                                2 => {
                                    *retention_policy = match retention_policy {
                                        RetentionPolicyOption::Infinite => RetentionPolicyOption::Age,
                                        RetentionPolicyOption::Age => RetentionPolicyOption::Infinite,
                                    };
                                }
                                4 => {
                                    *timestamping_mode = match timestamping_mode {
                                        None => Some(TimestampingMode::ClientPrefer),
                                        Some(TimestampingMode::ClientPrefer) => Some(TimestampingMode::ClientRequire),
                                        Some(TimestampingMode::ClientRequire) => Some(TimestampingMode::Arrival),
                                        Some(TimestampingMode::Arrival) => None,
                                    };
                                }
                                6 => {
                                    // Delete on empty: Never <-> After threshold
                                    *delete_on_empty_enabled = !*delete_on_empty_enabled;
                                }
                                _ => {}
                            }
                        }
                        _ => {}
                    }
                }
            }

            InputMode::ConfirmDeleteBasin { basin } => {
                match key.code {
                    KeyCode::Esc | KeyCode::Char('n') | KeyCode::Char('N') => {
                        self.input_mode = InputMode::Normal;
                    }
                    KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => {
                        let basin = basin.clone();
                        self.delete_basin(basin, tx.clone());
                    }
                    _ => {}
                }
            }

            InputMode::ConfirmDeleteStream { basin, stream } => {
                match key.code {
                    KeyCode::Esc | KeyCode::Char('n') | KeyCode::Char('N') => {
                        self.input_mode = InputMode::Normal;
                    }
                    KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => {
                        let basin = basin.clone();
                        let stream = stream.clone();
                        self.delete_stream(basin, stream, tx.clone());
                    }
                    _ => {}
                }
            }

            InputMode::ReconfigureBasin {
                basin,
                create_stream_on_append,
                create_stream_on_read,
                storage_class,
                retention_policy,
                retention_age_secs,
                timestamping_mode,
                timestamping_uncapped,
                selected,
                editing_age,
                age_input,
            } => {
                // Field indices:
                // 0: Storage class
                // 1: Retention policy
                // 2: Retention age (if Age-based)
                // 3: Timestamping mode
                // 4: Timestamping uncapped
                // 5: Create on append
                // 6: Create on read
                const BASIN_MAX_ROW: usize = 6;

                // If editing age, handle number input
                if *editing_age {
                    match key.code {
                        KeyCode::Esc | KeyCode::Enter => {
                            // Parse and apply the age
                            if let Ok(secs) = age_input.parse::<u64>() {
                                *retention_age_secs = secs;
                            }
                            *editing_age = false;
                        }
                        KeyCode::Backspace => {
                            age_input.pop();
                        }
                        KeyCode::Char(c) if c.is_ascii_digit() => {
                            age_input.push(c);
                        }
                        _ => {}
                    }
                    return;
                }

                match key.code {
                    KeyCode::Esc => {
                        self.input_mode = InputMode::Normal;
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        if *selected > 0 {
                            *selected -= 1;
                            // Skip retention age if not using Age policy
                            if *selected == 2 && *retention_policy != RetentionPolicyOption::Age {
                                *selected = 1;
                            }
                        }
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        if *selected < BASIN_MAX_ROW {
                            *selected += 1;
                            // Skip retention age if not using Age policy
                            if *selected == 2 && *retention_policy != RetentionPolicyOption::Age {
                                *selected = 3;
                            }
                        }
                    }
                    KeyCode::Char(' ') => {
                        // Toggle for boolean fields
                        match *selected {
                            4 => *timestamping_uncapped = Some(!timestamping_uncapped.unwrap_or(false)),
                            5 => *create_stream_on_append = Some(!create_stream_on_append.unwrap_or(false)),
                            6 => *create_stream_on_read = Some(!create_stream_on_read.unwrap_or(false)),
                            _ => {}
                        }
                    }
                    KeyCode::Enter => {
                        // Edit text fields
                        if *selected == 2 && *retention_policy == RetentionPolicyOption::Age {
                            *editing_age = true;
                            *age_input = retention_age_secs.to_string();
                        }
                    }
                    KeyCode::Left | KeyCode::Char('h') => {
                        // Cycle left for enum fields
                        match *selected {
                            0 => {
                                *storage_class = match storage_class {
                                    None => Some(StorageClass::Express),
                                    Some(StorageClass::Standard) => None,
                                    Some(StorageClass::Express) => Some(StorageClass::Standard),
                                };
                            }
                            1 => {
                                *retention_policy = match retention_policy {
                                    RetentionPolicyOption::Infinite => RetentionPolicyOption::Age,
                                    RetentionPolicyOption::Age => RetentionPolicyOption::Infinite,
                                };
                            }
                            3 => {
                                *timestamping_mode = match timestamping_mode {
                                    None => Some(TimestampingMode::Arrival),
                                    Some(TimestampingMode::ClientPrefer) => None,
                                    Some(TimestampingMode::ClientRequire) => Some(TimestampingMode::ClientPrefer),
                                    Some(TimestampingMode::Arrival) => Some(TimestampingMode::ClientRequire),
                                };
                            }
                            _ => {}
                        }
                    }
                    KeyCode::Right | KeyCode::Char('l') => {
                        // Cycle right for enum fields
                        match *selected {
                            0 => {
                                *storage_class = match storage_class {
                                    None => Some(StorageClass::Standard),
                                    Some(StorageClass::Standard) => Some(StorageClass::Express),
                                    Some(StorageClass::Express) => None,
                                };
                            }
                            1 => {
                                *retention_policy = match retention_policy {
                                    RetentionPolicyOption::Infinite => RetentionPolicyOption::Age,
                                    RetentionPolicyOption::Age => RetentionPolicyOption::Infinite,
                                };
                            }
                            3 => {
                                *timestamping_mode = match timestamping_mode {
                                    None => Some(TimestampingMode::ClientPrefer),
                                    Some(TimestampingMode::ClientPrefer) => Some(TimestampingMode::ClientRequire),
                                    Some(TimestampingMode::ClientRequire) => Some(TimestampingMode::Arrival),
                                    Some(TimestampingMode::Arrival) => None,
                                };
                            }
                            _ => {}
                        }
                    }
                    KeyCode::Char('s') => {
                        let b = basin.clone();
                        let config = BasinReconfigureConfig {
                            create_stream_on_append: *create_stream_on_append,
                            create_stream_on_read: *create_stream_on_read,
                            storage_class: storage_class.clone(),
                            retention_policy: *retention_policy,
                            retention_age_secs: *retention_age_secs,
                            timestamping_mode: timestamping_mode.clone(),
                            timestamping_uncapped: *timestamping_uncapped,
                        };
                        self.reconfigure_basin(b, config, tx.clone());
                    }
                    _ => {}
                }
            }

            InputMode::ReconfigureStream {
                basin,
                stream,
                storage_class,
                retention_policy,
                retention_age_secs,
                timestamping_mode,
                timestamping_uncapped,
                delete_on_empty_enabled,
                delete_on_empty_min_age,
                selected,
                editing_age,
                age_input,
            } => {
                // If editing age or delete-on-empty min age, handle text input
                if *editing_age {
                    match key.code {
                        KeyCode::Esc | KeyCode::Enter => {
                            // Check which field we're editing
                            if *selected == 2 {
                                // Retention age
                                if let Ok(secs) = age_input.parse::<u64>() {
                                    *retention_age_secs = secs;
                                }
                            } else if *selected == 6 {
                                // Delete-on-empty min age - no parsing needed, store as string
                            }
                            *editing_age = false;
                        }
                        KeyCode::Backspace => {
                            if *selected == 2 {
                                age_input.pop();
                            } else if *selected == 6 {
                                delete_on_empty_min_age.pop();
                            }
                        }
                        KeyCode::Char(c) => {
                            if *selected == 2 && c.is_ascii_digit() {
                                age_input.push(c);
                            } else if *selected == 6 && c.is_ascii_alphanumeric() {
                                delete_on_empty_min_age.push(c);
                            }
                        }
                        _ => {}
                    }
                    return;
                }

                // Stream has 7 rows:
                // 0: Storage class
                // 1: Retention policy
                // 2: Retention age (if Age-based)
                // 3: Timestamping mode
                // 4: Timestamping uncapped
                // 5: Delete on empty
                // 6: Delete on empty threshold (if enabled)
                const STREAM_MAX_ROW: usize = 6;

                match key.code {
                    KeyCode::Esc => {
                        self.input_mode = InputMode::Normal;
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        if *selected > 0 {
                            *selected -= 1;
                            // Skip delete-on-empty threshold if not enabled
                            if *selected == 6 && !*delete_on_empty_enabled {
                                *selected = 5;
                            }
                            // Skip retention age if not using Age policy
                            if *selected == 2 && *retention_policy != RetentionPolicyOption::Age {
                                *selected = 1;
                            }
                        }
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        if *selected < STREAM_MAX_ROW {
                            *selected += 1;
                            // Skip retention age if not using Age policy
                            if *selected == 2 && *retention_policy != RetentionPolicyOption::Age {
                                *selected = 3;
                            }
                            // Skip delete-on-empty threshold if not enabled
                            if *selected == 6 && !*delete_on_empty_enabled {
                                // Already at max, stay at 5
                                *selected = 5;
                            }
                        }
                    }
                    KeyCode::Char(' ') => {
                        // Toggle for boolean fields
                        if *selected == 4 {
                            *timestamping_uncapped = Some(!timestamping_uncapped.unwrap_or(false));
                        }
                    }
                    KeyCode::Enter => {
                        // Edit text fields
                        if *selected == 2 && *retention_policy == RetentionPolicyOption::Age {
                            *editing_age = true;
                            *age_input = retention_age_secs.to_string();
                        } else if *selected == 6 && *delete_on_empty_enabled {
                            *editing_age = true;
                        }
                    }
                    KeyCode::Left | KeyCode::Char('h') => {
                        // Cycle left for enum fields
                        match *selected {
                            0 => {
                                *storage_class = match storage_class {
                                    None => Some(StorageClass::Express),
                                    Some(StorageClass::Standard) => None,
                                    Some(StorageClass::Express) => Some(StorageClass::Standard),
                                };
                            }
                            1 => {
                                *retention_policy = match retention_policy {
                                    RetentionPolicyOption::Infinite => RetentionPolicyOption::Age,
                                    RetentionPolicyOption::Age => RetentionPolicyOption::Infinite,
                                };
                            }
                            3 => {
                                *timestamping_mode = match timestamping_mode {
                                    None => Some(TimestampingMode::Arrival),
                                    Some(TimestampingMode::ClientPrefer) => None,
                                    Some(TimestampingMode::ClientRequire) => Some(TimestampingMode::ClientPrefer),
                                    Some(TimestampingMode::Arrival) => Some(TimestampingMode::ClientRequire),
                                };
                            }
                            5 => {
                                // Delete on empty: Never <-> After threshold
                                *delete_on_empty_enabled = !*delete_on_empty_enabled;
                            }
                            _ => {}
                        }
                    }
                    KeyCode::Right | KeyCode::Char('l') => {
                        // Cycle right for enum fields
                        match *selected {
                            0 => {
                                *storage_class = match storage_class {
                                    None => Some(StorageClass::Standard),
                                    Some(StorageClass::Standard) => Some(StorageClass::Express),
                                    Some(StorageClass::Express) => None,
                                };
                            }
                            1 => {
                                *retention_policy = match retention_policy {
                                    RetentionPolicyOption::Infinite => RetentionPolicyOption::Age,
                                    RetentionPolicyOption::Age => RetentionPolicyOption::Infinite,
                                };
                            }
                            3 => {
                                *timestamping_mode = match timestamping_mode {
                                    None => Some(TimestampingMode::ClientPrefer),
                                    Some(TimestampingMode::ClientPrefer) => Some(TimestampingMode::ClientRequire),
                                    Some(TimestampingMode::ClientRequire) => Some(TimestampingMode::Arrival),
                                    Some(TimestampingMode::Arrival) => None,
                                };
                            }
                            5 => {
                                // Delete on empty: Never <-> After threshold
                                *delete_on_empty_enabled = !*delete_on_empty_enabled;
                            }
                            _ => {}
                        }
                    }
                    KeyCode::Char('s') => {
                        let b = basin.clone();
                        let s = stream.clone();
                        let config = StreamReconfigureConfig {
                            storage_class: storage_class.clone(),
                            retention_policy: *retention_policy,
                            retention_age_secs: *retention_age_secs,
                            timestamping_mode: timestamping_mode.clone(),
                            timestamping_uncapped: *timestamping_uncapped,
                            delete_on_empty_enabled: *delete_on_empty_enabled,
                            delete_on_empty_min_age: delete_on_empty_min_age.clone(),
                        };
                        self.reconfigure_stream(b, s, config, tx.clone());
                    }
                    _ => {}
                }
            }

            InputMode::CustomRead {
                basin,
                stream,
                start_from,
                seq_num_value,
                timestamp_value,
                ago_value,
                ago_unit,
                tail_offset_value,
                count_limit,
                byte_limit,
                until_timestamp,
                clamp,
                format,
                output_file,
                selected,
                editing,
            } => {
                // If editing a value, handle text input
                if *editing {
                    match key.code {
                        KeyCode::Esc | KeyCode::Enter => {
                            *editing = false;
                        }
                        KeyCode::Tab if *selected == 2 => {
                            // Cycle time unit while editing ago value
                            *ago_unit = ago_unit.next();
                        }
                        KeyCode::Backspace => {
                            match *selected {
                                0 => { seq_num_value.pop(); }
                                1 => { timestamp_value.pop(); }
                                2 => { ago_value.pop(); }
                                3 => { tail_offset_value.pop(); }
                                4 => { count_limit.pop(); }
                                5 => { byte_limit.pop(); }
                                6 => { until_timestamp.pop(); }
                                9 => { output_file.pop(); }
                                _ => {}
                            }
                        }
                        KeyCode::Char(c) if c.is_ascii_digit() => {
                            match *selected {
                                0 => seq_num_value.push(c),
                                1 => timestamp_value.push(c),
                                2 => ago_value.push(c),
                                3 => tail_offset_value.push(c),
                                4 => count_limit.push(c),
                                5 => byte_limit.push(c),
                                6 => until_timestamp.push(c),
                                _ => {}
                            }
                        }
                        KeyCode::Char(c) if *selected == 9 => {
                            // Output file accepts any printable char
                            output_file.push(c);
                        }
                        _ => {}
                    }
                    return;
                }

                // Navigation layout:
                // 0: Sequence number (radio + input)
                // 1: Timestamp (radio + input)
                // 2: Time ago (radio + input, tab=unit)
                // 3: Tail offset (radio + input)
                // 4: Max records
                // 5: Max bytes
                // 6: Until timestamp
                // 7: Clamp (checkbox)
                // 8: Format (selector)
                // 9: Output file
                // 10: Start button
                const MAX_ROW: usize = 10;

                match key.code {
                    KeyCode::Esc => {
                        self.input_mode = InputMode::Normal;
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        if *selected > 0 {
                            *selected -= 1;
                        }
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        if *selected < MAX_ROW {
                            *selected += 1;
                        }
                    }
                    KeyCode::Tab if *selected == 2 => {
                        // Cycle time unit for ago
                        *ago_unit = ago_unit.next();
                    }
                    KeyCode::Char(' ') => {
                        // Space = select/toggle
                        match *selected {
                            0 => *start_from = ReadStartFrom::SeqNum,
                            1 => *start_from = ReadStartFrom::Timestamp,
                            2 => *start_from = ReadStartFrom::Ago,
                            3 => *start_from = ReadStartFrom::TailOffset,
                            7 => *clamp = !*clamp,
                            8 => *format = format.next(),
                            _ => {}
                        }
                    }
                    KeyCode::Enter => {
                        // Enter = select + edit value, toggle, or run
                        match *selected {
                            0 => {
                                *start_from = ReadStartFrom::SeqNum;
                                *editing = true;
                            }
                            1 => {
                                *start_from = ReadStartFrom::Timestamp;
                                *editing = true;
                            }
                            2 => {
                                *start_from = ReadStartFrom::Ago;
                                *editing = true;
                            }
                            3 => {
                                *start_from = ReadStartFrom::TailOffset;
                                *editing = true;
                            }
                            4 => *editing = true, // count_limit
                            5 => *editing = true, // byte_limit
                            6 => *editing = true, // until_timestamp
                            7 => *clamp = !*clamp,
                            8 => *format = format.next(),
                            9 => *editing = true, // output_file
                            10 => {
                                // Start reading - clone all values first
                                let b = basin.clone();
                                let s = stream.clone();
                                let sf = *start_from;
                                let snv = seq_num_value.clone();
                                let tsv = timestamp_value.clone();
                                let agv = ago_value.clone();
                                let agu = *ago_unit;
                                let tov = tail_offset_value.clone();
                                let cl = count_limit.clone();
                                let bl = byte_limit.clone();
                                let ut = until_timestamp.clone();
                                let clp = *clamp;
                                let fmt = *format;
                                let of = output_file.clone();
                                self.input_mode = InputMode::Normal;
                                // Show message if writing to file
                                if !of.is_empty() {
                                    self.message = Some(StatusMessage {
                                        text: format!("Writing to {}", of),
                                        level: MessageLevel::Info,
                                    });
                                }
                                self.start_custom_read(b, s, sf, snv, tsv, agv, agu, tov, cl, bl, ut, clp, fmt, of, tx.clone());
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }

            InputMode::Fence {
                basin,
                stream,
                new_token,
                current_token,
                selected,
                editing,
            } => {
                if *editing {
                    match key.code {
                        KeyCode::Esc | KeyCode::Enter => {
                            *editing = false;
                        }
                        KeyCode::Backspace => {
                            match *selected {
                                0 => { new_token.pop(); }
                                1 => { current_token.pop(); }
                                _ => {}
                            }
                        }
                        KeyCode::Char(c) => {
                            match *selected {
                                0 => new_token.push(c),
                                1 => current_token.push(c),
                                _ => {}
                            }
                        }
                        _ => {}
                    }
                    return;
                }

                // Navigation: 0=new_token, 1=current_token, 2=submit
                match key.code {
                    KeyCode::Esc => {
                        self.input_mode = InputMode::Normal;
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        if *selected > 0 {
                            *selected -= 1;
                        }
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        if *selected < 2 {
                            *selected += 1;
                        }
                    }
                    KeyCode::Enter => {
                        match *selected {
                            0 | 1 => *editing = true,
                            2 => {
                                // Submit fence
                                if !new_token.is_empty() {
                                    let b = basin.clone();
                                    let s = stream.clone();
                                    let nt = new_token.clone();
                                    let ct = if current_token.is_empty() {
                                        None
                                    } else {
                                        Some(current_token.clone())
                                    };
                                    self.fence_stream(b, s, nt, ct, tx.clone());
                                }
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }

            InputMode::Trim {
                basin,
                stream,
                trim_point,
                fencing_token,
                selected,
                editing,
            } => {
                if *editing {
                    match key.code {
                        KeyCode::Esc | KeyCode::Enter => {
                            *editing = false;
                        }
                        KeyCode::Backspace => {
                            match *selected {
                                0 => { trim_point.pop(); }
                                1 => { fencing_token.pop(); }
                                _ => {}
                            }
                        }
                        KeyCode::Char(c) => {
                            match *selected {
                                0 if c.is_ascii_digit() => trim_point.push(c),
                                1 => fencing_token.push(c),
                                _ => {}
                            }
                        }
                        _ => {}
                    }
                    return;
                }

                // Navigation: 0=trim_point, 1=fencing_token, 2=submit
                match key.code {
                    KeyCode::Esc => {
                        self.input_mode = InputMode::Normal;
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        if *selected > 0 {
                            *selected -= 1;
                        }
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        if *selected < 2 {
                            *selected += 1;
                        }
                    }
                    KeyCode::Enter => {
                        match *selected {
                            0 | 1 => *editing = true,
                            2 => {
                                // Submit trim
                                if let Ok(tp) = trim_point.parse::<u64>() {
                                    let b = basin.clone();
                                    let s = stream.clone();
                                    let ft = if fencing_token.is_empty() {
                                        None
                                    } else {
                                        Some(fencing_token.clone())
                                    };
                                    self.trim_stream(b, s, tp, ft, tx.clone());
                                }
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }

            InputMode::IssueAccessToken {
                id,
                expiry,
                expiry_custom,
                basins_scope,
                basins_value,
                streams_scope,
                streams_value,
                tokens_scope,
                tokens_value,
                account_read,
                account_write,
                basin_read,
                basin_write,
                stream_read,
                stream_write,
                auto_prefix_streams,
                selected,
                editing,
            } => {
                // Fields: 0=id, 1=expiry, 2=expiry_custom, 3=basins_scope, 4=basins_value,
                //         5=streams_scope, 6=streams_value, 7=tokens_scope, 8=tokens_value,
                //         9=account_read, 10=account_write, 11=basin_read, 12=basin_write,
                //         13=stream_read, 14=stream_write, 15=auto_prefix, 16=submit
                const MAX_FIELD: usize = 16;

                if *editing {
                    match key.code {
                        KeyCode::Esc | KeyCode::Enter => {
                            *editing = false;
                        }
                        KeyCode::Backspace => {
                            match *selected {
                                0 => { id.pop(); }
                                2 => { expiry_custom.pop(); }
                                4 => { basins_value.pop(); }
                                6 => { streams_value.pop(); }
                                8 => { tokens_value.pop(); }
                                _ => {}
                            }
                        }
                        KeyCode::Char(c) => {
                            match *selected {
                                0 => {
                                    // Token ID: letters, numbers, hyphens, underscores
                                    if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                                        id.push(c);
                                    }
                                }
                                2 => {
                                    // Custom expiry: e.g., "30d", "1w", "24h"
                                    if c.is_ascii_alphanumeric() {
                                        expiry_custom.push(c);
                                    }
                                }
                                4 => basins_value.push(c),
                                6 => streams_value.push(c),
                                8 => tokens_value.push(c),
                                _ => {}
                            }
                        }
                        _ => {}
                    }
                    return;
                }

                match key.code {
                    KeyCode::Esc => {
                        self.input_mode = InputMode::Normal;
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        if *selected > 0 {
                            *selected -= 1;
                            // Skip value fields if scope doesn't need them
                            if *selected == 2 && *expiry != ExpiryOption::Custom {
                                *selected = 1;
                            }
                            if *selected == 4 && !matches!(basins_scope, ScopeOption::Prefix | ScopeOption::Exact) {
                                *selected = 3;
                            }
                            if *selected == 6 && !matches!(streams_scope, ScopeOption::Prefix | ScopeOption::Exact) {
                                *selected = 5;
                            }
                            if *selected == 8 && !matches!(tokens_scope, ScopeOption::Prefix | ScopeOption::Exact) {
                                *selected = 7;
                            }
                        }
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        if *selected < MAX_FIELD {
                            *selected += 1;
                            // Skip value fields if scope doesn't need them
                            if *selected == 2 && *expiry != ExpiryOption::Custom {
                                *selected = 3;
                            }
                            if *selected == 4 && !matches!(basins_scope, ScopeOption::Prefix | ScopeOption::Exact) {
                                *selected = 5;
                            }
                            if *selected == 6 && !matches!(streams_scope, ScopeOption::Prefix | ScopeOption::Exact) {
                                *selected = 7;
                            }
                            if *selected == 8 && !matches!(tokens_scope, ScopeOption::Prefix | ScopeOption::Exact) {
                                *selected = 9;
                            }
                        }
                    }
                    KeyCode::Left | KeyCode::Right => {
                        let forward = key.code == KeyCode::Right;
                        match *selected {
                            1 => *expiry = if forward { expiry.next() } else { expiry.prev() },
                            3 => *basins_scope = if forward { basins_scope.next() } else { basins_scope.prev() },
                            5 => *streams_scope = if forward { streams_scope.next() } else { streams_scope.prev() },
                            7 => *tokens_scope = if forward { tokens_scope.next() } else { tokens_scope.prev() },
                            _ => {}
                        }
                    }
                    KeyCode::Char(' ') | KeyCode::Enter => {
                        match *selected {
                            // Text inputs
                            0 | 2 | 4 | 6 | 8 => *editing = true,
                            // Cycle options
                            1 => *expiry = expiry.next(),
                            3 => *basins_scope = basins_scope.next(),
                            5 => *streams_scope = streams_scope.next(),
                            7 => *tokens_scope = tokens_scope.next(),
                            // Toggle checkboxes
                            9 => *account_read = !*account_read,
                            10 => *account_write = !*account_write,
                            11 => *basin_read = !*basin_read,
                            12 => *basin_write = !*basin_write,
                            13 => *stream_read = !*stream_read,
                            14 => *stream_write = !*stream_write,
                            15 => *auto_prefix_streams = !*auto_prefix_streams,
                            // Submit case (16) is handled before the match to avoid borrow issues
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }

            InputMode::ConfirmRevokeToken { token_id } => {
                match key.code {
                    KeyCode::Esc | KeyCode::Char('n') | KeyCode::Char('N') => {
                        self.input_mode = InputMode::Normal;
                    }
                    KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => {
                        let id = token_id.clone();
                        self.revoke_access_token(id, tx.clone());
                    }
                    _ => {}
                }
            }

            InputMode::ShowIssuedToken { .. } => {
                // Any key dismisses the token display
                match key.code {
                    KeyCode::Esc | KeyCode::Enter | KeyCode::Char(_) => {
                        self.input_mode = InputMode::Normal;
                    }
                    _ => {}
                }
            }

            InputMode::ViewTokenDetail { .. } => {
                // Esc or Enter to close detail view
                match key.code {
                    KeyCode::Esc | KeyCode::Enter | KeyCode::Char('q') => {
                        self.input_mode = InputMode::Normal;
                    }
                    _ => {}
                }
            }
        }
    }

    fn handle_basins_key(&mut self, key: KeyEvent, tx: mpsc::UnboundedSender<Event>) {
        let Screen::Basins(state) = &mut self.screen else {
            return;
        };

        // Handle filter mode
        if state.filter_active {
            match key.code {
                KeyCode::Esc => {
                    state.filter_active = false;
                    state.filter.clear();
                    state.selected = 0;
                }
                KeyCode::Enter => {
                    state.filter_active = false;
                }
                KeyCode::Backspace => {
                    state.filter.pop();
                    state.selected = 0;
                }
                KeyCode::Char(c) => {
                    state.filter.push(c);
                    state.selected = 0;
                }
                _ => {}
            }
            return;
        }

        // Get filtered list length for bounds checking
        let filtered: Vec<_> = state.basins.iter()
            .filter(|b| state.filter.is_empty() || b.name.to_string().contains(&state.filter))
            .collect();
        let filtered_len = filtered.len();

        match key.code {
            KeyCode::Char('/') => {
                state.filter_active = true;
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if state.selected > 0 {
                    state.selected -= 1;
                }
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if filtered_len > 0 && state.selected < filtered_len - 1 {
                    state.selected += 1;
                }
            }
            KeyCode::Char('g') => {
                state.selected = 0;
            }
            KeyCode::Char('G') => {
                if filtered_len > 0 {
                    state.selected = filtered_len - 1;
                }
            }
            KeyCode::Enter => {
                if let Some(basin) = filtered.get(state.selected) {
                    let basin_name = basin.name.clone();
                    self.screen = Screen::Streams(StreamsState {
                        basin_name: basin_name.clone(),
                        streams: Vec::new(),
                        selected: 0,
                        loading: true,
                        filter: String::new(),
                        filter_active: false,
                    });
                    self.load_streams(basin_name, tx);
                }
            }
            KeyCode::Char('r') => {
                state.loading = true;
                state.filter.clear();
                state.selected = 0;
                self.load_basins(tx);
            }
            KeyCode::Char('c') => {
                self.input_mode = InputMode::CreateBasin {
                    name: String::new(),
                    scope: BasinScopeOption::AwsUsEast1,
                    create_stream_on_append: false,
                    create_stream_on_read: false,
                    storage_class: None,
                    retention_policy: RetentionPolicyOption::Infinite,
                    retention_age_input: "7d".to_string(),
                    timestamping_mode: None,
                    timestamping_uncapped: false,
                    delete_on_empty_enabled: false,
                    delete_on_empty_min_age: "7d".to_string(),
                    selected: 0,
                    editing: false,
                };
            }
            KeyCode::Char('d') => {
                if let Some(basin) = filtered.get(state.selected) {
                    self.input_mode = InputMode::ConfirmDeleteBasin {
                        basin: basin.name.clone(),
                    };
                }
            }
            KeyCode::Char('e') => {
                if let Some(basin) = filtered.get(state.selected) {
                    let basin_name = basin.name.clone();
                    self.input_mode = InputMode::ReconfigureBasin {
                        basin: basin_name.clone(),
                        create_stream_on_append: None,
                        create_stream_on_read: None,
                        storage_class: None,
                        retention_policy: RetentionPolicyOption::Infinite,
                        retention_age_secs: 604800, // 1 week default
                        timestamping_mode: None,
                        timestamping_uncapped: None,
                        selected: 0,
                        editing_age: false,
                        age_input: String::new(),
                    };
                    // Load current config
                    self.load_basin_config(basin_name, tx);
                }
            }
            KeyCode::Char('M') => {
                // Basin Metrics for selected basin
                if let Some(basin) = filtered.get(state.selected) {
                    let basin_name = basin.name.clone();
                    self.open_basin_metrics(basin_name, tx);
                }
            }
            KeyCode::Char('A') => {
                // Account Metrics
                self.open_account_metrics(tx);
            }
            KeyCode::Esc => {
                if !state.filter.is_empty() {
                    state.filter.clear();
                    state.selected = 0;
                }
            }
            _ => {}
        }
    }

    fn handle_streams_key(&mut self, key: KeyEvent, tx: mpsc::UnboundedSender<Event>) {
        let Screen::Streams(state) = &mut self.screen else {
            return;
        };

        // Handle filter mode
        if state.filter_active {
            match key.code {
                KeyCode::Esc => {
                    state.filter_active = false;
                    state.filter.clear();
                    state.selected = 0;
                }
                KeyCode::Enter => {
                    state.filter_active = false;
                }
                KeyCode::Backspace => {
                    state.filter.pop();
                    state.selected = 0;
                }
                KeyCode::Char(c) => {
                    state.filter.push(c);
                    state.selected = 0;
                }
                _ => {}
            }
            return;
        }

        // Get filtered list length for bounds checking
        let filtered: Vec<_> = state.streams.iter()
            .filter(|s| state.filter.is_empty() || s.name.to_string().contains(&state.filter))
            .collect();
        let filtered_len = filtered.len();

        match key.code {
            KeyCode::Char('/') => {
                state.filter_active = true;
            }
            KeyCode::Esc => {
                if !state.filter.is_empty() {
                    state.filter.clear();
                    state.selected = 0;
                } else {
                    self.screen = Screen::Basins(BasinsState {
                        loading: true,
                        ..Default::default()
                    });
                    self.load_basins(tx);
                }
            }
            KeyCode::Char('q') => {
                self.screen = Screen::Basins(BasinsState {
                    loading: true,
                    ..Default::default()
                });
                self.load_basins(tx);
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if state.selected > 0 {
                    state.selected -= 1;
                }
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if filtered_len > 0 && state.selected < filtered_len - 1 {
                    state.selected += 1;
                }
            }
            KeyCode::Char('g') => {
                state.selected = 0;
            }
            KeyCode::Char('G') => {
                if filtered_len > 0 {
                    state.selected = filtered_len - 1;
                }
            }
            KeyCode::Enter => {
                if let Some(stream) = filtered.get(state.selected) {
                    let stream_name = stream.name.clone();
                    let basin_name = state.basin_name.clone();
                    self.screen = Screen::StreamDetail(StreamDetailState {
                        basin_name: basin_name.clone(),
                        stream_name: stream_name.clone(),
                        config: None,
                        tail_position: None,
                        selected_action: 0,
                        loading: true,
                    });
                    self.load_stream_detail(basin_name, stream_name, tx);
                }
            }
            KeyCode::Char('r') => {
                let basin_name = state.basin_name.clone();
                state.loading = true;
                state.filter.clear();
                state.selected = 0;
                self.load_streams(basin_name, tx);
            }
            KeyCode::Char('c') => {
                self.input_mode = InputMode::CreateStream {
                    basin: state.basin_name.clone(),
                    name: String::new(),
                    storage_class: None,
                    retention_policy: RetentionPolicyOption::Infinite,
                    retention_age_input: "7d".to_string(),
                    timestamping_mode: None,
                    timestamping_uncapped: false,
                    delete_on_empty_enabled: false,
                    delete_on_empty_min_age: "7d".to_string(),
                    selected: 0,
                    editing: false,
                };
            }
            KeyCode::Char('d') => {
                if let Some(stream) = filtered.get(state.selected) {
                    self.input_mode = InputMode::ConfirmDeleteStream {
                        basin: state.basin_name.clone(),
                        stream: stream.name.clone(),
                    };
                }
            }
            KeyCode::Char('e') => {
                if let Some(stream) = filtered.get(state.selected) {
                    let basin_name = state.basin_name.clone();
                    let stream_name = stream.name.clone();
                    self.input_mode = InputMode::ReconfigureStream {
                        basin: basin_name.clone(),
                        stream: stream_name.clone(),
                        storage_class: None,
                        retention_policy: RetentionPolicyOption::Infinite,
                        retention_age_secs: 604800,
                        timestamping_mode: None,
                        timestamping_uncapped: None,
                        delete_on_empty_enabled: false,
                        delete_on_empty_min_age: "7d".to_string(),
                        selected: 0,
                        editing_age: false,
                        age_input: String::new(),
                    };
                    // Load current config
                    self.load_stream_config_for_reconfig(basin_name, stream_name, tx);
                }
            }
            KeyCode::Char('M') => {
                // Basin Metrics
                let basin_name = state.basin_name.clone();
                self.open_basin_metrics(basin_name, tx);
            }
            _ => {}
        }
    }

    fn handle_stream_detail_key(&mut self, key: KeyEvent, tx: mpsc::UnboundedSender<Event>) {
        let Screen::StreamDetail(state) = &mut self.screen else {
            return;
        };

        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => {
                let basin_name = state.basin_name.clone();
                self.screen = Screen::Streams(StreamsState {
                    basin_name: basin_name.clone(),
                    streams: Vec::new(),
                    selected: 0,
                    loading: true,
                    filter: String::new(),
                    filter_active: false,
                });
                self.load_streams(basin_name, tx);
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if state.selected_action > 0 {
                    state.selected_action -= 1;
                }
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if state.selected_action < 4 {
                    // 5 actions: tail, custom read, append, fence, trim
                    state.selected_action += 1;
                }
            }
            KeyCode::Enter => {
                let basin_name = state.basin_name.clone();
                let stream_name = state.stream_name.clone();
                match state.selected_action {
                    0 => self.start_tail(basin_name, stream_name, tx), // Tail
                    1 => self.open_custom_read_dialog(basin_name, stream_name), // Custom read
                    2 => self.open_append_view(basin_name, stream_name), // Append
                    3 => self.open_fence_dialog(basin_name, stream_name), // Fence
                    4 => self.open_trim_dialog(basin_name, stream_name), // Trim
                    _ => {}
                }
            }
            KeyCode::Char('t') => {
                // Simple tail - s2 read with no flags (live follow from current position)
                let basin_name = state.basin_name.clone();
                let stream_name = state.stream_name.clone();
                self.start_tail(basin_name, stream_name, tx);
            }
            KeyCode::Char('r') => {
                // Custom read - open configuration dialog
                let basin_name = state.basin_name.clone();
                let stream_name = state.stream_name.clone();
                self.open_custom_read_dialog(basin_name, stream_name);
            }
            KeyCode::Char('a') => {
                // Append records
                let basin_name = state.basin_name.clone();
                let stream_name = state.stream_name.clone();
                self.open_append_view(basin_name, stream_name);
            }
            KeyCode::Char('e') => {
                let basin_name = state.basin_name.clone();
                let stream_name = state.stream_name.clone();
                self.input_mode = InputMode::ReconfigureStream {
                    basin: basin_name.clone(),
                    stream: stream_name.clone(),
                    storage_class: None,
                    retention_policy: RetentionPolicyOption::Infinite,
                    retention_age_secs: 604800,
                    timestamping_mode: None,
                    timestamping_uncapped: None,
                    delete_on_empty_enabled: false,
                    delete_on_empty_min_age: "7d".to_string(),
                    selected: 0,
                    editing_age: false,
                    age_input: String::new(),
                };
                self.load_stream_config_for_reconfig(basin_name, stream_name, tx);
            }
            KeyCode::Char('f') => {
                // Fence stream
                let basin_name = state.basin_name.clone();
                let stream_name = state.stream_name.clone();
                self.open_fence_dialog(basin_name, stream_name);
            }
            KeyCode::Char('m') => {
                // Trim stream
                let basin_name = state.basin_name.clone();
                let stream_name = state.stream_name.clone();
                self.open_trim_dialog(basin_name, stream_name);
            }
            KeyCode::Char('M') => {
                // Stream Metrics
                let basin_name = state.basin_name.clone();
                let stream_name = state.stream_name.clone();
                self.open_stream_metrics(basin_name, stream_name, tx);
            }
            _ => {}
        }
    }

    fn handle_read_view_key(&mut self, key: KeyEvent, tx: mpsc::UnboundedSender<Event>) {
        let Screen::ReadView(state) = &mut self.screen else {
            return;
        };

        // If showing detail panel, handle differently
        if state.show_detail {
            match key.code {
                KeyCode::Esc | KeyCode::Enter | KeyCode::Char('q') => {
                    state.show_detail = false;
                }
                _ => {}
            }
            return;
        }

        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => {
                // Go back to stream detail and reload data
                let basin_name = state.basin_name.clone();
                let stream_name = state.stream_name.clone();
                self.screen = Screen::StreamDetail(StreamDetailState {
                    basin_name: basin_name.clone(),
                    stream_name: stream_name.clone(),
                    config: None,
                    tail_position: None,
                    selected_action: 0,
                    loading: true,
                });
                self.load_stream_detail(basin_name, stream_name, tx);
            }
            KeyCode::Char(' ') => {
                state.paused = !state.paused;
                self.message = Some(StatusMessage {
                    text: if state.paused {
                        "Paused".to_string()
                    } else {
                        "Resumed".to_string()
                    },
                    level: MessageLevel::Info,
                });
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if state.selected > 0 {
                    state.selected -= 1;
                }
            }
            KeyCode::Down | KeyCode::Char('j') => {
                let max_idx = state.records.len().saturating_sub(1);
                if state.selected < max_idx {
                    state.selected += 1;
                }
            }
            KeyCode::Char('g') => {
                state.selected = 0;
            }
            KeyCode::Char('G') => {
                state.selected = state.records.len().saturating_sub(1);
            }
            KeyCode::Tab | KeyCode::Char('l') => {
                // Toggle list pane visibility
                state.hide_list = !state.hide_list;
            }
            KeyCode::Enter | KeyCode::Char('h') => {
                // Show headers popup
                if !state.records.is_empty() {
                    state.show_detail = true;
                }
            }
            _ => {}
        }
    }

    fn load_basins(&self, tx: mpsc::UnboundedSender<Event>) {
        let s2 = self.s2.clone();
        tokio::spawn(async move {
            let args = ListBasinsArgs {
                prefix: None,
                start_after: None,
                limit: Some(100),
                no_auto_paginate: false,
            };
            match ops::list_basins(&s2, args).await {
                Ok(stream) => {
                    let basins: Vec<_> = stream
                        .take(100)
                        .filter_map(|r| async { r.ok() })
                        .collect()
                        .await;
                    let _ = tx.send(Event::BasinsLoaded(Ok(basins)));
                }
                Err(e) => {
                    let _ = tx.send(Event::BasinsLoaded(Err(e)));
                }
            }
        });
    }

    fn load_streams(&self, basin_name: BasinName, tx: mpsc::UnboundedSender<Event>) {
        let s2 = self.s2.clone();
        tokio::spawn(async move {
            let args = ListStreamsArgs {
                uri: S2BasinAndMaybeStreamUri {
                    basin: basin_name,
                    stream: None,
                },
                prefix: None,
                start_after: None,
                limit: Some(100),
                no_auto_paginate: false,
            };
            match ops::list_streams(&s2, args).await {
                Ok(stream) => {
                    let streams: Vec<_> = stream
                        .take(100)
                        .filter_map(|r| async { r.ok() })
                        .collect()
                        .await;
                    let _ = tx.send(Event::StreamsLoaded(Ok(streams)));
                }
                Err(e) => {
                    let _ = tx.send(Event::StreamsLoaded(Err(e)));
                }
            }
        });
    }

    fn load_stream_detail(
        &self,
        basin_name: BasinName,
        stream_name: StreamName,
        tx: mpsc::UnboundedSender<Event>,
    ) {
        let s2 = self.s2.clone();
        let uri = S2BasinAndStreamUri {
            basin: basin_name.clone(),
            stream: stream_name.clone(),
        };

        // Load config
        let tx_config = tx.clone();
        let uri_config = uri.clone();
        let s2_config = s2.clone();
        tokio::spawn(async move {
            match ops::get_stream_config(&s2_config, uri_config).await {
                Ok(config) => {
                    let _ = tx_config.send(Event::StreamConfigLoaded(Ok(config.into())));
                }
                Err(e) => {
                    let _ = tx_config.send(Event::StreamConfigLoaded(Err(e)));
                }
            }
        });

        // Load tail position
        let tx_tail = tx;
        tokio::spawn(async move {
            match ops::check_tail(&s2, uri).await {
                Ok(pos) => {
                    let _ = tx_tail.send(Event::TailPositionLoaded(Ok(pos)));
                }
                Err(e) => {
                    let _ = tx_tail.send(Event::TailPositionLoaded(Err(e)));
                }
            }
        });
    }

    fn create_basin_with_config(&mut self, name: String, scope: BasinScopeOption, config: BasinConfig, tx: mpsc::UnboundedSender<Event>) {
        self.input_mode = InputMode::Normal;
        let s2 = self.s2.clone();
        let tx_refresh = tx.clone();
        tokio::spawn(async move {
            // Parse basin name
            let basin_name: BasinName = match name.parse() {
                Ok(n) => n,
                Err(e) => {
                    let _ = tx.send(Event::BasinCreated(Err(CliError::RecordWrite(format!("Invalid basin name: {e}")))));
                    return;
                }
            };

            // Build CreateBasinInput with scope
            let sdk_scope = match scope {
                BasinScopeOption::AwsUsEast1 => s2_sdk::types::BasinScope::AwsUsEast1,
            };
            let input = s2_sdk::types::CreateBasinInput::new(basin_name)
                .with_config(config.into())
                .with_scope(sdk_scope);

            match s2.create_basin(input).await.map_err(|e| CliError::op(crate::error::OpKind::CreateBasin, e)) {
                Ok(info) => {
                    let _ = tx.send(Event::BasinCreated(Ok(info)));
                    // Trigger refresh
                    let args = ListBasinsArgs {
                        prefix: None,
                        start_after: None,
                        limit: Some(100),
                        no_auto_paginate: false,
                    };
                    if let Ok(stream) = ops::list_basins(&s2, args).await {
                        let basins: Vec<_> = stream
                            .take(100)
                            .filter_map(|r| async { r.ok() })
                            .collect()
                            .await;
                        let _ = tx_refresh.send(Event::BasinsLoaded(Ok(basins)));
                    }
                }
                Err(e) => {
                    let _ = tx.send(Event::BasinCreated(Err(e)));
                }
            }
        });
    }

    fn delete_basin(&mut self, basin: BasinName, tx: mpsc::UnboundedSender<Event>) {
        let s2 = self.s2.clone();
        let tx_refresh = tx.clone();
        let name = basin.to_string();
        tokio::spawn(async move {
            match ops::delete_basin(&s2, &basin).await {
                Ok(()) => {
                    let _ = tx.send(Event::BasinDeleted(Ok(name)));
                    // Trigger refresh
                    let args = ListBasinsArgs {
                        prefix: None,
                        start_after: None,
                        limit: Some(100),
                        no_auto_paginate: false,
                    };
                    if let Ok(stream) = ops::list_basins(&s2, args).await {
                        let basins: Vec<_> = stream
                            .take(100)
                            .filter_map(|r| async { r.ok() })
                            .collect()
                            .await;
                        let _ = tx_refresh.send(Event::BasinsLoaded(Ok(basins)));
                    }
                }
                Err(e) => {
                    let _ = tx.send(Event::BasinDeleted(Err(e)));
                }
            }
        });
    }

    fn create_stream_with_config(&mut self, basin: BasinName, name: String, config: StreamConfig, tx: mpsc::UnboundedSender<Event>) {
        self.input_mode = InputMode::Normal;
        let s2 = self.s2.clone();
        let tx_refresh = tx.clone();
        let basin_clone = basin.clone();
        tokio::spawn(async move {
            // Parse stream name
            let stream_name: StreamName = match name.parse() {
                Ok(n) => n,
                Err(e) => {
                    let _ = tx.send(Event::StreamCreated(Err(CliError::RecordWrite(format!("Invalid stream name: {e}")))));
                    return;
                }
            };
            let args = CreateStreamArgs {
                uri: S2BasinAndStreamUri {
                    basin: basin.clone(),
                    stream: stream_name,
                },
                config,
            };
            match ops::create_stream(&s2, args).await {
                Ok(info) => {
                    let _ = tx.send(Event::StreamCreated(Ok(info)));
                    // Trigger refresh
                    let args = ListStreamsArgs {
                        uri: S2BasinAndMaybeStreamUri {
                            basin: basin_clone,
                            stream: None,
                        },
                        prefix: None,
                        start_after: None,
                        limit: Some(100),
                        no_auto_paginate: false,
                    };
                    if let Ok(stream) = ops::list_streams(&s2, args).await {
                        let streams: Vec<_> = stream
                            .take(100)
                            .filter_map(|r| async { r.ok() })
                            .collect()
                            .await;
                        let _ = tx_refresh.send(Event::StreamsLoaded(Ok(streams)));
                    }
                }
                Err(e) => {
                    let _ = tx.send(Event::StreamCreated(Err(e)));
                }
            }
        });
    }

    fn delete_stream(&mut self, basin: BasinName, stream: StreamName, tx: mpsc::UnboundedSender<Event>) {
        let s2 = self.s2.clone();
        let tx_refresh = tx.clone();
        let name = stream.to_string();
        let basin_clone = basin.clone();
        tokio::spawn(async move {
            let uri = S2BasinAndStreamUri {
                basin: basin.clone(),
                stream,
            };
            match ops::delete_stream(&s2, uri).await {
                Ok(()) => {
                    let _ = tx.send(Event::StreamDeleted(Ok(name)));
                    // Trigger refresh
                    let args = ListStreamsArgs {
                        uri: S2BasinAndMaybeStreamUri {
                            basin: basin_clone,
                            stream: None,
                        },
                        prefix: None,
                        start_after: None,
                        limit: Some(100),
                        no_auto_paginate: false,
                    };
                    if let Ok(stream) = ops::list_streams(&s2, args).await {
                        let streams: Vec<_> = stream
                            .take(100)
                            .filter_map(|r| async { r.ok() })
                            .collect()
                            .await;
                        let _ = tx_refresh.send(Event::StreamsLoaded(Ok(streams)));
                    }
                }
                Err(e) => {
                    let _ = tx.send(Event::StreamDeleted(Err(e)));
                }
            }
        });
    }

    /// Simple tail - like `s2 read` with no flags (live follow from current position)
    fn start_tail(
        &mut self,
        basin_name: BasinName,
        stream_name: StreamName,
        tx: mpsc::UnboundedSender<Event>,
    ) {
        self.screen = Screen::ReadView(ReadViewState {
            basin_name: basin_name.clone(),
            stream_name: stream_name.clone(),
            records: VecDeque::new(),
            is_tailing: true,
            selected: 0,
            paused: false,
            loading: true,
            show_detail: false,
            hide_list: false,
            output_file: None,
        });

        let s2 = self.s2.clone();
        let uri = S2BasinAndStreamUri {
            basin: basin_name,
            stream: stream_name,
        };

        tokio::spawn(async move {
            // Simple tail: no flags = TailOffset(0) = start at current tail, wait for new records
            let args = ReadArgs {
                uri,
                seq_num: None,
                timestamp: None,
                ago: None,
                tail_offset: None, // Defaults to TailOffset(0) in ops::read
                count: None,
                bytes: None,
                clamp: true,
                until: None,
                format: RecordFormat::default(),
                output: RecordsOut::Stdout,
            };

            match ops::read(&s2, &args).await {
                Ok(mut batch_stream) => {
                    use futures::StreamExt;
                    while let Some(batch_result) = batch_stream.next().await {
                        match batch_result {
                            Ok(batch) => {
                                for record in batch.records {
                                    if tx.send(Event::RecordReceived(Ok(record))).is_err() {
                                        return;
                                    }
                                }
                            }
                            Err(e) => {
                                let _ = tx.send(Event::RecordReceived(Err(crate::error::CliError::op(
                                    crate::error::OpKind::Read,
                                    e,
                                ))));
                                return;
                            }
                        }
                    }
                    let _ = tx.send(Event::ReadEnded);
                }
                Err(e) => {
                    let _ = tx.send(Event::Error(e));
                }
            }
        });
    }

    /// Open custom read configuration dialog
    fn open_custom_read_dialog(&mut self, basin: BasinName, stream: StreamName) {
        self.input_mode = InputMode::CustomRead {
            basin,
            stream,
            start_from: ReadStartFrom::SeqNum, // Default to reading from beginning
            seq_num_value: "0".to_string(),
            timestamp_value: String::new(),
            ago_value: "5".to_string(),
            ago_unit: AgoUnit::Minutes,
            tail_offset_value: "10".to_string(),
            count_limit: String::new(),
            byte_limit: String::new(),
            until_timestamp: String::new(),
            clamp: true,
            format: ReadFormat::Text,
            output_file: String::new(),
            selected: 0,
            editing: false,
        };
    }

    /// Start reading with custom configuration
    fn start_custom_read(
        &mut self,
        basin_name: BasinName,
        stream_name: StreamName,
        start_from: ReadStartFrom,
        seq_num_value: String,
        timestamp_value: String,
        ago_value: String,
        ago_unit: AgoUnit,
        tail_offset_value: String,
        count_limit: String,
        byte_limit: String,
        until_timestamp: String,
        clamp: bool,
        format: ReadFormat,
        output_file: String,
        tx: mpsc::UnboundedSender<Event>,
    ) {
        let has_output = !output_file.is_empty();
        self.screen = Screen::ReadView(ReadViewState {
            basin_name: basin_name.clone(),
            stream_name: stream_name.clone(),
            records: VecDeque::new(),
            is_tailing: true,
            selected: 0,
            paused: false,
            loading: true,
            show_detail: false,
            hide_list: false,
            output_file: if has_output { Some(output_file.clone()) } else { None },
        });

        let s2 = self.s2.clone();
        let uri = S2BasinAndStreamUri {
            basin: basin_name,
            stream: stream_name,
        };

        tokio::spawn(async move {
            // Parse values
            let seq_num = if start_from == ReadStartFrom::SeqNum {
                seq_num_value.parse().ok()
            } else {
                None
            };

            let timestamp = if start_from == ReadStartFrom::Timestamp {
                timestamp_value.parse().ok()
            } else {
                None
            };

            let ago = if start_from == ReadStartFrom::Ago {
                ago_value.parse::<u64>().ok().map(|v| {
                    let secs = ago_unit.to_seconds(v);
                    humantime::Duration::from(std::time::Duration::from_secs(secs))
                })
            } else {
                None
            };

            let tail_offset = if start_from == ReadStartFrom::TailOffset {
                tail_offset_value.parse().ok()
            } else {
                None
            };

            let count = count_limit.parse().ok().filter(|&v| v > 0);
            let bytes = byte_limit.parse().ok().filter(|&v| v > 0);
            let until = until_timestamp.parse().ok().filter(|&v| v > 0);

            let record_format = match format {
                ReadFormat::Text => RecordFormat::Text,
                ReadFormat::Json => RecordFormat::Json,
                ReadFormat::JsonBase64 => RecordFormat::JsonBase64,
            };

            // Set up output file if specified
            let output = if output_file.is_empty() {
                RecordsOut::Stdout
            } else {
                RecordsOut::File(std::path::PathBuf::from(&output_file))
            };

            let args = ReadArgs {
                uri,
                seq_num,
                timestamp,
                ago,
                tail_offset,
                count,
                bytes,
                clamp,
                until,
                format: record_format,
                output: output.clone(),
            };

            // Open file writer if output file is specified
            let mut file_writer: Option<tokio::fs::File> = if !output_file.is_empty() {
                match tokio::fs::File::create(&output_file).await {
                    Ok(f) => Some(f),
                    Err(e) => {
                        let _ = tx.send(Event::Error(crate::error::CliError::RecordWrite(e.to_string())));
                        return;
                    }
                }
            } else {
                None
            };

            match ops::read(&s2, &args).await {
                Ok(mut batch_stream) => {
                    use futures::StreamExt;
                    use tokio::io::AsyncWriteExt;
                    while let Some(batch_result) = batch_stream.next().await {
                        match batch_result {
                            Ok(batch) => {
                                for record in batch.records {
                                    // Write to file if specified
                                    if let Some(ref mut writer) = file_writer {
                                        let line = match record_format {
                                            RecordFormat::Text => {
                                                format!("{}\n", String::from_utf8_lossy(&record.body))
                                            }
                                            RecordFormat::Json => {
                                                format!("{}\n", serde_json::json!({
                                                    "seq_num": record.seq_num,
                                                    "timestamp": record.timestamp,
                                                    "headers": record.headers.iter().map(|h| {
                                                        serde_json::json!({
                                                            "name": String::from_utf8_lossy(&h.name),
                                                            "value": String::from_utf8_lossy(&h.value)
                                                        })
                                                    }).collect::<Vec<_>>(),
                                                    "body": String::from_utf8_lossy(&record.body).to_string()
                                                }))
                                            }
                                            RecordFormat::JsonBase64 => {
                                                format!("{}\n", serde_json::json!({
                                                    "seq_num": record.seq_num,
                                                    "timestamp": record.timestamp,
                                                    "headers": record.headers.iter().map(|h| {
                                                        serde_json::json!({
                                                            "name": String::from_utf8_lossy(&h.name),
                                                            "value": String::from_utf8_lossy(&h.value)
                                                        })
                                                    }).collect::<Vec<_>>(),
                                                    "body": base64ct::Base64::encode_string(&record.body)
                                                }))
                                            }
                                        };
                                        let _ = writer.write_all(line.as_bytes()).await;
                                    }

                                    if tx.send(Event::RecordReceived(Ok(record))).is_err() {
                                        return;
                                    }
                                }
                            }
                            Err(e) => {
                                let _ = tx.send(Event::RecordReceived(Err(crate::error::CliError::op(
                                    crate::error::OpKind::Read,
                                    e,
                                ))));
                                return;
                            }
                        }
                    }
                    let _ = tx.send(Event::ReadEnded);
                }
                Err(e) => {
                    let _ = tx.send(Event::Error(e));
                }
            }
        });
    }

    fn load_basin_config(&self, basin: BasinName, tx: mpsc::UnboundedSender<Event>) {
        let s2 = self.s2.clone();
        tokio::spawn(async move {
            match ops::get_basin_config(&s2, &basin).await {
                Ok(config) => {
                    // Extract default stream config info
                    let (storage_class, retention_age_secs, timestamping_mode, timestamping_uncapped) =
                        if let Some(default_config) = &config.default_stream_config {
                            let sc = default_config.storage_class.map(StorageClass::from);
                            let age = match default_config.retention_policy {
                                Some(s2_sdk::types::RetentionPolicy::Age(secs)) => Some(secs),
                                _ => None,
                            };
                            let ts_mode = default_config.timestamping.as_ref()
                                .and_then(|t| t.mode.map(TimestampingMode::from));
                            let ts_uncapped = default_config.timestamping.as_ref()
                                .map(|t| t.uncapped)
                                .unwrap_or(false);
                            (sc, age, ts_mode, ts_uncapped)
                        } else {
                            (None, None, None, false)
                        };

                    let info = BasinConfigInfo {
                        create_stream_on_append: config.create_stream_on_append,
                        create_stream_on_read: config.create_stream_on_read,
                        storage_class,
                        retention_age_secs,
                        timestamping_mode,
                        timestamping_uncapped,
                    };
                    let _ = tx.send(Event::BasinConfigLoaded(Ok(info)));
                }
                Err(e) => {
                    let _ = tx.send(Event::BasinConfigLoaded(Err(e)));
                }
            }
        });
    }

    fn load_stream_config_for_reconfig(
        &self,
        basin: BasinName,
        stream: StreamName,
        tx: mpsc::UnboundedSender<Event>,
    ) {
        let s2 = self.s2.clone();
        let uri = S2BasinAndStreamUri { basin, stream };
        tokio::spawn(async move {
            match ops::get_stream_config(&s2, uri).await {
                Ok(config) => {
                    let storage_class = config.storage_class.map(StorageClass::from);
                    let retention_age_secs = match config.retention_policy {
                        Some(s2_sdk::types::RetentionPolicy::Age(secs)) => Some(secs),
                        _ => None,
                    };
                    let timestamping_mode = config.timestamping.as_ref()
                        .and_then(|t| t.mode.map(TimestampingMode::from));
                    let timestamping_uncapped = config.timestamping.as_ref()
                        .map(|t| t.uncapped)
                        .unwrap_or(false);
                    let delete_on_empty_min_age_secs = config.delete_on_empty
                        .map(|d| d.min_age_secs);

                    let info = StreamConfigInfo {
                        storage_class,
                        retention_age_secs,
                        timestamping_mode,
                        timestamping_uncapped,
                        delete_on_empty_min_age_secs,
                    };
                    let _ = tx.send(Event::StreamConfigForReconfigLoaded(Ok(info)));
                }
                Err(e) => {
                    let _ = tx.send(Event::StreamConfigForReconfigLoaded(Err(e)));
                }
            }
        });
    }

    fn reconfigure_basin(
        &mut self,
        basin: BasinName,
        config: BasinReconfigureConfig,
        tx: mpsc::UnboundedSender<Event>,
    ) {
        let s2 = self.s2.clone();
        let tx_refresh = tx.clone();
        tokio::spawn(async move {
            // Build the default stream config
            let retention_policy = match config.retention_policy {
                RetentionPolicyOption::Infinite => Some(crate::types::RetentionPolicy::Infinite),
                RetentionPolicyOption::Age => Some(crate::types::RetentionPolicy::Age(Duration::from_secs(config.retention_age_secs))),
            };

            let timestamping = if config.timestamping_mode.is_some() || config.timestamping_uncapped.is_some() {
                Some(crate::types::TimestampingConfig {
                    timestamping_mode: config.timestamping_mode,
                    timestamping_uncapped: config.timestamping_uncapped,
                })
            } else {
                None
            };

            let default_stream_config = StreamConfig {
                storage_class: config.storage_class,
                retention_policy,
                timestamping,
                delete_on_empty: None,
            };

            let args = ReconfigureBasinArgs {
                basin: S2BasinUri(basin),
                create_stream_on_append: config.create_stream_on_append,
                create_stream_on_read: config.create_stream_on_read,
                default_stream_config,
            };
            match ops::reconfigure_basin(&s2, args).await {
                Ok(_) => {
                    let _ = tx.send(Event::BasinReconfigured(Ok(())));
                    // Trigger refresh
                    let args = ListBasinsArgs {
                        prefix: None,
                        start_after: None,
                        limit: Some(100),
                        no_auto_paginate: false,
                    };
                    if let Ok(stream) = ops::list_basins(&s2, args).await {
                        let basins: Vec<_> = stream
                            .take(100)
                            .filter_map(|r| async { r.ok() })
                            .collect()
                            .await;
                        let _ = tx_refresh.send(Event::BasinsLoaded(Ok(basins)));
                    }
                }
                Err(e) => {
                    let _ = tx.send(Event::BasinReconfigured(Err(e)));
                }
            }
        });
    }

    fn reconfigure_stream(
        &mut self,
        basin: BasinName,
        stream: StreamName,
        config: StreamReconfigureConfig,
        tx: mpsc::UnboundedSender<Event>,
    ) {
        let s2 = self.s2.clone();
        let basin_clone = basin.clone();
        let tx_refresh = tx.clone();
        tokio::spawn(async move {
            let retention_policy = match config.retention_policy {
                RetentionPolicyOption::Infinite => Some(crate::types::RetentionPolicy::Infinite),
                RetentionPolicyOption::Age => Some(crate::types::RetentionPolicy::Age(Duration::from_secs(config.retention_age_secs))),
            };

            let timestamping = if config.timestamping_mode.is_some() || config.timestamping_uncapped.is_some() {
                Some(crate::types::TimestampingConfig {
                    timestamping_mode: config.timestamping_mode,
                    timestamping_uncapped: config.timestamping_uncapped,
                })
            } else {
                None
            };

            let delete_on_empty = if config.delete_on_empty_enabled {
                humantime::parse_duration(&config.delete_on_empty_min_age)
                    .ok()
                    .map(|d| crate::types::DeleteOnEmptyConfig { delete_on_empty_min_age: d })
            } else {
                None
            };

            let args = ReconfigureStreamArgs {
                uri: S2BasinAndStreamUri { basin, stream },
                config: StreamConfig {
                    storage_class: config.storage_class,
                    retention_policy,
                    timestamping,
                    delete_on_empty,
                },
            };
            match ops::reconfigure_stream(&s2, args).await {
                Ok(_) => {
                    let _ = tx.send(Event::StreamReconfigured(Ok(())));
                    // Trigger refresh
                    let args = ListStreamsArgs {
                        uri: S2BasinAndMaybeStreamUri {
                            basin: basin_clone,
                            stream: None,
                        },
                        prefix: None,
                        start_after: None,
                        limit: Some(100),
                        no_auto_paginate: false,
                    };
                    if let Ok(stream) = ops::list_streams(&s2, args).await {
                        let streams: Vec<_> = stream
                            .take(100)
                            .filter_map(|r| async { r.ok() })
                            .collect()
                            .await;
                        let _ = tx_refresh.send(Event::StreamsLoaded(Ok(streams)));
                    }
                }
                Err(e) => {
                    let _ = tx.send(Event::StreamReconfigured(Err(e)));
                }
            }
        });
    }

    /// Open the append view
    fn open_append_view(&mut self, basin_name: BasinName, stream_name: StreamName) {
        self.screen = Screen::AppendView(AppendViewState {
            basin_name,
            stream_name,
            body: String::new(),
            headers: Vec::new(),
            match_seq_num: String::new(),
            fencing_token: String::new(),
            selected: 0,
            editing: false,
            header_key_input: String::new(),
            header_value_input: String::new(),
            editing_header_key: true,
            history: Vec::new(),
            appending: false,
        });
    }

    /// Handle keys in append view
    /// Layout: 0=body, 1=headers, 2=match_seq, 3=fencing, 4=send
    fn handle_append_view_key(&mut self, key: KeyEvent, tx: mpsc::UnboundedSender<Event>) {
        let Screen::AppendView(state) = &mut self.screen else {
            return;
        };

        // Don't handle keys while appending
        if state.appending {
            return;
        }

        // If editing a field, handle text input
        if state.editing {
            match key.code {
                KeyCode::Esc => {
                    state.editing = false;
                }
                KeyCode::Enter => {
                    if state.selected == 1 {
                        // Headers: if editing key, move to value; if editing value, add header
                        if state.editing_header_key {
                            if !state.header_key_input.is_empty() {
                                state.editing_header_key = false;
                            }
                        } else {
                            // Add the header if key is not empty
                            if !state.header_key_input.is_empty() {
                                state.headers.push((
                                    state.header_key_input.clone(),
                                    state.header_value_input.clone(),
                                ));
                                state.header_key_input.clear();
                                state.header_value_input.clear();
                                state.editing_header_key = true;
                            }
                            state.editing = false;
                        }
                    } else {
                        state.editing = false;
                    }
                }
                KeyCode::Tab if state.selected == 1 => {
                    // Toggle between key and value in headers
                    state.editing_header_key = !state.editing_header_key;
                }
                KeyCode::Backspace => {
                    match state.selected {
                        0 => { state.body.pop(); }
                        1 => {
                            if state.editing_header_key {
                                state.header_key_input.pop();
                            } else {
                                state.header_value_input.pop();
                            }
                        }
                        2 => { state.match_seq_num.pop(); }
                        3 => { state.fencing_token.pop(); }
                        _ => {}
                    }
                }
                KeyCode::Char(c) => {
                    match state.selected {
                        0 => { state.body.push(c); }
                        1 => {
                            if state.editing_header_key {
                                state.header_key_input.push(c);
                            } else {
                                state.header_value_input.push(c);
                            }
                        }
                        2 => {
                            // Only allow digits for match_seq_num
                            if c.is_ascii_digit() {
                                state.match_seq_num.push(c);
                            }
                        }
                        3 => { state.fencing_token.push(c); }
                        _ => {}
                    }
                }
                _ => {}
            }
            return;
        }

        // Not editing - handle navigation
        match key.code {
            KeyCode::Esc => {
                // Go back to stream detail
                let basin_name = state.basin_name.clone();
                let stream_name = state.stream_name.clone();
                self.screen = Screen::StreamDetail(StreamDetailState {
                    basin_name: basin_name.clone(),
                    stream_name: stream_name.clone(),
                    config: None,
                    tail_position: None,
                    selected_action: 2, // Append action
                    loading: true,
                });
                self.load_stream_detail(basin_name, stream_name, tx);
            }
            KeyCode::Char('j') | KeyCode::Down => {
                state.selected = (state.selected + 1).min(4);
            }
            KeyCode::Char('k') | KeyCode::Up => {
                state.selected = state.selected.saturating_sub(1);
            }
            KeyCode::Char('d') if state.selected == 1 => {
                // Delete last header
                state.headers.pop();
            }
            KeyCode::Enter => {
                if state.selected == 4 {
                    // Send button - append the record
                    if !state.body.is_empty() {
                        let basin_name = state.basin_name.clone();
                        let stream_name = state.stream_name.clone();
                        let body = state.body.clone();
                        let headers = state.headers.clone();
                        let match_seq_num = state.match_seq_num.parse::<u64>().ok();
                        let fencing_token = if state.fencing_token.is_empty() {
                            None
                        } else {
                            Some(state.fencing_token.clone())
                        };
                        state.body.clear();
                        // Keep headers for convenience (user might want to send similar records)
                        state.appending = true;
                        self.append_record(basin_name, stream_name, body, headers, match_seq_num, fencing_token, tx);
                    }
                } else {
                    // Start editing the selected field
                    state.editing = true;
                    if state.selected == 1 {
                        state.editing_header_key = true;
                    }
                }
            }
            _ => {}
        }
    }

    /// Append a single record to the stream
    fn append_record(
        &self,
        basin_name: BasinName,
        stream_name: StreamName,
        body: String,
        headers: Vec<(String, String)>,
        match_seq_num: Option<u64>,
        fencing_token: Option<String>,
        tx: mpsc::UnboundedSender<Event>,
    ) {
        let s2 = self.s2.clone();
        let body_preview = if body.len() > 50 {
            format!("{}...", &body[..50])
        } else {
            body.clone()
        };
        let header_count = headers.len();

        tokio::spawn(async move {
            use s2_sdk::types::{AppendInput, AppendRecord, AppendRecordBatch, FencingToken, Header};

            let stream = s2.basin(basin_name).stream(stream_name);

            let mut record = match AppendRecord::new(body.into_bytes()) {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Event::RecordAppended(Err(crate::error::CliError::RecordWrite(
                        e.to_string(),
                    ))));
                    return;
                }
            };

            // Add headers if any
            if !headers.is_empty() {
                let parsed_headers: Vec<Header> = headers
                    .into_iter()
                    .map(|(k, v)| Header::new(k.into_bytes(), v.into_bytes()))
                    .collect();
                record = match record.with_headers(parsed_headers) {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = tx.send(Event::RecordAppended(Err(crate::error::CliError::RecordWrite(
                            e.to_string(),
                        ))));
                        return;
                    }
                };
            }

            let records = match AppendRecordBatch::try_from_iter([record]) {
                Ok(batch) => batch,
                Err(e) => {
                    let _ = tx.send(Event::RecordAppended(Err(crate::error::CliError::RecordWrite(
                        e.to_string(),
                    ))));
                    return;
                }
            };

            let mut input = AppendInput::new(records);

            // Add match_seq_num if specified
            if let Some(seq) = match_seq_num {
                input = input.with_match_seq_num(seq);
            }

            // Add fencing token if specified
            if let Some(token_str) = fencing_token {
                match token_str.parse::<FencingToken>() {
                    Ok(token) => {
                        input = input.with_fencing_token(token);
                    }
                    Err(e) => {
                        let _ = tx.send(Event::RecordAppended(Err(crate::error::CliError::RecordWrite(
                            format!("Invalid fencing token: {}", e),
                        ))));
                        return;
                    }
                }
            }

            match stream.append(input).await {
                Ok(output) => {
                    let _ = tx.send(Event::RecordAppended(Ok((output.start.seq_num, body_preview, header_count))));
                }
                Err(e) => {
                    let _ = tx.send(Event::RecordAppended(Err(crate::error::CliError::op(
                        crate::error::OpKind::Append,
                        e,
                    ))));
                }
            }
        });
    }

    /// Open fence dialog
    fn open_fence_dialog(&mut self, basin: BasinName, stream: StreamName) {
        self.input_mode = InputMode::Fence {
            basin,
            stream,
            new_token: String::new(),
            current_token: String::new(),
            selected: 0,
            editing: false,
        };
    }

    /// Open trim dialog
    fn open_trim_dialog(&mut self, basin: BasinName, stream: StreamName) {
        self.input_mode = InputMode::Trim {
            basin,
            stream,
            trim_point: String::new(),
            fencing_token: String::new(),
            selected: 0,
            editing: false,
        };
    }

    /// Fence a stream
    fn fence_stream(
        &self,
        basin: BasinName,
        stream: StreamName,
        new_token: String,
        current_token: Option<String>,
        tx: mpsc::UnboundedSender<Event>,
    ) {
        let s2 = self.s2.clone();
        let new_token_clone = new_token.clone();

        tokio::spawn(async move {
            use s2_sdk::types::{AppendInput, AppendRecordBatch, CommandRecord, FencingToken};

            let stream_client = s2.basin(basin).stream(stream);

            // Parse the new fencing token
            let new_fencing_token = match new_token.parse::<FencingToken>() {
                Ok(token) => token,
                Err(e) => {
                    let _ = tx.send(Event::StreamFenced(Err(crate::error::CliError::RecordWrite(
                        format!("Invalid new fencing token: {}", e),
                    ))));
                    return;
                }
            };

            // Create fence command record
            let command = CommandRecord::fence(new_fencing_token);
            let record: s2_sdk::types::AppendRecord = command.into();
            let records = match AppendRecordBatch::try_from_iter([record]) {
                Ok(batch) => batch,
                Err(e) => {
                    let _ = tx.send(Event::StreamFenced(Err(crate::error::CliError::RecordWrite(
                        e.to_string(),
                    ))));
                    return;
                }
            };

            let mut input = AppendInput::new(records);

            // Add current fencing token if specified
            if let Some(token_str) = current_token {
                if !token_str.is_empty() {
                    match token_str.parse::<FencingToken>() {
                        Ok(token) => {
                            input = input.with_fencing_token(token);
                        }
                        Err(e) => {
                            let _ = tx.send(Event::StreamFenced(Err(crate::error::CliError::RecordWrite(
                                format!("Invalid current fencing token: {}", e),
                            ))));
                            return;
                        }
                    }
                }
            }

            match stream_client.append(input).await {
                Ok(_) => {
                    let _ = tx.send(Event::StreamFenced(Ok(new_token_clone)));
                }
                Err(e) => {
                    let _ = tx.send(Event::StreamFenced(Err(crate::error::CliError::op(
                        crate::error::OpKind::Fence,
                        e,
                    ))));
                }
            }
        });
    }

    /// Trim a stream
    fn trim_stream(
        &self,
        basin: BasinName,
        stream: StreamName,
        trim_point: u64,
        fencing_token: Option<String>,
        tx: mpsc::UnboundedSender<Event>,
    ) {
        let s2 = self.s2.clone();

        tokio::spawn(async move {
            use s2_sdk::types::{AppendInput, AppendRecordBatch, CommandRecord, FencingToken};

            let stream_client = s2.basin(basin).stream(stream);

            // Create trim command record
            let command = CommandRecord::trim(trim_point);
            let record: s2_sdk::types::AppendRecord = command.into();
            let records = match AppendRecordBatch::try_from_iter([record]) {
                Ok(batch) => batch,
                Err(e) => {
                    let _ = tx.send(Event::StreamTrimmed(Err(crate::error::CliError::RecordWrite(
                        e.to_string(),
                    ))));
                    return;
                }
            };

            let mut input = AppendInput::new(records);

            // Add fencing token if specified
            if let Some(token_str) = fencing_token {
                if !token_str.is_empty() {
                    match token_str.parse::<FencingToken>() {
                        Ok(token) => {
                            input = input.with_fencing_token(token);
                        }
                        Err(e) => {
                            let _ = tx.send(Event::StreamTrimmed(Err(crate::error::CliError::RecordWrite(
                                format!("Invalid fencing token: {}", e),
                            ))));
                            return;
                        }
                    }
                }
            }

            match stream_client.append(input).await {
                Ok(output) => {
                    let _ = tx.send(Event::StreamTrimmed(Ok((trim_point, output.tail.seq_num))));
                }
                Err(e) => {
                    let _ = tx.send(Event::StreamTrimmed(Err(crate::error::CliError::op(
                        crate::error::OpKind::Trim,
                        e,
                    ))));
                }
            }
        });
    }

    /// Switch between tabs
    fn switch_tab(&mut self, tx: mpsc::UnboundedSender<Event>) {
        match self.tab {
            Tab::Basins => {
                self.tab = Tab::AccessTokens;
                self.screen = Screen::AccessTokens(AccessTokensState {
                    loading: true,
                    ..Default::default()
                });
                self.load_access_tokens(tx);
            }
            Tab::AccessTokens => {
                self.tab = Tab::Basins;
                self.screen = Screen::Basins(BasinsState {
                    loading: true,
                    ..Default::default()
                });
                self.load_basins(tx);
            }
        }
    }

    /// Handle keys on access tokens screen
    fn handle_access_tokens_key(&mut self, key: KeyEvent, tx: mpsc::UnboundedSender<Event>) {
        let Screen::AccessTokens(state) = &mut self.screen else {
            return;
        };

        // Handle filter mode
        if state.filter_active {
            match key.code {
                KeyCode::Esc => {
                    state.filter_active = false;
                    state.filter.clear();
                    state.selected = 0;
                }
                KeyCode::Enter => {
                    state.filter_active = false;
                }
                KeyCode::Backspace => {
                    state.filter.pop();
                    state.selected = 0;
                }
                KeyCode::Char(c) => {
                    state.filter.push(c);
                    state.selected = 0;
                }
                _ => {}
            }
            return;
        }

        // Get filtered tokens for navigation
        let filtered_tokens: Vec<_> = state
            .tokens
            .iter()
            .filter(|t| {
                state.filter.is_empty()
                    || t.id.to_string().to_lowercase().contains(&state.filter.to_lowercase())
            })
            .collect();

        match key.code {
            KeyCode::Char('q') => {
                self.should_quit = true;
            }
            KeyCode::Char('j') | KeyCode::Down => {
                if !filtered_tokens.is_empty() && state.selected < filtered_tokens.len() - 1 {
                    state.selected += 1;
                }
            }
            KeyCode::Char('k') | KeyCode::Up => {
                if state.selected > 0 {
                    state.selected -= 1;
                }
            }
            KeyCode::Char('g') | KeyCode::Home => {
                state.selected = 0;
            }
            KeyCode::Char('G') | KeyCode::End => {
                if !filtered_tokens.is_empty() {
                    state.selected = filtered_tokens.len() - 1;
                }
            }
            KeyCode::Char('/') => {
                state.filter_active = true;
            }
            KeyCode::Char('c') => {
                // Create/Issue new token
                self.input_mode = InputMode::IssueAccessToken {
                    id: String::new(),
                    expiry: ExpiryOption::ThirtyDays,
                    expiry_custom: String::new(),
                    basins_scope: ScopeOption::All,
                    basins_value: String::new(),
                    streams_scope: ScopeOption::All,
                    streams_value: String::new(),
                    tokens_scope: ScopeOption::All,
                    tokens_value: String::new(),
                    account_read: true,
                    account_write: false,
                    basin_read: true,
                    basin_write: false,
                    stream_read: true,
                    stream_write: false,
                    auto_prefix_streams: false,
                    selected: 0,
                    editing: false,
                };
            }
            KeyCode::Char('d') => {
                // Delete/Revoke selected token
                if let Some(token) = filtered_tokens.get(state.selected) {
                    self.input_mode = InputMode::ConfirmRevokeToken {
                        token_id: token.id.to_string(),
                    };
                }
            }
            KeyCode::Char('r') => {
                // Refresh
                state.loading = true;
                self.load_access_tokens(tx);
            }
            KeyCode::Char('i') | KeyCode::Enter => {
                // View token details
                if let Some(token) = filtered_tokens.get(state.selected) {
                    self.input_mode = InputMode::ViewTokenDetail {
                        token: (*token).clone(),
                    };
                }
            }
            _ => {}
        }
    }

    /// Load access tokens
    fn load_access_tokens(&self, tx: mpsc::UnboundedSender<Event>) {
        let s2 = self.s2.clone();
        tokio::spawn(async move {
            let args = ListAccessTokensArgs {
                prefix: None,
                start_after: None,
                limit: Some(100),
                no_auto_paginate: false,
            };
            match ops::list_access_tokens(&s2, args).await {
                Ok(stream) => {
                    let tokens: Vec<_> = stream
                        .take(100)
                        .filter_map(|r| async { r.ok() })
                        .collect()
                        .await;
                    let _ = tx.send(Event::AccessTokensLoaded(Ok(tokens)));
                }
                Err(e) => {
                    let _ = tx.send(Event::AccessTokensLoaded(Err(e)));
                }
            }
        });
    }

    /// Issue a new access token (v2 with full options)
    #[allow(clippy::too_many_arguments)]
    fn issue_access_token_v2(
        &self,
        id: String,
        expiry: ExpiryOption,
        expiry_custom: String,
        basins_scope: ScopeOption,
        basins_value: String,
        streams_scope: ScopeOption,
        streams_value: String,
        tokens_scope: ScopeOption,
        tokens_value: String,
        account_read: bool,
        account_write: bool,
        basin_read: bool,
        basin_write: bool,
        stream_read: bool,
        stream_write: bool,
        auto_prefix_streams: bool,
        tx: mpsc::UnboundedSender<Event>,
    ) {
        let s2 = self.s2.clone();
        let tx_refresh = tx.clone();

        tokio::spawn(async move {
            // Parse token ID
            let token_id: AccessTokenId = match id.parse() {
                Ok(id) => id,
                Err(e) => {
                    let _ = tx.send(Event::AccessTokenIssued(Err(CliError::InvalidArgs(
                        miette::miette!("Invalid token ID: {}", e),
                    ))));
                    return;
                }
            };

            // Build operations list based on read/write checkboxes
            let mut operations: Vec<Operation> = Vec::new();

            // Account level operations
            if account_read {
                operations.push(Operation::ListBasins);
                operations.push(Operation::GetAccountMetrics);
            }
            // (No account-write ops at account level)

            // Basin level operations
            if basin_read {
                operations.push(Operation::GetBasinConfig);
                operations.push(Operation::GetBasinMetrics);
                operations.push(Operation::ListStreams);
            }
            if basin_write {
                operations.push(Operation::CreateBasin);
                operations.push(Operation::DeleteBasin);
                operations.push(Operation::ReconfigureBasin);
            }

            // Stream level operations
            if stream_read {
                operations.push(Operation::GetStreamConfig);
                operations.push(Operation::GetStreamMetrics);
                operations.push(Operation::Read);
                operations.push(Operation::CheckTail);
            }
            if stream_write {
                operations.push(Operation::CreateStream);
                operations.push(Operation::DeleteStream);
                operations.push(Operation::ReconfigureStream);
                operations.push(Operation::Append);
                operations.push(Operation::Fence);
                operations.push(Operation::Trim);
            }

            // Token operations (based on tokens scope)
            if !matches!(tokens_scope, ScopeOption::None) {
                if account_read {
                    operations.push(Operation::ListAccessTokens);
                }
                if account_write {
                    operations.push(Operation::IssueAccessToken);
                    operations.push(Operation::RevokeAccessToken);
                }
            }

            // Build expiration
            let expires_in_str = match expiry {
                ExpiryOption::Never => None,
                ExpiryOption::Custom => {
                    if expiry_custom.is_empty() { None } else { Some(expiry_custom.clone()) }
                }
                _ => expiry.to_duration_str().map(|s| s.to_string()),
            };

            // Build scope matchers
            let basins_matcher = match basins_scope {
                ScopeOption::All => None,
                ScopeOption::None => Some("".to_string()), // Empty string = no basins
                ScopeOption::Prefix => Some(basins_value.clone()),
                ScopeOption::Exact => Some(format!("={}", basins_value)),
            };

            let streams_matcher = match streams_scope {
                ScopeOption::All => None,
                ScopeOption::None => Some("".to_string()),
                ScopeOption::Prefix => Some(streams_value.clone()),
                ScopeOption::Exact => Some(format!("={}", streams_value)),
            };

            let tokens_matcher = match tokens_scope {
                ScopeOption::All => None,
                ScopeOption::None => Some("".to_string()),
                ScopeOption::Prefix => Some(tokens_value.clone()),
                ScopeOption::Exact => Some(format!("={}", tokens_value)),
            };

            // Build args
            let args = IssueAccessTokenArgs {
                id: token_id,
                expires_in: expires_in_str.and_then(|s| s.parse().ok()),
                expires_at: None,
                auto_prefix_streams,
                basins: basins_matcher.and_then(|s| if s.is_empty() && matches!(basins_scope, ScopeOption::None) {
                    // For "None" scope, we don't pass anything (API default is all)
                    // Actually, to restrict to none, we need special handling
                    None
                } else if s.is_empty() {
                    None
                } else {
                    s.parse().ok()
                }),
                streams: streams_matcher.and_then(|s| if s.is_empty() { None } else { s.parse().ok() }),
                access_tokens: tokens_matcher.and_then(|s| if s.is_empty() { None } else { s.parse().ok() }),
                op_group_perms: None,
                ops: operations,
            };

            match ops::issue_access_token(&s2, args).await {
                Ok(token) => {
                    let _ = tx.send(Event::AccessTokenIssued(Ok(token)));
                    // Trigger refresh
                    let list_args = ListAccessTokensArgs {
                        prefix: None,
                        start_after: None,
                        limit: Some(100),
                        no_auto_paginate: false,
                    };
                    if let Ok(stream) = ops::list_access_tokens(&s2, list_args).await {
                        let tokens: Vec<_> = stream
                            .take(100)
                            .filter_map(|r| async { r.ok() })
                            .collect()
                            .await;
                        let _ = tx_refresh.send(Event::AccessTokensLoaded(Ok(tokens)));
                    }
                }
                Err(e) => {
                    let _ = tx.send(Event::AccessTokenIssued(Err(e)));
                }
            }
        });
    }

    /// Revoke an access token
    fn revoke_access_token(&self, id: String, tx: mpsc::UnboundedSender<Event>) {
        let s2 = self.s2.clone();
        let tx_refresh = tx.clone();

        tokio::spawn(async move {
            // Parse token ID
            let token_id: AccessTokenId = match id.parse() {
                Ok(id) => id,
                Err(e) => {
                    let _ = tx.send(Event::AccessTokenRevoked(Err(CliError::InvalidArgs(
                        miette::miette!("Invalid token ID: {}", e),
                    ))));
                    return;
                }
            };

            match ops::revoke_access_token(&s2, token_id.clone()).await {
                Ok(()) => {
                    let _ = tx.send(Event::AccessTokenRevoked(Ok(id)));
                    // Trigger refresh
                    let list_args = ListAccessTokensArgs {
                        prefix: None,
                        start_after: None,
                        limit: Some(100),
                        no_auto_paginate: false,
                    };
                    if let Ok(stream) = ops::list_access_tokens(&s2, list_args).await {
                        let tokens: Vec<_> = stream
                            .take(100)
                            .filter_map(|r| async { r.ok() })
                            .collect()
                            .await;
                        let _ = tx_refresh.send(Event::AccessTokensLoaded(Ok(tokens)));
                    }
                }
                Err(e) => {
                    let _ = tx.send(Event::AccessTokenRevoked(Err(e)));
                }
            }
        });
    }

    /// Open basin metrics view
    /// Open account metrics view
    fn open_account_metrics(&mut self, tx: mpsc::UnboundedSender<Event>) {
        self.screen = Screen::MetricsView(MetricsViewState {
            metrics_type: MetricsType::Account,
            metrics: Vec::new(),
            selected_category: MetricCategory::ActiveBasins,
            loading: true,
            scroll: 0,
        });
        self.load_account_metrics(MetricCategory::ActiveBasins, tx);
    }

    fn open_basin_metrics(&mut self, basin_name: BasinName, tx: mpsc::UnboundedSender<Event>) {
        self.screen = Screen::MetricsView(MetricsViewState {
            metrics_type: MetricsType::Basin { basin_name: basin_name.clone() },
            metrics: Vec::new(),
            selected_category: MetricCategory::Storage,
            loading: true,
            scroll: 0,
        });
        self.load_basin_metrics(basin_name, MetricCategory::Storage, tx);
    }

    /// Open stream metrics view
    fn open_stream_metrics(&mut self, basin_name: BasinName, stream_name: StreamName, tx: mpsc::UnboundedSender<Event>) {
        self.screen = Screen::MetricsView(MetricsViewState {
            metrics_type: MetricsType::Stream { basin_name: basin_name.clone(), stream_name: stream_name.clone() },
            metrics: Vec::new(),
            selected_category: MetricCategory::Storage,
            loading: true,
            scroll: 0,
        });
        self.load_stream_metrics(basin_name, stream_name, tx);
    }

    /// Load basin metrics
    /// Load account metrics
    fn load_account_metrics(&self, category: MetricCategory, tx: mpsc::UnboundedSender<Event>) {
        use s2_sdk::types::AccountMetricSet;

        let s2 = self.s2.clone();

        tokio::spawn(async move {
            // Get metrics for last 24 hours
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as u32;
            let day_ago = now.saturating_sub(24 * 60 * 60);

            let set = match category {
                MetricCategory::ActiveBasins => AccountMetricSet::ActiveBasins(TimeRange::new(day_ago, now)),
                MetricCategory::AccountOps => AccountMetricSet::AccountOps(
                    s2_sdk::types::TimeRangeAndInterval::new(day_ago, now)
                ),
                _ => return, // Other categories not valid for account
            };

            let input = s2_sdk::types::GetAccountMetricsInput::new(set);
            match s2.get_account_metrics(input).await {
                Ok(metrics) => {
                    let _ = tx.send(Event::AccountMetricsLoaded(Ok(metrics)));
                }
                Err(e) => {
                    let _ = tx.send(Event::AccountMetricsLoaded(Err(CliError::op(
                        crate::error::OpKind::GetAccountMetrics,
                        e,
                    ))));
                }
            }
        });
    }

    fn load_basin_metrics(&self, basin_name: BasinName, category: MetricCategory, tx: mpsc::UnboundedSender<Event>) {
        let s2 = self.s2.clone();

        tokio::spawn(async move {
            // Get metrics for last 24 hours
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as u32;
            let day_ago = now.saturating_sub(24 * 60 * 60);

            let set = match category {
                MetricCategory::Storage => BasinMetricSet::Storage(TimeRange::new(day_ago, now)),
                MetricCategory::AppendOps => BasinMetricSet::AppendOps(
                    s2_sdk::types::TimeRangeAndInterval::new(day_ago, now)
                ),
                MetricCategory::ReadOps => BasinMetricSet::ReadOps(
                    s2_sdk::types::TimeRangeAndInterval::new(day_ago, now)
                ),
                MetricCategory::AppendThroughput => BasinMetricSet::AppendThroughput(
                    s2_sdk::types::TimeRangeAndInterval::new(day_ago, now)
                ),
                MetricCategory::ReadThroughput => BasinMetricSet::ReadThroughput(
                    s2_sdk::types::TimeRangeAndInterval::new(day_ago, now)
                ),
                _ => return, // Account metrics not valid for basin
            };

            let input = s2_sdk::types::GetBasinMetricsInput::new(basin_name, set);
            match s2.get_basin_metrics(input).await {
                Ok(metrics) => {
                    let _ = tx.send(Event::BasinMetricsLoaded(Ok(metrics)));
                }
                Err(e) => {
                    let _ = tx.send(Event::BasinMetricsLoaded(Err(CliError::op(
                        crate::error::OpKind::GetBasinMetrics,
                        e,
                    ))));
                }
            }
        });
    }

    /// Load stream metrics
    fn load_stream_metrics(&self, basin_name: BasinName, stream_name: StreamName, tx: mpsc::UnboundedSender<Event>) {
        let s2 = self.s2.clone();

        tokio::spawn(async move {
            // Get metrics for last 24 hours
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as u32;
            let day_ago = now.saturating_sub(24 * 60 * 60);

            let set = StreamMetricSet::Storage(TimeRange::new(day_ago, now));

            let input = s2_sdk::types::GetStreamMetricsInput::new(basin_name, stream_name, set);
            match s2.get_stream_metrics(input).await {
                Ok(metrics) => {
                    let _ = tx.send(Event::StreamMetricsLoaded(Ok(metrics)));
                }
                Err(e) => {
                    let _ = tx.send(Event::StreamMetricsLoaded(Err(CliError::op(
                        crate::error::OpKind::GetStreamMetrics,
                        e,
                    ))));
                }
            }
        });
    }

    /// Handle keys in metrics view
    fn handle_metrics_view_key(&mut self, key: KeyEvent, tx: mpsc::UnboundedSender<Event>) {
        // Extract data from state first to avoid borrow issues
        let (metrics_type, selected_category) = {
            let Screen::MetricsView(state) = &self.screen else {
                return;
            };
            (state.metrics_type.clone(), state.selected_category)
        };

        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => {
                // Go back to previous screen
                match &metrics_type {
                    MetricsType::Account => {
                        // Go back to basins list
                        self.screen = Screen::Basins(BasinsState {
                            basins: Vec::new(),
                            selected: 0,
                            loading: true,
                            filter: String::new(),
                            filter_active: false,
                        });
                        self.load_basins(tx);
                    }
                    MetricsType::Basin { basin_name } => {
                        let basin_name = basin_name.clone();
                        self.screen = Screen::Streams(StreamsState {
                            basin_name: basin_name.clone(),
                            streams: Vec::new(),
                            selected: 0,
                            loading: true,
                            filter: String::new(),
                            filter_active: false,
                        });
                        self.load_streams(basin_name, tx);
                    }
                    MetricsType::Stream { basin_name, stream_name } => {
                        let basin_name = basin_name.clone();
                        let stream_name = stream_name.clone();
                        self.screen = Screen::StreamDetail(StreamDetailState {
                            basin_name: basin_name.clone(),
                            stream_name: stream_name.clone(),
                            config: None,
                            tail_position: None,
                            selected_action: 0,
                            loading: true,
                        });
                        self.load_stream_detail(basin_name, stream_name, tx);
                    }
                }
            }
            KeyCode::Left | KeyCode::Char('h') => {
                // Previous metric category (for basin or account metrics)
                match &metrics_type {
                    MetricsType::Account => {
                        let new_category = selected_category.prev();
                        if let Screen::MetricsView(state) = &mut self.screen {
                            state.selected_category = new_category;
                            state.loading = true;
                            state.metrics.clear();
                        }
                        self.load_account_metrics(new_category, tx);
                    }
                    MetricsType::Basin { basin_name } => {
                        let basin_name = basin_name.clone();
                        let new_category = selected_category.prev();
                        if let Screen::MetricsView(state) = &mut self.screen {
                            state.selected_category = new_category;
                            state.loading = true;
                            state.metrics.clear();
                        }
                        self.load_basin_metrics(basin_name, new_category, tx);
                    }
                    MetricsType::Stream { .. } => {} // No category switching for stream
                }
            }
            KeyCode::Right | KeyCode::Char('l') => {
                // Next metric category (for basin or account metrics)
                match &metrics_type {
                    MetricsType::Account => {
                        let new_category = selected_category.next();
                        if let Screen::MetricsView(state) = &mut self.screen {
                            state.selected_category = new_category;
                            state.loading = true;
                            state.metrics.clear();
                        }
                        self.load_account_metrics(new_category, tx);
                    }
                    MetricsType::Basin { basin_name } => {
                        let basin_name = basin_name.clone();
                        let new_category = selected_category.next();
                        if let Screen::MetricsView(state) = &mut self.screen {
                            state.selected_category = new_category;
                            state.loading = true;
                            state.metrics.clear();
                        }
                        self.load_basin_metrics(basin_name, new_category, tx);
                    }
                    MetricsType::Stream { .. } => {} // No category switching for stream
                }
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if let Screen::MetricsView(state) = &mut self.screen {
                    if state.scroll > 0 {
                        state.scroll -= 1;
                    }
                }
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if let Screen::MetricsView(state) = &mut self.screen {
                    state.scroll += 1;
                }
            }
            KeyCode::Char('r') => {
                // Refresh
                if let Screen::MetricsView(state) = &mut self.screen {
                    state.loading = true;
                    state.metrics.clear();
                }
                match &metrics_type {
                    MetricsType::Account => {
                        self.load_account_metrics(selected_category, tx);
                    }
                    MetricsType::Basin { basin_name } => {
                        self.load_basin_metrics(basin_name.clone(), selected_category, tx);
                    }
                    MetricsType::Stream { basin_name, stream_name } => {
                        self.load_stream_metrics(basin_name.clone(), stream_name.clone(), tx);
                    }
                }
            }
            _ => {}
        }
    }
}
