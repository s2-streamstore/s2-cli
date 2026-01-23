use std::collections::VecDeque;
use std::time::Duration;

use crossterm::event::{self, Event as CrosstermEvent, KeyCode, KeyEvent, KeyModifiers};
use futures::StreamExt;
use ratatui::{Terminal, prelude::Backend};
use s2_sdk::types::{BasinInfo, BasinName, StreamInfo, StreamName, StreamPosition};
use tokio::sync::mpsc;

use crate::cli::{CreateBasinArgs, CreateStreamArgs, ListBasinsArgs, ListStreamsArgs, ReadArgs, ReconfigureBasinArgs, ReconfigureStreamArgs};
use crate::error::CliError;
use crate::ops;
use crate::record_format::{RecordFormat, RecordsOut};
use crate::types::{BasinConfig, S2BasinAndMaybeStreamUri, S2BasinAndStreamUri, S2BasinUri, StorageClass, StreamConfig, TimestampingMode};

use super::event::{BasinConfigInfo, Event, StreamConfigInfo};
use super::ui;

/// Maximum records to keep in read view buffer
const MAX_RECORDS_BUFFER: usize = 1000;

/// Current screen being displayed
#[derive(Debug, Clone)]
pub enum Screen {
    Basins(BasinsState),
    Streams(StreamsState),
    StreamDetail(StreamDetailState),
    ReadView(ReadViewState),
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
    pub scroll_offset: usize,
    pub paused: bool,
    pub loading: bool,
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
    CreateBasin { input: String },
    /// Creating a new stream
    CreateStream { basin: BasinName, input: String },
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
        // Options
        until_timestamp: String,
        // UI state
        selected: usize,
        editing: bool,
    },
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
    pub fn as_str(&self) -> &'static str {
        match self {
            AgoUnit::Seconds => "s",
            AgoUnit::Minutes => "m",
            AgoUnit::Hours => "h",
            AgoUnit::Days => "d",
        }
    }

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
}

impl Default for InputMode {
    fn default() -> Self {
        Self::Normal
    }
}

/// Main application state
pub struct App {
    pub screen: Screen,
    pub s2: s2_sdk::S2,
    pub message: Option<StatusMessage>,
    pub show_help: bool,
    pub input_mode: InputMode,
    should_quit: bool,
}

impl App {
    pub fn new(s2: s2_sdk::S2) -> Self {
        Self {
            screen: Screen::Basins(BasinsState {
                loading: true,
                ..Default::default()
            }),
            s2,
            message: None,
            show_help: false,
            input_mode: InputMode::Normal,
            should_quit: false,
        }
    }

    pub async fn run<B: Backend>(mut self, terminal: &mut Terminal<B>) -> Result<(), CliError> {
        let (tx, mut rx) = mpsc::unbounded_channel();

        // Initial data load
        self.load_basins(tx.clone());

        loop {
            // Render
            terminal
                .draw(|f| ui::draw(f, &self))
                .map_err(|e| CliError::RecordWrite(format!("Failed to draw: {e}")))?;

            // Handle events
            tokio::select! {
                // Handle async events from background tasks
                Some(event) = rx.recv() => {
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

        // Screen-specific keys - handle in place to avoid borrow issues
        match &self.screen {
            Screen::Basins(_) => self.handle_basins_key(key, tx),
            Screen::Streams(_) => self.handle_streams_key(key, tx),
            Screen::StreamDetail(_) => self.handle_stream_detail_key(key, tx),
            Screen::ReadView(_) => self.handle_read_view_key(key),
        }
    }

    fn handle_input_key(&mut self, key: KeyEvent, tx: mpsc::UnboundedSender<Event>) {
        match &mut self.input_mode {
            InputMode::Normal => {}

            InputMode::CreateBasin { input } => {
                match key.code {
                    KeyCode::Esc => {
                        self.input_mode = InputMode::Normal;
                    }
                    KeyCode::Enter => {
                        if !input.is_empty() {
                            let name = input.clone();
                            self.create_basin(name, tx.clone());
                        }
                    }
                    KeyCode::Backspace => {
                        input.pop();
                    }
                    KeyCode::Char(c) => {
                        // Basin names: lowercase letters, numbers, hyphens
                        if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' {
                            input.push(c);
                        }
                    }
                    _ => {}
                }
            }

            InputMode::CreateStream { basin, input } => {
                match key.code {
                    KeyCode::Esc => {
                        self.input_mode = InputMode::Normal;
                    }
                    KeyCode::Enter => {
                        if !input.is_empty() {
                            let name = input.clone();
                            let basin = basin.clone();
                            self.create_stream(basin, name, tx.clone());
                        }
                    }
                    KeyCode::Backspace => {
                        input.pop();
                    }
                    KeyCode::Char(c) => {
                        input.push(c);
                    }
                    _ => {}
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

                // Basin has 7 rows: append, read, storage, retention_type, retention_age, ts_mode, ts_uncapped
                const BASIN_MAX_ROW: usize = 6;

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
                        if *selected < BASIN_MAX_ROW {
                            *selected += 1;
                        }
                    }
                    KeyCode::Char(' ') | KeyCode::Enter => {
                        match *selected {
                            0 => *create_stream_on_append = Some(!create_stream_on_append.unwrap_or(false)),
                            1 => *create_stream_on_read = Some(!create_stream_on_read.unwrap_or(false)),
                            2 => {
                                // Cycle storage class
                                *storage_class = match storage_class {
                                    None => Some(StorageClass::Express),
                                    Some(StorageClass::Express) => Some(StorageClass::Standard),
                                    Some(StorageClass::Standard) => None,
                                };
                            }
                            3 => {
                                // Toggle retention policy
                                *retention_policy = match retention_policy {
                                    RetentionPolicyOption::Infinite => RetentionPolicyOption::Age,
                                    RetentionPolicyOption::Age => RetentionPolicyOption::Infinite,
                                };
                            }
                            4 => {
                                // Edit retention age
                                if *retention_policy == RetentionPolicyOption::Age {
                                    *editing_age = true;
                                    *age_input = retention_age_secs.to_string();
                                }
                            }
                            5 => {
                                // Cycle timestamping mode
                                *timestamping_mode = match timestamping_mode {
                                    None => Some(TimestampingMode::ClientPrefer),
                                    Some(TimestampingMode::ClientPrefer) => Some(TimestampingMode::ClientRequire),
                                    Some(TimestampingMode::ClientRequire) => Some(TimestampingMode::Arrival),
                                    Some(TimestampingMode::Arrival) => None,
                                };
                            }
                            6 => *timestamping_uncapped = Some(!timestamping_uncapped.unwrap_or(false)),
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
                selected,
                editing_age,
                age_input,
            } => {
                // If editing age, handle number input
                if *editing_age {
                    match key.code {
                        KeyCode::Esc | KeyCode::Enter => {
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

                // Stream has 5 rows: storage, retention_type, retention_age, ts_mode, ts_uncapped
                const STREAM_MAX_ROW: usize = 4;

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
                        if *selected < STREAM_MAX_ROW {
                            *selected += 1;
                        }
                    }
                    KeyCode::Char(' ') | KeyCode::Enter => {
                        match *selected {
                            0 => {
                                *storage_class = match storage_class {
                                    None => Some(StorageClass::Express),
                                    Some(StorageClass::Express) => Some(StorageClass::Standard),
                                    Some(StorageClass::Standard) => None,
                                };
                            }
                            1 => {
                                *retention_policy = match retention_policy {
                                    RetentionPolicyOption::Infinite => RetentionPolicyOption::Age,
                                    RetentionPolicyOption::Age => RetentionPolicyOption::Infinite,
                                };
                            }
                            2 => {
                                if *retention_policy == RetentionPolicyOption::Age {
                                    *editing_age = true;
                                    *age_input = retention_age_secs.to_string();
                                }
                            }
                            3 => {
                                *timestamping_mode = match timestamping_mode {
                                    None => Some(TimestampingMode::ClientPrefer),
                                    Some(TimestampingMode::ClientPrefer) => Some(TimestampingMode::ClientRequire),
                                    Some(TimestampingMode::ClientRequire) => Some(TimestampingMode::Arrival),
                                    Some(TimestampingMode::Arrival) => None,
                                };
                            }
                            4 => *timestamping_uncapped = Some(!timestamping_uncapped.unwrap_or(false)),
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
                selected,
                editing,
            } => {
                // If editing a value, handle text input
                if *editing {
                    match key.code {
                        KeyCode::Esc => {
                            *editing = false;
                        }
                        KeyCode::Enter => {
                            *editing = false;
                        }
                        KeyCode::Backspace => {
                            match *selected {
                                1 => { seq_num_value.pop(); }
                                2 => { timestamp_value.pop(); }
                                3 => { ago_value.pop(); }
                                5 => { tail_offset_value.pop(); }
                                6 => { count_limit.pop(); }
                                7 => { byte_limit.pop(); }
                                8 => { until_timestamp.pop(); }
                                _ => {}
                            }
                        }
                        KeyCode::Char(c) if c.is_ascii_digit() => {
                            match *selected {
                                1 => seq_num_value.push(c),
                                2 => timestamp_value.push(c),
                                3 => ago_value.push(c),
                                5 => tail_offset_value.push(c),
                                6 => count_limit.push(c),
                                7 => byte_limit.push(c),
                                8 => until_timestamp.push(c),
                                _ => {}
                            }
                        }
                        _ => {}
                    }
                    return;
                }

                // Navigation and selection
                // Rows:
                // 0: start_from selector
                // 1: seq_num (if SeqNum)
                // 2: timestamp (if Timestamp)
                // 3: ago value (if Ago)
                // 4: ago unit (if Ago)
                // 5: tail_offset (if TailOffset)
                // 6: count_limit
                // 7: byte_limit
                // 8: until_timestamp
                // 9: [Start Reading] button
                const MAX_ROW: usize = 9;

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
                    KeyCode::Char(' ') | KeyCode::Enter => {
                        match *selected {
                            0 => {
                                // Cycle start_from
                                *start_from = match start_from {
                                    ReadStartFrom::Tail => ReadStartFrom::SeqNum,
                                    ReadStartFrom::SeqNum => ReadStartFrom::Timestamp,
                                    ReadStartFrom::Timestamp => ReadStartFrom::Ago,
                                    ReadStartFrom::Ago => ReadStartFrom::TailOffset,
                                    ReadStartFrom::TailOffset => ReadStartFrom::Tail,
                                };
                            }
                            1 if *start_from == ReadStartFrom::SeqNum => {
                                *editing = true;
                            }
                            2 if *start_from == ReadStartFrom::Timestamp => {
                                *editing = true;
                            }
                            3 if *start_from == ReadStartFrom::Ago => {
                                *editing = true;
                            }
                            4 if *start_from == ReadStartFrom::Ago => {
                                *ago_unit = ago_unit.next();
                            }
                            5 if *start_from == ReadStartFrom::TailOffset => {
                                *editing = true;
                            }
                            6 => *editing = true, // count_limit
                            7 => *editing = true, // byte_limit
                            8 => *editing = true, // until_timestamp
                            9 => {
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
                                self.input_mode = InputMode::Normal;
                                self.start_custom_read(b, s, sf, snv, tsv, agv, agu, tov, cl, bl, ut, tx.clone());
                            }
                            _ => {}
                        }
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
                self.input_mode = InputMode::CreateBasin { input: String::new() };
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
                    input: String::new(),
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
                        selected: 0,
                        editing_age: false,
                        age_input: String::new(),
                    };
                    // Load current config
                    self.load_stream_config_for_reconfig(basin_name, stream_name, tx);
                }
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
                if state.selected_action < 1 {
                    // 2 actions: tail, custom read
                    state.selected_action += 1;
                }
            }
            KeyCode::Enter => {
                let basin_name = state.basin_name.clone();
                let stream_name = state.stream_name.clone();
                match state.selected_action {
                    0 => self.start_tail(basin_name, stream_name, tx), // Tail
                    1 => self.open_custom_read_dialog(basin_name, stream_name), // Custom read
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
                    selected: 0,
                    editing_age: false,
                    age_input: String::new(),
                };
                self.load_stream_config_for_reconfig(basin_name, stream_name, tx);
            }
            _ => {}
        }
    }

    fn handle_read_view_key(&mut self, key: KeyEvent) {
        let Screen::ReadView(state) = &mut self.screen else {
            return;
        };

        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => {
                // Go back to stream detail
                self.screen = Screen::StreamDetail(StreamDetailState {
                    basin_name: state.basin_name.clone(),
                    stream_name: state.stream_name.clone(),
                    config: None,
                    tail_position: None,
                    selected_action: 0,
                    loading: false,
                });
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
                if state.scroll_offset > 0 {
                    state.scroll_offset -= 1;
                }
            }
            KeyCode::Down | KeyCode::Char('j') => {
                let max_offset = state.records.len().saturating_sub(1);
                if state.scroll_offset < max_offset {
                    state.scroll_offset += 1;
                }
            }
            KeyCode::Char('g') => {
                state.scroll_offset = 0;
            }
            KeyCode::Char('G') => {
                state.scroll_offset = state.records.len().saturating_sub(1);
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

    fn create_basin(&mut self, name: String, tx: mpsc::UnboundedSender<Event>) {
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
            let args = CreateBasinArgs {
                basin: S2BasinUri(basin_name),
                config: BasinConfig {
                    default_stream_config: StreamConfig::default(),
                    create_stream_on_append: false,
                    create_stream_on_read: false,
                },
            };
            match ops::create_basin(&s2, args).await {
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

    fn create_stream(&mut self, basin: BasinName, name: String, tx: mpsc::UnboundedSender<Event>) {
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
                config: StreamConfig::default(),
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
            scroll_offset: 0,
            paused: false,
            loading: true,
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
        tx: mpsc::UnboundedSender<Event>,
    ) {
        self.screen = Screen::ReadView(ReadViewState {
            basin_name: basin_name.clone(),
            stream_name: stream_name.clone(),
            records: VecDeque::new(),
            is_tailing: true,
            scroll_offset: 0,
            paused: false,
            loading: true,
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
            } else if start_from == ReadStartFrom::Tail {
                Some(0) // Tail means TailOffset(0)
            } else {
                None
            };

            let count = count_limit.parse().ok().filter(|&v| v > 0);
            let bytes = byte_limit.parse().ok().filter(|&v| v > 0);
            let until = until_timestamp.parse().ok().filter(|&v| v > 0);

            let args = ReadArgs {
                uri,
                seq_num,
                timestamp,
                ago,
                tail_offset,
                count,
                bytes,
                clamp: true,
                until,
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

                    let info = StreamConfigInfo {
                        storage_class,
                        retention_age_secs,
                        timestamping_mode,
                        timestamping_uncapped,
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

            let args = ReconfigureStreamArgs {
                uri: S2BasinAndStreamUri { basin, stream },
                config: StreamConfig {
                    storage_class: config.storage_class,
                    retention_policy,
                    timestamping,
                    delete_on_empty: None,
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
}
