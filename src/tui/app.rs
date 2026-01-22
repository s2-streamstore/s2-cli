use std::collections::VecDeque;
use std::time::Duration;

use crossterm::event::{self, Event as CrosstermEvent, KeyCode, KeyEvent, KeyModifiers};
use futures::StreamExt;
use ratatui::{Terminal, prelude::Backend};
use s2_sdk::types::{BasinInfo, BasinName, StreamInfo, StreamName, StreamPosition};
use tokio::sync::mpsc;

use crate::cli::{CreateBasinArgs, CreateStreamArgs, ListBasinsArgs, ListStreamsArgs, TailArgs};
use crate::error::CliError;
use crate::ops;
use crate::record_format::{RecordFormat, RecordsOut};
use crate::types::{BasinConfig, S2BasinAndMaybeStreamUri, S2BasinAndStreamUri, S2BasinUri, StreamConfig};

use super::event::Event;
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
                    // 2 actions: read, tail
                    state.selected_action += 1;
                }
            }
            KeyCode::Enter => {
                let basin_name = state.basin_name.clone();
                let stream_name = state.stream_name.clone();
                match state.selected_action {
                    0 => {
                        // Read from start
                        self.start_read(basin_name, stream_name, false, tx);
                    }
                    1 => {
                        // Tail
                        self.start_read(basin_name, stream_name, true, tx);
                    }
                    _ => {}
                }
            }
            KeyCode::Char('r') => {
                let basin_name = state.basin_name.clone();
                let stream_name = state.stream_name.clone();
                self.start_read(basin_name, stream_name, false, tx);
            }
            KeyCode::Char('t') => {
                let basin_name = state.basin_name.clone();
                let stream_name = state.stream_name.clone();
                self.start_read(basin_name, stream_name, true, tx);
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

    fn start_read(
        &mut self,
        basin_name: BasinName,
        stream_name: StreamName,
        is_tailing: bool,
        tx: mpsc::UnboundedSender<Event>,
    ) {
        self.screen = Screen::ReadView(ReadViewState {
            basin_name: basin_name.clone(),
            stream_name: stream_name.clone(),
            records: VecDeque::new(),
            is_tailing,
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
            let args = TailArgs {
                uri,
                lines: if is_tailing { 10 } else { 100 },
                follow: is_tailing,
                format: RecordFormat::Text,
                output: RecordsOut::Stdout,
            };

            match ops::tail(&s2, &args).await {
                Ok(mut stream) => {
                    while let Some(result) = stream.next().await {
                        match result {
                            Ok(record) => {
                                if tx.send(Event::RecordReceived(Ok(record))).is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                let _ = tx.send(Event::RecordReceived(Err(e)));
                                break;
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
}
