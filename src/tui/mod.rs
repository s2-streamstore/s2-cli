mod app;
mod event;
mod screens;
mod ui;
mod widgets;

use std::io;

use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{Terminal, prelude::CrosstermBackend};

use crate::config::{load_cli_config, sdk_config};
use crate::error::CliError;
use app::App;

pub async fn run() -> Result<(), CliError> {
    // Load config and create SDK client
    let cli_config = load_cli_config()?;
    let sdk_config = sdk_config(&cli_config)?;
    let s2 = s2_sdk::S2::new(sdk_config).map_err(CliError::SdkInit)?;

    // Setup terminal
    enable_raw_mode().map_err(|e| CliError::RecordWrite(format!("Failed to enable raw mode: {e}")))?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)
        .map_err(|e| CliError::RecordWrite(format!("Failed to setup terminal: {e}")))?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)
        .map_err(|e| CliError::RecordWrite(format!("Failed to create terminal: {e}")))?;

    // Create and run app
    let app = App::new(s2);
    let result = app.run(&mut terminal).await;

    // Restore terminal
    disable_raw_mode().ok();
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )
    .ok();
    terminal.show_cursor().ok();

    result
}
