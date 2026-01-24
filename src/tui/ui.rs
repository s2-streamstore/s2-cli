use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, List, ListItem, Padding, Paragraph},
};

use crate::types::{StorageClass, TimestampingMode};

use super::app::{AccessTokensState, App, AgoUnit, AppendViewState, BasinsState, CompressionOption, ExpiryOption, InputMode, MessageLevel, MetricCategory, MetricsType, MetricsViewState, ReadStartFrom, ReadViewState, RetentionPolicyOption, ScopeOption, Screen, SettingsState, SetupState, StreamDetailState, StreamsState, Tab};

// S2 Console dark theme
const GREEN: Color = Color::Rgb(34, 197, 94);            // Active green
const YELLOW: Color = Color::Rgb(250, 204, 21);          // Warning yellow
const RED: Color = Color::Rgb(239, 68, 68);              // Error red
const CYAN: Color = Color::Rgb(34, 211, 238);            // Cyan accent
const WHITE: Color = Color::Rgb(255, 255, 255);          // Pure white
const GRAY_100: Color = Color::Rgb(243, 244, 246);       // Near white
const GRAY_500: Color = Color::Rgb(107, 114, 128);       // Muted gray
const BG_DARK: Color = Color::Rgb(17, 17, 17);           // Main background
const BG_PANEL: Color = Color::Rgb(24, 24, 27);          // Panel background
const BORDER: Color = Color::Rgb(63, 63, 70);            // Border gray

// Semantic aliases
const ACCENT: Color = WHITE;
const SUCCESS: Color = GREEN;
const WARNING: Color = YELLOW;
const ERROR: Color = RED;
const TEXT_PRIMARY: Color = WHITE;
const TEXT_SECONDARY: Color = GRAY_100;
const TEXT_MUTED: Color = GRAY_500;

pub fn draw(f: &mut Frame, app: &App) {
    // Clear with dark CRT background
    let area = f.area();
    f.render_widget(Block::default().style(Style::default().bg(BG_DARK)), area);

    // Splash screen uses full area
    if matches!(app.screen, Screen::Splash) {
        draw_splash(f, area);
        return;
    }

    // Setup screen uses full area (no tabs or status bar)
    if matches!(app.screen, Screen::Setup(_)) {
        if let Screen::Setup(state) = &app.screen {
            draw_setup(f, area, state);
        }
        return;
    }

    // Check if we should show tabs (only on top-level screens)
    let show_tabs = matches!(app.screen, Screen::Basins(_) | Screen::AccessTokens(_) | Screen::Settings(_));

    let chunks = if show_tabs {
        Layout::default()
            .direction(Direction::Vertical)
            .margin(1)
            .constraints([
                Constraint::Length(1), // Tab bar
                Constraint::Min(3),    // Main content
                Constraint::Length(1), // Status bar
            ])
            .split(area)
    } else {
        Layout::default()
            .direction(Direction::Vertical)
            .margin(1)
            .constraints([
                Constraint::Length(0), // No tab bar
                Constraint::Min(3),    // Main content
                Constraint::Length(1), // Status bar
            ])
            .split(area)
    };

    // Draw tab bar if on top-level screen
    if show_tabs {
        draw_tab_bar(f, chunks[0], app.tab);
    }

    // Draw main content based on screen
    match &app.screen {
        Screen::Splash => unreachable!(),
        Screen::Setup(_) => unreachable!(), // Handled above
        Screen::Basins(state) => draw_basins(f, chunks[1], state),
        Screen::Streams(state) => draw_streams(f, chunks[1], state),
        Screen::StreamDetail(state) => draw_stream_detail(f, chunks[1], state),
        Screen::ReadView(state) => draw_read_view(f, chunks[1], state),
        Screen::AppendView(state) => draw_append_view(f, chunks[1], state),
        Screen::AccessTokens(state) => draw_access_tokens(f, chunks[1], state),
        Screen::MetricsView(state) => draw_metrics_view(f, chunks[1], state),
        Screen::Settings(state) => draw_settings(f, chunks[1], state),
    }

    // Draw time picker popup if open
    if let Screen::MetricsView(state) = &app.screen {
        if state.time_picker_open {
            draw_time_picker(f, state);
        }
        if state.calendar_open {
            draw_calendar_picker(f, state);
        }
    }

    // Draw status bar
    draw_status_bar(f, chunks[2], app);

    // Draw help overlay if visible
    if app.show_help {
        draw_help_overlay(f, &app.screen);
    }

    // Draw input dialog if in input mode
    if !matches!(app.input_mode, InputMode::Normal) {
        draw_input_dialog(f, &app.input_mode);
    }
}

fn draw_splash(f: &mut Frame, area: Rect) {
    // Draw aurora background effect
    draw_aurora_background(f, area);

    // S2 logo
    let logo = vec![
        "   █████████████████████████    ",
        "  ██████████████████████████████ ",
        " ███████████████████████████████ ",
        "█████████████████████████████████",
        "█████████████████████████████████  ",
        "███████████████                  ",
        "███████████████                  ",
        "██████████████   ████████████████",
        "██████████████   ████████████████",
        "██████████████   ████████████████",
        "███████████████           ███████",
        "██████████████████          █████",
        "█████████████████████████    ████",
        "█████████████████████████   █████",
        "██████                     ██████",
        "█████                    ████████",
        " ███    ██████████████████████ ",
        "  ██    ██████████████████████ ",
        "         ████████████████████    ",
    ];

    // Create lines with logo (centered)
    let mut lines: Vec<Line> = logo
        .iter()
        .map(|&line| Line::from(Span::styled(line, Style::default().fg(Color::White))))
        .collect();

    // Add tagline below logo
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "Streams as a cloud",
        Style::default().fg(Color::White).bold(),
    )));
    lines.push(Line::from(Span::styled(
        "storage primitive",
        Style::default().fg(Color::White).bold(),
    )));
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "The serverless API for unlimited, durable, real-time streams.",
        Style::default().fg(TEXT_MUTED),
    )));

    let content_height = lines.len() as u16;

    // Center vertically
    let y = area.y + area.height.saturating_sub(content_height) / 2;

    let centered_area = Rect::new(area.x, y, area.width, content_height);
    let logo_widget = Paragraph::new(lines).alignment(Alignment::Center);
    f.render_widget(logo_widget, centered_area);
}

/// Draw the setup screen (first-time token entry)
fn draw_setup(f: &mut Frame, area: Rect, state: &SetupState) {
    // Draw aurora background
    draw_aurora_background(f, area);

    // S2 logo (same as splash)
    let logo = vec![
        "   █████████████████████████    ",
        "  ██████████████████████████████ ",
        " ███████████████████████████████ ",
        "█████████████████████████████████",
        "█████████████████████████████████  ",
        "███████████████                  ",
        "███████████████                  ",
        "██████████████   ████████████████",
        "██████████████   ████████████████",
        "██████████████   ████████████████",
        "███████████████           ███████",
        "██████████████████          █████",
        "█████████████████████████    ████",
        "█████████████████████████   █████",
        "██████                     ██████",
        "█████                    ████████",
        " ███    ██████████████████████ ",
        "  ██    ██████████████████████ ",
        "         ████████████████████    ",
    ];

    // Build content
    let mut lines: Vec<Line> = logo
        .iter()
        .map(|&line| Line::from(Span::styled(line, Style::default().fg(Color::White))))
        .collect();

    // Tagline
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "Streams as a cloud storage primitive",
        Style::default().fg(WHITE).bold(),
    )));
    lines.push(Line::from(Span::styled(
        "The serverless API for unlimited, durable, real-time streams.",
        Style::default().fg(TEXT_MUTED),
    )));

    // Spacer
    lines.push(Line::from(""));
    lines.push(Line::from(""));

    // Token input - minimal style
    let token_display = if state.access_token.is_empty() {
        vec![
            Span::styled("Token ", Style::default().fg(TEXT_MUTED)),
            Span::styled("› ", Style::default().fg(BORDER)),
            Span::styled("_", Style::default().fg(CYAN)),
        ]
    } else {
        let display = if state.access_token.len() > 40 {
            format!("{}...", &state.access_token[..40])
        } else {
            state.access_token.clone()
        };
        vec![
            Span::styled("Token ", Style::default().fg(TEXT_MUTED)),
            Span::styled("› ", Style::default().fg(GREEN)),
            Span::styled(display, Style::default().fg(WHITE)),
            Span::styled("_", Style::default().fg(CYAN)),
        ]
    };
    lines.push(Line::from(token_display));

    // Status/error line
    lines.push(Line::from(""));
    if let Some(error) = &state.error {
        lines.push(Line::from(Span::styled(
            error.as_str(),
            Style::default().fg(ERROR),
        )));
    } else if state.validating {
        lines.push(Line::from(Span::styled(
            "Validating...",
            Style::default().fg(YELLOW),
        )));
    } else {
        lines.push(Line::from(vec![
            Span::styled("Get token: ", Style::default().fg(TEXT_MUTED)),
            Span::styled("s2.dev/dashboard/access-tokens", Style::default().fg(CYAN)),
        ]));
    }

    // Footer hint
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "Enter to continue · Esc to quit",
        Style::default().fg(BORDER),
    )));

    let content_height = lines.len() as u16;
    let y = area.y + area.height.saturating_sub(content_height) / 2;
    let centered_area = Rect::new(area.x, y, area.width, content_height);

    let content = Paragraph::new(lines).alignment(Alignment::Center);
    f.render_widget(content, centered_area);
}

/// Draw the settings screen
fn draw_settings(f: &mut Frame, area: Rect, state: &SettingsState) {
    use ratatui::widgets::BorderType;

    // Layout: Title bar + content
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Title bar (consistent height)
            Constraint::Min(1),    // Content
        ])
        .split(area);

    // === TITLE BAR ===
    let title_block = Block::default()
        .borders(Borders::BOTTOM)
        .border_style(Style::default().fg(BORDER))
        .style(Style::default().bg(BG_PANEL));
    let title_content = Paragraph::new(Line::from(vec![
        Span::styled(" ⚙ ", Style::default().fg(GREEN)),
        Span::styled("Settings", Style::default().fg(TEXT_PRIMARY).bold()),
    ]))
    .block(title_block);
    f.render_widget(title_content, chunks[0]);

    // === CONTENT ===
    let content_area = chunks[1];

    // Create centered settings panel
    let panel_width = 70.min(content_area.width.saturating_sub(4));
    let panel_x = content_area.x + (content_area.width.saturating_sub(panel_width)) / 2;
    let panel_area = Rect::new(panel_x, content_area.y + 1, panel_width, content_area.height.saturating_sub(2));

    let settings_block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(BORDER))
        .style(Style::default().bg(BG_PANEL))
        .padding(Padding::new(2, 2, 1, 1));
    let inner = settings_block.inner(panel_area);
    f.render_widget(settings_block, panel_area);

    // Settings fields
    let field_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4), // Access Token (label + 3-line bordered input)
            Constraint::Length(4), // Account Endpoint
            Constraint::Length(4), // Basin Endpoint
            Constraint::Length(3), // Compression (label + pills, no border)
            Constraint::Length(1), // Spacer
            Constraint::Length(3), // Save button
            Constraint::Min(1),    // Message/footer
        ])
        .split(inner);

    // Access Token field
    // Always show actual value when editing, otherwise respect mask flag
    let is_editing_token = state.editing && state.selected == 0;
    let token_display = if is_editing_token {
        // When editing, always show actual value
        state.access_token.clone()
    } else if state.access_token_masked && !state.access_token.is_empty() {
        format!("{}...", "*".repeat(20.min(state.access_token.len())))
    } else if state.access_token.is_empty() {
        "(not set)".to_string()
    } else {
        state.access_token.clone()
    };

    draw_settings_field(
        f,
        field_chunks[0],
        "Access Token",
        token_display,
        state.selected == 0,
        is_editing_token,
        Some("Space to toggle visibility"),
    );

    // Account Endpoint field
    draw_settings_field(
        f,
        field_chunks[1],
        "Account Endpoint",
        if state.account_endpoint.is_empty() {
            "(default)".to_string()
        } else {
            state.account_endpoint.clone()
        },
        state.selected == 1,
        state.editing && state.selected == 1,
        None,
    );

    // Basin Endpoint field
    draw_settings_field(
        f,
        field_chunks[2],
        "Basin Endpoint",
        if state.basin_endpoint.is_empty() {
            "(default)".to_string()
        } else {
            state.basin_endpoint.clone()
        },
        state.selected == 2,
        state.editing && state.selected == 2,
        None,
    );

    // Compression field (pill selector)
    let compression_label = Line::from(vec![
        Span::styled("Compression", Style::default().fg(TEXT_SECONDARY)),
    ]);
    f.render_widget(Paragraph::new(compression_label), Rect::new(field_chunks[3].x, field_chunks[3].y, field_chunks[3].width, 1));

    let options = [CompressionOption::None, CompressionOption::Gzip, CompressionOption::Zstd];
    let pills: Vec<Span> = options.iter().map(|opt| {
        let is_selected = *opt == state.compression;
        let style = if is_selected {
            Style::default().fg(BG_DARK).bg(GREEN).bold()
        } else {
            Style::default().fg(TEXT_MUTED).bg(BG_DARK)
        };
        Span::styled(format!(" {} ", opt.as_str()), style)
    }).collect();

    let mut pill_line = vec![Span::styled("  ", Style::default())];
    for (i, pill) in pills.into_iter().enumerate() {
        if i > 0 {
            pill_line.push(Span::styled(" ", Style::default()));
        }
        pill_line.push(pill);
    }
    if state.selected == 3 {
        pill_line.push(Span::styled("  ← h/l →", Style::default().fg(TEXT_MUTED)));
    }

    let compression_row = Rect::new(field_chunks[3].x, field_chunks[3].y + 1, field_chunks[3].width, 1);
    f.render_widget(
        Paragraph::new(Line::from(pill_line))
            .style(Style::default().bg(BG_DARK)),
        compression_row,
    );

    // Save button
    let save_style = if state.selected == 4 {
        Style::default().fg(BG_DARK).bg(GREEN).bold()
    } else if state.has_changes {
        Style::default().fg(GREEN)
    } else {
        Style::default().fg(TEXT_MUTED)
    };
    let save_text = if state.has_changes {
        " ● Save Changes "
    } else {
        "   Save Changes "
    };
    let save_button = Paragraph::new(Line::from(Span::styled(save_text, save_style)))
        .alignment(Alignment::Center);
    f.render_widget(save_button, field_chunks[5]);

    // Message/footer
    if let Some(msg) = &state.message {
        let msg_style = if msg.contains("success") || msg.contains("saved") {
            Style::default().fg(SUCCESS)
        } else {
            Style::default().fg(ERROR)
        };
        let msg_para = Paragraph::new(Line::from(Span::styled(msg.as_str(), msg_style)))
            .alignment(Alignment::Center);
        f.render_widget(msg_para, field_chunks[6]);
    } else {
        let footer = Paragraph::new(Line::from(Span::styled(
            "j/k navigate • e/Enter edit • r reload",
            Style::default().fg(TEXT_MUTED),
        )))
        .alignment(Alignment::Center);
        f.render_widget(footer, field_chunks[6]);
    }
}

/// Helper to draw a settings field
fn draw_settings_field(
    f: &mut Frame,
    area: Rect,
    label: &str,
    value: String,
    selected: bool,
    editing: bool,
    hint: Option<&str>,
) {
    let label_line = Line::from(vec![
        Span::styled(label, Style::default().fg(TEXT_SECONDARY)),
        if let Some(h) = hint {
            Span::styled(format!("  ({})", h), Style::default().fg(TEXT_MUTED))
        } else {
            Span::raw("")
        },
    ]);
    f.render_widget(Paragraph::new(label_line), Rect::new(area.x, area.y, area.width, 1));

    let border_style = if selected {
        Style::default().fg(GREEN)
    } else {
        Style::default().fg(BORDER)
    };

    let value_display = if editing {
        format!("{}█", value)
    } else {
        value
    };

    let value_style = if value_display.starts_with('(') {
        Style::default().fg(TEXT_MUTED)
    } else {
        Style::default().fg(TEXT_PRIMARY)
    };

    let value_block = Block::default()
        .borders(Borders::ALL)
        .border_style(border_style)
        .style(Style::default().bg(BG_DARK));
    let value_para = Paragraph::new(Span::styled(value_display, value_style))
        .block(value_block);
    // Height must be 3: top border (1) + content (1) + bottom border (1)
    f.render_widget(value_para, Rect::new(area.x, area.y + 1, area.width, 3));
}

/// Draw a subtle aurora/gradient background effect
fn draw_aurora_background(f: &mut Frame, area: Rect) {
    let width = area.width as f64;
    let height = area.height as f64;

    for row in 0..area.height {
        let mut spans: Vec<Span> = Vec::new();
        for col in 0..area.width {
            // Normalize coordinates
            let x = col as f64 / width;
            let y = row as f64 / height;

            // Create aurora effect - subtle glow from bottom-right and center
            // Distance from bottom-right corner
            let dist_br = ((x - 0.8).powi(2) + (y - 0.9).powi(2)).sqrt();
            // Distance from center-bottom
            let dist_cb = ((x - 0.5).powi(2) + (y - 0.85).powi(2)).sqrt();

            // Aurora intensity (stronger near bottom)
            let intensity_br = (1.0 - dist_br * 1.5).max(0.0) * 0.4;
            let intensity_cb = (1.0 - dist_cb * 1.8).max(0.0) * 0.3;
            let intensity = (intensity_br + intensity_cb).min(1.0);

            // Base dark color with subtle blue/teal tint
            let base_r = 8;
            let base_g = 12;
            let base_b = 18;

            // Aurora colors (teal/cyan)
            let aurora_r = 0;
            let aurora_g = 40;
            let aurora_b = 60;

            let r = base_r + ((aurora_r - base_r as i32) as f64 * intensity) as u8;
            let g = base_g + ((aurora_g - base_g as i32) as f64 * intensity) as u8;
            let b = base_b + ((aurora_b - base_b as i32) as f64 * intensity) as u8;

            spans.push(Span::styled(" ", Style::default().bg(Color::Rgb(r, g, b))));
        }
        let line = Line::from(spans);
        let row_area = Rect::new(area.x, area.y + row, area.width, 1);
        f.render_widget(Paragraph::new(line), row_area);
    }
}

fn draw_tab_bar(f: &mut Frame, area: Rect, current_tab: Tab) {
    let basins_style = if current_tab == Tab::Basins {
        Style::default().fg(GREEN).bold()
    } else {
        Style::default().fg(TEXT_MUTED)
    };

    let tokens_style = if current_tab == Tab::AccessTokens {
        Style::default().fg(GREEN).bold()
    } else {
        Style::default().fg(TEXT_MUTED)
    };

    let settings_style = if current_tab == Tab::Settings {
        Style::default().fg(GREEN).bold()
    } else {
        Style::default().fg(TEXT_MUTED)
    };

    let line = Line::from(vec![
        Span::styled("Basins", basins_style),
        Span::styled("  │  ", Style::default().fg(BORDER)),
        Span::styled("Access Tokens", tokens_style),
        Span::styled("  │  ", Style::default().fg(BORDER)),
        Span::styled("Settings", settings_style),
        Span::styled("  (Tab to switch)", Style::default().fg(TEXT_MUTED)),
    ]);

    let paragraph = Paragraph::new(line);
    f.render_widget(paragraph, area);
}

fn draw_access_tokens(f: &mut Frame, area: Rect, state: &AccessTokensState) {
    // Layout: Title bar, Search bar, Header, Table rows
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Title bar (consistent height)
            Constraint::Length(3), // Search bar
            Constraint::Length(2), // Header
            Constraint::Min(1),    // Table rows
        ])
        .split(area);

    // === Title Bar ===
    let count_text = if state.loading {
        " loading...".to_string()
    } else {
        let filtered_count = state.tokens.iter()
            .filter(|t| state.filter.is_empty() || t.id.to_lowercase().contains(&state.filter.to_lowercase()))
            .count();
        if filtered_count != state.tokens.len() {
            format!("  {}/{} tokens", filtered_count, state.tokens.len())
        } else {
            format!("  {} tokens", state.tokens.len())
        }
    };
    let title_lines = vec![
        Line::from(""),
        Line::from(vec![
            Span::styled("  Access Tokens", Style::default().fg(GREEN).bold()),
            Span::styled(&count_text, Style::default().fg(Color::Rgb(100, 100, 100))),
        ]),
    ];
    let title_block = Paragraph::new(title_lines)
        .block(Block::default()
            .borders(Borders::BOTTOM)
            .border_style(Style::default().fg(Color::Rgb(40, 40, 40))));
    f.render_widget(title_block, chunks[0]);

    // === Search Bar ===
    let search_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(if state.filter_active { GREEN } else { BORDER }))
        .style(Style::default().bg(BG_PANEL));

    let search_text = if state.filter_active {
        Line::from(vec![
            Span::styled(" [/] ", Style::default().fg(GREEN)),
            Span::styled(&state.filter, Style::default().fg(TEXT_PRIMARY)),
            Span::styled("_", Style::default().fg(GREEN)),
        ])
    } else if !state.filter.is_empty() {
        Line::from(vec![
            Span::styled(" [/] ", Style::default().fg(TEXT_MUTED)),
            Span::styled(&state.filter, Style::default().fg(TEXT_PRIMARY)),
        ])
    } else {
        Line::from(vec![
            Span::styled(" [/] Filter by token ID...", Style::default().fg(TEXT_MUTED)),
        ])
    };

    let search_para = Paragraph::new(search_text)
        .block(search_block)
        .style(Style::default().bg(BG_PANEL));
    f.render_widget(search_para, chunks[1]);

    // === Header ===
    // Column widths: prefix(2) + token_id(30) + expires_at(28) + scope(rest)
    let header = Line::from(vec![
        Span::styled("  ", Style::default()),  // Space for selection prefix
        Span::styled(
            format!("{:<30}", "TOKEN ID"),
            Style::default().fg(TEXT_MUTED).bold(),
        ),
        Span::styled(
            format!("{:<28}", "EXPIRES AT"),
            Style::default().fg(TEXT_MUTED).bold(),
        ),
        Span::styled("SCOPE", Style::default().fg(TEXT_MUTED).bold()),
    ]);
    let header_para = Paragraph::new(header);
    f.render_widget(header_para, chunks[2]);

    // === Token List ===
    // Filter tokens
    let filtered_tokens: Vec<_> = state
        .tokens
        .iter()
        .filter(|t| {
            state.filter.is_empty()
                || t.id.to_string().to_lowercase().contains(&state.filter.to_lowercase())
        })
        .collect();

    if state.loading {
        let loading = Paragraph::new(Line::from(Span::styled(
            "Loading access tokens...",
            Style::default().fg(TEXT_MUTED),
        )));
        f.render_widget(loading, chunks[3]);
    } else if filtered_tokens.is_empty() {
        let empty_msg = if state.tokens.is_empty() {
            "No access tokens. Press 'c' to issue a new token."
        } else {
            "No tokens match filter."
        };
        let empty = Paragraph::new(Line::from(Span::styled(
            empty_msg,
            Style::default().fg(TEXT_MUTED),
        )));
        f.render_widget(empty, chunks[3]);
    } else {
        let list_height = chunks[3].height as usize;
        let start = state.selected.saturating_sub(list_height / 2);
        let visible_tokens = filtered_tokens.iter().skip(start).take(list_height);

        let lines: Vec<Line> = visible_tokens
            .enumerate()
            .map(|(i, token)| {
                let actual_index = start + i;
                let is_selected = actual_index == state.selected;

                // Format scope summary
                let scope_summary = format_scope_summary(token);

                let style = if is_selected {
                    Style::default().fg(GREEN).bold()
                } else {
                    Style::default().fg(TEXT_PRIMARY)
                };

                let prefix = if is_selected { "▶ " } else { "  " };

                // Truncate token ID if too long (max 28 chars to leave room for padding)
                let token_id_str = token.id.to_string();
                let token_id_display = if token_id_str.len() > 28 {
                    format!("{}…", &token_id_str[..27])
                } else {
                    token_id_str
                };

                // Format expires_at more compactly
                let expires_str = token.expires_at.to_string();
                let expires_display = if expires_str.len() > 26 {
                    format!("{}…", &expires_str[..25])
                } else {
                    expires_str
                };

                Line::from(vec![
                    Span::styled(prefix, style),
                    Span::styled(format!("{:<30}", token_id_display), style),
                    Span::styled(format!("{:<28}", expires_display), Style::default().fg(TEXT_MUTED)),
                    Span::styled(scope_summary, Style::default().fg(TEXT_MUTED)),
                ])
            })
            .collect();

        let list = Paragraph::new(lines);
        f.render_widget(list, chunks[3]);
    }
}

/// Format a summary of the token scope
fn format_scope_summary(token: &s2_sdk::types::AccessTokenInfo) -> String {
    let ops_count = token.scope.ops.len();
    let has_basins = token.scope.basins.is_some();
    let has_streams = token.scope.streams.is_some();

    let mut parts = vec![format!("{} ops", ops_count)];
    if has_basins {
        parts.push("basins".to_string());
    }
    if has_streams {
        parts.push("streams".to_string());
    }
    parts.join(", ")
}

/// Format a basin matcher for display
fn format_basin_matcher(matcher: &Option<s2_sdk::types::BasinMatcher>) -> String {
    use s2_sdk::types::BasinMatcher;
    match matcher {
        None => "All".to_string(),
        Some(BasinMatcher::None) => "None".to_string(),
        Some(BasinMatcher::Prefix(p)) => format!("Prefix: {}", p),
        Some(BasinMatcher::Exact(e)) => format!("Exact: {}", e),
    }
}

/// Format a stream matcher for display
fn format_stream_matcher(matcher: &Option<s2_sdk::types::StreamMatcher>) -> String {
    use s2_sdk::types::StreamMatcher;
    match matcher {
        None => "All".to_string(),
        Some(StreamMatcher::None) => "None".to_string(),
        Some(StreamMatcher::Prefix(p)) => format!("Prefix: {}", p),
        Some(StreamMatcher::Exact(e)) => format!("Exact: {}", e),
    }
}

/// Format an access token matcher for display
fn format_token_matcher(matcher: &Option<s2_sdk::types::AccessTokenMatcher>) -> String {
    use s2_sdk::types::AccessTokenMatcher;
    match matcher {
        None => "All".to_string(),
        Some(AccessTokenMatcher::None) => "None".to_string(),
        Some(AccessTokenMatcher::Prefix(p)) => format!("Prefix: {}", p),
        Some(AccessTokenMatcher::Exact(e)) => format!("Exact: {}", e),
    }
}

/// Format an operation for display
fn format_operation(op: &s2_sdk::types::Operation) -> String {
    use s2_sdk::types::Operation as SdkOp;
    match op {
        SdkOp::ListBasins => "list_basins",
        SdkOp::CreateBasin => "create_basin",
        SdkOp::DeleteBasin => "delete_basin",
        SdkOp::GetBasinConfig => "get_basin_config",
        SdkOp::ReconfigureBasin => "reconfigure_basin",
        SdkOp::GetBasinMetrics => "get_basin_metrics",
        SdkOp::ListStreams => "list_streams",
        SdkOp::CreateStream => "create_stream",
        SdkOp::DeleteStream => "delete_stream",
        SdkOp::GetStreamConfig => "get_stream_config",
        SdkOp::ReconfigureStream => "reconfigure_stream",
        SdkOp::GetStreamMetrics => "get_stream_metrics",
        SdkOp::CheckTail => "check_tail",
        SdkOp::Read => "read",
        SdkOp::Append => "append",
        SdkOp::Fence => "fence",
        SdkOp::Trim => "trim",
        SdkOp::GetAccountMetrics => "get_account_metrics",
        SdkOp::ListAccessTokens => "list_access_tokens",
        SdkOp::IssueAccessToken => "issue_access_token",
        SdkOp::RevokeAccessToken => "revoke_access_token",
    }.to_string()
}

/// Check if operation is account-level
fn is_account_op(op: &s2_sdk::types::Operation) -> bool {
    use s2_sdk::types::Operation as SdkOp;
    matches!(op, SdkOp::ListBasins | SdkOp::GetAccountMetrics)
}

/// Check if operation is basin-level
fn is_basin_op(op: &s2_sdk::types::Operation) -> bool {
    use s2_sdk::types::Operation as SdkOp;
    matches!(op,
        SdkOp::CreateBasin | SdkOp::DeleteBasin |
        SdkOp::GetBasinConfig | SdkOp::ReconfigureBasin |
        SdkOp::ListStreams | SdkOp::GetBasinMetrics)
}

/// Check if operation is stream-level
fn is_stream_op(op: &s2_sdk::types::Operation) -> bool {
    use s2_sdk::types::Operation as SdkOp;
    matches!(op,
        SdkOp::CreateStream | SdkOp::DeleteStream |
        SdkOp::GetStreamConfig | SdkOp::ReconfigureStream |
        SdkOp::Read | SdkOp::Append | SdkOp::CheckTail |
        SdkOp::Fence | SdkOp::Trim | SdkOp::GetStreamMetrics)
}

/// Check if operation is token-related
fn is_token_op(op: &s2_sdk::types::Operation) -> bool {
    use s2_sdk::types::Operation as SdkOp;
    matches!(op, SdkOp::ListAccessTokens | SdkOp::IssueAccessToken | SdkOp::RevokeAccessToken)
}

fn draw_metrics_view(f: &mut Frame, area: Rect, state: &MetricsViewState) {
    use s2_sdk::types::Metric;

    // Layout: Title+tabs, Stats header, Main graph area, Timeline
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // Title with tabs
            Constraint::Length(3),  // Stats header row
            Constraint::Min(12),    // Main graph (area chart)
            Constraint::Length(6),  // Timeline (scrollable)
        ])
        .split(area);

    // === Title with integrated category tabs ===
    let title = match &state.metrics_type {
        MetricsType::Account => "Account".to_string(),
        MetricsType::Basin { basin_name } => basin_name.to_string(),
        MetricsType::Stream { basin_name, stream_name } => format!("{}/{}", basin_name, stream_name),
    };

    if matches!(state.metrics_type, MetricsType::Account) {
        // Account metrics have different categories
        let categories = [
            MetricCategory::ActiveBasins,
            MetricCategory::AccountOps,
        ];

        let mut title_spans: Vec<Span> = vec![
            Span::styled(" [ ", Style::default().fg(BORDER)),
            Span::styled(&title, Style::default().fg(GREEN).bold()),
            Span::styled(" ]  ", Style::default().fg(BORDER)),
        ];

        for (i, cat) in categories.iter().enumerate() {
            if i > 0 {
                title_spans.push(Span::styled(" | ", Style::default().fg(BORDER)));
            }
            let style = if *cat == state.selected_category {
                Style::default().fg(BG_DARK).bg(GREEN).bold()
            } else {
                Style::default().fg(TEXT_MUTED)
            };
            title_spans.push(Span::styled(format!(" {} ", cat.as_str()), style));
        }

        // Add time range to title
        title_spans.push(Span::styled("  ", Style::default()));
        title_spans.push(Span::styled(format!("[{}]", state.time_range.as_str()), Style::default().fg(CYAN)));

        let title_block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(GREEN))
            .title_bottom(Line::from(Span::styled(" ←/→ category  t time picker ", Style::default().fg(TEXT_MUTED))))
            .style(Style::default().bg(BG_PANEL));

        let title_para = Paragraph::new(Line::from(title_spans))
            .block(title_block)
            .alignment(Alignment::Center);
        f.render_widget(title_para, chunks[0]);
    } else if matches!(state.metrics_type, MetricsType::Basin { .. }) {
        let categories = [
            MetricCategory::Storage,
            MetricCategory::AppendOps,
            MetricCategory::ReadOps,
            MetricCategory::AppendThroughput,
            MetricCategory::ReadThroughput,
        ];

        let mut title_spans: Vec<Span> = vec![
            Span::styled(" [ ", Style::default().fg(BORDER)),
            Span::styled(&title, Style::default().fg(GREEN).bold()),
            Span::styled(" ]  ", Style::default().fg(BORDER)),
        ];

        for (i, cat) in categories.iter().enumerate() {
            if i > 0 {
                title_spans.push(Span::styled(" | ", Style::default().fg(BORDER)));
            }
            let style = if *cat == state.selected_category {
                Style::default().fg(BG_DARK).bg(GREEN).bold()
            } else {
                Style::default().fg(TEXT_MUTED)
            };
            title_spans.push(Span::styled(format!(" {} ", cat.as_str()), style));
        }

        // Add time range to title
        title_spans.push(Span::styled("  ", Style::default()));
        title_spans.push(Span::styled(format!("[{}]", state.time_range.as_str()), Style::default().fg(CYAN)));

        let title_block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(GREEN))
            .title_bottom(Line::from(Span::styled(" ←/→ category  t time picker ", Style::default().fg(TEXT_MUTED))))
            .style(Style::default().bg(BG_PANEL));

        let title_para = Paragraph::new(Line::from(title_spans))
            .block(title_block)
            .alignment(Alignment::Center);
        f.render_widget(title_para, chunks[0]);
    } else {
        let title_block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(GREEN))
            .title_bottom(Line::from(Span::styled(" t time picker ", Style::default().fg(TEXT_MUTED))))
            .style(Style::default().bg(BG_PANEL));

        let title_para = Paragraph::new(Line::from(vec![
            Span::styled(" [ ", Style::default().fg(BORDER)),
            Span::styled(&title, Style::default().fg(GREEN).bold()),
            Span::styled(" ]  ", Style::default().fg(BORDER)),
            Span::styled(format!("Storage [{}]", state.time_range.as_str()), Style::default().fg(TEXT_PRIMARY)),
        ]))
        .block(title_block)
        .alignment(Alignment::Center);
        f.render_widget(title_para, chunks[0]);
    }

    // === Loading / Empty states ===
    if state.loading {
        let loading_block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BORDER))
            .style(Style::default().bg(BG_DARK));
        let loading = Paragraph::new(vec![
            Line::from(""),
            Line::from(Span::styled("Loading metrics...", Style::default().fg(TEXT_MUTED))),
        ])
        .block(loading_block)
        .alignment(Alignment::Center);

        let remaining = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(1)])
            .split(chunks[1]);
        f.render_widget(loading, remaining[0]);
        return;
    }

    if state.metrics.is_empty() {
        let empty_block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BORDER))
            .style(Style::default().bg(BG_DARK));
        let empty = Paragraph::new(vec![
            Line::from(""),
            Line::from(Span::styled("No data in the last 24 hours", Style::default().fg(TEXT_MUTED))),
        ])
        .block(empty_block)
        .alignment(Alignment::Center);

        let remaining = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(1)])
            .split(chunks[1]);
        f.render_widget(empty, remaining[0]);
        return;
    }

    // Check for Label metrics first (like Active Basins)
    let mut label_values: Vec<String> = Vec::new();
    let mut label_name = String::new();

    for metric in &state.metrics {
        if let Metric::Label(m) = metric {
            label_name = m.name.clone();
            label_values.extend(m.values.iter().cloned());
        }
    }

    // If we have label metrics, render them differently
    if !label_values.is_empty() {
        render_label_metric(f, chunks, &label_name, &label_values, state);
        return;
    }

    // Check if we have multiple Accumulation metrics (like Account Ops)
    let accumulation_metrics: Vec<_> = state.metrics.iter()
        .filter_map(|m| if let Metric::Accumulation(a) = m { Some(a) } else { None })
        .collect();

    if accumulation_metrics.len() > 1 {
        // Multiple metrics - render as operations breakdown
        render_multi_metric(f, chunks, &accumulation_metrics, state);
        return;
    }

    // Collect all time-series values for rendering (single metric)
    let mut all_values: Vec<(u32, f64)> = Vec::new();
    let mut metric_name = String::new();
    let mut metric_unit = s2_sdk::types::MetricUnit::Bytes;

    for metric in &state.metrics {
        match metric {
            Metric::Gauge(m) => {
                metric_name = m.name.clone();
                metric_unit = m.unit;
                all_values.extend(m.values.iter().cloned());
            }
            Metric::Accumulation(m) => {
                metric_name = m.name.clone();
                metric_unit = m.unit;
                all_values.extend(m.values.iter().cloned());
            }
            Metric::Scalar(m) => {
                metric_name = m.name.clone();
                metric_unit = m.unit;
                all_values.push((0, m.value));
            }
            Metric::Label(_) => {} // Handled above
        }
    }

    if all_values.is_empty() {
        return;
    }

    // Sort by timestamp
    all_values.sort_by_key(|(ts, _)| *ts);

    // Calculate stats
    let values_only: Vec<f64> = all_values.iter().map(|(_, v)| *v).collect();
    let min_val = values_only.iter().cloned().fold(f64::MAX, f64::min);
    let max_val = values_only.iter().cloned().fold(f64::MIN, f64::max);
    let avg_val = if !values_only.is_empty() {
        values_only.iter().sum::<f64>() / values_only.len() as f64
    } else {
        0.0
    };
    let latest_val = values_only.last().cloned().unwrap_or(0.0);
    let first_val = values_only.first().cloned().unwrap_or(0.0);

    // Calculate change for trend indicator
    let change = if first_val > 0.0 {
        ((latest_val - first_val) / first_val) * 100.0
    } else if latest_val > 0.0 {
        100.0
    } else {
        0.0
    };

    // Time range
    let first_ts = all_values.first().map(|(ts, _)| *ts).unwrap_or(0);
    let last_ts = all_values.last().map(|(ts, _)| *ts).unwrap_or(0);

    // === Stats header row ===
    let stats_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(BORDER))
        .style(Style::default().bg(BG_PANEL));

    let stats_inner = stats_block.inner(chunks[1]);
    f.render_widget(stats_block, chunks[1]);

    // Trend indicator
    let (trend_arrow, trend_color) = if change > 1.0 {
        ("^", Color::Rgb(34, 197, 94))
    } else if change < -1.0 {
        ("v", Color::Rgb(239, 68, 68))
    } else {
        ("=", TEXT_MUTED)
    };
    let trend_text = if change.abs() > 0.1 {
        format!("{:+.1}%", change)
    } else {
        "stable".to_string()
    };

    let stats_line = Line::from(vec![
        Span::styled(" NOW ", Style::default().fg(BG_DARK).bg(GREEN).bold()),
        Span::styled(format!(" {} ", format_metric_value_f64(latest_val, metric_unit)), Style::default().fg(GREEN).bold()),
        Span::styled(trend_arrow, Style::default().fg(trend_color).bold()),
        Span::styled(format!("{} ", trend_text), Style::default().fg(trend_color)),
        Span::styled("  |  ", Style::default().fg(BORDER)),
        Span::styled("min ", Style::default().fg(TEXT_MUTED)),
        Span::styled(format_metric_value_f64(min_val, metric_unit), Style::default().fg(Color::Rgb(96, 165, 250))),
        Span::styled("  max ", Style::default().fg(TEXT_MUTED)),
        Span::styled(format_metric_value_f64(max_val, metric_unit), Style::default().fg(Color::Rgb(251, 191, 36))),
        Span::styled("  avg ", Style::default().fg(TEXT_MUTED)),
        Span::styled(format_metric_value_f64(avg_val, metric_unit), Style::default().fg(Color::Rgb(167, 139, 250))),
        Span::styled(format!("  |  {} pts", all_values.len()), Style::default().fg(TEXT_MUTED)),
    ]);
    let stats_para = Paragraph::new(stats_line).alignment(Alignment::Center);
    f.render_widget(stats_para, stats_inner);

    // === Main Area Chart ===
    let chart_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(GREEN))
        .title(Line::from(vec![
            Span::styled(" ", Style::default()),
            Span::styled(&metric_name, Style::default().fg(GREEN).bold()),
            Span::styled(" ", Style::default()),
        ]))
        .style(Style::default().bg(BG_DARK));

    let chart_inner = chart_block.inner(chunks[2]);
    f.render_widget(chart_block, chunks[2]);

    // Render the area chart
    render_area_chart(f, chart_inner, &all_values, min_val, max_val, metric_unit, first_ts, last_ts);

    // === Timeline (scrollable detail) ===
    let timeline_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(BORDER))
        .title(Line::from(vec![
            Span::styled(" Data Points ", Style::default().fg(TEXT_PRIMARY)),
            Span::styled(format!("[{}/{}]", state.scroll + 1, all_values.len()), Style::default().fg(TEXT_MUTED)),
        ]))
        .title_bottom(Line::from(Span::styled(" j/k scroll ", Style::default().fg(TEXT_MUTED))))
        .style(Style::default().bg(BG_DARK));

    let timeline_inner = timeline_block.inner(chunks[3]);
    f.render_widget(timeline_block, chunks[3]);

    // Compact timeline bars
    let bar_width = timeline_inner.width.saturating_sub(26) as usize;
    let visible_rows = timeline_inner.height as usize;

    let bars: Vec<Line> = all_values
        .iter()
        .skip(state.scroll)
        .take(visible_rows)
        .map(|(ts, value)| {
            let bar_len = if max_val > 0.0 {
                ((*value / max_val) * bar_width as f64) as usize
            } else {
                0
            };
            let intensity = if max_val > 0.0 { *value / max_val } else { 0.0 };

            // Gradient color based on intensity
            let bar_color = intensity_to_color(intensity);

            let bar: String = (0..bar_len).map(|i| {
                let pos = i as f64 / bar_len.max(1) as f64;
                if pos > 0.9 { '█' } else if pos > 0.7 { '▓' } else if pos > 0.4 { '▒' } else { '░' }
            }).collect();

            let time_str = format_metric_timestamp_short(*ts);

            Line::from(vec![
                Span::styled(format!(" {:>8} ", time_str), Style::default().fg(TEXT_MUTED)),
                Span::styled(bar, Style::default().fg(bar_color)),
                Span::styled(format!(" {:>10}", format_metric_value_f64(*value, metric_unit)), Style::default().fg(TEXT_SECONDARY)),
            ])
        })
        .collect();

    let bars_para = Paragraph::new(bars);
    f.render_widget(bars_para, timeline_inner);
}

/// Convert intensity (0.0-1.0) to a green gradient color
fn intensity_to_color(intensity: f64) -> Color {
    if intensity > 0.8 {
        Color::Rgb(34, 197, 94)   // bright green
    } else if intensity > 0.6 {
        Color::Rgb(74, 222, 128)
    } else if intensity > 0.4 {
        Color::Rgb(134, 239, 172)
    } else if intensity > 0.2 {
        Color::Rgb(187, 247, 208)
    } else {
        Color::Rgb(220, 252, 231) // pale green
    }
}

/// Render multiple accumulation metrics (like Account Ops breakdown)
fn render_multi_metric(
    f: &mut Frame,
    chunks: std::rc::Rc<[Rect]>,
    metrics: &[&s2_sdk::types::AccumulationMetric],
    state: &MetricsViewState,
) {
    use std::collections::BTreeMap;

    // Color palette for different operation types (purple/blue gradient like web console)
    let colors = [
        Color::Rgb(139, 92, 246),   // Purple (primary)
        Color::Rgb(124, 58, 237),   // Violet
        Color::Rgb(99, 102, 241),   // Indigo
        Color::Rgb(79, 70, 229),    // Deep indigo
        Color::Rgb(59, 130, 246),   // Blue
        Color::Rgb(37, 99, 235),    // Royal blue
        Color::Rgb(96, 165, 250),   // Light blue
        Color::Rgb(147, 197, 253),  // Pale blue
        Color::Rgb(250, 204, 21),   // Yellow (for highlights)
        Color::Rgb(251, 146, 60),   // Orange
    ];

    // Calculate totals for each metric and sort by total
    let mut metric_totals: Vec<(String, f64, usize)> = metrics
        .iter()
        .enumerate()
        .map(|(i, m)| {
            let total: f64 = m.values.iter().map(|(_, v)| v).sum();
            (m.name.clone(), total, i)
        })
        .collect();
    metric_totals.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    // Aggregate all values by timestamp for the area chart (sum of all operation types)
    let mut time_buckets: BTreeMap<u32, f64> = BTreeMap::new();
    for metric in metrics.iter() {
        for (ts, val) in &metric.values {
            *time_buckets.entry(*ts).or_default() += val;
        }
    }

    let all_values: Vec<(u32, f64)> = time_buckets.into_iter().collect();
    let values_only: Vec<f64> = all_values.iter().map(|(_, v)| *v).collect();

    let grand_total: f64 = values_only.iter().sum();
    let min_val = values_only.iter().cloned().fold(f64::MAX, f64::min);
    let max_val = values_only.iter().cloned().fold(f64::MIN, f64::max);
    let avg_val = if !values_only.is_empty() {
        grand_total / values_only.len() as f64
    } else {
        0.0
    };
    let latest_val = values_only.last().cloned().unwrap_or(0.0);
    let first_val = values_only.first().cloned().unwrap_or(0.0);
    let first_ts = all_values.first().map(|(ts, _)| *ts).unwrap_or(0);
    let last_ts = all_values.last().map(|(ts, _)| *ts).unwrap_or(0);

    // Calculate change for trend indicator
    let change = if first_val > 0.0 {
        ((latest_val - first_val) / first_val) * 100.0
    } else if latest_val > 0.0 {
        100.0
    } else {
        0.0
    };

    // === Stats header row (nerdy style) ===
    let stats_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(BORDER))
        .style(Style::default().bg(BG_PANEL));

    let stats_inner = stats_block.inner(chunks[1]);
    f.render_widget(stats_block, chunks[1]);

    let (trend_arrow, trend_color) = if change > 1.0 {
        ("↑", Color::Rgb(34, 197, 94))
    } else if change < -1.0 {
        ("↓", Color::Rgb(239, 68, 68))
    } else {
        ("→", TEXT_MUTED)
    };
    let trend_text = if change.abs() > 0.1 {
        format!("{:+.1}%", change)
    } else {
        "stable".to_string()
    };

    let stats_line = Line::from(vec![
        Span::styled(" NOW ", Style::default().fg(BG_DARK).bg(GREEN).bold()),
        Span::styled(format!(" {} ", format_count(latest_val as u64)), Style::default().fg(GREEN).bold()),
        Span::styled(trend_arrow, Style::default().fg(trend_color).bold()),
        Span::styled(format!("{} ", trend_text), Style::default().fg(trend_color)),
        Span::styled("  |  ", Style::default().fg(BORDER)),
        Span::styled("min ", Style::default().fg(TEXT_MUTED)),
        Span::styled(format_count(min_val as u64), Style::default().fg(Color::Rgb(96, 165, 250))),
        Span::styled("  max ", Style::default().fg(TEXT_MUTED)),
        Span::styled(format_count(max_val as u64), Style::default().fg(Color::Rgb(251, 191, 36))),
        Span::styled("  avg ", Style::default().fg(TEXT_MUTED)),
        Span::styled(format_count(avg_val as u64), Style::default().fg(Color::Rgb(167, 139, 250))),
        Span::styled(format!("  |  {} pts", all_values.len()), Style::default().fg(TEXT_MUTED)),
    ]);
    let stats_para = Paragraph::new(stats_line).alignment(Alignment::Center);
    f.render_widget(stats_para, stats_inner);

    // === Main Area Chart (total operations over time) ===
    let chart_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(GREEN))
        .title(Line::from(vec![
            Span::styled(" ", Style::default()),
            Span::styled("Total Operations", Style::default().fg(GREEN).bold()),
            Span::styled(" ", Style::default()),
        ]))
        .style(Style::default().bg(BG_DARK));

    let chart_inner = chart_block.inner(chunks[2]);
    f.render_widget(chart_block, chunks[2]);

    if !all_values.is_empty() {
        render_area_chart(f, chart_inner, &all_values, min_val, max_val, s2_sdk::types::MetricUnit::Operations, first_ts, last_ts);
    }

    // === Timeline/Legend area - show breakdown by operation type ===
    let timeline_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(BORDER))
        .title(Line::from(vec![
            Span::styled(" Breakdown ", Style::default().fg(TEXT_PRIMARY)),
            Span::styled(format!("({} operation types)", metrics.len()), Style::default().fg(TEXT_MUTED)),
        ]))
        .title_bottom(Line::from(Span::styled(" j/k scroll ", Style::default().fg(TEXT_MUTED))))
        .style(Style::default().bg(BG_DARK));

    let timeline_inner = timeline_block.inner(chunks[3]);
    f.render_widget(timeline_block, chunks[3]);

    // Render breakdown as horizontal bars
    let visible_rows = timeline_inner.height as usize;
    let bar_width = timeline_inner.width.saturating_sub(28) as usize;
    let max_total = metric_totals.iter().map(|(_, t, _)| *t).fold(0.0, f64::max);

    let lines: Vec<Line> = metric_totals
        .iter()
        .enumerate()
        .skip(state.scroll)
        .take(visible_rows)
        .map(|(_i, (name, total, orig_idx))| {
            let color = colors[*orig_idx % colors.len()];
            let bar_len = if max_total > 0.0 {
                ((total / max_total) * bar_width as f64) as usize
            } else {
                0
            };

            let bar: String = "█".repeat(bar_len);
            let percentage = if grand_total > 0.0 {
                (total / grand_total) * 100.0
            } else {
                0.0
            };

            let display_name = name.replace('_', " ");
            let display_name = if display_name.len() > 14 {
                format!("{}…", &display_name[..13])
            } else {
                display_name
            };

            Line::from(vec![
                Span::styled(format!(" {:>14} ", display_name), Style::default().fg(TEXT_PRIMARY)),
                Span::styled(bar, Style::default().fg(color)),
                Span::styled(
                    format!(" {:>6} ({:>4.1}%)", format_count(*total as u64), percentage),
                    Style::default().fg(TEXT_SECONDARY)
                ),
            ])
        })
        .collect();

    let bars_para = Paragraph::new(lines);
    f.render_widget(bars_para, timeline_inner);
}

/// Render a label metric (list of string values, like Active Basins)
fn render_label_metric(
    f: &mut Frame,
    chunks: std::rc::Rc<[Rect]>,
    metric_name: &str,
    values: &[String],
    state: &MetricsViewState,
) {
    // Stats header showing count
    let stats_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(BORDER))
        .style(Style::default().bg(BG_PANEL));

    let stats_inner = stats_block.inner(chunks[1]);
    f.render_widget(stats_block, chunks[1]);

    let stats_line = Line::from(vec![
        Span::styled(" TOTAL ", Style::default().fg(BG_DARK).bg(GREEN).bold()),
        Span::styled(format!(" {} ", values.len()), Style::default().fg(GREEN).bold()),
        Span::styled(format!(" {} in selected time range", metric_name.to_lowercase()), Style::default().fg(TEXT_MUTED)),
    ]);
    let stats_para = Paragraph::new(stats_line).alignment(Alignment::Center);
    f.render_widget(stats_para, stats_inner);

    // Main list area
    let list_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(GREEN))
        .title(Line::from(vec![
            Span::styled(" ", Style::default()),
            Span::styled(metric_name, Style::default().fg(GREEN).bold()),
            Span::styled(" ", Style::default()),
        ]))
        .style(Style::default().bg(BG_DARK));

    let list_inner = list_block.inner(chunks[2]);
    f.render_widget(list_block, chunks[2]);

    if values.is_empty() {
        let empty = Paragraph::new(Span::styled("No data", Style::default().fg(TEXT_MUTED)))
            .alignment(Alignment::Center);
        f.render_widget(empty, list_inner);
    } else {
        // Render the list of values
        let visible_rows = list_inner.height as usize;
        let total_items = values.len();

        let items: Vec<Line> = values
            .iter()
            .enumerate()
            .skip(state.scroll)
            .take(visible_rows)
            .map(|(i, value)| {
                Line::from(vec![
                    Span::styled(format!(" {:>3}. ", i + 1), Style::default().fg(TEXT_MUTED)),
                    Span::styled(value, Style::default().fg(GREEN)),
                ])
            })
            .collect();

        let list_para = Paragraph::new(items);
        f.render_widget(list_para, list_inner);

        // Scroll indicator in timeline area
        let scroll_block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BORDER))
            .title(Line::from(vec![
                Span::styled(
                    format!(" Showing {}-{} of {} ",
                        state.scroll + 1,
                        (state.scroll + visible_rows).min(total_items),
                        total_items
                    ),
                    Style::default().fg(TEXT_MUTED)
                ),
            ]))
            .title_bottom(Line::from(Span::styled(" j/k scroll ", Style::default().fg(TEXT_MUTED))))
            .style(Style::default().bg(BG_DARK));

        f.render_widget(scroll_block, chunks[3]);
    }
}

/// Render a beautiful area chart with Y-axis, filled area, and X-axis
fn render_area_chart(
    f: &mut Frame,
    area: Rect,
    values: &[(u32, f64)],
    min_val: f64,
    max_val: f64,
    unit: s2_sdk::types::MetricUnit,
    first_ts: u32,
    last_ts: u32,
) {
    let height = area.height.saturating_sub(1) as usize; // Leave room for X-axis
    let y_axis_width = 10u16;
    let width = area.width.saturating_sub(y_axis_width + 1) as usize;

    if height < 2 || width < 10 {
        return;
    }

    // Calculate value range with some padding
    let chart_min = if min_val > 0.0 { 0.0 } else { min_val };
    let chart_max = max_val * 1.1; // 10% headroom
    let chart_range = chart_max - chart_min;

    // Resample values to fit width
    let values_only: Vec<f64> = values.iter().map(|(_, v)| *v).collect();
    let step = values_only.len() as f64 / width as f64;

    // Build the chart row by row (top to bottom)
    let mut lines: Vec<Line> = Vec::new();

    // Block characters for smooth area fill
    // Using vertical eighths: ' ', '▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'
    let fill_chars = [' ', '▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

    for row in 0..height {
        let y_frac_top = 1.0 - (row as f64 / height as f64);
        let y_frac_bot = 1.0 - ((row + 1) as f64 / height as f64);

        // Y-axis label (only on certain rows)
        let y_label: String = if row == 0 {
            format!("{:>9} ", format_metric_value_f64(chart_max, unit))
        } else if row == height / 2 {
            format!("{:>9} ", format_metric_value_f64((chart_max + chart_min) / 2.0, unit))
        } else if row == height - 1 {
            format!("{:>9} ", format_metric_value_f64(chart_min, unit))
        } else {
            "          ".to_string()
        };

        let mut spans: Vec<Span> = vec![
            Span::styled(y_label, Style::default().fg(TEXT_MUTED)),
        ];

        // Draw each column
        for col in 0..width {
            let idx = ((col as f64) * step) as usize;
            let val = values_only.get(idx).cloned().unwrap_or(0.0);

            // Normalize value to chart coordinates
            let val_norm = (val - chart_min) / chart_range;
            let val_y = val_norm; // 0.0 = bottom, 1.0 = top

            // Determine what character to draw
            let char_and_color = if val_y >= y_frac_top {
                // Value is above this row - full fill
                ('█', intensity_to_color(val_norm))
            } else if val_y > y_frac_bot {
                // Value is within this row - partial fill
                let fill_frac = (val_y - y_frac_bot) / (y_frac_top - y_frac_bot);
                let char_idx = (fill_frac * 8.0) as usize;
                (fill_chars[char_idx.min(8)], intensity_to_color(val_norm))
            } else {
                // Value is below this row - empty or grid
                if col % 10 == 0 {
                    ('·', Color::Rgb(50, 50, 50))
                } else {
                    (' ', BG_DARK)
                }
            };

            spans.push(Span::styled(
                char_and_color.0.to_string(),
                Style::default().fg(char_and_color.1),
            ));
        }

        lines.push(Line::from(spans));
    }

    // X-axis with time labels
    let first_time = format_metric_timestamp_short(first_ts);
    let last_time = format_metric_timestamp_short(last_ts);
    let mid_ts = first_ts + (last_ts - first_ts) / 2;
    let mid_time = format_metric_timestamp_short(mid_ts);

    let x_axis_padding = " ".repeat(y_axis_width as usize);

    let mut x_axis_spans = vec![
        Span::styled(&x_axis_padding, Style::default()),
        Span::styled(&first_time, Style::default().fg(TEXT_MUTED)),
    ];

    let remaining_after_first = width.saturating_sub(first_time.len() + mid_time.len() / 2);
    let padding_to_mid = remaining_after_first / 2;
    x_axis_spans.push(Span::styled(" ".repeat(padding_to_mid), Style::default()));
    x_axis_spans.push(Span::styled(&mid_time, Style::default().fg(TEXT_MUTED)));

    let remaining_after_mid = width.saturating_sub(first_time.len() + padding_to_mid + mid_time.len() + last_time.len());
    x_axis_spans.push(Span::styled(" ".repeat(remaining_after_mid), Style::default()));
    x_axis_spans.push(Span::styled(&last_time, Style::default().fg(TEXT_MUTED)));

    lines.push(Line::from(x_axis_spans));

    let chart_para = Paragraph::new(lines);
    f.render_widget(chart_para, area);
}

/// Render a sparkline with gradient coloring (unused but kept for reference)
#[allow(dead_code)]
fn render_sparkline_gradient(values: &[(u32, f64)], width: usize) -> String {
    if values.is_empty() {
        return "-".repeat(width);
    }

    // Sparkline characters from lowest to highest
    let spark_chars = [' ', '▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

    let values_only: Vec<f64> = values.iter().map(|(_, v)| *v).collect();
    let min_val = values_only.iter().cloned().fold(f64::MAX, f64::min);
    let max_val = values_only.iter().cloned().fold(f64::MIN, f64::max);
    let range = max_val - min_val;

    // Resample values to fit width
    let step = values_only.len() as f64 / width as f64;
    let mut sparkline = String::new();

    for i in 0..width {
        let idx = (i as f64 * step) as usize;
        let val = values_only.get(idx).cloned().unwrap_or(0.0);

        let normalized = if range > 0.0 {
            ((val - min_val) / range).clamp(0.0, 1.0)
        } else {
            0.5
        };

        let char_idx = (normalized * (spark_chars.len() - 1) as f64) as usize;
        sparkline.push(spark_chars[char_idx]);
    }

    sparkline
}
/// Format timestamp in short form for bar chart
fn format_metric_timestamp_short(ts: u32) -> String {
    use std::time::{Duration, UNIX_EPOCH};
    let time = UNIX_EPOCH + Duration::from_secs(ts as u64);
    // Just show time portion
    humantime::format_rfc3339_seconds(time)
        .to_string()
        .chars()
        .skip(11) // Skip date portion
        .take(8)  // Take HH:MM:SS
        .collect()
}

/// Format a metric value (f64) with appropriate unit
fn format_metric_value_f64(value: f64, unit: s2_sdk::types::MetricUnit) -> String {
    use s2_sdk::types::MetricUnit;
    match unit {
        MetricUnit::Bytes => format_bytes(value as u64),
        MetricUnit::Operations => format_count(value as u64),
    }
}

/// Format bytes with appropriate unit
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Format count with K/M suffixes
fn format_count(count: u64) -> String {
    if count >= 1_000_000 {
        format!("{:.1}M", count as f64 / 1_000_000.0)
    } else if count >= 1_000 {
        format!("{:.1}K", count as f64 / 1_000.0)
    } else {
        count.to_string()
    }
}

/// Draw time range picker popup
fn draw_time_picker(f: &mut Frame, state: &MetricsViewState) {
    use super::app::TimeRangeOption;

    let area = f.area();

    // PRESETS.len() + 1 for "Custom" option
    let item_count = TimeRangeOption::PRESETS.len() + 1;

    // Calculate popup dimensions
    let popup_width = 30u16;
    let popup_height = (item_count as u16) + 4; // Items + borders + title

    // Center the popup
    let popup_x = (area.width.saturating_sub(popup_width)) / 2;
    let popup_y = (area.height.saturating_sub(popup_height)) / 2;

    let popup_area = Rect::new(popup_x, popup_y, popup_width, popup_height);

    // Clear the area behind the popup
    f.render_widget(Clear, popup_area);

    // Build the list items
    let mut items: Vec<ListItem> = TimeRangeOption::PRESETS
        .iter()
        .enumerate()
        .map(|(i, option)| {
            let is_selected = i == state.time_picker_selected;
            let is_current = std::mem::discriminant(option) == std::mem::discriminant(&state.time_range);

            let style = if is_selected {
                Style::default().fg(BG_DARK).bg(GREEN).bold()
            } else if is_current {
                Style::default().fg(GREEN)
            } else {
                Style::default().fg(TEXT_PRIMARY)
            };

            let marker = if is_current { " ✓" } else { "" };
            ListItem::new(format!(" {}{} ", option.as_label(), marker)).style(style)
        })
        .collect();

    // Add "Custom" option at index 7
    let custom_index = TimeRangeOption::PRESETS.len();
    let is_custom_selected = state.time_picker_selected == custom_index;
    let is_custom_current = matches!(state.time_range, TimeRangeOption::Custom { .. });
    let custom_style = if is_custom_selected {
        Style::default().fg(BG_DARK).bg(GREEN).bold()
    } else if is_custom_current {
        Style::default().fg(GREEN)
    } else {
        Style::default().fg(CYAN)
    };
    let custom_marker = if is_custom_current { " ✓" } else { "" };
    items.push(ListItem::new(format!(" Custom range...{} ", custom_marker)).style(custom_style));

    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(GREEN))
                .title(Line::from(vec![
                    Span::styled(" ", Style::default()),
                    Span::styled("Time Range", Style::default().fg(GREEN).bold()),
                    Span::styled(" ", Style::default()),
                ]))
                .title_bottom(Line::from(Span::styled(
                    " Enter select  Esc close ",
                    Style::default().fg(TEXT_MUTED),
                )))
                .style(Style::default().bg(BG_PANEL)),
        );

    f.render_widget(list, popup_area);
}

/// Draw calendar date picker
fn draw_calendar_picker(f: &mut Frame, state: &MetricsViewState) {
    use chrono::{Datelike, NaiveDate};

    let area = f.area();

    // Calendar dimensions: 7 columns * 4 chars + padding = 32, height for header + 6 weeks + status
    let popup_width = 36u16;
    let popup_height = 14u16;

    // Center the popup
    let popup_x = (area.width.saturating_sub(popup_width)) / 2;
    let popup_y = (area.height.saturating_sub(popup_height)) / 2;

    let popup_area = Rect::new(popup_x, popup_y, popup_width, popup_height);

    // Clear the area behind the popup
    f.render_widget(Clear, popup_area);

    // Month names
    let month_names = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ];
    let month_name = month_names.get(state.calendar_month as usize - 1).unwrap_or(&"???");

    // Calculate first day of month and days in month
    let first_of_month = NaiveDate::from_ymd_opt(state.calendar_year, state.calendar_month, 1)
        .unwrap_or(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap());
    let first_weekday = first_of_month.weekday().num_days_from_sunday() as usize;
    let days_in_month = {
        let next_month = if state.calendar_month == 12 {
            NaiveDate::from_ymd_opt(state.calendar_year + 1, 1, 1)
        } else {
            NaiveDate::from_ymd_opt(state.calendar_year, state.calendar_month + 1, 1)
        };
        next_month.unwrap().pred_opt().map(|d| d.day()).unwrap_or(28)
    };

    // Build calendar lines
    let mut lines: Vec<Line> = Vec::new();

    // Month/Year header with navigation hints
    lines.push(Line::from(vec![
        Span::styled(" [", Style::default().fg(TEXT_MUTED)),
        Span::styled(format!("{} {}", month_name, state.calendar_year), Style::default().fg(GREEN).bold()),
        Span::styled("] ", Style::default().fg(TEXT_MUTED)),
    ]));

    // Day headers
    lines.push(Line::from(vec![
        Span::styled(" Su ", Style::default().fg(TEXT_MUTED)),
        Span::styled(" Mo ", Style::default().fg(TEXT_MUTED)),
        Span::styled(" Tu ", Style::default().fg(TEXT_MUTED)),
        Span::styled(" We ", Style::default().fg(TEXT_MUTED)),
        Span::styled(" Th ", Style::default().fg(TEXT_MUTED)),
        Span::styled(" Fr ", Style::default().fg(TEXT_MUTED)),
        Span::styled(" Sa ", Style::default().fg(TEXT_MUTED)),
    ]));

    // Calendar grid
    let mut day = 1u32;
    for week in 0..6 {
        let mut spans: Vec<Span> = Vec::new();
        for weekday in 0..7 {
            let cell_idx = week * 7 + weekday;
            if cell_idx < first_weekday || day > days_in_month {
                spans.push(Span::styled("    ", Style::default()));
            } else {
                let is_selected = day == state.calendar_day;
                let current_date = (state.calendar_year, state.calendar_month, day);

                // Check if this day is the start or end of selection
                let is_start = state.calendar_start == Some(current_date);
                let is_end = state.calendar_end == Some(current_date);

                // Check if this day is in the selected range
                let in_range = match (state.calendar_start, state.calendar_end) {
                    (Some(start), Some(end)) => {
                        let (s, e) = if start <= end { (start, end) } else { (end, start) };
                        current_date >= s && current_date <= e
                    }
                    (Some(start), None) if state.calendar_selecting_end => {
                        // Show range preview while selecting end
                        let (s, e) = if start <= current_date { (start, current_date) } else { (current_date, start) };
                        current_date >= s && current_date <= e && is_selected
                    }
                    _ => false,
                };

                let style = if is_selected {
                    Style::default().fg(BG_DARK).bg(GREEN).bold()
                } else if is_start || is_end {
                    Style::default().fg(BG_DARK).bg(CYAN).bold()
                } else if in_range {
                    Style::default().fg(GREEN).bg(BG_PANEL)
                } else {
                    Style::default().fg(TEXT_PRIMARY)
                };

                spans.push(Span::styled(format!("{:>3} ", day), style));
                day += 1;
            }
        }
        lines.push(Line::from(spans));
        if day > days_in_month {
            break;
        }
    }

    // Selection status
    let status = match (state.calendar_start, state.calendar_end) {
        (Some((sy, sm, sd)), Some((ey, em, ed))) => {
            format!("{:02}/{:02}/{} - {:02}/{:02}/{}", sm, sd, sy, em, ed, ey)
        }
        (Some((sy, sm, sd)), None) => {
            if state.calendar_selecting_end {
                format!("{:02}/{:02}/{} - select end", sm, sd, sy)
            } else {
                format!("Select start date")
            }
        }
        _ => "Select start date".to_string(),
    };
    lines.push(Line::from(Span::styled(format!(" {} ", status), Style::default().fg(CYAN))));

    let calendar_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(GREEN))
        .title(Line::from(vec![
            Span::styled(" ", Style::default()),
            Span::styled("Select Date Range", Style::default().fg(GREEN).bold()),
            Span::styled(" ", Style::default()),
        ]))
        .title_bottom(Line::from(Span::styled(
            " ←→↑↓ nav  [/] month  Enter select  Esc cancel ",
            Style::default().fg(TEXT_MUTED),
        )))
        .style(Style::default().bg(BG_PANEL));

    let calendar_para = Paragraph::new(lines)
        .block(calendar_block)
        .alignment(Alignment::Center);

    f.render_widget(calendar_para, popup_area);
}

fn draw_basins(f: &mut Frame, area: Rect, state: &BasinsState) {
    // Layout: Title bar, Search bar, Header, Table rows
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Title bar (consistent height)
            Constraint::Length(3), // Search bar
            Constraint::Length(2), // Header
            Constraint::Min(1),    // Table rows
        ])
        .split(area);

    // === Title Bar ===
    let count_text = if state.loading {
        " loading...".to_string()
    } else {
        let filtered_count = state.basins.iter()
            .filter(|b| state.filter.is_empty() || b.name.to_string().to_lowercase().contains(&state.filter.to_lowercase()))
            .count();
        if filtered_count != state.basins.len() {
            format!("  {}/{} basins", filtered_count, state.basins.len())
        } else {
            format!("  {} basins", state.basins.len())
        }
    };
    let title_lines = vec![
        Line::from(""),
        Line::from(vec![
            Span::styled("  Basins", Style::default().fg(GREEN).bold()),
            Span::styled(&count_text, Style::default().fg(Color::Rgb(100, 100, 100))),
        ]),
    ];
    let title_block = Paragraph::new(title_lines)
        .block(Block::default()
            .borders(Borders::BOTTOM)
            .border_style(Style::default().fg(Color::Rgb(40, 40, 40))));
    f.render_widget(title_block, chunks[0]);

    // === Search Bar ===
    let search_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(if state.filter_active { GREEN } else { BORDER }))
        .style(Style::default().bg(BG_PANEL));

    let search_text = if state.filter_active {
        Line::from(vec![
            Span::styled(" [/] ", Style::default().fg(GREEN)),
            Span::styled(&state.filter, Style::default().fg(TEXT_PRIMARY)),
            Span::styled("_", Style::default().fg(GREEN)),
        ])
    } else if state.filter.is_empty() {
        Line::from(vec![
            Span::styled(" [/] Filter by prefix...", Style::default().fg(TEXT_MUTED)),
        ])
    } else {
        Line::from(vec![
            Span::styled(" [/] ", Style::default().fg(TEXT_MUTED)),
            Span::styled(&state.filter, Style::default().fg(TEXT_PRIMARY)),
        ])
    };

    let search = Paragraph::new(search_text).block(search_block);
    f.render_widget(search, chunks[1]);

    // === Table Header ===
    let header_area = chunks[2];
    // Calculate column widths: Name takes most space, State and Scope are fixed
    let total_width = header_area.width as usize;
    let state_col = 12;
    let scope_col = 16;
    let name_col = total_width.saturating_sub(state_col + scope_col + 4);

    let header = Line::from(vec![
        Span::styled(format!("  {:<width$}", "Name", width = name_col), Style::default().fg(TEXT_MUTED)),
        Span::styled(format!("{:<width$}", "State", width = state_col), Style::default().fg(TEXT_MUTED)),
        Span::styled("Scope", Style::default().fg(TEXT_MUTED)),
    ]);
    f.render_widget(Paragraph::new(header), Rect::new(header_area.x, header_area.y, header_area.width, 1));

    // Header separator
    let sep = "─".repeat(total_width);
    f.render_widget(
        Paragraph::new(Span::styled(sep, Style::default().fg(BORDER))),
        Rect::new(header_area.x, header_area.y + 1, header_area.width, 1),
    );

    // === Filter basins ===
    let filtered: Vec<_> = state.basins.iter()
        .filter(|b| state.filter.is_empty() || b.name.to_string().to_lowercase().contains(&state.filter.to_lowercase()))
        .collect();

    // === Table Rows ===
    let table_area = chunks[3];

    if filtered.is_empty() && !state.loading {
        let msg = if state.filter.is_empty() {
            "No basins found. Press c to create one."
        } else {
            "No basins match the filter."
        };
        let text = Paragraph::new(Span::styled(msg, Style::default().fg(TEXT_MUTED)))
            .alignment(Alignment::Center);
        f.render_widget(text, Rect::new(table_area.x, table_area.y + 2, table_area.width, 1));
        return;
    }

    if state.loading {
        let text = Paragraph::new(Span::styled("Loading basins...", Style::default().fg(TEXT_MUTED)))
            .alignment(Alignment::Center);
        f.render_widget(text, Rect::new(table_area.x, table_area.y + 2, table_area.width, 1));
        return;
    }

    let visible_height = table_area.height as usize;
    let total = filtered.len();
    let selected = state.selected.min(total.saturating_sub(1));

    // Scroll offset
    let scroll_offset = if selected >= visible_height {
        selected - visible_height + 1
    } else {
        0
    };

    // Draw rows
    for (view_idx, basin) in filtered.iter().enumerate().skip(scroll_offset).take(visible_height) {
        let y = table_area.y + (view_idx - scroll_offset) as u16;
        if y >= table_area.y + table_area.height {
            break;
        }

        let is_selected = view_idx == selected;
        let row_area = Rect::new(table_area.x, y, table_area.width, 1);

        // Selection highlight
        if is_selected {
            f.render_widget(
                Block::default().style(Style::default().bg(Color::Rgb(39, 39, 42))),
                row_area,
            );
        }

        // Name column
        let name = basin.name.to_string();
        let display_name = if name.len() > name_col - 2 {
            format!("{}…", &name[..name_col - 3])
        } else {
            name
        };

        // State badge
        let (state_text, state_bg) = match basin.state {
            s2_sdk::types::BasinState::Active => ("Active", Color::Rgb(22, 101, 52)),
            s2_sdk::types::BasinState::Creating => ("Creating", Color::Rgb(113, 63, 18)),
            s2_sdk::types::BasinState::Deleting => ("Deleting", Color::Rgb(127, 29, 29)),
        };

        // Scope
        let scope = basin.scope.as_ref()
            .map(|s| match s { s2_sdk::types::BasinScope::AwsUsEast1 => "aws:us-east-1" })
            .unwrap_or("—");

        // Render name
        let name_style = if is_selected {
            Style::default().fg(TEXT_PRIMARY).bold()
        } else {
            Style::default().fg(TEXT_SECONDARY)
        };
        f.render_widget(
            Paragraph::new(Span::styled(format!("  {}", display_name), name_style)),
            Rect::new(row_area.x, y, name_col as u16, 1),
        );

        // Render state badge
        let badge_x = row_area.x + name_col as u16;
        f.render_widget(
            Paragraph::new(Span::styled(
                format!(" {} ", state_text),
                Style::default().fg(WHITE).bg(state_bg),
            )),
            Rect::new(badge_x, y, state_col as u16, 1),
        );

        // Render scope
        let scope_x = badge_x + state_col as u16;
        f.render_widget(
            Paragraph::new(Span::styled(scope, Style::default().fg(TEXT_MUTED))),
            Rect::new(scope_x, y, scope_col as u16, 1),
        );
    }
}

fn draw_streams(f: &mut Frame, area: Rect, state: &StreamsState) {
    // Layout: Title bar, Search bar, Header, Table rows
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Title bar with basin name (consistent height)
            Constraint::Length(3), // Search bar
            Constraint::Length(2), // Header
            Constraint::Min(1),    // Table rows
        ])
        .split(area);

    // === Title Bar ===
    let count_text = if state.loading {
        " loading...".to_string()
    } else {
        let filtered_count = state.streams.iter()
            .filter(|s| state.filter.is_empty() || s.name.to_string().to_lowercase().contains(&state.filter.to_lowercase()))
            .count();
        if filtered_count != state.streams.len() {
            format!("  {}/{} streams", filtered_count, state.streams.len())
        } else {
            format!("  {} streams", state.streams.len())
        }
    };

    let basin_name_str = state.basin_name.to_string();
    let title_lines = vec![
        Line::from(""),
        Line::from(vec![
            Span::styled("  ← ", Style::default().fg(Color::Rgb(100, 100, 100))),
            Span::styled(&basin_name_str, Style::default().fg(GREEN).bold()),
            Span::styled(&count_text, Style::default().fg(Color::Rgb(100, 100, 100))),
        ]),
    ];
    let title_block = Paragraph::new(title_lines)
        .block(Block::default()
            .borders(Borders::BOTTOM)
            .border_style(Style::default().fg(Color::Rgb(40, 40, 40))));
    f.render_widget(title_block, chunks[0]);

    // === Search Bar ===
    let search_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(if state.filter_active { GREEN } else { BORDER }))
        .style(Style::default().bg(BG_PANEL));

    let search_text = if state.filter_active {
        Line::from(vec![
            Span::styled(" [/] ", Style::default().fg(GREEN)),
            Span::styled(&state.filter, Style::default().fg(TEXT_PRIMARY)),
            Span::styled("_", Style::default().fg(GREEN)),
        ])
    } else if state.filter.is_empty() {
        Line::from(vec![
            Span::styled(" [/] Filter by prefix...", Style::default().fg(TEXT_MUTED)),
        ])
    } else {
        Line::from(vec![
            Span::styled(" [/] ", Style::default().fg(TEXT_MUTED)),
            Span::styled(&state.filter, Style::default().fg(TEXT_PRIMARY)),
        ])
    };

    let search = Paragraph::new(search_text).block(search_block);
    f.render_widget(search, chunks[1]);

    // === Table Header ===
    let header_area = chunks[2];
    let total_width = header_area.width as usize;
    let created_col = 24;
    let name_col = total_width.saturating_sub(created_col + 4);

    let header = Line::from(vec![
        Span::styled(format!("  {:<width$}", "Name", width = name_col), Style::default().fg(TEXT_MUTED)),
        Span::styled("Created", Style::default().fg(TEXT_MUTED)),
    ]);
    f.render_widget(Paragraph::new(header), Rect::new(header_area.x, header_area.y, header_area.width, 1));

    // Header separator
    let sep = "─".repeat(total_width);
    f.render_widget(
        Paragraph::new(Span::styled(sep, Style::default().fg(BORDER))),
        Rect::new(header_area.x, header_area.y + 1, header_area.width, 1),
    );

    // === Filter streams ===
    let filtered: Vec<_> = state.streams.iter()
        .filter(|s| state.filter.is_empty() || s.name.to_string().to_lowercase().contains(&state.filter.to_lowercase()))
        .collect();

    // === Table Rows ===
    let table_area = chunks[3];

    if filtered.is_empty() && !state.loading {
        let msg = if state.filter.is_empty() {
            "No streams found. Press c to create one."
        } else {
            "No streams match the filter."
        };
        let text = Paragraph::new(Span::styled(msg, Style::default().fg(TEXT_MUTED)))
            .alignment(Alignment::Center);
        f.render_widget(text, Rect::new(table_area.x, table_area.y + 2, table_area.width, 1));
        return;
    }

    if state.loading {
        let text = Paragraph::new(Span::styled("Loading streams...", Style::default().fg(TEXT_MUTED)))
            .alignment(Alignment::Center);
        f.render_widget(text, Rect::new(table_area.x, table_area.y + 2, table_area.width, 1));
        return;
    }

    let visible_height = table_area.height as usize;
    let total = filtered.len();
    let selected = state.selected.min(total.saturating_sub(1));

    // Scroll offset
    let scroll_offset = if selected >= visible_height {
        selected - visible_height + 1
    } else {
        0
    };

    // Draw rows
    for (view_idx, stream) in filtered.iter().enumerate().skip(scroll_offset).take(visible_height) {
        let y = table_area.y + (view_idx - scroll_offset) as u16;
        if y >= table_area.y + table_area.height {
            break;
        }

        let is_selected = view_idx == selected;
        let row_area = Rect::new(table_area.x, y, table_area.width, 1);

        // Selection highlight
        if is_selected {
            f.render_widget(
                Block::default().style(Style::default().bg(Color::Rgb(39, 39, 42))),
                row_area,
            );
        }

        // Name column
        let name = stream.name.to_string();
        let display_name = if name.len() > name_col - 2 {
            format!("{}…", &name[..name_col - 3])
        } else {
            name
        };

        // Created timestamp - S2DateTime implements Display
        let created = stream.created_at.to_string();

        // Render name
        let name_style = if is_selected {
            Style::default().fg(TEXT_PRIMARY).bold()
        } else {
            Style::default().fg(TEXT_SECONDARY)
        };
        f.render_widget(
            Paragraph::new(Span::styled(format!("  {}", display_name), name_style)),
            Rect::new(row_area.x, y, name_col as u16, 1),
        );

        // Render created timestamp
        let created_x = row_area.x + name_col as u16;
        f.render_widget(
            Paragraph::new(Span::styled(created, Style::default().fg(TEXT_MUTED))),
            Rect::new(created_x, y, created_col as u16, 1),
        );
    }
}

fn draw_stream_detail(f: &mut Frame, area: Rect, state: &StreamDetailState) {
    // Vertical layout: Header, Stats row, Actions
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // Header with URI (consistent height)
            Constraint::Length(5),  // Stats cards
            Constraint::Min(12),    // Actions
        ])
        .split(area);

    // === HEADER ===
    let basin_str = state.basin_name.to_string();
    let stream_str = state.stream_name.to_string();
    let header_lines = vec![
        Line::from(""),
        Line::from(vec![
            Span::styled("  ← ", Style::default().fg(Color::Rgb(100, 100, 100))),
            Span::styled(&basin_str, Style::default().fg(Color::Rgb(150, 150, 150))),
            Span::styled(" / ", Style::default().fg(Color::Rgb(80, 80, 80))),
            Span::styled(&stream_str, Style::default().fg(GREEN).bold()),
        ]),
    ];
    let header = Paragraph::new(header_lines)
        .block(Block::default()
            .borders(Borders::BOTTOM)
            .border_style(Style::default().fg(Color::Rgb(40, 40, 40))));
    f.render_widget(header, chunks[0]);

    // === STATS ROW ===
    let stats_area = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(2),  // Left padding
            Constraint::Min(20),    // Stats content
            Constraint::Length(2),  // Right padding
        ])
        .split(chunks[1])[1];

    let stats_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Ratio(1, 4),
            Constraint::Ratio(1, 4),
            Constraint::Ratio(1, 4),
            Constraint::Ratio(1, 4),
        ])
        .split(stats_area);

    // Stat card helper with icon and color
    fn render_stat_card_v2(f: &mut Frame, area: Rect, icon: &str, label: &str, value: &str, value_color: Color) {
        let lines = vec![
            Line::from(vec![
                Span::styled(icon, Style::default().fg(value_color)),
                Span::styled(format!(" {}", label), Style::default().fg(Color::Rgb(120, 120, 120))),
            ]),
            Line::from(vec![
                Span::styled("  ", Style::default()),
                Span::styled(value, Style::default().fg(value_color).bold()),
            ]),
        ];
        let widget = Paragraph::new(lines)
            .block(Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Rgb(50, 50, 50)))
                .border_type(ratatui::widgets::BorderType::Rounded));
        f.render_widget(widget, area);
    }

    // Tail Position
    let (tail_val, tail_color) = if let Some(pos) = &state.tail_position {
        if pos.seq_num > 0 {
            (format!("{}", pos.seq_num), Color::Rgb(34, 211, 238)) // Cyan
        } else {
            ("0".to_string(), Color::Rgb(100, 100, 100))
        }
    } else if state.loading {
        ("...".to_string(), Color::Rgb(100, 100, 100))
    } else {
        ("--".to_string(), Color::Rgb(100, 100, 100))
    };
    render_stat_card_v2(f, stats_chunks[0], "▌", "Records", &tail_val, tail_color);

    // Last Write
    let (ts_val, ts_color) = if let Some(pos) = &state.tail_position {
        if pos.timestamp > 0 {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let age_secs = now_ms.saturating_sub(pos.timestamp) / 1000;
            let val = if age_secs < 60 {
                format!("{}s ago", age_secs)
            } else if age_secs < 3600 {
                format!("{}m ago", age_secs / 60)
            } else if age_secs < 86400 {
                format!("{}h ago", age_secs / 3600)
            } else {
                format!("{}d ago", age_secs / 86400)
            };
            // Color based on recency
            let color = if age_secs < 60 {
                Color::Rgb(74, 222, 128) // Green - very recent
            } else if age_secs < 3600 {
                Color::Rgb(250, 204, 21) // Yellow - recent
            } else {
                Color::Rgb(180, 180, 180) // Gray - old
            };
            (val, color)
        } else {
            ("never".to_string(), Color::Rgb(100, 100, 100))
        }
    } else {
        ("--".to_string(), Color::Rgb(100, 100, 100))
    };
    render_stat_card_v2(f, stats_chunks[1], "◷", "Last Write", &ts_val, ts_color);

    // Storage Class
    let (storage_val, storage_color) = if let Some(config) = &state.config {
        let val = config.storage_class
            .as_ref()
            .map(|s| format!("{:?}", s).to_lowercase())
            .unwrap_or_else(|| "default".to_string());
        let color = match val.as_str() {
            "express" => Color::Rgb(251, 146, 60), // Orange for express
            "standard" => Color::Rgb(147, 197, 253), // Light blue for standard
            _ => Color::Rgb(180, 180, 180),
        };
        (val, color)
    } else {
        ("--".to_string(), Color::Rgb(100, 100, 100))
    };
    render_stat_card_v2(f, stats_chunks[2], "◈", "Storage", &storage_val, storage_color);

    // Retention
    let (retention_val, retention_color) = if let Some(config) = &state.config {
        let val = config.retention_policy
            .as_ref()
            .map(|p| match p {
                crate::types::RetentionPolicy::Age(dur) => {
                    let secs = dur.as_secs();
                    if secs >= 86400 * 365 {
                        format!("{}y", secs / (86400 * 365))
                    } else if secs >= 86400 {
                        format!("{}d", secs / 86400)
                    } else if secs >= 3600 {
                        format!("{}h", secs / 3600)
                    } else {
                        format!("{}s", secs)
                    }
                }
                crate::types::RetentionPolicy::Infinite => "∞".to_string(),
            })
            .unwrap_or_else(|| "∞".to_string());
        let color = if val == "∞" {
            Color::Rgb(167, 139, 250) // Purple for infinite
        } else {
            Color::Rgb(180, 180, 180)
        };
        (val, color)
    } else {
        ("--".to_string(), Color::Rgb(100, 100, 100))
    };
    render_stat_card_v2(f, stats_chunks[3], "◔", "Retention", &retention_val, retention_color);

    // === ACTIONS ===
    let actions_outer = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(2),
            Constraint::Min(20),
            Constraint::Length(2),
        ])
        .split(chunks[2])[1];

    // Split into two columns: Data Operations | Stream Management
    let action_cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Ratio(1, 2),
            Constraint::Ratio(1, 2),
        ])
        .split(actions_outer);

    // Data operations (left column)
    let data_ops: Vec<(&str, &str, &str, &str)> = vec![
        ("t", "Tail", "Live stream, see records as they arrive", "◉"),
        ("r", "Read", "Read with custom start position & limits", "◎"),
        ("a", "Append", "Write new records to stream", "◆"),
    ];

    // Stream management (right column)
    let mgmt_ops: Vec<(&str, &str, &str, &str)> = vec![
        ("f", "Fence", "Set token for exclusive writes", "⊘"),
        ("m", "Trim", "Delete records before seq number", "✂"),
    ];

    fn render_action_column(f: &mut Frame, area: Rect, title: &str, actions: &[(&str, &str, &str, &str)], selected: usize, offset: usize) {
        let title_width = title.len() + 4;
        let line_width = area.width.saturating_sub(title_width as u16 + 2) as usize;

        let mut lines = vec![
            Line::from(vec![
                Span::styled(format!("  {} ", title), Style::default().fg(Color::Rgb(34, 211, 238)).bold()),
                Span::styled("─".repeat(line_width), Style::default().fg(Color::Rgb(40, 40, 40))),
            ]),
            Line::from(""),
        ];

        for (i, (key, name, desc, icon)) in actions.iter().enumerate() {
            let actual_idx = i + offset;
            let is_selected = actual_idx == selected;

            if is_selected {
                // Selected action - highlighted card style
                lines.push(Line::from(vec![
                    Span::styled("  ▶ ", Style::default().fg(GREEN)),
                    Span::styled(*icon, Style::default().fg(GREEN)),
                    Span::styled(format!(" {} ", name), Style::default().fg(Color::White).bold()),
                    Span::styled(format!("[{}]", key), Style::default().fg(GREEN).bold()),
                ]));
                lines.push(Line::from(vec![
                    Span::styled("      ", Style::default()),
                    Span::styled(*desc, Style::default().fg(Color::Rgb(180, 180, 180)).italic()),
                ]));
            } else {
                // Unselected action - dimmed
                lines.push(Line::from(vec![
                    Span::styled("    ", Style::default()),
                    Span::styled(*icon, Style::default().fg(Color::Rgb(80, 80, 80))),
                    Span::styled(format!(" {} ", name), Style::default().fg(Color::Rgb(140, 140, 140))),
                    Span::styled(format!("[{}]", key), Style::default().fg(Color::Rgb(80, 80, 80))),
                ]));
            }
            lines.push(Line::from(""));
        }

        let widget = Paragraph::new(lines)
            .block(Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Rgb(50, 50, 50)))
                .border_type(ratatui::widgets::BorderType::Rounded));
        f.render_widget(widget, area);
    }

    render_action_column(f, action_cols[0], "Data Operations", &data_ops, state.selected_action, 0);
    render_action_column(f, action_cols[1], "Stream Management", &mgmt_ops, state.selected_action, 3);
}

fn draw_read_view(f: &mut Frame, area: Rect, state: &ReadViewState) {
    let (mode_text, mode_color) = if state.is_tailing {
        if state.paused {
            ("PAUSED", WARNING)
        } else {
            ("LIVE", SUCCESS)
        }
    } else {
        ("READING", ACCENT)
    };

    // Split into header and content
    let main_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // Header (consistent height)
            Constraint::Min(1),     // Content
        ])
        .split(area);

    // === HEADER ===
    let basin_str = state.basin_name.to_string();
    let stream_str = state.stream_name.to_string();
    let record_count = format!("  {} records", state.records.len());

    let mut header_spans = vec![
        Span::styled("  ← ", Style::default().fg(Color::Rgb(100, 100, 100))),
        Span::styled(&basin_str, Style::default().fg(Color::Rgb(150, 150, 150))),
        Span::styled(" / ", Style::default().fg(Color::Rgb(80, 80, 80))),
        Span::styled(&stream_str, Style::default().fg(Color::Rgb(180, 180, 180))),
        Span::styled("  ", Style::default()),
        Span::styled(format!(" {} ", mode_text), Style::default().fg(BG_DARK).bg(mode_color).bold()),
        Span::styled(&record_count, Style::default().fg(Color::Rgb(100, 100, 100))),
    ];

    // Add output file indicator if writing to file
    if let Some(ref output) = state.output_file {
        header_spans.push(Span::styled("  → ", Style::default().fg(Color::Rgb(100, 100, 100))));
        header_spans.push(Span::styled(output, Style::default().fg(YELLOW)));
    }

    let header_lines = vec![
        Line::from(""),
        Line::from(header_spans),
    ];
    let header = Paragraph::new(header_lines)
        .block(Block::default()
            .borders(Borders::BOTTOM)
            .border_style(Style::default().fg(Color::Rgb(40, 40, 40))));
    f.render_widget(header, main_chunks[0]);

    // === CONTENT ===
    let content_area = main_chunks[1];

    // Main container
    let outer_block = Block::default()
        .borders(Borders::NONE);

    let inner_area = outer_block.inner(content_area);
    f.render_widget(outer_block, content_area);

    if state.records.is_empty() {
        let text = if state.loading {
            "Waiting for records..."
        } else {
            "No records"
        };
        let para = Paragraph::new(Span::styled(text, Style::default().fg(TEXT_MUTED)))
            .alignment(Alignment::Center);
        f.render_widget(para, Rect::new(inner_area.x, inner_area.y + 2, inner_area.width, 1));
        return;
    }

    let total_records = state.records.len();
    let selected = state.selected.min(total_records.saturating_sub(1));

    // Layout depends on whether list is hidden
    let body_area = if state.hide_list {
        // Full width for body when list hidden
        inner_area
    } else {
        // Split into left (record list) and right (body preview) panes
        let panes = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(28),  // Record list - compact
                Constraint::Min(20),     // Body preview - takes remaining space
            ])
            .split(inner_area);

        let list_area = panes[0];
        let visible_height = list_area.height as usize;

        // Keep selected record in view
        let scroll_offset = if state.is_tailing && !state.paused {
            // Auto-scroll to show latest
            total_records.saturating_sub(visible_height)
        } else if selected >= visible_height {
            selected - visible_height + 1
        } else {
            0
        };

        // === Left pane: Record list ===
        for (view_idx, record) in state.records.iter().enumerate().skip(scroll_offset).take(visible_height) {
            let y = list_area.y + (view_idx - scroll_offset) as u16;
            if y >= list_area.y + list_area.height {
                break;
            }

            let is_selected = view_idx == selected;
            let has_headers = !record.headers.is_empty();
            let row_area = Rect::new(list_area.x, y, list_area.width, 1);

            // Selection highlight
            if is_selected {
                f.render_widget(
                    Block::default().style(Style::default().bg(Color::Rgb(39, 39, 42))),
                    row_area,
                );
            }

            let sel_indicator = if is_selected { "▸" } else { " " };
            let header_indicator = if has_headers { "⌘" } else { " " };

            let line = Line::from(vec![
                Span::styled(sel_indicator, Style::default().fg(GREEN)),
                Span::styled(
                    format!("#{:<8}", record.seq_num),
                    Style::default().fg(if is_selected { GREEN } else { TEXT_SECONDARY }).bold(),
                ),
                Span::styled(
                    format!("{:>13}", record.timestamp),
                    Style::default().fg(TEXT_MUTED),
                ),
                Span::styled(
                    format!(" {}", header_indicator),
                    Style::default().fg(if has_headers { YELLOW } else { BORDER }),
                ),
            ]);
            f.render_widget(Paragraph::new(line), row_area);
        }

        // Vertical separator
        let sep_x = panes[1].x.saturating_sub(1);
        for y in 0..inner_area.height {
            f.render_widget(
                Paragraph::new(Span::styled("│", Style::default().fg(BORDER))),
                Rect::new(sep_x, inner_area.y + y, 1, 1),
            );
        }

        panes[1]
    };

    // === Body preview of selected record ===
    if let Some(record) = state.records.get(selected) {
        let body = String::from_utf8_lossy(&record.body);
        let body_width = body_area.width.saturating_sub(2) as usize;
        let body_height = body_area.height as usize;

        // Cinema mode: when list is hidden and tailing, show raw body without chrome
        let cinema_mode = state.hide_list && state.is_tailing && !state.paused;

        let (content_start_y, content_height) = if cinema_mode {
            // Full height for body in cinema mode
            (body_area.y, body_height)
        } else {
            // Header line with metadata
            let header_line = Line::from(vec![
                Span::styled(format!(" #{}", record.seq_num), Style::default().fg(GREEN).bold()),
                Span::styled(format!("  {}ms", record.timestamp), Style::default().fg(TEXT_MUTED)),
                Span::styled(format!("  {} bytes", record.body.len()), Style::default().fg(TEXT_MUTED)),
                if !record.headers.is_empty() {
                    Span::styled(format!("  ⌘{}", record.headers.len()), Style::default().fg(YELLOW))
                } else {
                    Span::styled("", Style::default())
                },
            ]);
            f.render_widget(Paragraph::new(header_line), Rect::new(body_area.x, body_area.y, body_area.width, 1));

            // Separator
            let sep = "─".repeat(body_width);
            f.render_widget(
                Paragraph::new(Span::styled(format!(" {}", sep), Style::default().fg(BORDER))),
                Rect::new(body_area.x, body_area.y + 1, body_area.width, 1),
            );

            (body_area.y + 2, body_height.saturating_sub(2))
        };

        if body.is_empty() {
            f.render_widget(
                Paragraph::new(Span::styled(" (empty body)", Style::default().fg(TEXT_MUTED).italic())),
                Rect::new(body_area.x, content_start_y, body_area.width, 1),
            );
        } else {
            // Display body text line by line (no wrapping for ASCII art)
            let mut display_lines: Vec<Line> = Vec::new();

            for line in body.lines().take(content_height) {
                // For cinema mode, preserve spacing for ASCII art; otherwise wrap
                if cinema_mode {
                    display_lines.push(Line::from(Span::styled(line.to_string(), Style::default().fg(TEXT_PRIMARY))));
                } else {
                    let chars: Vec<char> = line.chars().collect();
                    if chars.is_empty() {
                        display_lines.push(Line::from(""));
                    } else {
                        for chunk in chars.chunks(body_width.max(1)) {
                            let text: String = chunk.iter().collect();
                            display_lines.push(Line::from(Span::styled(text, Style::default().fg(TEXT_PRIMARY))));
                            if display_lines.len() >= content_height {
                                break;
                            }
                        }
                    }
                }

                if display_lines.len() >= content_height {
                    break;
                }
            }

            let body_para = Paragraph::new(display_lines)
                .block(Block::default().padding(Padding::horizontal(if cinema_mode { 0 } else { 1 })));
            f.render_widget(body_para, Rect::new(body_area.x, content_start_y, body_area.width, content_height as u16));
        }
    }

    // Draw headers popup if showing
    if state.show_detail {
        if let Some(record) = state.records.get(selected) {
            draw_headers_popup(f, record);
        }
    }
}

fn draw_headers_popup(f: &mut Frame, record: &s2_sdk::types::SequencedRecord) {
    // Size popup based on number of headers (min height for "no headers" message)
    let content_lines = if record.headers.is_empty() { 1 } else { record.headers.len() };
    let height = (content_lines + 5).min(20) as u16;
    let area = centered_rect(50, height * 100 / f.area().height.max(1), f.area());

    let mut lines = vec![
        Line::from(""),
        Line::from(vec![
            Span::styled(format!("  Record #{}", record.seq_num), Style::default().fg(GREEN).bold()),
        ]),
        Line::from(""),
    ];

    if record.headers.is_empty() {
        lines.push(Line::from(vec![
            Span::styled("  No headers", Style::default().fg(TEXT_MUTED).italic()),
        ]));
    } else {
        for header in &record.headers {
            let name = String::from_utf8_lossy(&header.name);
            let value = String::from_utf8_lossy(&header.value);
            lines.push(Line::from(vec![
                Span::styled("  ", Style::default()),
                Span::styled(format!("{}", name), Style::default().fg(YELLOW)),
                Span::styled(" = ", Style::default().fg(BORDER)),
                Span::styled(format!("{}", value), Style::default().fg(TEXT_PRIMARY)),
            ]));
        }
    }

    let (title, border_color) = if record.headers.is_empty() {
        (" Headers ", BORDER)
    } else {
        (" Headers ", YELLOW)
    };

    let block = Block::default()
        .title(Line::from(Span::styled(title, Style::default().fg(border_color).bold())))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(border_color))
        .style(Style::default().bg(BG_DARK));

    f.render_widget(Clear, area);
    let para = Paragraph::new(lines).block(block);
    f.render_widget(para, area);
}

fn draw_append_view(f: &mut Frame, area: Rect, state: &AppendViewState) {
    // Split into header and content
    let outer_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // Header (consistent height)
            Constraint::Min(1),     // Content
        ])
        .split(area);

    // === HEADER ===
    let basin_str = state.basin_name.to_string();
    let stream_str = state.stream_name.to_string();
    let header_lines = vec![
        Line::from(""),
        Line::from(vec![
            Span::styled("  ← ", Style::default().fg(Color::Rgb(100, 100, 100))),
            Span::styled(&basin_str, Style::default().fg(Color::Rgb(150, 150, 150))),
            Span::styled(" / ", Style::default().fg(Color::Rgb(80, 80, 80))),
            Span::styled(&stream_str, Style::default().fg(Color::Rgb(180, 180, 180))),
            Span::styled("  ", Style::default()),
            Span::styled(" APPEND ", Style::default().fg(BG_DARK).bg(GREEN).bold()),
        ]),
    ];
    let header = Paragraph::new(header_lines)
        .block(Block::default()
            .borders(Borders::BOTTOM)
            .border_style(Style::default().fg(Color::Rgb(40, 40, 40))));
    f.render_widget(header, outer_chunks[0]);

    // === CONTENT ===
    // Split into form (left) and history (right)
    let main_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(50),  // Form
            Constraint::Percentage(50),  // History
        ])
        .split(outer_chunks[1]);

    // === Form pane ===
    let form_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Rgb(50, 50, 50)))
        .border_type(ratatui::widgets::BorderType::Rounded)
        .padding(Padding::new(2, 2, 1, 1));

    let form_inner = form_block.inner(main_chunks[0]);
    f.render_widget(form_block, main_chunks[0]);

    // Helper functions
    let cursor = |editing: bool| if editing { "▎" } else { "" };
    let selected_marker = |sel: bool| if sel { "▸ " } else { "  " };

    let mut lines: Vec<Line> = Vec::new();

    // Row 0: Body
    let body_selected = state.selected == 0;
    let body_editing = body_selected && state.editing;
    lines.push(Line::from(vec![
        Span::styled(selected_marker(body_selected), Style::default().fg(GREEN)),
        Span::styled("Body", Style::default().fg(if body_selected { TEXT_PRIMARY } else { TEXT_MUTED })),
    ]));
    lines.push(Line::from(vec![
        Span::styled("  ", Style::default()),
        Span::styled(
            if state.body.is_empty() && !body_editing {
                "(empty)".to_string()
            } else {
                format!("{}{}", &state.body, cursor(body_editing))
            },
            Style::default().fg(if body_editing { GREEN } else if state.body.is_empty() { TEXT_MUTED } else { TEXT_SECONDARY })
        ),
    ]));
    lines.push(Line::from(""));

    // Row 1: Headers
    let headers_selected = state.selected == 1;
    let headers_editing = headers_selected && state.editing;
    lines.push(Line::from(vec![
        Span::styled(selected_marker(headers_selected), Style::default().fg(GREEN)),
        Span::styled("Headers", Style::default().fg(if headers_selected { TEXT_PRIMARY } else { TEXT_MUTED })),
        Span::styled(format!("  ({} added)", state.headers.len()), Style::default().fg(TEXT_MUTED)),
        if headers_selected && !headers_editing {
            Span::styled("  d=del", Style::default().fg(BORDER))
        } else {
            Span::raw("")
        },
    ]));

    // Show existing headers
    for (key, value) in &state.headers {
        lines.push(Line::from(vec![
            Span::styled("    ", Style::default()),
            Span::styled(key, Style::default().fg(YELLOW)),
            Span::styled(": ", Style::default().fg(TEXT_MUTED)),
            Span::styled(value, Style::default().fg(TEXT_SECONDARY)),
        ]));
    }

    // Show header input if editing
    if headers_editing {
        lines.push(Line::from(vec![
            Span::styled("  + ", Style::default().fg(GREEN)),
            Span::styled(
                format!("{}{}", &state.header_key_input, if state.editing_header_key { "▎" } else { "" }),
                Style::default().fg(if state.editing_header_key { GREEN } else { YELLOW })
            ),
            Span::styled(": ", Style::default().fg(TEXT_MUTED)),
            Span::styled(
                format!("{}{}", &state.header_value_input, if !state.editing_header_key { "▎" } else { "" }),
                Style::default().fg(if !state.editing_header_key { GREEN } else { TEXT_SECONDARY })
            ),
            Span::styled("  ⇥=switch", Style::default().fg(BORDER)),
        ]));
    } else if headers_selected {
        lines.push(Line::from(vec![
            Span::styled("  ", Style::default()),
            Span::styled("Press Enter to add header", Style::default().fg(TEXT_MUTED).italic()),
        ]));
    }
    lines.push(Line::from(""));

    // Row 2: Match seq num
    let match_selected = state.selected == 2;
    let match_editing = match_selected && state.editing;
    lines.push(Line::from(vec![
        Span::styled(selected_marker(match_selected), Style::default().fg(GREEN)),
        Span::styled("Match Seq#", Style::default().fg(if match_selected { TEXT_PRIMARY } else { TEXT_MUTED })),
        Span::styled("  ", Style::default()),
        Span::styled(
            if state.match_seq_num.is_empty() && !match_editing {
                "(none)".to_string()
            } else {
                format!("{}{}", &state.match_seq_num, cursor(match_editing))
            },
            Style::default().fg(if match_editing { GREEN } else if state.match_seq_num.is_empty() { TEXT_MUTED } else { TEXT_SECONDARY })
        ),
    ]));
    lines.push(Line::from(""));

    // Row 3: Fencing token
    let fence_selected = state.selected == 3;
    let fence_editing = fence_selected && state.editing;
    lines.push(Line::from(vec![
        Span::styled(selected_marker(fence_selected), Style::default().fg(GREEN)),
        Span::styled("Fencing Token", Style::default().fg(if fence_selected { TEXT_PRIMARY } else { TEXT_MUTED })),
        Span::styled("  ", Style::default()),
        Span::styled(
            if state.fencing_token.is_empty() && !fence_editing {
                "(none)".to_string()
            } else {
                format!("{}{}", &state.fencing_token, cursor(fence_editing))
            },
            Style::default().fg(if fence_editing { GREEN } else if state.fencing_token.is_empty() { TEXT_MUTED } else { TEXT_SECONDARY })
        ),
    ]));
    lines.push(Line::from(""));

    // Row 4: Send button
    let send_selected = state.selected == 4;
    let can_send = !state.body.is_empty() && !state.appending;
    let (btn_fg, btn_bg) = if state.appending {
        (BG_DARK, YELLOW)
    } else if send_selected && can_send {
        (BG_DARK, GREEN)
    } else {
        (if can_send { GREEN } else { TEXT_MUTED }, BG_PANEL)
    };
    lines.push(Line::from(vec![
        Span::styled(selected_marker(send_selected), Style::default().fg(GREEN)),
        Span::styled(
            if state.appending { " ◌ SENDING... " } else { " ▶ SEND " },
            Style::default().fg(btn_fg).bg(btn_bg).bold()
        ),
    ]));

    let form_para = Paragraph::new(lines);
    f.render_widget(form_para, form_inner);

    // === History pane ===
    let history_block = Block::default()
        .title(Line::from(vec![
            Span::styled(" History ", Style::default().fg(TEXT_PRIMARY)),
            Span::styled(format!(" {} appended", state.history.len()), Style::default().fg(TEXT_MUTED)),
        ]))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(BORDER));

    if state.history.is_empty() {
        let text = Paragraph::new(Span::styled(
            "No records appended yet",
            Style::default().fg(TEXT_MUTED).italic(),
        ))
        .alignment(Alignment::Center)
        .block(history_block);
        f.render_widget(text, main_chunks[1]);
    } else {
        let history_inner = history_block.inner(main_chunks[1]);
        f.render_widget(history_block, main_chunks[1]);

        let visible_height = history_inner.height as usize;
        let start = state.history.len().saturating_sub(visible_height);

        let mut history_lines: Vec<Line> = Vec::new();
        for result in state.history.iter().skip(start) {
            let mut spans = vec![
                Span::styled(format!("#{:<8}", result.seq_num), Style::default().fg(GREEN)),
            ];
            if result.header_count > 0 {
                spans.push(Span::styled(format!(" ⌘{}", result.header_count), Style::default().fg(YELLOW)));
            }
            spans.push(Span::styled(format!(" {}", &result.body_preview), Style::default().fg(TEXT_SECONDARY)));
            history_lines.push(Line::from(spans));
        }

        let history_para = Paragraph::new(history_lines);
        f.render_widget(history_para, history_inner);
    }
}

fn draw_status_bar(f: &mut Frame, area: Rect, app: &App) {
    let width = area.width as usize;

    // Get hints based on available width - full, medium, or compact
    let hints = get_responsive_hints(&app.screen, width);

    let message_span = app
        .message
        .as_ref()
        .map(|m| {
            let color = match m.level {
                MessageLevel::Info => ACCENT,
                MessageLevel::Success => SUCCESS,
                MessageLevel::Error => ERROR,
            };
            Span::styled(&m.text, Style::default().fg(color))
        });

    // Calculate available width for hints after message
    let msg_len = app.message.as_ref().map(|m| m.text.len() + 2).unwrap_or(0);
    let available = width.saturating_sub(msg_len);

    // Truncate hints if needed
    let display_hints: String = if hints.len() > available && available > 3 {
        format!("{}...", &hints[..available.saturating_sub(3)])
    } else {
        hints
    };

    let line = if let Some(msg) = message_span {
        Line::from(vec![
            msg,
            Span::styled("  ", Style::default()),
            Span::styled(display_hints, Style::default().fg(TEXT_MUTED)),
        ])
    } else {
        Line::from(Span::styled(display_hints, Style::default().fg(TEXT_MUTED)))
    };

    let status = Paragraph::new(line);
    f.render_widget(status, area);
}

/// Get responsive hints based on screen width
fn get_responsive_hints(screen: &Screen, width: usize) -> String {
    // Width thresholds
    let wide = width >= 100;
    let medium = width >= 60;

    match screen {
        Screen::Splash | Screen::Setup(_) => String::new(),
        Screen::Settings(_) => {
            if wide {
                "jk nav | e edit | hl compression | space toggle | ⏎ save | r reload | ⇥ switch | q".to_string()
            } else if medium {
                "jk e hl space ⏎ r ⇥ q".to_string()
            } else {
                "jk e ⏎ r ⇥ q".to_string()
            }
        }
        Screen::Basins(_) => {
            if wide {
                "/ filter | jk nav | ⏎ open | M basin metrics | A acct metrics | c new | e cfg | d del | r ref | ?".to_string()
            } else if medium {
                "/ | jk ⏎ | M basin | A acct | c d e r ?".to_string()
            } else {
                "jk ⏎ M A c d ?".to_string()
            }
        }
        Screen::Streams(_) => {
            if wide {
                "/ filter | jk nav | ⏎ open | M metrics | c new | e cfg | d del | esc".to_string()
            } else if medium {
                "/ filter | jk nav | ⏎ open | M c e d | esc".to_string()
            } else {
                "jk ⏎ M c d esc".to_string()
            }
        }
        Screen::StreamDetail(_) => {
            if wide {
                "t tail | r read | a append | f fence | m trim | M metrics | e cfg | esc".to_string()
            } else if medium {
                "t tail | r read | a append | f m M e | esc".to_string()
            } else {
                "t r a f m M esc".to_string()
            }
        }
        Screen::ReadView(s) => {
            if s.show_detail {
                "esc/⏎ close".to_string()
            } else if s.is_tailing {
                if wide {
                    "jk nav | h headers | ⇥ list | space pause | gG top/bot | esc".to_string()
                } else if medium {
                    "jk nav | h hdrs | ⇥ | space | gG | esc".to_string()
                } else {
                    "jk h ⇥ space gG esc".to_string()
                }
            } else {
                if wide {
                    "jk nav | h headers | ⇥ list | gG top/bot | esc".to_string()
                } else if medium {
                    "jk nav | h hdrs | ⇥ | gG | esc".to_string()
                } else {
                    "jk h ⇥ gG esc".to_string()
                }
            }
        }
        Screen::AppendView(s) => {
            if s.editing {
                if s.selected == 1 {
                    "type | ⇥ key/val | ⏎ add | esc done".to_string()
                } else {
                    "type | ⏎ done | esc cancel".to_string()
                }
            } else {
                if wide {
                    "jk nav | ⏎ edit/send | d del header | esc back".to_string()
                } else {
                    "jk ⏎ edit | d del | esc".to_string()
                }
            }
        }
        Screen::AccessTokens(_) => {
            if wide {
                "/ filter | jk nav | c issue | d revoke | r ref | ⇥ switch | ? | q".to_string()
            } else if medium {
                "/ | jk | c issue | d rev | r | ⇥ | ? q".to_string()
            } else {
                "jk c d r ⇥ ? q".to_string()
            }
        }
        Screen::MetricsView(state) => {
            if matches!(state.metrics_type, MetricsType::Basin { .. } | MetricsType::Account) {
                if wide {
                    "←→ category | jk scroll | r refresh | esc back | q quit".to_string()
                } else {
                    "←→ cat | jk | r | esc q".to_string()
                }
            } else {
                if wide {
                    "jk scroll | r refresh | esc back | q quit".to_string()
                } else {
                    "jk | r | esc q".to_string()
                }
            }
        }
    }
}

fn draw_help_overlay(f: &mut Frame, screen: &Screen) {
    let area = centered_rect(50, 50, f.area());

    let help_text = match screen {
        Screen::Splash | Screen::Setup(_) => vec![], // Never shown
        Screen::Settings(_) => vec![
            Line::from(""),
            Line::from(vec![
                Span::styled("  j/k ", Style::default().fg(GREEN).bold()),
                Span::styled("Navigate", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    e ", Style::default().fg(GREEN).bold()),
                Span::styled("Edit field", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("  h/l ", Style::default().fg(GREEN).bold()),
                Span::styled("Change compression", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("space ", Style::default().fg(GREEN).bold()),
                Span::styled("Toggle token visibility", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("enter ", Style::default().fg(GREEN).bold()),
                Span::styled("Save changes", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    r ", Style::default().fg(GREEN).bold()),
                Span::styled("Reload from file", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("  tab ", Style::default().fg(GREEN).bold()),
                Span::styled("Switch tabs", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    q ", Style::default().fg(GREEN).bold()),
                Span::styled("Quit", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(""),
        ],
        Screen::Basins(_) => vec![
            Line::from(""),
            Line::from(vec![
                Span::styled("  j/k ", Style::default().fg(GREEN).bold()),
                Span::styled("Navigate", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("  g/G ", Style::default().fg(GREEN).bold()),
                Span::styled("Top / Bottom", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    / ", Style::default().fg(GREEN).bold()),
                Span::styled("Filter", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("enter ", Style::default().fg(GREEN).bold()),
                Span::styled("Select basin", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    c ", Style::default().fg(GREEN).bold()),
                Span::styled("Create basin", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    e ", Style::default().fg(GREEN).bold()),
                Span::styled("Reconfigure basin", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    d ", Style::default().fg(GREEN).bold()),
                Span::styled("Delete basin", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    r ", Style::default().fg(GREEN).bold()),
                Span::styled("Refresh", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    M ", Style::default().fg(GREEN).bold()),
                Span::styled("Basin metrics", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    A ", Style::default().fg(GREEN).bold()),
                Span::styled("Account metrics", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("  tab ", Style::default().fg(GREEN).bold()),
                Span::styled("Switch to Access Tokens", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    q ", Style::default().fg(GREEN).bold()),
                Span::styled("Quit", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(""),
        ],
        Screen::Streams(_) => vec![
            Line::from(""),
            Line::from(vec![
                Span::styled("  j/k ", Style::default().fg(GREEN).bold()),
                Span::styled("Navigate", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    / ", Style::default().fg(GREEN).bold()),
                Span::styled("Filter", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("enter ", Style::default().fg(GREEN).bold()),
                Span::styled("Select stream", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    c ", Style::default().fg(GREEN).bold()),
                Span::styled("Create stream", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    e ", Style::default().fg(GREEN).bold()),
                Span::styled("Reconfigure stream", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    d ", Style::default().fg(GREEN).bold()),
                Span::styled("Delete stream", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    r ", Style::default().fg(GREEN).bold()),
                Span::styled("Refresh", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("  esc ", Style::default().fg(GREEN).bold()),
                Span::styled("Back", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(""),
        ],
        Screen::StreamDetail(_) => vec![
            Line::from(""),
            Line::from(vec![
                Span::styled("  j/k ", Style::default().fg(GREEN).bold()),
                Span::styled("Navigate actions", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("enter ", Style::default().fg(GREEN).bold()),
                Span::styled("Execute action", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    t ", Style::default().fg(GREEN).bold()),
                Span::styled("Tail (live follow)", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    r ", Style::default().fg(GREEN).bold()),
                Span::styled("Read records", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    a ", Style::default().fg(GREEN).bold()),
                Span::styled("Append records", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    f ", Style::default().fg(GREEN).bold()),
                Span::styled("Fence stream", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    m ", Style::default().fg(GREEN).bold()),
                Span::styled("Trim stream", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    e ", Style::default().fg(GREEN).bold()),
                Span::styled("Reconfigure stream", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    M ", Style::default().fg(GREEN).bold()),
                Span::styled("Stream metrics", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("  esc ", Style::default().fg(GREEN).bold()),
                Span::styled("Back", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(""),
        ],
        Screen::ReadView(_) => vec![
            Line::from(""),
            Line::from(vec![
                Span::styled("  j/k ", Style::default().fg(GREEN).bold()),
                Span::styled("Scroll", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("  g/G ", Style::default().fg(GREEN).bold()),
                Span::styled("Top / Bottom", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("space ", Style::default().fg(GREEN).bold()),
                Span::styled("Pause / Resume", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("  esc ", Style::default().fg(GREEN).bold()),
                Span::styled("Back", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(""),
        ],
        Screen::AppendView(_) => vec![
            Line::from(""),
            Line::from(vec![
                Span::styled("  j/k ", Style::default().fg(GREEN).bold()),
                Span::styled("Navigate fields", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("enter ", Style::default().fg(GREEN).bold()),
                Span::styled("Edit field / Send record", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    d ", Style::default().fg(GREEN).bold()),
                Span::styled("Delete last header", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("  tab ", Style::default().fg(GREEN).bold()),
                Span::styled("Switch header key/value", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("  esc ", Style::default().fg(GREEN).bold()),
                Span::styled("Stop editing / Back", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(""),
        ],
        Screen::AccessTokens(_) => vec![
            Line::from(""),
            Line::from(vec![
                Span::styled("  j/k ", Style::default().fg(GREEN).bold()),
                Span::styled("Navigate", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("  g/G ", Style::default().fg(GREEN).bold()),
                Span::styled("Top / Bottom", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    / ", Style::default().fg(GREEN).bold()),
                Span::styled("Filter", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    c ", Style::default().fg(GREEN).bold()),
                Span::styled("Issue new token", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    d ", Style::default().fg(GREEN).bold()),
                Span::styled("Revoke token", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    r ", Style::default().fg(GREEN).bold()),
                Span::styled("Refresh", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("  tab ", Style::default().fg(GREEN).bold()),
                Span::styled("Switch to Basins", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(vec![
                Span::styled("    q ", Style::default().fg(GREEN).bold()),
                Span::styled("Quit", Style::default().fg(TEXT_SECONDARY)),
            ]),
            Line::from(""),
        ],
        Screen::MetricsView(state) => {
            let mut lines = vec![
                Line::from(""),
                Line::from(vec![
                    Span::styled("  j/k ", Style::default().fg(GREEN).bold()),
                    Span::styled("Scroll", Style::default().fg(TEXT_SECONDARY)),
                ]),
                Line::from(vec![
                    Span::styled("    r ", Style::default().fg(GREEN).bold()),
                    Span::styled("Refresh", Style::default().fg(TEXT_SECONDARY)),
                ]),
            ];
            if matches!(state.metrics_type, MetricsType::Basin { .. }) {
                lines.push(Line::from(vec![
                    Span::styled("  ←/→ ", Style::default().fg(GREEN).bold()),
                    Span::styled("Change metric", Style::default().fg(TEXT_SECONDARY)),
                ]));
            }
            lines.push(Line::from(vec![
                Span::styled("  esc ", Style::default().fg(GREEN).bold()),
                Span::styled("Back", Style::default().fg(TEXT_SECONDARY)),
            ]));
            lines.push(Line::from(vec![
                Span::styled("    q ", Style::default().fg(GREEN).bold()),
                Span::styled("Quit", Style::default().fg(TEXT_SECONDARY)),
            ]));
            lines.push(Line::from(""));
            lines
        },
    };

    let block = Block::default()
        .title(Line::from(Span::styled(" Help ", Style::default().fg(TEXT_PRIMARY).bold())))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(ACCENT))
        .style(Style::default().bg(BG_DARK));

    let help = Paragraph::new(help_text).block(block);

    f.render_widget(Clear, area);
    f.render_widget(help, area);
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

fn draw_input_dialog(f: &mut Frame, mode: &InputMode) {
    let (title, content, hint) = match mode {
        InputMode::Normal => return,

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
            use crate::tui::app::BasinScopeOption;

            let cursor = "▎";
            let name_valid = name.len() >= 8 && name.len() <= 48;

            // Modern toggle switch rendering
            let toggle = |on: bool, selected: bool| -> Vec<Span<'static>> {
                if on {
                    vec![
                        Span::styled("", Style::default().fg(if selected { GREEN } else { Color::Rgb(60, 60, 60) })),
                        Span::styled(" ON ", Style::default().fg(BG_DARK).bg(GREEN).bold()),
                        Span::styled("", Style::default().fg(GREEN)),
                    ]
                } else {
                    vec![
                        Span::styled("", Style::default().fg(if selected { TEXT_MUTED } else { Color::Rgb(60, 60, 60) })),
                        Span::styled(" OFF ", Style::default().fg(TEXT_MUTED).bg(Color::Rgb(60, 60, 60))),
                        Span::styled("", Style::default().fg(Color::Rgb(60, 60, 60))),
                    ]
                }
            };

            // Pill-style selector for enum options
            let pill = |label: &str, is_selected: bool, is_active: bool| -> Span<'static> {
                let label = label.to_string();
                if is_active {
                    Span::styled(format!(" {} ", label), Style::default().fg(BG_DARK).bg(GREEN).bold())
                } else if is_selected {
                    Span::styled(format!(" {} ", label), Style::default().fg(TEXT_PRIMARY).bg(Color::Rgb(50, 50, 50)))
                } else {
                    Span::styled(format!(" {} ", label), Style::default().fg(TEXT_MUTED))
                }
            };


            // Field row with label and value
            let field_row = |idx: usize, label: &str, sel: usize| -> (Span<'static>, Span<'static>) {
                let is_sel = sel == idx;
                let indicator = if is_sel {
                    Span::styled(" > ", Style::default().fg(GREEN).bold())
                } else {
                    Span::styled("   ", Style::default())
                };
                let label_style = if is_sel {
                    Style::default().fg(TEXT_PRIMARY).bold()
                } else {
                    Style::default().fg(TEXT_MUTED)
                };
                (indicator, Span::styled(label.to_string(), label_style))
            };

            // Scope options
            let scope_opts = [
                ("AWS us-east-1", *scope == BasinScopeOption::AwsUsEast1),
            ];

            // Storage class options
            let storage_opts = [
                ("Default", storage_class.is_none()),
                ("Standard", matches!(storage_class, Some(StorageClass::Standard))),
                ("Express", matches!(storage_class, Some(StorageClass::Express))),
            ];

            // Timestamping mode options
            let ts_opts = [
                ("Default", timestamping_mode.is_none()),
                ("ClientPrefer", matches!(timestamping_mode, Some(TimestampingMode::ClientPrefer))),
                ("ClientRequire", matches!(timestamping_mode, Some(TimestampingMode::ClientRequire))),
                ("Arrival", matches!(timestamping_mode, Some(TimestampingMode::Arrival))),
            ];

            // Retention options
            let ret_opts = [
                ("Infinite", *retention_policy == RetentionPolicyOption::Infinite),
                ("Age-based", *retention_policy == RetentionPolicyOption::Age),
            ];

            let mut lines = vec![];

            // ═══════════════════════════════════════════════════════════════
            // BASIN NAME SECTION
            // ═══════════════════════════════════════════════════════════════
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::styled("   Basin name ", Style::default().fg(CYAN).bold()),
                Span::styled("─".repeat(35), Style::default().fg(Color::Rgb(50, 50, 50))),
            ]));
            lines.push(Line::from(""));

            // Basin Name
            let (ind, lbl) = field_row(0, "Name", *selected);
            let name_display = if name.is_empty() {
                Span::styled("enter name...", Style::default().fg(Color::Rgb(80, 80, 80)).italic())
            } else {
                let color = if name_valid { GREEN } else { YELLOW };
                Span::styled(name.clone(), Style::default().fg(color))
            };
            let cursor_span = if *selected == 0 && *editing {
                Span::styled(cursor, Style::default().fg(GREEN))
            } else {
                Span::raw("")
            };

            lines.push(Line::from(vec![ind, lbl, Span::raw("  "), name_display, cursor_span]));

            // Validation hint
            let hint_text = if name.is_empty() {
                "lowercase, numbers, hyphens (8-48 chars)".to_string()
            } else if name.len() < 8 {
                format!("{} more chars needed", 8 - name.len())
            } else if name.len() > 48 {
                "name too long".to_string()
            } else {
                format!("{}/48 chars", name.len())
            };
            let hint_color = if name_valid { Color::Rgb(80, 80, 80) } else if name.is_empty() { Color::Rgb(80, 80, 80) } else { YELLOW };
            lines.push(Line::from(vec![
                Span::raw("              "),
                Span::styled(hint_text, Style::default().fg(hint_color).italic()),
            ]));

            // Basin Scope (Cloud Provider/Region)
            lines.push(Line::from(""));
            let (ind, lbl) = field_row(1, "Region", *selected);
            let mut scope_spans = vec![ind, lbl, Span::raw("  ")];
            for (label, active) in &scope_opts {
                scope_spans.push(pill(label, *selected == 1, *active));
                scope_spans.push(Span::raw(" "));
            }
            lines.push(Line::from(scope_spans));

            // ═══════════════════════════════════════════════════════════════
            // DEFAULT STREAM CONFIGURATION SECTION
            // ═══════════════════════════════════════════════════════════════
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::styled("   Default stream configuration ", Style::default().fg(CYAN).bold()),
                Span::styled("─".repeat(17), Style::default().fg(Color::Rgb(50, 50, 50))),
            ]));
            lines.push(Line::from(""));

            // Storage Class
            let (ind, lbl) = field_row(2, "Storage", *selected);
            let mut storage_spans = vec![ind, lbl, Span::raw("  ")];
            for (label, active) in &storage_opts {
                storage_spans.push(pill(label, *selected == 2, *active));
                storage_spans.push(Span::raw(" "));
            }
            lines.push(Line::from(storage_spans));

            // Retention
            lines.push(Line::from(""));
            let (ind, lbl) = field_row(3, "Retention", *selected);
            let mut ret_spans = vec![ind, lbl, Span::raw("  ")];
            for (label, active) in &ret_opts {
                ret_spans.push(pill(label, *selected == 3, *active));
                ret_spans.push(Span::raw(" "));
            }
            lines.push(Line::from(ret_spans));

            // Retention Age (conditional)
            if *retention_policy == RetentionPolicyOption::Age {
                let (ind, lbl) = field_row(4, "  Duration", *selected);
                let age_cursor = if *selected == 4 && *editing { cursor } else { "" };
                lines.push(Line::from(vec![
                    ind, lbl, Span::raw("  "),
                    Span::styled(retention_age_input.clone(), Style::default().fg(YELLOW)),
                    Span::styled(age_cursor, Style::default().fg(GREEN)),
                    Span::styled("  e.g. 7d, 30d, 1y", Style::default().fg(Color::Rgb(80, 80, 80)).italic()),
                ]));
            }

            // Timestamping Mode
            lines.push(Line::from(""));
            let (ind, lbl) = field_row(5, "Timestamps", *selected);
            let mut ts_spans = vec![ind, lbl, Span::raw("  ")];
            for (label, active) in &ts_opts {
                ts_spans.push(pill(label, *selected == 5, *active));
                ts_spans.push(Span::raw(" "));
            }
            lines.push(Line::from(ts_spans));

            // Uncapped Timestamps
            let (ind, lbl) = field_row(6, "  Uncapped", *selected);
            let mut uncapped_spans = vec![ind, lbl, Span::raw("  ")];
            uncapped_spans.extend(toggle(*timestamping_uncapped, *selected == 6));
            lines.push(Line::from(uncapped_spans));

            // Delete on Empty
            let delete_opts = [
                ("Never", !*delete_on_empty_enabled),
                ("After threshold", *delete_on_empty_enabled),
            ];
            lines.push(Line::from(""));
            let (ind, lbl) = field_row(7, "Delete on empty", *selected);
            let mut del_spans = vec![ind, lbl, Span::raw("  ")];
            for (label, active) in &delete_opts {
                del_spans.push(pill(label, *selected == 7, *active));
                del_spans.push(Span::raw(" "));
            }
            lines.push(Line::from(del_spans));

            // Delete on Empty Threshold (conditional)
            if *delete_on_empty_enabled {
                let (ind, lbl) = field_row(8, "  Threshold", *selected);
                let age_cursor = if *selected == 8 && *editing { cursor } else { "" };
                lines.push(Line::from(vec![
                    ind, lbl, Span::raw("  "),
                    Span::styled(delete_on_empty_min_age.clone(), Style::default().fg(YELLOW)),
                    Span::styled(age_cursor, Style::default().fg(GREEN)),
                    Span::styled("  e.g. 1h, 7d", Style::default().fg(Color::Rgb(80, 80, 80)).italic()),
                ]));
            }

            // ═══════════════════════════════════════════════════════════════
            // CREATE STREAMS AUTOMATICALLY SECTION
            // ═══════════════════════════════════════════════════════════════
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::styled("   Create streams automatically ", Style::default().fg(CYAN).bold()),
                Span::styled("─".repeat(17), Style::default().fg(Color::Rgb(50, 50, 50))),
            ]));
            lines.push(Line::from(""));

            // On Append
            let (ind, lbl) = field_row(9, "On append", *selected);
            let mut append_spans = vec![ind, lbl, Span::raw("  ")];
            append_spans.extend(toggle(*create_stream_on_append, *selected == 9));
            lines.push(Line::from(append_spans));

            // On Read
            lines.push(Line::from(""));
            let (ind, lbl) = field_row(10, "On read", *selected);
            let mut read_spans = vec![ind, lbl, Span::raw("  ")];
            read_spans.extend(toggle(*create_stream_on_read, *selected == 10));
            lines.push(Line::from(read_spans));

            // ═══════════════════════════════════════════════════════════════
            // CREATE BUTTON
            // ═══════════════════════════════════════════════════════════════
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::styled("─".repeat(52), Style::default().fg(Color::Rgb(50, 50, 50))),
            ]));
            lines.push(Line::from(""));

            let can_create = name_valid;
            let btn_style = if *selected == 11 && can_create {
                Style::default().fg(BG_DARK).bg(GREEN).bold()
            } else if can_create {
                Style::default().fg(GREEN).bold()
            } else {
                Style::default().fg(Color::Rgb(80, 80, 80))
            };

            let btn_indicator = if *selected == 11 {
                Span::styled(" > ", Style::default().fg(GREEN).bold())
            } else {
                Span::raw("   ")
            };

            lines.push(Line::from(vec![
                btn_indicator,
                Span::styled("  CREATE BASIN  ", btn_style),
                if !can_create {
                    Span::styled("  (enter valid name)", Style::default().fg(Color::Rgb(80, 80, 80)).italic())
                } else {
                    Span::raw("")
                },
            ]));

            lines.push(Line::from(""));

            (
                " Create Basin ",
                lines,
                "j/k navigate | h/l cycle | Space toggle | Enter edit | Esc cancel",
            )
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
            let cursor = "▎";

            // Modern toggle switch rendering
            let toggle = |on: bool, is_selected: bool| -> Vec<Span<'static>> {
                if on {
                    vec![
                        Span::styled("", Style::default().fg(if is_selected { GREEN } else { Color::Rgb(60, 60, 60) })),
                        Span::styled(" ON ", Style::default().fg(BG_DARK).bg(GREEN).bold()),
                        Span::styled("▁▂▃", Style::default().fg(GREEN)),
                    ]
                } else {
                    vec![
                        Span::styled("▃▂▁", Style::default().fg(Color::Rgb(80, 80, 80))),
                        Span::styled(" OFF ", Style::default().fg(TEXT_MUTED).bg(Color::Rgb(50, 50, 50))),
                        Span::styled("", Style::default().fg(if is_selected { GREEN } else { Color::Rgb(60, 60, 60) })),
                    ]
                }
            };

            // Pill-style option renderer
            let pill = |label: &str, is_row_selected: bool, is_active: bool| -> Span<'static> {
                if is_active {
                    Span::styled(
                        format!(" {} ", label),
                        Style::default()
                            .fg(BG_DARK)
                            .bg(if is_row_selected { GREEN } else { Color::Rgb(120, 120, 120) })
                            .bold()
                    )
                } else {
                    Span::styled(
                        format!(" {} ", label),
                        Style::default()
                            .fg(if is_row_selected { TEXT_PRIMARY } else { Color::Rgb(80, 80, 80) })
                    )
                }
            };

            // Field row helper with selection indicator
            let field_row = |field_idx: usize, label: &str, current_selected: usize| -> (Span<'static>, Span<'static>) {
                let is_selected = field_idx == current_selected;
                let indicator = if is_selected {
                    Span::styled(" > ", Style::default().fg(GREEN).bold())
                } else {
                    Span::raw("   ")
                };
                let label_span = Span::styled(
                    format!("{:<15}", label),
                    Style::default().fg(if is_selected { TEXT_PRIMARY } else { TEXT_MUTED })
                );
                (indicator, label_span)
            };

            // Storage options
            let storage_opts = [
                ("Default", storage_class.is_none()),
                ("Standard", matches!(storage_class, Some(StorageClass::Standard))),
                ("Express", matches!(storage_class, Some(StorageClass::Express))),
            ];

            // Timestamping mode options
            let ts_opts = [
                ("Default", timestamping_mode.is_none()),
                ("ClientPrefer", matches!(timestamping_mode, Some(TimestampingMode::ClientPrefer))),
                ("ClientRequire", matches!(timestamping_mode, Some(TimestampingMode::ClientRequire))),
                ("Arrival", matches!(timestamping_mode, Some(TimestampingMode::Arrival))),
            ];

            // Retention options
            let ret_opts = [
                ("Infinite", *retention_policy == RetentionPolicyOption::Infinite),
                ("Age-based", *retention_policy == RetentionPolicyOption::Age),
            ];

            let mut lines = vec![];

            // ═══════════════════════════════════════════════════════════════
            // STREAM NAME SECTION
            // ═══════════════════════════════════════════════════════════════
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::styled("   Stream name ", Style::default().fg(CYAN).bold()),
                Span::styled("─".repeat(34), Style::default().fg(Color::Rgb(50, 50, 50))),
            ]));
            lines.push(Line::from(""));

            // Show which basin this stream will be created in
            lines.push(Line::from(vec![
                Span::raw("   "),
                Span::styled("in basin: ", Style::default().fg(Color::Rgb(80, 80, 80))),
                Span::styled(basin.to_string(), Style::default().fg(TEXT_SECONDARY)),
            ]));
            lines.push(Line::from(""));

            // Stream Name
            let (ind, lbl) = field_row(0, "Name", *selected);
            let name_display = if name.is_empty() {
                Span::styled("enter name...", Style::default().fg(Color::Rgb(80, 80, 80)).italic())
            } else {
                Span::styled(name.clone(), Style::default().fg(GREEN))
            };
            let cursor_span = if *selected == 0 && *editing {
                Span::styled(cursor, Style::default().fg(GREEN))
            } else {
                Span::raw("")
            };

            lines.push(Line::from(vec![ind, lbl, Span::raw("  "), name_display, cursor_span]));

            // ═══════════════════════════════════════════════════════════════
            // STREAM CONFIGURATION SECTION
            // ═══════════════════════════════════════════════════════════════
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::styled("   Stream configuration ", Style::default().fg(CYAN).bold()),
                Span::styled("─".repeat(25), Style::default().fg(Color::Rgb(50, 50, 50))),
            ]));
            lines.push(Line::from(""));

            // Storage Class
            let (ind, lbl) = field_row(1, "Storage", *selected);
            let mut storage_spans = vec![ind, lbl, Span::raw("  ")];
            for (label, active) in &storage_opts {
                storage_spans.push(pill(label, *selected == 1, *active));
                storage_spans.push(Span::raw(" "));
            }
            lines.push(Line::from(storage_spans));

            // Retention
            lines.push(Line::from(""));
            let (ind, lbl) = field_row(2, "Retention", *selected);
            let mut ret_spans = vec![ind, lbl, Span::raw("  ")];
            for (label, active) in &ret_opts {
                ret_spans.push(pill(label, *selected == 2, *active));
                ret_spans.push(Span::raw(" "));
            }
            lines.push(Line::from(ret_spans));

            // Retention Age (conditional)
            if *retention_policy == RetentionPolicyOption::Age {
                let (ind, lbl) = field_row(3, "  Duration", *selected);
                let age_cursor = if *selected == 3 && *editing { cursor } else { "" };
                lines.push(Line::from(vec![
                    ind, lbl, Span::raw("  "),
                    Span::styled(retention_age_input.clone(), Style::default().fg(YELLOW)),
                    Span::styled(age_cursor, Style::default().fg(GREEN)),
                    Span::styled("  e.g. 7d, 30d, 1y", Style::default().fg(Color::Rgb(80, 80, 80)).italic()),
                ]));
            }

            // Timestamping Mode
            lines.push(Line::from(""));
            let (ind, lbl) = field_row(4, "Timestamps", *selected);
            let mut ts_spans = vec![ind, lbl, Span::raw("  ")];
            for (label, active) in &ts_opts {
                ts_spans.push(pill(label, *selected == 4, *active));
                ts_spans.push(Span::raw(" "));
            }
            lines.push(Line::from(ts_spans));

            // Uncapped Timestamps
            let (ind, lbl) = field_row(5, "  Uncapped", *selected);
            let mut uncapped_spans = vec![ind, lbl, Span::raw("  ")];
            uncapped_spans.extend(toggle(*timestamping_uncapped, *selected == 5));
            lines.push(Line::from(uncapped_spans));

            // Delete on Empty
            let delete_opts = [
                ("Never", !*delete_on_empty_enabled),
                ("After threshold", *delete_on_empty_enabled),
            ];
            lines.push(Line::from(""));
            let (ind, lbl) = field_row(6, "Delete on empty", *selected);
            let mut del_spans = vec![ind, lbl, Span::raw("  ")];
            for (label, active) in &delete_opts {
                del_spans.push(pill(label, *selected == 6, *active));
                del_spans.push(Span::raw(" "));
            }
            lines.push(Line::from(del_spans));

            // Delete on Empty Threshold (conditional)
            if *delete_on_empty_enabled {
                let (ind, lbl) = field_row(7, "  Threshold", *selected);
                let age_cursor = if *selected == 7 && *editing { cursor } else { "" };
                lines.push(Line::from(vec![
                    ind, lbl, Span::raw("  "),
                    Span::styled(delete_on_empty_min_age.clone(), Style::default().fg(YELLOW)),
                    Span::styled(age_cursor, Style::default().fg(GREEN)),
                    Span::styled("  e.g. 1h, 7d", Style::default().fg(Color::Rgb(80, 80, 80)).italic()),
                ]));
            }

            // ═══════════════════════════════════════════════════════════════
            // CREATE BUTTON
            // ═══════════════════════════════════════════════════════════════
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::styled("─".repeat(52), Style::default().fg(Color::Rgb(50, 50, 50))),
            ]));
            lines.push(Line::from(""));

            let can_create = !name.is_empty();
            let btn_style = if *selected == 8 && can_create {
                Style::default().fg(BG_DARK).bg(GREEN).bold()
            } else if can_create {
                Style::default().fg(GREEN).bold()
            } else {
                Style::default().fg(Color::Rgb(80, 80, 80))
            };

            let btn_indicator = if *selected == 8 {
                Span::styled(" > ", Style::default().fg(GREEN).bold())
            } else {
                Span::raw("   ")
            };

            lines.push(Line::from(vec![
                btn_indicator,
                Span::styled("  CREATE STREAM  ", btn_style),
                if !can_create {
                    Span::styled("  (enter stream name)", Style::default().fg(Color::Rgb(80, 80, 80)).italic())
                } else {
                    Span::raw("")
                },
            ]));

            lines.push(Line::from(""));

            (
                " Create Stream ",
                lines,
                "j/k navigate | h/l cycle | Space toggle | Enter edit | Esc cancel",
            )
        }

        InputMode::ConfirmDeleteBasin { basin } => (
            " Delete Basin ",
            vec![
                Line::from(""),
                Line::from(vec![
                    Span::styled("Delete basin ", Style::default().fg(TEXT_SECONDARY)),
                    Span::styled(basin.to_string(), Style::default().fg(ERROR).bold()),
                    Span::styled("?", Style::default().fg(TEXT_SECONDARY)),
                ]),
                Line::from(""),
                Line::from(vec![
                    Span::styled("This will delete all streams in this basin.", Style::default().fg(TEXT_MUTED)),
                ]),
                Line::from(vec![
                    Span::styled("This action cannot be undone.", Style::default().fg(ERROR)),
                ]),
            ],
            "y confirm  n/esc cancel",
        ),

        InputMode::ConfirmDeleteStream { basin, stream } => (
            " Delete Stream ",
            vec![
                Line::from(""),
                Line::from(vec![
                    Span::styled("Delete stream ", Style::default().fg(TEXT_SECONDARY)),
                    Span::styled(stream.to_string(), Style::default().fg(ERROR).bold()),
                    Span::styled("?", Style::default().fg(TEXT_SECONDARY)),
                ]),
                Line::from(""),
                Line::from(vec![
                    Span::styled("from basin ", Style::default().fg(TEXT_MUTED)),
                    Span::styled(basin.to_string(), Style::default().fg(TEXT_SECONDARY)),
                ]),
                Line::from(""),
                Line::from(vec![
                    Span::styled("This action cannot be undone.", Style::default().fg(ERROR)),
                ]),
            ],
            "y confirm  n/esc cancel",
        ),

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
            let cursor = "▎";

            // Modern toggle switch rendering
            let toggle = |on: bool, is_selected: bool| -> Vec<Span<'static>> {
                if on {
                    vec![
                        Span::styled("", Style::default().fg(if is_selected { GREEN } else { Color::Rgb(60, 60, 60) })),
                        Span::styled(" ON ", Style::default().fg(BG_DARK).bg(GREEN).bold()),
                        Span::styled("", Style::default().fg(GREEN)),
                    ]
                } else {
                    vec![
                        Span::styled("", Style::default().fg(if is_selected { TEXT_MUTED } else { Color::Rgb(60, 60, 60) })),
                        Span::styled(" OFF ", Style::default().fg(TEXT_MUTED).bg(Color::Rgb(60, 60, 60))),
                        Span::styled("", Style::default().fg(Color::Rgb(60, 60, 60))),
                    ]
                }
            };

            // Pill-style selector
            let pill = |label: &str, is_selected: bool, is_active: bool| -> Span<'static> {
                let label = label.to_string();
                if is_active {
                    Span::styled(format!(" {} ", label), Style::default().fg(BG_DARK).bg(GREEN).bold())
                } else if is_selected {
                    Span::styled(format!(" {} ", label), Style::default().fg(TEXT_PRIMARY).bg(Color::Rgb(50, 50, 50)))
                } else {
                    Span::styled(format!(" {} ", label), Style::default().fg(TEXT_MUTED))
                }
            };

            // Field row helper
            let field_row = |idx: usize, label: &str, sel: usize| -> (Span<'static>, Span<'static>) {
                let is_sel = sel == idx;
                let indicator = if is_sel {
                    Span::styled(" > ", Style::default().fg(GREEN).bold())
                } else {
                    Span::styled("   ", Style::default())
                };
                let label_style = if is_sel {
                    Style::default().fg(TEXT_PRIMARY).bold()
                } else {
                    Style::default().fg(TEXT_MUTED)
                };
                (indicator, Span::styled(label.to_string(), label_style))
            };

            // Options
            let storage_opts = [
                ("Default", storage_class.is_none()),
                ("Standard", matches!(storage_class, Some(StorageClass::Standard))),
                ("Express", matches!(storage_class, Some(StorageClass::Express))),
            ];
            let ret_opts = [
                ("Infinite", *retention_policy == RetentionPolicyOption::Infinite),
                ("Age-based", *retention_policy == RetentionPolicyOption::Age),
            ];
            let ts_opts = [
                ("Default", timestamping_mode.is_none()),
                ("ClientPrefer", matches!(timestamping_mode, Some(TimestampingMode::ClientPrefer))),
                ("ClientRequire", matches!(timestamping_mode, Some(TimestampingMode::ClientRequire))),
                ("Arrival", matches!(timestamping_mode, Some(TimestampingMode::Arrival))),
            ];

            let mut lines = vec![];

            // Basin name header
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::styled("   ", Style::default()),
                Span::styled(basin.to_string(), Style::default().fg(GREEN).bold()),
            ]));

            // Default stream configuration section
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::styled("   Default stream configuration ", Style::default().fg(CYAN).bold()),
                Span::styled("─".repeat(17), Style::default().fg(Color::Rgb(50, 50, 50))),
            ]));
            lines.push(Line::from(""));

            // Storage Class
            let (ind, lbl) = field_row(0, "Storage", *selected);
            let mut storage_spans = vec![ind, lbl, Span::raw("  ")];
            for (label, active) in &storage_opts {
                storage_spans.push(pill(label, *selected == 0, *active));
                storage_spans.push(Span::raw(" "));
            }
            lines.push(Line::from(storage_spans));

            // Retention
            lines.push(Line::from(""));
            let (ind, lbl) = field_row(1, "Retention", *selected);
            let mut ret_spans = vec![ind, lbl, Span::raw("  ")];
            for (label, active) in &ret_opts {
                ret_spans.push(pill(label, *selected == 1, *active));
                ret_spans.push(Span::raw(" "));
            }
            lines.push(Line::from(ret_spans));

            // Retention Age (conditional)
            if *retention_policy == RetentionPolicyOption::Age {
                let (ind, lbl) = field_row(2, "  Duration", *selected);
                let age_cursor = if *selected == 2 && *editing_age { cursor } else { "" };
                let age_display = if *editing_age { age_input.clone() } else { format!("{}s", retention_age_secs) };
                lines.push(Line::from(vec![
                    ind, lbl, Span::raw("  "),
                    Span::styled(age_display, Style::default().fg(YELLOW)),
                    Span::styled(age_cursor, Style::default().fg(GREEN)),
                    Span::styled("  e.g. 604800 (7 days)", Style::default().fg(Color::Rgb(80, 80, 80)).italic()),
                ]));
            }

            // Timestamping Mode
            lines.push(Line::from(""));
            let (ind, lbl) = field_row(3, "Timestamps", *selected);
            let mut ts_spans = vec![ind, lbl, Span::raw("  ")];
            for (label, active) in &ts_opts {
                ts_spans.push(pill(label, *selected == 3, *active));
                ts_spans.push(Span::raw(" "));
            }
            lines.push(Line::from(ts_spans));

            // Uncapped Timestamps
            let (ind, lbl) = field_row(4, "  Uncapped", *selected);
            let mut uncapped_spans = vec![ind, lbl, Span::raw("  ")];
            uncapped_spans.extend(toggle(timestamping_uncapped.unwrap_or(false), *selected == 4));
            lines.push(Line::from(uncapped_spans));

            // Create streams automatically section
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::styled("   Create streams automatically ", Style::default().fg(CYAN).bold()),
                Span::styled("─".repeat(17), Style::default().fg(Color::Rgb(50, 50, 50))),
            ]));
            lines.push(Line::from(""));

            // On Append
            let (ind, lbl) = field_row(5, "On append", *selected);
            let mut append_spans = vec![ind, lbl, Span::raw("  ")];
            append_spans.extend(toggle(create_stream_on_append.unwrap_or(false), *selected == 5));
            lines.push(Line::from(append_spans));

            // On Read
            lines.push(Line::from(""));
            let (ind, lbl) = field_row(6, "On read", *selected);
            let mut read_spans = vec![ind, lbl, Span::raw("  ")];
            read_spans.extend(toggle(create_stream_on_read.unwrap_or(false), *selected == 6));
            lines.push(Line::from(read_spans));

            lines.push(Line::from(""));

            (
                " Reconfigure Basin ",
                lines,
                "j/k navigate | h/l cycle | Space toggle | Enter edit | s save | Esc cancel",
            )
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
            let cursor = "▎";

            // Modern toggle switch rendering
            let toggle = |on: bool, is_selected: bool| -> Vec<Span<'static>> {
                if on {
                    vec![
                        Span::styled("", Style::default().fg(if is_selected { GREEN } else { Color::Rgb(60, 60, 60) })),
                        Span::styled(" ON ", Style::default().fg(BG_DARK).bg(GREEN).bold()),
                        Span::styled("▁▂▃", Style::default().fg(GREEN)),
                    ]
                } else {
                    vec![
                        Span::styled("▃▂▁", Style::default().fg(Color::Rgb(80, 80, 80))),
                        Span::styled(" OFF ", Style::default().fg(TEXT_MUTED).bg(Color::Rgb(50, 50, 50))),
                        Span::styled("", Style::default().fg(if is_selected { GREEN } else { Color::Rgb(60, 60, 60) })),
                    ]
                }
            };

            // Pill-style selector
            let pill = |label: &str, is_row_selected: bool, is_active: bool| -> Span<'static> {
                if is_active {
                    Span::styled(
                        format!(" {} ", label),
                        Style::default()
                            .fg(BG_DARK)
                            .bg(if is_row_selected { GREEN } else { Color::Rgb(120, 120, 120) })
                            .bold()
                    )
                } else {
                    Span::styled(
                        format!(" {} ", label),
                        Style::default()
                            .fg(if is_row_selected { TEXT_PRIMARY } else { Color::Rgb(80, 80, 80) })
                    )
                }
            };

            // Field row helper
            let field_row = |idx: usize, label: &str, sel: usize| -> (Span<'static>, Span<'static>) {
                let is_sel = sel == idx;
                let indicator = if is_sel {
                    Span::styled(" > ", Style::default().fg(GREEN).bold())
                } else {
                    Span::styled("   ", Style::default())
                };
                let label_style = if is_sel {
                    Style::default().fg(TEXT_PRIMARY)
                } else {
                    Style::default().fg(TEXT_MUTED)
                };
                (indicator, Span::styled(format!("{:<15}", label), label_style))
            };

            // Options
            let storage_opts = [
                ("Default", storage_class.is_none()),
                ("Standard", matches!(storage_class, Some(StorageClass::Standard))),
                ("Express", matches!(storage_class, Some(StorageClass::Express))),
            ];
            let ret_opts = [
                ("Infinite", *retention_policy == RetentionPolicyOption::Infinite),
                ("Age-based", *retention_policy == RetentionPolicyOption::Age),
            ];
            let ts_opts = [
                ("Default", timestamping_mode.is_none()),
                ("ClientPrefer", matches!(timestamping_mode, Some(TimestampingMode::ClientPrefer))),
                ("ClientRequire", matches!(timestamping_mode, Some(TimestampingMode::ClientRequire))),
                ("Arrival", matches!(timestamping_mode, Some(TimestampingMode::Arrival))),
            ];

            let mut lines = vec![];

            // Stream name header
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::styled("   ", Style::default()),
                Span::styled(format!("{}/{}", basin, stream), Style::default().fg(GREEN).bold()),
            ]));

            // Stream configuration section
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::styled("   Stream configuration ", Style::default().fg(CYAN).bold()),
                Span::styled("─".repeat(25), Style::default().fg(Color::Rgb(50, 50, 50))),
            ]));
            lines.push(Line::from(""));

            // Storage Class
            let (ind, lbl) = field_row(0, "Storage", *selected);
            let mut storage_spans = vec![ind, lbl, Span::raw("  ")];
            for (label, active) in &storage_opts {
                storage_spans.push(pill(label, *selected == 0, *active));
                storage_spans.push(Span::raw(" "));
            }
            lines.push(Line::from(storage_spans));

            // Retention
            lines.push(Line::from(""));
            let (ind, lbl) = field_row(1, "Retention", *selected);
            let mut ret_spans = vec![ind, lbl, Span::raw("  ")];
            for (label, active) in &ret_opts {
                ret_spans.push(pill(label, *selected == 1, *active));
                ret_spans.push(Span::raw(" "));
            }
            lines.push(Line::from(ret_spans));

            // Retention Age (conditional)
            if *retention_policy == RetentionPolicyOption::Age {
                let (ind, lbl) = field_row(2, "  Duration", *selected);
                let age_cursor = if *selected == 2 && *editing_age { cursor } else { "" };
                let age_display = if *selected == 2 && *editing_age { age_input.clone() } else { format!("{}s", retention_age_secs) };
                lines.push(Line::from(vec![
                    ind, lbl, Span::raw("  "),
                    Span::styled(age_display, Style::default().fg(YELLOW)),
                    Span::styled(age_cursor, Style::default().fg(GREEN)),
                    Span::styled("  e.g. 604800 (7 days)", Style::default().fg(Color::Rgb(80, 80, 80)).italic()),
                ]));
            }

            // Timestamping Mode
            lines.push(Line::from(""));
            let (ind, lbl) = field_row(3, "Timestamps", *selected);
            let mut ts_spans = vec![ind, lbl, Span::raw("  ")];
            for (label, active) in &ts_opts {
                ts_spans.push(pill(label, *selected == 3, *active));
                ts_spans.push(Span::raw(" "));
            }
            lines.push(Line::from(ts_spans));

            // Uncapped Timestamps
            let (ind, lbl) = field_row(4, "  Uncapped", *selected);
            let mut uncapped_spans = vec![ind, lbl, Span::raw("  ")];
            uncapped_spans.extend(toggle(timestamping_uncapped.unwrap_or(false), *selected == 4));
            lines.push(Line::from(uncapped_spans));

            // Delete on Empty
            let delete_opts = [
                ("Never", !*delete_on_empty_enabled),
                ("After threshold", *delete_on_empty_enabled),
            ];
            lines.push(Line::from(""));
            let (ind, lbl) = field_row(5, "Delete on empty", *selected);
            let mut del_spans = vec![ind, lbl, Span::raw("  ")];
            for (label, active) in &delete_opts {
                del_spans.push(pill(label, *selected == 5, *active));
                del_spans.push(Span::raw(" "));
            }
            lines.push(Line::from(del_spans));

            // Delete on Empty Threshold (conditional)
            if *delete_on_empty_enabled {
                let (ind, lbl) = field_row(6, "  Threshold", *selected);
                let age_cursor = if *selected == 6 && *editing_age { cursor } else { "" };
                lines.push(Line::from(vec![
                    ind, lbl, Span::raw("  "),
                    Span::styled(delete_on_empty_min_age.clone(), Style::default().fg(YELLOW)),
                    Span::styled(age_cursor, Style::default().fg(GREEN)),
                    Span::styled("  e.g. 1h, 7d", Style::default().fg(Color::Rgb(80, 80, 80)).italic()),
                ]));
            }

            lines.push(Line::from(""));

            (
                " Reconfigure Stream ",
                lines,
                "j/k navigate | h/l cycle | Space toggle | Enter edit | s save | Esc cancel",
            )
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
            // Stylish indicators
            let radio = |active: bool| if active { "●" } else { "○" };
            let check = |on: bool| if on { "x" } else { " " };

            // Value display - clean with ∞ for unlimited
            let show_val = |value: &str, is_editing: bool, placeholder: &str| -> String {
                if is_editing {
                    if value.is_empty() {
                        "▎".to_string()
                    } else {
                        format!("{}▎", value)
                    }
                } else if value.is_empty() {
                    placeholder.to_string()
                } else {
                    value.to_string()
                }
            };

            let unit_str = match ago_unit {
                AgoUnit::Seconds => "sec",
                AgoUnit::Minutes => "min",
                AgoUnit::Hours => "hr",
                AgoUnit::Days => "day",
            };

            let mut lines = vec![
                Line::from(vec![
                    Span::styled("  ", Style::default()),
                    Span::styled(format!("s2://{}/{}", basin, stream), Style::default().fg(GREEN)),
                ]),
                Line::from(""),
                Line::from(Span::styled("  START POSITION", Style::default().fg(TEXT_MUTED))),
            ];

            // Row 0: Sequence number
            let is_seq = *start_from == ReadStartFrom::SeqNum;
            lines.push(Line::from(vec![
                Span::styled(if *selected == 0 { "  ▸ " } else { "    " }, Style::default().fg(GREEN)),
                Span::styled(format!("{} ", radio(is_seq)), Style::default().fg(if is_seq { GREEN } else { BORDER })),
                Span::styled("Sequence #  ", Style::default().fg(if *selected == 0 { TEXT_PRIMARY } else { TEXT_MUTED })),
                Span::styled(
                    show_val(seq_num_value, *editing && *selected == 0, "0"),
                    Style::default().fg(if *editing && *selected == 0 { GREEN } else if is_seq { TEXT_PRIMARY } else { TEXT_MUTED })
                ),
            ]));

            // Row 1: Timestamp
            let is_ts = *start_from == ReadStartFrom::Timestamp;
            lines.push(Line::from(vec![
                Span::styled(if *selected == 1 { "  ▸ " } else { "    " }, Style::default().fg(GREEN)),
                Span::styled(format!("{} ", radio(is_ts)), Style::default().fg(if is_ts { GREEN } else { BORDER })),
                Span::styled("Timestamp   ", Style::default().fg(if *selected == 1 { TEXT_PRIMARY } else { TEXT_MUTED })),
                Span::styled(
                    show_val(timestamp_value, *editing && *selected == 1, "0"),
                    Style::default().fg(if *editing && *selected == 1 { GREEN } else if is_ts { TEXT_PRIMARY } else { TEXT_MUTED })
                ),
                Span::styled(" ms", Style::default().fg(TEXT_MUTED)),
            ]));

            // Row 2: Time ago
            let is_ago = *start_from == ReadStartFrom::Ago;
            lines.push(Line::from(vec![
                Span::styled(if *selected == 2 { "  ▸ " } else { "    " }, Style::default().fg(GREEN)),
                Span::styled(format!("{} ", radio(is_ago)), Style::default().fg(if is_ago { GREEN } else { BORDER })),
                Span::styled("Time ago    ", Style::default().fg(if *selected == 2 { TEXT_PRIMARY } else { TEXT_MUTED })),
                Span::styled(
                    show_val(ago_value, *editing && *selected == 2, "5"),
                    Style::default().fg(if *editing && *selected == 2 { GREEN } else if is_ago { TEXT_PRIMARY } else { TEXT_MUTED })
                ),
                Span::styled(format!(" {} ", unit_str), Style::default().fg(if is_ago { TEXT_SECONDARY } else { TEXT_MUTED })),
                Span::styled("‹tab›", Style::default().fg(BORDER)),
            ]));

            // Row 3: Tail offset
            let is_off = *start_from == ReadStartFrom::TailOffset;
            lines.push(Line::from(vec![
                Span::styled(if *selected == 3 { "  ▸ " } else { "    " }, Style::default().fg(GREEN)),
                Span::styled(format!("{} ", radio(is_off)), Style::default().fg(if is_off { GREEN } else { BORDER })),
                Span::styled("Tail offset ", Style::default().fg(if *selected == 3 { TEXT_PRIMARY } else { TEXT_MUTED })),
                Span::styled(
                    show_val(tail_offset_value, *editing && *selected == 3, "10"),
                    Style::default().fg(if *editing && *selected == 3 { GREEN } else if is_off { TEXT_PRIMARY } else { TEXT_MUTED })
                ),
                Span::styled(" back", Style::default().fg(TEXT_MUTED)),
            ]));

            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled("  LIMITS", Style::default().fg(TEXT_MUTED))));

            // Row 4: Count
            lines.push(Line::from(vec![
                Span::styled(if *selected == 4 { "  ▸ " } else { "    " }, Style::default().fg(GREEN)),
                Span::styled("Max records ", Style::default().fg(if *selected == 4 { TEXT_PRIMARY } else { TEXT_MUTED })),
                Span::styled(
                    show_val(count_limit, *editing && *selected == 4, "∞"),
                    Style::default().fg(if *editing && *selected == 4 { GREEN } else { TEXT_SECONDARY })
                ),
            ]));

            // Row 5: Bytes
            lines.push(Line::from(vec![
                Span::styled(if *selected == 5 { "  ▸ " } else { "    " }, Style::default().fg(GREEN)),
                Span::styled("Max bytes   ", Style::default().fg(if *selected == 5 { TEXT_PRIMARY } else { TEXT_MUTED })),
                Span::styled(
                    show_val(byte_limit, *editing && *selected == 5, "∞"),
                    Style::default().fg(if *editing && *selected == 5 { GREEN } else { TEXT_SECONDARY })
                ),
            ]));

            // Row 6: Until
            lines.push(Line::from(vec![
                Span::styled(if *selected == 6 { "  ▸ " } else { "    " }, Style::default().fg(GREEN)),
                Span::styled("Until       ", Style::default().fg(if *selected == 6 { TEXT_PRIMARY } else { TEXT_MUTED })),
                Span::styled(
                    show_val(until_timestamp, *editing && *selected == 6, "∞"),
                    Style::default().fg(if *editing && *selected == 6 { GREEN } else { TEXT_SECONDARY })
                ),
                Span::styled(" ms", Style::default().fg(TEXT_MUTED)),
            ]));

            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled("  OPTIONS", Style::default().fg(TEXT_MUTED))));

            // Row 7: Clamp
            lines.push(Line::from(vec![
                Span::styled(if *selected == 7 { "  ▸ " } else { "    " }, Style::default().fg(GREEN)),
                Span::styled(format!("[{}] ", check(*clamp)), Style::default().fg(if *clamp { GREEN } else { BORDER })),
                Span::styled("Clamp to tail", Style::default().fg(if *selected == 7 { TEXT_PRIMARY } else { TEXT_MUTED })),
            ]));

            // Row 8: Format
            lines.push(Line::from(vec![
                Span::styled(if *selected == 8 { "  ▸ " } else { "    " }, Style::default().fg(GREEN)),
                Span::styled("Format      ", Style::default().fg(if *selected == 8 { TEXT_PRIMARY } else { TEXT_MUTED })),
                Span::styled(format!("‹ {} ›", format.as_str()), Style::default().fg(if *selected == 8 { GREEN } else { TEXT_SECONDARY })),
            ]));

            // Row 9: Output file
            lines.push(Line::from(vec![
                Span::styled(if *selected == 9 { "  ▸ " } else { "    " }, Style::default().fg(GREEN)),
                Span::styled("Output      ", Style::default().fg(if *selected == 9 { TEXT_PRIMARY } else { TEXT_MUTED })),
                Span::styled(
                    show_val(output_file, *editing && *selected == 9, "(display only)"),
                    Style::default().fg(if *editing && *selected == 9 { GREEN } else if output_file.is_empty() { TEXT_MUTED } else { TEXT_SECONDARY })
                ),
            ]));

            lines.push(Line::from(""));

            // Row 10: Start button
            let (btn_fg, btn_bg) = if *selected == 10 {
                (BG_DARK, GREEN)
            } else {
                (GREEN, BG_PANEL)
            };
            lines.push(Line::from(vec![
                Span::styled(if *selected == 10 { "  ▸ " } else { "    " }, Style::default().fg(GREEN)),
                Span::styled(" ▶ START ", Style::default().fg(btn_fg).bg(btn_bg).bold()),
            ]));

            (
                " Read ",
                lines,
                "↑↓ nav  ⏎ edit  ␣ toggle  ⇥ unit  esc",
            )
        }

        InputMode::Fence {
            basin,
            stream,
            new_token,
            current_token,
            selected,
            editing,
        } => {
            let cursor = |is_editing: bool| if is_editing { "▎" } else { "" };
            let marker = |sel: bool| if sel { "▸ " } else { "  " };

            let mut lines = vec![
                Line::from(vec![
                    Span::styled("  ", Style::default()),
                    Span::styled(format!("s2://{}/{}", basin, stream), Style::default().fg(GREEN)),
                ]),
                Line::from(""),
                Line::from(Span::styled("  Set a new fencing token to block other writers.", Style::default().fg(TEXT_MUTED))),
                Line::from(""),
            ];

            // Row 0: New token
            let new_editing = *editing && *selected == 0;
            lines.push(Line::from(vec![
                Span::styled(marker(*selected == 0), Style::default().fg(GREEN)),
                Span::styled("New Token     ", Style::default().fg(if *selected == 0 { TEXT_PRIMARY } else { TEXT_MUTED })),
                Span::styled(
                    if new_token.is_empty() && !new_editing {
                        "(required)".to_string()
                    } else {
                        format!("{}{}", new_token, cursor(new_editing))
                    },
                    Style::default().fg(if new_editing { GREEN } else if new_token.is_empty() { WARNING } else { TEXT_SECONDARY })
                ),
            ]));

            // Row 1: Current token
            let cur_editing = *editing && *selected == 1;
            lines.push(Line::from(vec![
                Span::styled(marker(*selected == 1), Style::default().fg(GREEN)),
                Span::styled("Current Token ", Style::default().fg(if *selected == 1 { TEXT_PRIMARY } else { TEXT_MUTED })),
                Span::styled(
                    if current_token.is_empty() && !cur_editing {
                        "(none)".to_string()
                    } else {
                        format!("{}{}", current_token, cursor(cur_editing))
                    },
                    Style::default().fg(if cur_editing { GREEN } else if current_token.is_empty() { TEXT_MUTED } else { TEXT_SECONDARY })
                ),
            ]));

            lines.push(Line::from(""));

            // Row 2: Submit button
            let can_submit = !new_token.is_empty();
            let (btn_fg, btn_bg) = if *selected == 2 && can_submit {
                (BG_DARK, GREEN)
            } else {
                (if can_submit { GREEN } else { TEXT_MUTED }, BG_PANEL)
            };
            lines.push(Line::from(vec![
                Span::styled(marker(*selected == 2), Style::default().fg(GREEN)),
                Span::styled(" ▶ FENCE ", Style::default().fg(btn_fg).bg(btn_bg).bold()),
            ]));

            (
                " Fence Stream ",
                lines,
                "↑↓ nav  ⏎ edit/submit  esc",
            )
        }

        InputMode::Trim {
            basin,
            stream,
            trim_point,
            fencing_token,
            selected,
            editing,
        } => {
            let cursor = |is_editing: bool| if is_editing { "▎" } else { "" };
            let marker = |sel: bool| if sel { "▸ " } else { "  " };

            let mut lines = vec![
                Line::from(vec![
                    Span::styled("  ", Style::default()),
                    Span::styled(format!("s2://{}/{}", basin, stream), Style::default().fg(GREEN)),
                ]),
                Line::from(""),
                Line::from(Span::styled("  Delete all records before the trim point.", Style::default().fg(TEXT_MUTED))),
                Line::from(Span::styled("  This is eventually consistent.", Style::default().fg(TEXT_MUTED))),
                Line::from(""),
            ];

            // Row 0: Trim point
            let trim_editing = *editing && *selected == 0;
            lines.push(Line::from(vec![
                Span::styled(marker(*selected == 0), Style::default().fg(GREEN)),
                Span::styled("Trim Point    ", Style::default().fg(if *selected == 0 { TEXT_PRIMARY } else { TEXT_MUTED })),
                Span::styled(
                    if trim_point.is_empty() && !trim_editing {
                        "(seq num)".to_string()
                    } else {
                        format!("{}{}", trim_point, cursor(trim_editing))
                    },
                    Style::default().fg(if trim_editing { GREEN } else if trim_point.is_empty() { WARNING } else { TEXT_SECONDARY })
                ),
            ]));

            // Row 1: Fencing token
            let fence_editing = *editing && *selected == 1;
            lines.push(Line::from(vec![
                Span::styled(marker(*selected == 1), Style::default().fg(GREEN)),
                Span::styled("Fencing Token ", Style::default().fg(if *selected == 1 { TEXT_PRIMARY } else { TEXT_MUTED })),
                Span::styled(
                    if fencing_token.is_empty() && !fence_editing {
                        "(none)".to_string()
                    } else {
                        format!("{}{}", fencing_token, cursor(fence_editing))
                    },
                    Style::default().fg(if fence_editing { GREEN } else if fencing_token.is_empty() { TEXT_MUTED } else { TEXT_SECONDARY })
                ),
            ]));

            lines.push(Line::from(""));

            // Row 2: Submit button
            let can_submit = !trim_point.is_empty() && trim_point.parse::<u64>().is_ok();
            let (btn_fg, btn_bg) = if *selected == 2 && can_submit {
                (BG_DARK, WARNING)
            } else {
                (if can_submit { WARNING } else { TEXT_MUTED }, BG_PANEL)
            };
            lines.push(Line::from(vec![
                Span::styled(marker(*selected == 2), Style::default().fg(GREEN)),
                Span::styled(" ▶ TRIM ", Style::default().fg(btn_fg).bg(btn_bg).bold()),
            ]));

            (
                " Trim Stream ",
                lines,
                "↑↓ nav  ⏎ edit/submit  esc",
            )
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
            let cursor = |is_editing: bool| if is_editing { "▎" } else { "" };
            let marker = |sel: bool| if sel { "▸ " } else { "  " };
            let checkbox = |checked: bool| if checked { "[x]" } else { "[ ]" };

            let mut lines = vec![];

            // Row 0: Token ID
            let id_editing = *editing && *selected == 0;
            lines.push(Line::from(vec![
                Span::styled(marker(*selected == 0), Style::default().fg(GREEN)),
                Span::styled("Token ID        ", Style::default().fg(if *selected == 0 { TEXT_PRIMARY } else { TEXT_MUTED })),
                Span::styled(
                    if id.is_empty() && !id_editing {
                        "(required)".to_string()
                    } else {
                        format!("{}{}", id, cursor(id_editing))
                    },
                    Style::default().fg(if id_editing { GREEN } else if id.is_empty() { WARNING } else { TEXT_SECONDARY })
                ),
            ]));

            // Row 1: Expiration (cycle)
            lines.push(Line::from(vec![
                Span::styled(marker(*selected == 1), Style::default().fg(GREEN)),
                Span::styled("Expiration      ", Style::default().fg(if *selected == 1 { TEXT_PRIMARY } else { TEXT_MUTED })),
                Span::styled(format!("< {} >", expiry.as_str()), Style::default().fg(TEXT_SECONDARY)),
            ]));

            // Row 2: Custom expiration (only if Custom selected)
            if *expiry == ExpiryOption::Custom {
                let custom_editing = *editing && *selected == 2;
                lines.push(Line::from(vec![
                    Span::styled(marker(*selected == 2), Style::default().fg(GREEN)),
                    Span::styled("  Custom        ", Style::default().fg(if *selected == 2 { TEXT_PRIMARY } else { TEXT_MUTED })),
                    Span::styled(
                        if expiry_custom.is_empty() && !custom_editing {
                            "(e.g., 30d, 1w)".to_string()
                        } else {
                            format!("{}{}", expiry_custom, cursor(custom_editing))
                        },
                        Style::default().fg(if custom_editing { GREEN } else { TEXT_SECONDARY })
                    ),
                ]));
            }

            // Resources section header
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled("── Resources ──", Style::default().fg(TEXT_MUTED))));

            // Row 3: Basins scope
            lines.push(Line::from(vec![
                Span::styled(marker(*selected == 3), Style::default().fg(GREEN)),
                Span::styled("Basins          ", Style::default().fg(if *selected == 3 { TEXT_PRIMARY } else { TEXT_MUTED })),
                Span::styled(format!("< {} >", basins_scope.as_str()), Style::default().fg(TEXT_SECONDARY)),
            ]));

            // Row 4: Basins value (only if Prefix/Exact)
            if matches!(basins_scope, ScopeOption::Prefix | ScopeOption::Exact) {
                let basins_editing = *editing && *selected == 4;
                lines.push(Line::from(vec![
                    Span::styled(marker(*selected == 4), Style::default().fg(GREEN)),
                    Span::styled("  Pattern       ", Style::default().fg(if *selected == 4 { TEXT_PRIMARY } else { TEXT_MUTED })),
                    Span::styled(
                        if basins_value.is_empty() && !basins_editing {
                            "(enter pattern)".to_string()
                        } else {
                            format!("{}{}", basins_value, cursor(basins_editing))
                        },
                        Style::default().fg(if basins_editing { GREEN } else { TEXT_SECONDARY })
                    ),
                ]));
            }

            // Row 5: Streams scope
            lines.push(Line::from(vec![
                Span::styled(marker(*selected == 5), Style::default().fg(GREEN)),
                Span::styled("Streams         ", Style::default().fg(if *selected == 5 { TEXT_PRIMARY } else { TEXT_MUTED })),
                Span::styled(format!("< {} >", streams_scope.as_str()), Style::default().fg(TEXT_SECONDARY)),
            ]));

            // Row 6: Streams value (only if Prefix/Exact)
            if matches!(streams_scope, ScopeOption::Prefix | ScopeOption::Exact) {
                let streams_editing = *editing && *selected == 6;
                lines.push(Line::from(vec![
                    Span::styled(marker(*selected == 6), Style::default().fg(GREEN)),
                    Span::styled("  Pattern       ", Style::default().fg(if *selected == 6 { TEXT_PRIMARY } else { TEXT_MUTED })),
                    Span::styled(
                        if streams_value.is_empty() && !streams_editing {
                            "(enter pattern)".to_string()
                        } else {
                            format!("{}{}", streams_value, cursor(streams_editing))
                        },
                        Style::default().fg(if streams_editing { GREEN } else { TEXT_SECONDARY })
                    ),
                ]));
            }

            // Row 7: Access Tokens scope
            lines.push(Line::from(vec![
                Span::styled(marker(*selected == 7), Style::default().fg(GREEN)),
                Span::styled("Access Tokens   ", Style::default().fg(if *selected == 7 { TEXT_PRIMARY } else { TEXT_MUTED })),
                Span::styled(format!("< {} >", tokens_scope.as_str()), Style::default().fg(TEXT_SECONDARY)),
            ]));

            // Row 8: Tokens value (only if Prefix/Exact)
            if matches!(tokens_scope, ScopeOption::Prefix | ScopeOption::Exact) {
                let tokens_editing = *editing && *selected == 8;
                lines.push(Line::from(vec![
                    Span::styled(marker(*selected == 8), Style::default().fg(GREEN)),
                    Span::styled("  Pattern       ", Style::default().fg(if *selected == 8 { TEXT_PRIMARY } else { TEXT_MUTED })),
                    Span::styled(
                        if tokens_value.is_empty() && !tokens_editing {
                            "(enter pattern)".to_string()
                        } else {
                            format!("{}{}", tokens_value, cursor(tokens_editing))
                        },
                        Style::default().fg(if tokens_editing { GREEN } else { TEXT_SECONDARY })
                    ),
                ]));
            }

            // Operations section header
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled("── Operations ──", Style::default().fg(TEXT_MUTED))));

            // Row 9-10: Account operations
            lines.push(Line::from(vec![
                Span::styled(marker(*selected == 9), Style::default().fg(GREEN)),
                Span::styled(format!("{} ", checkbox(*account_read)), Style::default().fg(if *account_read { GREEN } else { TEXT_MUTED })),
                Span::styled("Account Read   ", Style::default().fg(if *selected == 9 { TEXT_PRIMARY } else { TEXT_MUTED })),
                Span::styled(marker(*selected == 10), Style::default().fg(GREEN)),
                Span::styled(format!("{} ", checkbox(*account_write)), Style::default().fg(if *account_write { GREEN } else { TEXT_MUTED })),
                Span::styled("Write", Style::default().fg(if *selected == 10 { TEXT_PRIMARY } else { TEXT_MUTED })),
            ]));

            // Row 11-12: Basin operations
            lines.push(Line::from(vec![
                Span::styled(marker(*selected == 11), Style::default().fg(GREEN)),
                Span::styled(format!("{} ", checkbox(*basin_read)), Style::default().fg(if *basin_read { GREEN } else { TEXT_MUTED })),
                Span::styled("Basin Read     ", Style::default().fg(if *selected == 11 { TEXT_PRIMARY } else { TEXT_MUTED })),
                Span::styled(marker(*selected == 12), Style::default().fg(GREEN)),
                Span::styled(format!("{} ", checkbox(*basin_write)), Style::default().fg(if *basin_write { GREEN } else { TEXT_MUTED })),
                Span::styled("Write", Style::default().fg(if *selected == 12 { TEXT_PRIMARY } else { TEXT_MUTED })),
            ]));

            // Row 13-14: Stream operations
            lines.push(Line::from(vec![
                Span::styled(marker(*selected == 13), Style::default().fg(GREEN)),
                Span::styled(format!("{} ", checkbox(*stream_read)), Style::default().fg(if *stream_read { GREEN } else { TEXT_MUTED })),
                Span::styled("Stream Read    ", Style::default().fg(if *selected == 13 { TEXT_PRIMARY } else { TEXT_MUTED })),
                Span::styled(marker(*selected == 14), Style::default().fg(GREEN)),
                Span::styled(format!("{} ", checkbox(*stream_write)), Style::default().fg(if *stream_write { GREEN } else { TEXT_MUTED })),
                Span::styled("Write", Style::default().fg(if *selected == 14 { TEXT_PRIMARY } else { TEXT_MUTED })),
            ]));

            // Options section
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled("── Options ──", Style::default().fg(TEXT_MUTED))));

            // Row 15: Auto-prefix streams
            lines.push(Line::from(vec![
                Span::styled(marker(*selected == 15), Style::default().fg(GREEN)),
                Span::styled(format!("{} ", checkbox(*auto_prefix_streams)), Style::default().fg(if *auto_prefix_streams { GREEN } else { TEXT_MUTED })),
                Span::styled("Auto-prefix streams", Style::default().fg(if *selected == 15 { TEXT_PRIMARY } else { TEXT_MUTED })),
            ]));

            lines.push(Line::from(""));

            // Row 16: Submit button
            let can_submit = !id.is_empty();
            let (btn_fg, btn_bg) = if *selected == 16 && can_submit {
                (BG_DARK, SUCCESS)
            } else {
                (if can_submit { SUCCESS } else { TEXT_MUTED }, BG_PANEL)
            };
            lines.push(Line::from(vec![
                Span::styled(marker(*selected == 16), Style::default().fg(GREEN)),
                Span::styled(" ▶ ISSUE TOKEN ", Style::default().fg(btn_fg).bg(btn_bg).bold()),
            ]));

            (
                " Issue Access Token ",
                lines,
                "↑↓ nav  ←→ cycle  space toggle  ⏎ edit/submit  esc",
            )
        }

        InputMode::ConfirmRevokeToken { token_id } => (
            " Revoke Access Token ",
            vec![
                Line::from(""),
                Line::from(vec![
                    Span::styled("Revoke token ", Style::default().fg(TEXT_SECONDARY)),
                    Span::styled(token_id, Style::default().fg(ERROR).bold()),
                    Span::styled("?", Style::default().fg(TEXT_SECONDARY)),
                ]),
                Line::from(""),
                Line::from(vec![
                    Span::styled("The token will be immediately invalidated.", Style::default().fg(TEXT_MUTED)),
                ]),
                Line::from(vec![
                    Span::styled("This action cannot be undone.", Style::default().fg(ERROR)),
                ]),
            ],
            "y confirm  n/esc cancel",
        ),

        InputMode::ShowIssuedToken { token } => (
            " Access Token Issued ",
            vec![
                Line::from(""),
                Line::from(Span::styled("Copy this token now - it won't be shown again!", Style::default().fg(WARNING).bold())),
                Line::from(""),
                Line::from(Span::styled(token, Style::default().fg(GREEN))),
                Line::from(""),
            ],
            "press any key to dismiss",
        ),

        InputMode::ViewTokenDetail { token } => {
            let mut lines = vec![
                Line::from(""),
                Line::from(vec![
                    Span::styled("Token ID:      ", Style::default().fg(TEXT_MUTED)),
                    Span::styled(token.id.to_string(), Style::default().fg(TEXT_PRIMARY).bold()),
                ]),
                Line::from(vec![
                    Span::styled("Expires At:    ", Style::default().fg(TEXT_MUTED)),
                    Span::styled(token.expires_at.to_string(), Style::default().fg(TEXT_PRIMARY)),
                ]),
                Line::from(vec![
                    Span::styled("Auto-prefix:   ", Style::default().fg(TEXT_MUTED)),
                    Span::styled(
                        if token.auto_prefix_streams { "Yes" } else { "No" },
                        Style::default().fg(if token.auto_prefix_streams { GREEN } else { TEXT_MUTED }),
                    ),
                ]),
                Line::from(""),
                Line::from(Span::styled("─── Resource Scope ───", Style::default().fg(BORDER))),
            ];

            // Basins scope
            let basins_str = format_basin_matcher(&token.scope.basins);
            lines.push(Line::from(vec![
                Span::styled("Basins:        ", Style::default().fg(TEXT_MUTED)),
                Span::styled(basins_str, Style::default().fg(TEXT_PRIMARY)),
            ]));

            // Streams scope
            let streams_str = format_stream_matcher(&token.scope.streams);
            lines.push(Line::from(vec![
                Span::styled("Streams:       ", Style::default().fg(TEXT_MUTED)),
                Span::styled(streams_str, Style::default().fg(TEXT_PRIMARY)),
            ]));

            // Access tokens scope
            let tokens_str = format_token_matcher(&token.scope.access_tokens);
            lines.push(Line::from(vec![
                Span::styled("Tokens:        ", Style::default().fg(TEXT_MUTED)),
                Span::styled(tokens_str, Style::default().fg(TEXT_PRIMARY)),
            ]));

            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled("─── Operations ───", Style::default().fg(BORDER))));

            // Group operations by category
            let ops = &token.scope.ops;

            // Account operations
            let account_ops: Vec<_> = ops.iter()
                .filter(|o| is_account_op(o))
                .map(format_operation)
                .collect();
            if !account_ops.is_empty() {
                lines.push(Line::from(vec![
                    Span::styled("Account:       ", Style::default().fg(TEXT_MUTED)),
                    Span::styled(account_ops.join(", "), Style::default().fg(TEXT_PRIMARY)),
                ]));
            }

            // Basin operations
            let basin_ops: Vec<_> = ops.iter()
                .filter(|o| is_basin_op(o))
                .map(format_operation)
                .collect();
            if !basin_ops.is_empty() {
                lines.push(Line::from(vec![
                    Span::styled("Basin:         ", Style::default().fg(TEXT_MUTED)),
                    Span::styled(basin_ops.join(", "), Style::default().fg(TEXT_PRIMARY)),
                ]));
            }

            // Stream operations
            let stream_ops: Vec<_> = ops.iter()
                .filter(|o| is_stream_op(o))
                .map(format_operation)
                .collect();
            if !stream_ops.is_empty() {
                lines.push(Line::from(vec![
                    Span::styled("Stream:        ", Style::default().fg(TEXT_MUTED)),
                    Span::styled(stream_ops.join(", "), Style::default().fg(TEXT_PRIMARY)),
                ]));
            }

            // Token operations
            let token_ops: Vec<_> = ops.iter()
                .filter(|o| is_token_op(o))
                .map(format_operation)
                .collect();
            if !token_ops.is_empty() {
                lines.push(Line::from(vec![
                    Span::styled("Tokens:        ", Style::default().fg(TEXT_MUTED)),
                    Span::styled(token_ops.join(", "), Style::default().fg(TEXT_PRIMARY)),
                ]));
            }

            lines.push(Line::from(""));

            (
                " Access Token Details ",
                lines,
                "esc/enter close",
            )
        },
    };

    let area = centered_rect(60, 85, f.area());

    let block = Block::default()
        .title(Line::from(Span::styled(title, Style::default().fg(TEXT_PRIMARY).bold())))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(GREEN))
        .style(Style::default().bg(BG_DARK))
        .padding(Padding::horizontal(2));

    // Split area for content and hint
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(1),
            Constraint::Length(1),
        ])
        .split(area);

    f.render_widget(Clear, area);

    let dialog = Paragraph::new(content).block(block);
    f.render_widget(dialog, chunks[0]);

    let hint_line = Line::from(Span::styled(hint, Style::default().fg(TEXT_MUTED)));
    let hint_para = Paragraph::new(hint_line).alignment(Alignment::Center);
    f.render_widget(hint_para, chunks[1]);
}
