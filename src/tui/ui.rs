use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Padding, Paragraph},
};

use crate::types::{StorageClass, TimestampingMode};

use super::app::{AccessTokensState, App, AgoUnit, AppendViewState, BasinsState, ExpiryOption, InputMode, MessageLevel, ReadStartFrom, ReadViewState, RetentionPolicyOption, ScopeOption, Screen, StreamDetailState, StreamsState, Tab};

// S2 Console dark theme
const GREEN: Color = Color::Rgb(34, 197, 94);            // Active green
const GREEN_DIM: Color = Color::Rgb(22, 163, 74);        // Dimmer green
const YELLOW: Color = Color::Rgb(250, 204, 21);          // Warning yellow
const RED: Color = Color::Rgb(239, 68, 68);              // Error red
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

    // Check if we should show tabs (only on top-level screens)
    let show_tabs = matches!(app.screen, Screen::Basins(_) | Screen::AccessTokens(_));

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
        Screen::Basins(state) => draw_basins(f, chunks[1], state),
        Screen::Streams(state) => draw_streams(f, chunks[1], state),
        Screen::StreamDetail(state) => draw_stream_detail(f, chunks[1], state),
        Screen::ReadView(state) => draw_read_view(f, chunks[1], state),
        Screen::AppendView(state) => draw_append_view(f, chunks[1], state),
        Screen::AccessTokens(state) => draw_access_tokens(f, chunks[1], state),
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

    let line = Line::from(vec![
        Span::styled("Basins", basins_style),
        Span::styled("  │  ", Style::default().fg(BORDER)),
        Span::styled("Access Tokens", tokens_style),
        Span::styled("  (Tab to switch)", Style::default().fg(TEXT_MUTED)),
    ]);

    let paragraph = Paragraph::new(line);
    f.render_widget(paragraph, area);
}

fn draw_access_tokens(f: &mut Frame, area: Rect, state: &AccessTokensState) {
    // Layout: Search bar, Header, Table rows
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Search bar
            Constraint::Length(2), // Header
            Constraint::Min(1),    // Table rows
        ])
        .split(area);

    // === Search Bar ===
    let search_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(if state.filter_active { GREEN } else { BORDER }))
        .style(Style::default().bg(BG_PANEL));

    let search_text = if state.filter_active {
        Line::from(vec![
            Span::styled("/ ", Style::default().fg(GREEN)),
            Span::styled(&state.filter, Style::default().fg(TEXT_PRIMARY)),
            Span::styled("█", Style::default().fg(GREEN)), // Cursor
        ])
    } else if !state.filter.is_empty() {
        Line::from(vec![
            Span::styled("Filter: ", Style::default().fg(TEXT_MUTED)),
            Span::styled(&state.filter, Style::default().fg(TEXT_PRIMARY)),
        ])
    } else {
        Line::from(Span::styled(
            "Press / to search access tokens...",
            Style::default().fg(TEXT_MUTED),
        ))
    };

    let search_para = Paragraph::new(search_text)
        .block(search_block)
        .style(Style::default().bg(BG_PANEL));
    f.render_widget(search_para, chunks[0]);

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
    f.render_widget(header_para, chunks[1]);

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
        f.render_widget(loading, chunks[2]);
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
        f.render_widget(empty, chunks[2]);
    } else {
        let list_height = chunks[2].height as usize;
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
        f.render_widget(list, chunks[2]);
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

fn draw_basins(f: &mut Frame, area: Rect, state: &BasinsState) {
    // Layout: Search bar, Header, Table rows
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Search bar
            Constraint::Length(2), // Header
            Constraint::Min(1),    // Table rows
        ])
        .split(area);

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
    f.render_widget(search, chunks[0]);

    // === Table Header ===
    let header_area = chunks[1];
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
    let table_area = chunks[2];

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
            Constraint::Length(2), // Title bar with basin name
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
            format!(" ({}/{} streams)", filtered_count, state.streams.len())
        } else {
            format!(" ({} streams)", state.streams.len())
        }
    };

    let basin_name_str = state.basin_name.to_string();
    let title = Line::from(vec![
        Span::styled(" ← ", Style::default().fg(TEXT_MUTED)),
        Span::styled(&basin_name_str, Style::default().fg(GREEN).bold()),
        Span::styled(count_text, Style::default().fg(TEXT_MUTED)),
    ]);
    f.render_widget(Paragraph::new(title), chunks[0]);

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
            Constraint::Length(3),  // Header with URI
            Constraint::Length(7),  // Stats cards
            Constraint::Min(8),     // Actions
        ])
        .split(area);

    // === Header ===
    let uri = format!("s2://{}/{}", state.basin_name, state.stream_name);
    let header = Paragraph::new(Line::from(vec![
        Span::styled("  ", Style::default()),
        Span::styled(&uri, Style::default().fg(GREEN).bold()),
    ]))
    .block(Block::default()
        .borders(Borders::BOTTOM)
        .border_style(Style::default().fg(BORDER)));
    f.render_widget(header, chunks[0]);

    // === Stats Row ===
    let stats_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Ratio(1, 4),
            Constraint::Ratio(1, 4),
            Constraint::Ratio(1, 4),
            Constraint::Ratio(1, 4),
        ])
        .split(chunks[1]);

    // Stat card helper function
    fn render_stat_card(f: &mut Frame, area: Rect, label: &str, value: &str, sub: Option<&str>) {
        let mut lines = vec![
            Line::from(Span::styled(label, Style::default().fg(TEXT_MUTED))),
            Line::from(Span::styled(value, Style::default().fg(TEXT_PRIMARY).bold())),
        ];
        if let Some(s) = sub {
            lines.push(Line::from(Span::styled(s, Style::default().fg(TEXT_MUTED))));
        }
        let widget = Paragraph::new(lines)
            .block(Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(BORDER))
                .padding(Padding::horizontal(1)))
            .alignment(Alignment::Center);
        f.render_widget(widget, area);
    }

    // Tail Position
    let (tail_val, tail_sub): (String, Option<&str>) = if let Some(pos) = &state.tail_position {
        (format!("{}", pos.seq_num), Some("records"))
    } else if state.loading {
        ("...".to_string(), None)
    } else {
        ("--".to_string(), None)
    };
    render_stat_card(f, stats_chunks[0], "Tail Position", &tail_val, tail_sub);

    // Last Timestamp
    let ts_val = if let Some(pos) = &state.tail_position {
        if pos.timestamp > 0 {
            // Format as relative time if recent
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let age_secs = now_ms.saturating_sub(pos.timestamp) / 1000;
            if age_secs < 60 {
                format!("{}s ago", age_secs)
            } else if age_secs < 3600 {
                format!("{}m ago", age_secs / 60)
            } else if age_secs < 86400 {
                format!("{}h ago", age_secs / 3600)
            } else {
                format!("{}d ago", age_secs / 86400)
            }
        } else {
            "never".to_string()
        }
    } else {
        "--".to_string()
    };
    render_stat_card(f, stats_chunks[1], "Last Write", &ts_val, None);

    // Storage Class
    let storage_val = if let Some(config) = &state.config {
        config.storage_class
            .as_ref()
            .map(|s| format!("{:?}", s).to_lowercase())
            .unwrap_or_else(|| "default".to_string())
    } else {
        "--".to_string()
    };
    render_stat_card(f, stats_chunks[2], "Storage", &storage_val, None);

    // Retention
    let retention_val = if let Some(config) = &state.config {
        config.retention_policy
            .as_ref()
            .map(|p| match p {
                crate::types::RetentionPolicy::Age(dur) => {
                    let secs = dur.as_secs();
                    if secs >= 86400 {
                        format!("{}d", secs / 86400)
                    } else if secs >= 3600 {
                        format!("{}h", secs / 3600)
                    } else {
                        format!("{}s", secs)
                    }
                }
                crate::types::RetentionPolicy::Infinite => "infinite".to_string(),
            })
            .unwrap_or_else(|| "infinite".to_string())
    } else {
        "--".to_string()
    };
    render_stat_card(f, stats_chunks[3], "Retention", &retention_val, None);

    // === Actions ===
    let actions_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(BORDER))
        .padding(Padding::new(2, 2, 1, 1));

    let actions = vec![
        ("t", "Tail", "Live follow from current position - see new records as they arrive"),
        ("r", "Read", "Configure start position, limits, and time range"),
        ("a", "Append", "Write records to this stream"),
        ("f", "Fence", "Set a fencing token to block other writers"),
        ("m", "Trim", "Delete records before a sequence number"),
    ];

    let mut action_lines = vec![];

    for (i, (key, title, desc)) in actions.iter().enumerate() {
        let is_selected = i == state.selected_action;

        if is_selected {
            action_lines.push(Line::from(vec![
                Span::styled("> ", Style::default().fg(GREEN)),
                Span::styled(format!("[{}] ", key), Style::default().fg(GREEN).bold()),
                Span::styled(*title, Style::default().fg(TEXT_PRIMARY).bold()),
            ]));
            action_lines.push(Line::from(vec![
                Span::styled("     ", Style::default()),
                Span::styled(*desc, Style::default().fg(TEXT_SECONDARY)),
            ]));
        } else {
            action_lines.push(Line::from(vec![
                Span::styled("  ", Style::default()),
                Span::styled(format!("[{}] ", key), Style::default().fg(GREEN_DIM)),
                Span::styled(*title, Style::default().fg(TEXT_MUTED)),
            ]));
        }
        action_lines.push(Line::from(""));
    }

    let actions_paragraph = Paragraph::new(action_lines).block(actions_block);
    f.render_widget(actions_paragraph, chunks[2]);
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

    let uri = format!("s2://{}/{}", state.basin_name, state.stream_name);

    // Build title spans
    let mut title_spans = vec![
        Span::styled(" ", Style::default()),
        Span::styled(mode_text, Style::default().fg(mode_color).bold()),
        Span::styled("  ", Style::default()),
        Span::styled(&uri, Style::default().fg(TEXT_SECONDARY)),
        Span::styled(
            format!("  {} records ", state.records.len()),
            Style::default().fg(TEXT_MUTED),
        ),
    ];

    // Add output file indicator if writing to file
    if let Some(ref output) = state.output_file {
        title_spans.push(Span::styled(" → ", Style::default().fg(TEXT_MUTED)));
        title_spans.push(Span::styled(output, Style::default().fg(YELLOW)));
    }

    // Main container with title
    let outer_block = Block::default()
        .title(Line::from(title_spans))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(if state.is_tailing && !state.paused { GREEN } else { BORDER }));

    let inner_area = outer_block.inner(area);
    f.render_widget(outer_block, area);

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
    let uri = format!("s2://{}/{}", state.basin_name, state.stream_name);

    // Split into form (left) and history (right)
    let main_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(50),  // Form
            Constraint::Percentage(50),  // History
        ])
        .split(area);

    // === Form pane ===
    let form_block = Block::default()
        .title(Line::from(vec![
            Span::styled(" APPEND ", Style::default().fg(GREEN).bold()),
            Span::styled(" ", Style::default()),
            Span::styled(&uri, Style::default().fg(TEXT_SECONDARY)),
        ]))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(GREEN))
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
    let hints = match &app.screen {
        Screen::Splash => "", // Never shown
        Screen::Basins(_) => "/ filter | jk nav | ret open | c new | e cfg | d del | r ref | ? | q",
        Screen::Streams(_) => "/ filter | jk nav | ret open | c new | e cfg | d del | r ref | esc",
        Screen::StreamDetail(_) => "jk | ret | t tail | r read | a append | f fence | m trim | e cfg | esc",
        Screen::ReadView(s) => {
            if s.show_detail {
                "esc/⏎ close"
            } else if s.is_tailing {
                "jk nav | h headers | ⇥ list | space pause | gG top/bot | esc"
            } else {
                "jk nav | h headers | ⇥ list | gG top/bot | esc"
            }
        }
        Screen::AppendView(s) => {
            if s.editing {
                if s.selected == 1 {
                    "type | ⇥ key/val | ⏎ add | esc done"
                } else {
                    "type | ⏎ done | esc cancel"
                }
            } else {
                "jk nav | ⏎ edit/send | d del header | esc back"
            }
        }
        Screen::AccessTokens(_) => "/ filter | jk nav | c issue | d revoke | r ref | ⇥ switch | ? | q",
    };

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

    let line = if let Some(msg) = message_span {
        Line::from(vec![
            msg,
            Span::styled("  ", Style::default()),
            Span::styled(hints, Style::default().fg(TEXT_MUTED)),
        ])
    } else {
        Line::from(Span::styled(hints, Style::default().fg(TEXT_MUTED)))
    };

    let status = Paragraph::new(line);
    f.render_widget(status, area);
}

fn draw_help_overlay(f: &mut Frame, screen: &Screen) {
    let area = centered_rect(50, 50, f.area());

    let help_text = match screen {
        Screen::Splash => vec![], // Never shown
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

        InputMode::CreateBasin { input } => (
            " Create Basin ",
            vec![
                Line::from(""),
                Line::from(vec![
                    Span::styled("Name: ", Style::default().fg(TEXT_MUTED)),
                    Span::styled(input, Style::default().fg(TEXT_PRIMARY)),
                    Span::styled("_", Style::default().fg(GREEN)),
                ]),
                Line::from(""),
                Line::from(vec![
                    Span::styled("8-48 chars: lowercase, numbers, hyphens", Style::default().fg(TEXT_MUTED)),
                ]),
            ],
            "enter confirm  esc cancel",
        ),

        InputMode::CreateStream { basin, input } => (
            " Create Stream ",
            vec![
                Line::from(""),
                Line::from(vec![
                    Span::styled("Basin: ", Style::default().fg(TEXT_MUTED)),
                    Span::styled(basin.to_string(), Style::default().fg(TEXT_SECONDARY)),
                ]),
                Line::from(""),
                Line::from(vec![
                    Span::styled("Stream: ", Style::default().fg(TEXT_MUTED)),
                    Span::styled(input, Style::default().fg(TEXT_PRIMARY)),
                    Span::styled("_", Style::default().fg(GREEN)),
                ]),
            ],
            "enter confirm  esc cancel",
        ),

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
            let checkbox = |checked: bool| if checked { "[x]" } else { "[ ]" };
            let sel = |idx: usize, s: &usize| if idx == *s { ">" } else { " " };
            let sc_str = match storage_class {
                None => "default",
                Some(StorageClass::Express) => "express",
                Some(StorageClass::Standard) => "standard",
            };
            let ts_str = match timestamping_mode {
                None => "default",
                Some(TimestampingMode::ClientPrefer) => "client-prefer",
                Some(TimestampingMode::ClientRequire) => "client-require",
                Some(TimestampingMode::Arrival) => "arrival",
            };

            let mut lines = vec![
                Line::from(vec![
                    Span::styled(basin.to_string(), Style::default().fg(GREEN).bold()),
                ]),
                Line::from(""),
                Line::from(Span::styled("-- Create Streams Automatically --", Style::default().fg(TEXT_MUTED))),
                Line::from(vec![
                    Span::styled(sel(0, selected), Style::default().fg(GREEN)),
                    Span::styled(format!(" {} on append", checkbox(create_stream_on_append.unwrap_or(false))), Style::default().fg(TEXT_SECONDARY)),
                ]),
                Line::from(vec![
                    Span::styled(sel(1, selected), Style::default().fg(GREEN)),
                    Span::styled(format!(" {} on read", checkbox(create_stream_on_read.unwrap_or(false))), Style::default().fg(TEXT_SECONDARY)),
                ]),
                Line::from(""),
                Line::from(Span::styled("-- Default Stream Config --", Style::default().fg(TEXT_MUTED))),
                Line::from(vec![
                    Span::styled(sel(2, selected), Style::default().fg(GREEN)),
                    Span::styled(format!(" Storage class: < {} >", sc_str), Style::default().fg(TEXT_SECONDARY)),
                ]),
                Line::from(vec![
                    Span::styled(sel(3, selected), Style::default().fg(GREEN)),
                    Span::styled(format!(" Retention: < {} >", if *retention_policy == RetentionPolicyOption::Infinite { "infinite" } else { "age-based" }), Style::default().fg(TEXT_SECONDARY)),
                ]),
            ];

            if *retention_policy == RetentionPolicyOption::Age {
                let age_display = if *editing_age {
                    format!("{}_ secs", age_input)
                } else {
                    format!("{} secs", retention_age_secs)
                };
                lines.push(Line::from(vec![
                    Span::styled(sel(4, selected), Style::default().fg(GREEN)),
                    Span::styled(format!("   Age: {}", age_display), Style::default().fg(if *editing_age { GREEN } else { TEXT_SECONDARY })),
                ]));
            } else {
                lines.push(Line::from(vec![
                    Span::styled("    Age: (n/a)", Style::default().fg(BORDER)),
                ]));
            }

            lines.extend(vec![
                Line::from(vec![
                    Span::styled(sel(5, selected), Style::default().fg(GREEN)),
                    Span::styled(format!(" Timestamping: < {} >", ts_str), Style::default().fg(TEXT_SECONDARY)),
                ]),
                Line::from(vec![
                    Span::styled(sel(6, selected), Style::default().fg(GREEN)),
                    Span::styled(format!(" {} Allow ts > arrival", checkbox(timestamping_uncapped.unwrap_or(false))), Style::default().fg(TEXT_SECONDARY)),
                ]),
            ]);

            (
                " Reconfigure Basin ",
                lines,
                "jk nav | space/enter toggle | s save | esc cancel",
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
            selected,
            editing_age,
            age_input,
        } => {
            let checkbox = |checked: bool| if checked { "[x]" } else { "[ ]" };
            let sel = |idx: usize, s: &usize| if idx == *s { ">" } else { " " };
            let sc_str = match storage_class {
                None => "default",
                Some(StorageClass::Express) => "express",
                Some(StorageClass::Standard) => "standard",
            };
            let ts_str = match timestamping_mode {
                None => "default",
                Some(TimestampingMode::ClientPrefer) => "client-prefer",
                Some(TimestampingMode::ClientRequire) => "client-require",
                Some(TimestampingMode::Arrival) => "arrival",
            };

            let mut lines = vec![
                Line::from(vec![
                    Span::styled(format!("{}/{}", basin, stream), Style::default().fg(GREEN).bold()),
                ]),
                Line::from(""),
                Line::from(vec![
                    Span::styled(sel(0, selected), Style::default().fg(GREEN)),
                    Span::styled(format!(" Storage class: < {} >", sc_str), Style::default().fg(TEXT_SECONDARY)),
                ]),
                Line::from(vec![
                    Span::styled(sel(1, selected), Style::default().fg(GREEN)),
                    Span::styled(format!(" Retention: < {} >", if *retention_policy == RetentionPolicyOption::Infinite { "infinite" } else { "age-based" }), Style::default().fg(TEXT_SECONDARY)),
                ]),
            ];

            if *retention_policy == RetentionPolicyOption::Age {
                let age_display = if *editing_age {
                    format!("{}_ secs", age_input)
                } else {
                    format!("{} secs", retention_age_secs)
                };
                lines.push(Line::from(vec![
                    Span::styled(sel(2, selected), Style::default().fg(GREEN)),
                    Span::styled(format!("   Age: {}", age_display), Style::default().fg(if *editing_age { GREEN } else { TEXT_SECONDARY })),
                ]));
            } else {
                lines.push(Line::from(vec![
                    Span::styled("    Age: (n/a)", Style::default().fg(BORDER)),
                ]));
            }

            lines.extend(vec![
                Line::from(vec![
                    Span::styled(sel(3, selected), Style::default().fg(GREEN)),
                    Span::styled(format!(" Timestamping: < {} >", ts_str), Style::default().fg(TEXT_SECONDARY)),
                ]),
                Line::from(vec![
                    Span::styled(sel(4, selected), Style::default().fg(GREEN)),
                    Span::styled(format!(" {} Allow ts > arrival", checkbox(timestamping_uncapped.unwrap_or(false))), Style::default().fg(TEXT_SECONDARY)),
                ]),
            ]);

            (
                " Reconfigure Stream ",
                lines,
                "jk nav | space/enter toggle | s save | esc cancel",
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
            let check = |on: bool| if on { "✓" } else { " " };

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
    };

    let area = centered_rect(55, 85, f.area());

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
