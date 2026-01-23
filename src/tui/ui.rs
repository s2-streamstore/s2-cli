use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Padding, Paragraph, Wrap},
};

use crate::types::{StorageClass, TimestampingMode};

use super::app::{App, AgoUnit, BasinsState, InputMode, MessageLevel, ReadStartFrom, ReadViewState, RetentionPolicyOption, Screen, StreamDetailState, StreamsState};

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

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([
            Constraint::Min(3),    // Main content
            Constraint::Length(1), // Status bar (slimmer)
        ])
        .split(area);

    // Draw main content based on screen
    match &app.screen {
        Screen::Basins(state) => draw_basins(f, chunks[0], state),
        Screen::Streams(state) => draw_streams(f, chunks[0], state),
        Screen::StreamDetail(state) => draw_stream_detail(f, chunks[0], state),
        Screen::ReadView(state) => draw_read_view(f, chunks[0], state),
    }

    // Draw status bar
    draw_status_bar(f, chunks[1], app);

    // Draw help overlay if visible
    if app.show_help {
        draw_help_overlay(f, &app.screen);
    }

    // Draw input dialog if in input mode
    if !matches!(app.input_mode, InputMode::Normal) {
        draw_input_dialog(f, &app.input_mode);
    }
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
        .title(Line::from(vec![
            Span::styled(" Read Stream ", Style::default().fg(TEXT_PRIMARY).bold()),
        ]))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(BORDER))
        .padding(Padding::new(2, 2, 1, 1));

    let actions = vec![
        ("t", "Tail", "Live follow from current position - see new records as they arrive"),
        ("r", "Custom Read", "Configure start position, limits, and time range"),
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

    let block = Block::default()
        .title(Line::from(vec![
            Span::styled(" ", Style::default()),
            Span::styled(mode_text, Style::default().fg(mode_color).bold()),
            Span::styled("  ", Style::default()),
            Span::styled(&uri, Style::default().fg(TEXT_SECONDARY)),
            Span::styled(
                format!("  {} records ", state.records.len()),
                Style::default().fg(TEXT_MUTED),
            ),
        ]))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(if state.is_tailing && !state.paused { GREEN } else { BORDER }))
        .padding(Padding::horizontal(1));

    if state.records.is_empty() {
        let text = if state.loading {
            Line::from(Span::styled("Waiting for records...", Style::default().fg(TEXT_MUTED)))
        } else {
            Line::from(Span::styled("No records", Style::default().fg(TEXT_MUTED)))
        };
        let para = Paragraph::new(text).block(block);
        f.render_widget(para, area);
        return;
    }

    // Calculate visible records
    let inner_height = area.height.saturating_sub(2) as usize;
    let total_records = state.records.len();
    let records_per_view = inner_height / 3;

    // Auto-scroll to bottom when tailing
    let scroll_offset = if state.is_tailing && !state.paused {
        total_records.saturating_sub(records_per_view)
    } else {
        state.scroll_offset.min(total_records.saturating_sub(1))
    };

    let lines: Vec<Line> = state
        .records
        .iter()
        .skip(scroll_offset)
        .take(records_per_view + 1)
        .flat_map(|record| {
            let body = String::from_utf8_lossy(&record.body);
            let body_preview: String = body.chars().take(200).collect();

            vec![
                Line::from(vec![
                    Span::styled(
                        format!("#{}", record.seq_num),
                        Style::default().fg(GREEN).bold(),
                    ),
                    Span::styled(
                        format!("  ts={}", record.timestamp),
                        Style::default().fg(TEXT_MUTED),
                    ),
                ]),
                Line::from(Span::styled(body_preview, Style::default().fg(TEXT_SECONDARY))),
                Line::from(""),
            ]
        })
        .collect();

    let para = Paragraph::new(lines)
        .block(block)
        .wrap(Wrap { trim: true });
    f.render_widget(para, area);
}

fn draw_status_bar(f: &mut Frame, area: Rect, app: &App) {
    let hints = match &app.screen {
        Screen::Basins(_) => "/ filter | jk nav | ret open | c new | e cfg | d del | r ref | ? | q",
        Screen::Streams(_) => "/ filter | jk nav | ret open | c new | e cfg | d del | r ref | esc",
        Screen::StreamDetail(_) => "jk nav | ret run | t tail | r custom | e cfg | esc",
        Screen::ReadView(s) => {
            if s.is_tailing {
                "space pause | jk scroll | esc"
            } else {
                "jk scroll | gG top/bot | esc"
            }
        }
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
                Span::styled("Custom read", Style::default().fg(TEXT_SECONDARY)),
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
            selected,
            editing,
        } => {
            // Radio button display
            let radio = |opt: ReadStartFrom, current: &ReadStartFrom| {
                if opt == *current { "(o)" } else { "( )" }
            };
            let checkbox = |checked: bool| if checked { "[x]" } else { "[ ]" };

            // Row selection indicator
            let sel = |idx: usize, s: &usize| if idx == *s { ">" } else { " " };

            // Input field styling - shows cursor when editing
            let input_field = |value: &str, is_editing: bool, placeholder: &str| -> String {
                if is_editing {
                    format!("[{}|]", value)
                } else if value.is_empty() {
                    format!("[{}]", placeholder)
                } else {
                    format!("[{}]", value)
                }
            };

            let unit_str = match ago_unit {
                AgoUnit::Seconds => "s",
                AgoUnit::Minutes => "m",
                AgoUnit::Hours => "h",
                AgoUnit::Days => "d",
            };

            let mut lines = vec![
                Line::from(vec![
                    Span::styled(format!("{}/{}", basin, stream), Style::default().fg(GREEN).bold()),
                ]),
                Line::from(Span::styled("Start from:", Style::default().fg(TEXT_MUTED))),
            ];

            // Row 0: From sequence number
            let is_seq = *start_from == ReadStartFrom::SeqNum;
            let seq_field = input_field(seq_num_value, *editing && *selected == 0, "0");
            lines.push(Line::from(vec![
                Span::styled(sel(0, selected), Style::default().fg(GREEN)),
                Span::styled(format!("{} ", radio(ReadStartFrom::SeqNum, start_from)),
                    Style::default().fg(if is_seq { GREEN } else { TEXT_MUTED })),
                Span::styled("seq ", Style::default().fg(if *selected == 0 { TEXT_PRIMARY } else { TEXT_SECONDARY })),
                Span::styled(seq_field, Style::default().fg(if *selected == 0 && *editing { GREEN } else if is_seq { TEXT_PRIMARY } else { TEXT_MUTED })),
            ]));

            // Row 1: From timestamp
            let is_ts = *start_from == ReadStartFrom::Timestamp;
            let ts_field = input_field(timestamp_value, *editing && *selected == 1, "0");
            lines.push(Line::from(vec![
                Span::styled(sel(1, selected), Style::default().fg(GREEN)),
                Span::styled(format!("{} ", radio(ReadStartFrom::Timestamp, start_from)),
                    Style::default().fg(if is_ts { GREEN } else { TEXT_MUTED })),
                Span::styled("ts  ", Style::default().fg(if *selected == 1 { TEXT_PRIMARY } else { TEXT_SECONDARY })),
                Span::styled(ts_field, Style::default().fg(if *selected == 1 && *editing { GREEN } else if is_ts { TEXT_PRIMARY } else { TEXT_MUTED })),
                Span::styled(" ms", Style::default().fg(TEXT_MUTED)),
            ]));

            // Row 2: From time ago
            let is_ago = *start_from == ReadStartFrom::Ago;
            let ago_field = input_field(ago_value, *editing && *selected == 2, "5");
            lines.push(Line::from(vec![
                Span::styled(sel(2, selected), Style::default().fg(GREEN)),
                Span::styled(format!("{} ", radio(ReadStartFrom::Ago, start_from)),
                    Style::default().fg(if is_ago { GREEN } else { TEXT_MUTED })),
                Span::styled("ago ", Style::default().fg(if *selected == 2 { TEXT_PRIMARY } else { TEXT_SECONDARY })),
                Span::styled(ago_field, Style::default().fg(if *selected == 2 && *editing { GREEN } else if is_ago { TEXT_PRIMARY } else { TEXT_MUTED })),
                Span::styled(format!(" <{}> tab", unit_str), Style::default().fg(if is_ago { GREEN_DIM } else { BORDER })),
            ]));

            // Row 3: From tail offset
            let is_off = *start_from == ReadStartFrom::TailOffset;
            let off_field = input_field(tail_offset_value, *editing && *selected == 3, "10");
            lines.push(Line::from(vec![
                Span::styled(sel(3, selected), Style::default().fg(GREEN)),
                Span::styled(format!("{} ", radio(ReadStartFrom::TailOffset, start_from)),
                    Style::default().fg(if is_off { GREEN } else { TEXT_MUTED })),
                Span::styled("off ", Style::default().fg(if *selected == 3 { TEXT_PRIMARY } else { TEXT_SECONDARY })),
                Span::styled(off_field, Style::default().fg(if *selected == 3 && *editing { GREEN } else if is_off { TEXT_PRIMARY } else { TEXT_MUTED })),
                Span::styled(" back", Style::default().fg(TEXT_MUTED)),
            ]));

            lines.push(Line::from(Span::styled("Limits:", Style::default().fg(TEXT_MUTED))));

            // Row 4: Max records
            let cnt_field = input_field(count_limit, *editing && *selected == 4, "-");
            lines.push(Line::from(vec![
                Span::styled(sel(4, selected), Style::default().fg(GREEN)),
                Span::styled(" count ", Style::default().fg(if *selected == 4 { TEXT_PRIMARY } else { TEXT_SECONDARY })),
                Span::styled(cnt_field, Style::default().fg(if *selected == 4 && *editing { GREEN } else { TEXT_MUTED })),
            ]));

            // Row 5: Max bytes
            let byte_field = input_field(byte_limit, *editing && *selected == 5, "-");
            lines.push(Line::from(vec![
                Span::styled(sel(5, selected), Style::default().fg(GREEN)),
                Span::styled(" bytes ", Style::default().fg(if *selected == 5 { TEXT_PRIMARY } else { TEXT_SECONDARY })),
                Span::styled(byte_field, Style::default().fg(if *selected == 5 && *editing { GREEN } else { TEXT_MUTED })),
            ]));

            // Row 6: Until timestamp
            let until_field = input_field(until_timestamp, *editing && *selected == 6, "-");
            lines.push(Line::from(vec![
                Span::styled(sel(6, selected), Style::default().fg(GREEN)),
                Span::styled(" until ", Style::default().fg(if *selected == 6 { TEXT_PRIMARY } else { TEXT_SECONDARY })),
                Span::styled(until_field, Style::default().fg(if *selected == 6 && *editing { GREEN } else { TEXT_MUTED })),
                Span::styled(" ms", Style::default().fg(TEXT_MUTED)),
            ]));

            lines.push(Line::from(Span::styled("Options:", Style::default().fg(TEXT_MUTED))));

            // Row 7: Clamp
            lines.push(Line::from(vec![
                Span::styled(sel(7, selected), Style::default().fg(GREEN)),
                Span::styled(format!(" {} clamp to tail", checkbox(*clamp)),
                    Style::default().fg(if *selected == 7 { TEXT_PRIMARY } else { TEXT_SECONDARY })),
            ]));

            // Row 8: Format
            lines.push(Line::from(vec![
                Span::styled(sel(8, selected), Style::default().fg(GREEN)),
                Span::styled(format!(" format <{}>", format.as_str()),
                    Style::default().fg(if *selected == 8 { TEXT_PRIMARY } else { TEXT_SECONDARY })),
            ]));

            lines.push(Line::from(""));

            // Row 9: Start button
            let btn_style = if *selected == 9 {
                Style::default().fg(BG_DARK).bg(GREEN).bold()
            } else {
                Style::default().fg(GREEN).bold()
            };
            lines.push(Line::from(vec![
                Span::styled(sel(9, selected), Style::default().fg(GREEN)),
                Span::styled(" ", Style::default()),
                Span::styled(" Start Reading ", btn_style),
            ]));

            (
                " Custom Read ",
                lines,
                "jk nav | spc/ret select | tab unit | esc",
            )
        }
    };

    let area = centered_rect(50, 70, f.area());

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
