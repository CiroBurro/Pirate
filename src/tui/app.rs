//! TUI application — the terminal user interface built with [ratatui].
//!
//! Handles drawing three screens (torrent list, per-torrent detail, logs)
//! and dispatches keyboard input to client operations.

use crate::client::Client;
use crate::core::torrent::{TorrentId, TorrentInfo, TorrentState};
use crate::tui::screen::Screen;
use anyhow::Result;
use ratatui::{
    DefaultTerminal, Frame,
    crossterm::event::{KeyCode, KeyEvent, KeyModifiers},
    layout::{Constraint::{Length, Min, Percentage}, Layout, Rect},
    prelude::{Modifier, Stylize},
    style::{Color, Style},
    symbols::{self, Marker},
    text::Line,
    widgets::{Axis, Block, Chart, Clear, Dataset, GraphType, LineGauge, Paragraph, Row, Table, TableState},
};
use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc;

/// Main TUI application state.
pub struct App {
    screen: Screen,
    /// Snapshot of all torrent info, refreshed every 250 ms.
    torrents: Vec<TorrentInfo>,
    /// Per-torrent time-series download rate data for the chart.
    speed_data: HashMap<TorrentId, Vec<(f64, f64)>>,
    table_state: TableState,
    should_quit: bool,
    show_err_popup: bool,
    error: String,
    /// Rolling log buffer for the log screen.
    logs: VecDeque<String>,
    max_logs: usize,
}

impl Default for App {
    fn default() -> Self {
        Self {
            screen: Screen::default(),
            torrents: Vec::new(),
            speed_data: HashMap::new(),
            table_state: TableState::default(),
            should_quit: false,
            show_err_popup: false,
            error: String::new(),
            logs: VecDeque::new(),
            max_logs: 30,
        }
    }
}

impl App {
    /// Main TUI event loop.
    ///
    /// Runs a draw + input cycle at ~4 Hz (250 ms tick): on each tick the
    /// torrent list is refreshed, once per second the speed chart data is
    /// updated, and keyboard events are dispatched as they arrive.
    pub async fn run(
        &mut self,
        terminal: &mut DefaultTerminal,
        mut key_rx: mpsc::Receiver<KeyEvent>,
        client: &mut Client,
    ) -> Result<()> {
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(250));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut data_timer = tokio::time::interval(std::time::Duration::from_secs(1));

        while !self.should_quit {
            terminal.draw(|f| self.draw(f, client.get_log()))?;

            tokio::select! {
                _ = tick.tick() => self.torrents = client.all_torrents().await,
                _ = data_timer.tick() => self.update_speed_data(),
                Some(key) = key_rx.recv() => self.handle_key(key, client).await,
            }
        }

        client.shutdown().await;
        Ok(())
    }

    /// Append the latest download rate data point for each torrent's chart.
    /// The chart window is 200 seconds — older data is discarded.
    fn update_speed_data(&mut self) {
        self.torrents.iter().for_each(|i| {
            let id = i.id;
            let rate = i.download_rate / 1_000_000.0;

            if let std::collections::hash_map::Entry::Vacant(e) = self.speed_data.entry(id) {
                e.insert(vec![(0.0, rate)]);
            } else {
                let data = self.speed_data.get_mut(&id).unwrap();
                let last_instant = data.last().unwrap().0;
                if last_instant >= 200.0 {
                    data.clear();
                    data.push((0.0, rate));
                } else {
                    data.push((last_instant + 1.0, rate));
                }
            }
        })
    }

    /// Top-level draw function: dispatch to the active screen and draw
    /// the persistent footer + optional error popup.
    fn draw(&mut self, frame: &mut Frame<'_>, log: String) {
        // Append fresh log lines.
        if !log.is_empty() {
            self.logs.push_back(log);
        }
        let text = self
            .logs
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        if text.lines().count() >= self.max_logs {
            self.logs.pop_front();
        }

        let [main_area, footer] = Layout::vertical([Min(0), Length(1)])
            .spacing(1)
            .areas(frame.area());
        match self.screen {
            Screen::Main => {
                self.draw_main(frame, main_area);
            }
            Screen::Detail { selected } => {
                self.draw_detailed(frame, main_area, selected);
            }
            Screen::Log => {
                self.draw_log(frame, main_area, text);
            }
        }
        self.draw_footer(frame, footer);

        // Render error popup if active.
        if self.show_err_popup {
            let popup_block = Block::bordered().title("Error").bold().fg(Color::Red);
            let centered_area = frame.area().centered(Percentage(60), Percentage(20));
            frame.render_widget(Clear, centered_area);
            let paragraph = Paragraph::new(self.error.clone()).block(popup_block);
            frame.render_widget(paragraph, centered_area);
        }
    }

    /// Main screen: a table of all torrents (name, progress, downloaded, status).
    fn draw_main(&mut self, frame: &mut Frame<'_>, area: Rect) {
        let block = Block::bordered()
            .bold()
            .title("Torrent list")
            .fg(Color::Cyan)
            .bg(Color::Black);
        let header = Row::new(vec!["Name", "Progress", "Downloaded", "Status"]);
        let rows: Vec<Row> = self
            .torrents
            .iter()
            .map(|t| {
                Row::new(vec![
                    t.name.clone(),
                    format!("{:.1}%", t.progress),
                    format!("{:.2} MB", t.downloaded as f64 / 1_000_000.0),
                    t.state.to_string(),
                ])
            })
            .collect();
        let widths = [
            Percentage(30),
            Percentage(22),
            Percentage(22),
            Percentage(21),
        ];
        let table = Table::new(rows, widths)
            .header(header)
            .block(block)
            .row_highlight_style(Modifier::REVERSED);
        frame.render_stateful_widget(table, area, &mut self.table_state);
    }

    /// Detail screen: torrent info panel, download speed chart, and
    /// a progress gauge.
    fn draw_detailed(&self, frame: &mut Frame<'_>, area: Rect, selected: usize) {
        let info = &self.torrents[selected];
        let block = Block::bordered().bold().fg(Color::Cyan).bg(Color::Black);

        let inner_area = block.inner(area);

        frame.render_widget(block.clone().title(info.name.clone()), area);

        let [top, bottom] =
            inner_area.layout(&Layout::vertical([Percentage(70), Percentage(30)]).spacing(4));

        let [t_left, t_right] =
            top.layout(&Layout::horizontal([Percentage(40), Percentage(60)]).spacing(1));

        // Left panel: torrent metadata.
        let paragraph = Paragraph::new(info.to_string())
            .white()
            .block(block.clone().title("Torrent info"));

        // Progress gauge.
        let gauge = LineGauge::default()
            .filled_style(Style::new().white().on_cyan().bold())
            .unfilled_style(Style::new().cyan().on_black())
            .label("Progress:")
            .ratio(info.progress / 100.0)
            .filled_symbol(symbols::line::THICK_HORIZONTAL)
            .unfilled_symbol(symbols::line::THICK_HORIZONTAL);

        // Right panel: download speed line chart over the last 200 s.
        let dataset = Dataset::default()
            .marker(Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Color::Red)
            .data(self.speed_data.get(&info.id).unwrap());

        let x_axis = Axis::default()
            .title("Seconds".blue())
            .bounds([0.0, 200.0])
            .labels(["0", "100", "200"]);

        let y_axis = Axis::default()
            .title("MB".blue())
            .bounds([0.0, 15.0])
            .labels(["0", "10", "15"]);
        let chart = Chart::new(vec![dataset])
            .x_axis(x_axis)
            .y_axis(y_axis)
            .block(block.title("Download speed"));
        frame.render_widget(paragraph, t_left);
        frame.render_widget(chart, t_right);
        frame.render_widget(gauge, bottom);
    }

    /// Log screen: scrollable tracing output.
    fn draw_log(&mut self, frame: &mut Frame<'_>, area: Rect, text: String) {
        let block = Block::bordered()
            .bold()
            .title("Logs")
            .fg(Color::Cyan)
            .bg(Color::Black);

        let paragraph = Paragraph::new(text).white().block(block);

        frame.render_widget(paragraph, area);
    }

    /// Footer bar showing available keyboard shortcuts.
    fn draw_footer(&self, frame: &mut Frame<'_>, area: Rect) {
        let line = match self.screen {
            Screen::Main => Line::from(
                "↑↓ switch torrent  |  p pause  |  r resume  |  c cancel  |  ↵ details  |  tab show logs  |  q quit",
            ),
            _ => Line::from("tab return to main page"),
        }
        .cyan()
        .bg(Color::Black);

        frame.render_widget(line.centered(), area);
    }

    /// Route keyboard events: dismiss error popup or dispatch to screen handler.
    async fn handle_key(&mut self, key: KeyEvent, client: &mut Client) {
        if (key.code == KeyCode::Esc || key.code == KeyCode::Enter) && self.show_err_popup {
            self.show_err_popup = false;
            return;
        }

        match self.screen {
            Screen::Main => self.handle_key_main(key, client).await,
            _ => self.handle_key_secondary(key),
        }
    }

    /// Keyboard handler for the main torrent list screen.
    async fn handle_key_main(&mut self, key: KeyEvent, client: &mut Client) {
        let selected = self.table_state.selected();
        match key.code {
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.should_quit = true;
            }
            KeyCode::Char('q') => {
                self.should_quit = true;
            }
            KeyCode::Up | KeyCode::Char('k') => {
                let i = self.table_state.selected().unwrap_or(0);
                self.table_state.select(Some(i.saturating_sub(1)));
            }
            KeyCode::Down | KeyCode::Char('j') => {
                let i = self.table_state.selected().unwrap_or(0);
                let max = self.torrents.len().saturating_sub(1);
                self.table_state.select(Some((i + 1).min(max)));
            }
            KeyCode::Enter if selected.is_some() => {
                self.screen = Screen::Detail {
                    selected: selected.unwrap(),
                }
            }
            KeyCode::Char('p') if selected.is_some() => {
                let torrent_selected = self.torrents[selected.unwrap()].clone();
                match torrent_selected.state {
                    TorrentState::Paused | TorrentState::Stopped => {
                        self.error = String::from("The torrent selected is already paused");
                        self.show_err_popup = true;
                    }
                    TorrentState::Downloading | TorrentState::Seeding => {
                        client.pause(torrent_selected.id).await
                    }
                    _ => (),
                }
            }
            KeyCode::Char('r') if selected.is_some() => {
                let torrent_selected = self.torrents[selected.unwrap()].clone();
                match torrent_selected.state {
                    TorrentState::Downloading | TorrentState::Seeding => {
                        self.error = String::from(
                            "The torrent selected is already being downloaded or seeded",
                        );
                        self.show_err_popup = true;
                    }
                    TorrentState::Paused => client.resume(torrent_selected.id).await,
                    _ => (),
                }
            }
            KeyCode::Char('c') if selected.is_some() => {
                let torrent_selected = self.torrents[selected.unwrap()].clone();
                match client.remove(torrent_selected.id).await {
                    Ok(_) => (),
                    Err(e) => {
                        self.error = "Failed to remove torrent: ".to_owned() + &e.to_string();
                        self.show_err_popup = true;
                    }
                }
            }
            KeyCode::Tab => self.screen = Screen::Log,
            _ => (),
        }
    }

    /// Keyboard handler for detail/log screens: Esc/Tab returns to main.
    fn handle_key_secondary(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc | KeyCode::Tab => self.screen = Screen::Main,
            _ => (),
        }
    }
}
