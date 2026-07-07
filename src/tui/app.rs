use crate::client::Client;
use crate::core::torrent::{TorrentId, TorrentInfo, TorrentState};
use crate::tui::screen::Screen;
use anyhow::Result;
use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::layout::Constraint::{Length, Min, Percentage};
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::prelude::{Modifier, Stylize};
use ratatui::style::Color;
use ratatui::text::Line;
use ratatui::widgets::{Block, Clear, Paragraph, Row, Table, TableState};
use ratatui::{DefaultTerminal, Frame};
use tokio::sync::mpsc;

#[derive(Default)]
pub struct App {
    screen: Screen,
    torrents: Vec<TorrentInfo>,
    table_state: TableState,
    should_quit: bool,
    show_err_popup: bool,
    error: String,
}

impl App {
    pub async fn run(
        &mut self,
        terminal: &mut DefaultTerminal,
        mut key_rx: mpsc::Receiver<KeyEvent>,
        client: &mut Client,
    ) -> Result<()> {
        while !self.should_quit {
            terminal.draw(|f| self.draw(f))?;

            let mut tick = tokio::time::interval(std::time::Duration::from_millis(250));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            tokio::select! {
                _ = tick.tick() => self.torrents = client.all_torrents().await,
                Some(key) = key_rx.recv() => self.handle_key(key, client).await,
            }
        }

        client.shutdown().await;
        Ok(())
    }
    fn draw(&mut self, frame: &mut Frame<'_>) {
        let [main_area, footer] = Layout::vertical([Min(0), Length(1)]).areas(frame.area());
        match self.screen {
            Screen::Main => {
                self.draw_main(frame, main_area);
            }
            Screen::Detail { id } => {
                self.draw_detailed(frame, main_area, id);
            }
            Screen::Log => {
                self.draw_log(frame, main_area);
            }
        }
        self.draw_footer(frame, footer);

        if self.show_err_popup {
            let popup_block = Block::bordered().title("Error").bold().fg(Color::Red);
            let centered_area = frame
                .area()
                .centered(Constraint::Percentage(60), Constraint::Percentage(20));
            frame.render_widget(Clear, centered_area);
            let paragraph = Paragraph::new(self.error.clone()).block(popup_block);
            frame.render_widget(paragraph, centered_area);
        }
    }

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
    fn draw_detailed(&self, _frame: &mut Frame<'_>, _area: Rect, _id: TorrentId) {}
    fn draw_log(&self, _frame: &mut Frame<'_>, _area: Rect) {}
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

    async fn handle_key(&mut self, key: KeyEvent, client: &mut Client) {
        if key.code == KeyCode::Esc && self.show_err_popup {
            self.show_err_popup = false;
        }

        match self.screen {
            Screen::Main => self.handle_key_main(key, client).await,
            _ => self.handle_key_secondary(key),
        }
    }
    async fn handle_key_main(&mut self, key: KeyEvent, client: &mut Client) {
        let torrent_selected =
            self.torrents[self.table_state.selected().unwrap_or_default()].clone();
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
            KeyCode::Enter => {
                self.screen = Screen::Detail {
                    id: torrent_selected.id,
                }
            }
            KeyCode::Char('p') => match torrent_selected.state {
                TorrentState::Paused | TorrentState::Stopped => {
                    self.error = String::from("The torrent selected is already paused");
                    self.show_err_popup = true;
                }
                TorrentState::Downloading | TorrentState::Seeding => {
                    client.pause(torrent_selected.id).await
                }
                _ => (),
            },
            KeyCode::Char('r') => match torrent_selected.state {
                TorrentState::Downloading | TorrentState::Seeding => {
                    self.error =
                        String::from("The torrent selected is already being downloaded or seeded");
                    self.show_err_popup = true;
                }
                TorrentState::Paused => client.resume(torrent_selected.id).await,
                _ => (),
            },
            KeyCode::Char('c') => match client.remove(torrent_selected.id).await {
                Ok(_) => (),
                Err(e) => {
                    self.error = "Failed to remove torrent: ".to_owned() + &e.to_string();
                    self.show_err_popup = true;
                }
            },
            KeyCode::Tab => self.screen = Screen::Log,
            _ => (),
        }
    }
    fn handle_key_secondary(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc | KeyCode::Tab => self.screen = Screen::Main,
            _ => (),
        }
    }
}
