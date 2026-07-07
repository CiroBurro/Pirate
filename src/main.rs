pub mod client;
pub mod core;
pub mod persistence;
pub mod tui;

use crate::tui::app::App;
use client::Client;
use ratatui::crossterm;
use ratatui::crossterm::event::Event;
use std::time::Duration;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    /*
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(true)
        .init(); */

    let mut client = Client::new().await?;

    let (key_tx, key_rx) = mpsc::channel(32);
    std::thread::spawn(move || {
        loop {
            if crossterm::event::poll(Duration::from_millis(100)).unwrap_or(false)
                && let Ok(Event::Key(key)) = crossterm::event::read()
                && key_tx.blocking_send(key).is_err()
            {
                break;
            }
        }
    });

    let mut terminal = ratatui::init();
    App::default()
        .run(&mut terminal, key_rx, &mut client)
        .await?;

    ratatui::restore();
    Ok(())
}
