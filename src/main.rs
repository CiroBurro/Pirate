//! Pirate — an async BitTorrent client.
//!
//! Entry point, CLI dispatch, tracing initialization, and the TUI event loop.

pub mod cli;
pub mod client;
pub mod core;
pub mod log;
pub mod persistence;
pub mod tui;

use crate::cli::{Args, ConfigCommands, MainCommands};
use crate::client::{Client, Config};
use crate::log::LogBuffer;
use crate::persistence::Persistent;
use crate::tui::app::App;
use clap::Parser;
use ratatui::crossterm::{self, event::Event};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, Layer};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize the in-memory log buffer and wire it into tracing-subscriber.
    // This lets the TUI display logs captured from info!/warn!/error! calls.
    let log_buffer = LogBuffer::default();
    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .compact()
                .with_ansi(false)
                .with_writer(log_buffer.clone())
                .with_filter(LevelFilter::INFO),
        )
        .init();

    let args = Args::parse();
    let mut config: Config = Config::load("config").await.unwrap_or_default();
    let mut client;

    // CLI subcommand dispatch.
    // If a config command is given, apply the change and persist immediately.
    // If an add command is given, create the client and add the torrent.
    // If no command is given, create the client and open the TUI.
    if let Some(arg) = args.command {
        match arg {
            MainCommands::Config { action } => {
                return match action {
                    ConfigCommands::PeerId { id } => {
                        if id.len() != 20 {
                            println!("Invalid peer id length (it must be exactly 20 characters)");
                            return Ok(());
                        }
                        config.peer_id = id.as_bytes().try_into()?;
                        config.save("config").await
                    }
                    ConfigCommands::DownloadDir { dir } => {
                        if !dir.is_dir() || !dir.exists() {
                            println!(
                                "The specified directory path does not exist: {}",
                                dir.display()
                            );
                            return Ok(());
                        }
                        config.download_dir = dir;
                        config.save("config").await
                    }
                    ConfigCommands::ListenPort { port } => {
                        if port < 1024 {
                            println!("{} port is reserved", port);
                            return Ok(());
                        }
                        config.listen_port = port;
                        config.save("config").await
                    }
                };
            }
            MainCommands::Add {
                torrent_path,
                download_dir,
            } => {
                client = Client::new(config, log_buffer).await?;
                client
                    .add_torrent(torrent_path.canonicalize()?, download_dir)
                    .await?;
            }
        }
    } else {
        client = Client::new(config, log_buffer).await?;
    }

    // Spawn a dedicated OS thread to read keyboard events via crossterm.
    // We do this in a separate thread because crossterm's event API is blocking
    // and must not run on the tokio async runtime.
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

    // Enter the ratatui TUI: draw loop + key event handling.
    let mut terminal = ratatui::init();
    App::default()
        .run(&mut terminal, key_rx, &mut client)
        .await?;

    ratatui::restore();
    Ok(())
}
