pub mod cli;
pub mod client;
pub mod core;
pub mod persistence;
pub mod tui;

use crate::cli::{Args, ConfigCommands, MainCommands};
use crate::client::Config;
use crate::persistence::Persistent;
use crate::tui::app::App;
use clap::Parser;
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

    let args = Args::parse();
    let mut config: Config = Config::load("config").await.unwrap_or_default();
    let mut client;
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
                client = Client::new(config).await?;
                client.add_torrent(torrent_path, download_dir).await?;
            }
        }
    } else {
        client = Client::new(config).await?;
    }

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
