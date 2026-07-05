pub mod client;
pub mod core;

use client::{Client, ClientEvent, Config};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Notify;
use tracing_subscriber::{self, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_file(true)
        .with_line_number(true)
        .with_target(false)
        .init();

    let torrent_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("big-buck-bunny.torrent"));

    if !torrent_path.exists() {
        anyhow::bail!(
            "Torrent file not found: {}. Pass the path as an argument.",
            torrent_path.display()
        );
    }

    let mut client = Client::new(Config::default());
    let mut event_rx = client.events();

    let shutdown = Arc::new(Notify::new());
    let shutdown_listener = shutdown.clone();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                event = event_rx.recv() => {
                    match event {
                        Ok(ClientEvent::TorrentAdded { id, name }) =>
                            println!("[+] Torrent added: [{id}] {name}"),
                        Ok(ClientEvent::TorrentRemoved(id)) =>
                            println!("[-] Torrent removed: [{id}]"),
                        Ok(ClientEvent::StateChanged { id, state }) =>
                            println!("[~] [{id}] state -> {state:?}"),
                        Ok(ClientEvent::PieceCompleted { id, index }) =>
                            tracing::debug!("[{id}] piece {index} completed"),
                        Ok(ClientEvent::TorrentCompleted(id)) =>
                            println!("[✓] [{id}] download completed!"),
                        Ok(ClientEvent::Error { id, message }) =>
                            eprintln!("[!] [{id}] error: {message}"),
                        Ok(ClientEvent::ClientError(msg)) =>
                            eprintln!("[!] client error: {msg}"),
                        Ok(_) => {}
                        Err(_) => break,
                    }
                }
                _ = shutdown_listener.notified() => break,
            }
        }
    });

    let id = client.add_torrent(torrent_path).await?;
    println!("[+] Added torrent with id {id}");

    let mut tick = tokio::time::interval(std::time::Duration::from_secs(2));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = tick.tick() => {
                if let Some(info) = client.torrent_info(id).await {
                    println!(
                        "[{id}] {:>5.1}%  |  downloaded: {} MB  |  speed: {:.1} KB/s  |  state: {:?}",
                        info.progress,
                        info.downloaded / 1_000_000,
                        info.download_rate / 1000.0,
                        info.state,
                    );

                    if matches!(info.state, core::torrent::TorrentState::Completed) {
                        println!("[✓] Download completed!");
                        break;
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                println!("\nShutting down...");
                client.shutdown().await;
                shutdown.notify_waiters();
                break;
            }
        }
    }

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    println!("Bye!");
    Ok(())
}
