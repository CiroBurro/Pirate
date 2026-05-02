pub mod core;

use core::peer::Peer;
use core::torrent::Torrent;
use std::path::PathBuf;
use tokio;
use tracing_subscriber::{self, EnvFilter};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_file(true)
        .with_line_number(true)
        .with_target(false)
        .init();

    let torrent = Torrent::from_file(PathBuf::from(
        "/home/ciro/Documenti/Progetti/pirate/big-buck-bunny.torrent",
    ))
    .await?;

    let peers = Peer::get_peers(torrent).await?;

    Ok(())
}
