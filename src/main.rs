pub mod core;

use core::peer::Peer;
use core::torrent::Torrent;
use std::path::PathBuf;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let torrent = Torrent::from_file(PathBuf::from(
        "/home/ciro/Documenti/Progetti/pirate/big-buck-bunny.torrent",
    ))
    .await?;

    let peers = Peer::get_peers(torrent).await?;

    Ok(())
}
