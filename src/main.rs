pub mod client;
pub mod core;

use core::torrent_file::TorrentFile;
use std::path::PathBuf;
use tracing_subscriber::{self, EnvFilter};

use crate::core::torrent::Torrent;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_file(true)
        .with_line_number(true)
        .with_target(false)
        .init();

    let torrent_path = PathBuf::from("big-buck-bunny.torrent");
    let torrent_file = TorrentFile::from_file(torrent_path).await?;

    let peer_id = [1u8; 20];
    let torrent = Torrent::new(torrent_file, peer_id).await?;

    torrent
        .download(PathBuf::from("/home/ciro/Scaricati"))
        .await?;
    Ok(())
}
