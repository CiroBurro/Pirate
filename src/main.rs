pub mod client;
pub mod core;

use core::peer::Peer;
use core::torrent_file::TorrentFile;
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

    let torrent_path = PathBuf::from("big-buck-bunny.torrent");
    let torrent_file = TorrentFile::from_file(torrent_path).await?;
    
    let peer_id = *b"-PR1337-012345678901";
    tracing::info!("Inizializzazione Torrent e PiecePicker in corso...");
    let mut torrent = core::torrent::Torrent::new(torrent_file, peer_id).await?;

    tracing::info!("Inizializzazione completata. Trovati {} pezzi.", torrent.piece_picker.pieces.len());
    tracing::info!("Trovati {} peer dal tracker. Inizio handshake...", torrent.peers.len());

    let mut handles = Vec::new();
    let info_hash = torrent.info_hash;
    
    for mut peer in std::mem::take(&mut torrent.peers) {
        let handle = tokio::spawn(async move {
            match tokio::time::timeout(std::time::Duration::from_secs(3), peer.handshake(info_hash, peer_id)).await {
                Ok(Ok(_)) => {
                    tracing::info!("Handshake COMPLETATO con successo verso: {}", peer.to_string());
                }
                Ok(Err(e)) => {
                    tracing::debug!("Handshake fallito verso {}: {}", peer.to_string(), e);
                }
                Err(_) => {
                    tracing::debug!("Timeout: il peer {} non ha risposto all'handshake", peer.to_string());
                }
            }
            peer
        });
        handles.push(handle);
    }

    let mut active_peers = Vec::new();
    for handle in handles {
        if let Ok(peer) = handle.await {
            active_peers.push(peer);
        }
    }
    torrent.peers = active_peers;

    tracing::info!("Tentativi completati.");

    Ok(())
}
