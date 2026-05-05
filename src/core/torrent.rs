use crate::core::{peer::Peer, torrent_file::TorrentFile, tracker::get_peers};
use anyhow::{Context, Result};

pub struct Torrent {
    pub peer_id: [u8; 20],
    pub file: TorrentFile,
    pub info_hash: [u8; 20],
    pub peers: Vec<Peer>,
}

impl Torrent {
    pub async fn new(file: TorrentFile, peer_id: [u8; 20]) -> Result<Self> {
        let info_hash = &file
            .info
            .get_info_hash()
            .context("[!] Failed to calculate info hash")?;
        let mut torrent = Self {
            peer_id,
            file,
            info_hash: *info_hash,
            peers: Vec::new(),
        };

        let peers = get_peers(&torrent)
            .await
            .context("[!] Failed to get peers from tracker")?;

        torrent.peers = peers;

        Ok(torrent)
    }
}
