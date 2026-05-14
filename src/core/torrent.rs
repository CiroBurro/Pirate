use crate::core::{
    bitfield::BitField,
    message::MessageId,
    peer::Peer,
    piece::{Piece, PiecePicker},
    torrent_file::TorrentFile,
    tracker::get_peers,
};
use anyhow::{bail, Context, Result};
use std::sync::{Arc, Mutex};
use tracing::{debug, error, info, instrument};

pub struct Torrent {
    pub peer_id: [u8; 20],
    pub file: TorrentFile,
    pub info_hash: [u8; 20],
    pub peers: Vec<Peer>,
    pub piece_picker: Arc<Mutex<PiecePicker>>,
}

impl Torrent {
    #[instrument(skip(file, peer_id))]
    pub async fn new(file: TorrentFile, peer_id: [u8; 20]) -> Result<Self> {
        let info_hash = &file
            .info
            .get_info_hash()
            .context("[!] Failed to calculate info hash")?;

        let num_pieces: usize = file.info.pieces.len() / 20;
        let piece_length: usize = file.info.piece_length as usize;
        let total_len = file.info.total_len()?;

        let mut pieces: Vec<Piece> = Vec::with_capacity(num_pieces);

        for (i, hash) in file.info.pieces.chunks_exact(20).enumerate() {
            let hash_array: [u8; 20] = hash.try_into().context("Hash size mismatch")?;

            let is_last_piece = i == num_pieces - 1;
            let this_piece_length = if is_last_piece {
                total_len.saturating_sub(i * piece_length)
            } else {
                piece_length
            };

            let piece = Piece::new(i, hash_array, this_piece_length);
            pieces.push(piece);
        }

        let piece_picker = Arc::new(Mutex::new(PiecePicker::new(pieces)));

        let mut torrent = Self {
            peer_id,
            file,
            info_hash: *info_hash,
            peers: Vec::new(),
            piece_picker,
        };

        let peers = get_peers(&torrent)
            .await
            .context("[!] Failed to get peers from tracker")?;

        torrent.peers = peers;
        info!("Torrent created successfully");

        Ok(torrent)
    }

    #[instrument(skip(self))]
    pub async fn activate_peers(&mut self) {
        let info_hash = self.info_hash;
        let peer_id = self.peer_id;

        let mut handles = Vec::new();

        for mut peer in std::mem::take(&mut self.peers) {
            let handle = tokio::spawn(async move {
                match tokio::time::timeout(
                    std::time::Duration::from_secs(3),
                    peer.handshake(&info_hash, &peer_id),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        info!("Handshake successfully completed with {}", peer.to_string());
                    }
                    Ok(Err(e)) => {
                        debug!("[-] Failed handshake with {}: {}", peer.to_string(), e);
                    }
                    Err(_) => {
                        debug!(
                            "Timeout: the following peer {} did not respond back",
                            peer.to_string()
                        );
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
        self.peers = active_peers;
    }

    #[instrument(skip(self))]
    pub async fn download(mut self) -> Result<()> {
        self.activate_peers().await;
        if self.peers.len() == 0 {
            bail!("[!] The torrent has no active peers");
        }
        info!("Found {} active peers", self.peers.len());

        for mut peer in std::mem::take(&mut self.peers) {
            let piece_picker = Arc::clone(&self.piece_picker);
            tokio::task::spawn(async move {
                loop {
                    let msg_result = peer.read_msg().await;
                    if let Ok(msg) = msg_result {
                        match msg.id {
                            MessageId::KeepAlive => continue,
                            MessageId::BitField => {
                                peer.status.bitfield = BitField::from_payload(msg.payload);
                                if let Ok(mut picker) = piece_picker.lock() {
                                    picker.add_peer_bitfield(&peer.status.bitfield);
                                } else {
                                    error!("[!] Failed to lock the piece picker bitfield");
                                }

                                todo!("Inviare messaggio Interested");
                            }
                            MessageId::Choke => {
                                peer.status.choked = true;
                                continue;
                            }
                            MessageId::Unchoke => {
                                peer.status.choked = false;

                                todo!("Inviare messagio Request")
                            }
                            _ => todo!(),
                        }
                    } else if let Err(e) = msg_result {
                        error!("{e}")
                    }
                }
            });
        }
        Ok(())
    }
}
