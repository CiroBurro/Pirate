use crate::core::{
    bitfield::BitField,
    message::{Message, MessageId},
    peer::Peer,
    piece::{Block, Piece, PiecePicker},
    torrent_file::TorrentFile,
    tracker::get_peers,
};
use anyhow::{bail, Context, Result};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tracing::{debug, error, info, instrument, warn};

pub struct Torrent {
    pub peer_id: [u8; 20],
    pub file: Arc<TorrentFile>,
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
            file: Arc::new(file),
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
                let success = match tokio::time::timeout(
                    std::time::Duration::from_secs(3),
                    peer.handshake(&info_hash, &peer_id),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        info!("Handshake successfully completed with {}", peer);
                        true
                    }
                    Ok(Err(e)) => {
                        debug!("[-] Failed handshake with {}: {}", peer, e);
                        false
                    }
                    Err(_) => {
                        debug!("Timeout: the following peer {} did not respond back", peer);
                        false
                    }
                };
                (success, peer)
            });

            handles.push(handle);
        }

        let mut active_peers = Vec::new();
        for handle in handles {
            if let Ok((success, peer)) = handle.await
                && success
                && peer.status.stream.is_some()
            {
                active_peers.push(peer);
            }
        }
        self.peers = active_peers;
    }

    #[instrument(skip(self))]
    pub async fn download(mut self, path: PathBuf) -> Result<()> {
        self.activate_peers().await;
        if self.peers.is_empty() {
            bail!("[!] The torrent has no active peers");
        }
        info!("Found {} active peers", self.peers.len());

        for peer in std::mem::take(&mut self.peers) {
            let piece_picker = Arc::clone(&self.piece_picker);
            let file = Arc::clone(&self.file);
            tokio::task::spawn(Self::peer_loop(peer, piece_picker, file, path.clone()));
        }
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let mut picker = self.piece_picker.lock().unwrap();
            if picker.missing_pieces == 0 {
                info!("[*] Download complete");
                break;
            }
            picker.tick_timeouts();
        }
        Ok(())
    }

    #[instrument(skip_all)]
    async fn peer_loop(
        mut peer: Peer,
        piece_picker: Arc<Mutex<PiecePicker>>,
        file: Arc<TorrentFile>,
        path: PathBuf,
    ) -> Result<()> {
        loop {
            let readable = if let Some(stream) = peer.status.stream.as_ref() {
                tokio::time::timeout(std::time::Duration::from_secs(5), stream.readable()).await
            } else {
                return Err(anyhow::anyhow!("No stream"));
            };

            match readable {
                Ok(Ok(_)) => {
                    let msg_result = peer.read_msg().await;
                    if let Ok(msg) = msg_result {
                        if Self::handle_msg(
                            &mut peer,
                            &piece_picker,
                            msg,
                            file.clone(),
                            path.clone(),
                        )
                        .await?
                            == 1
                        {
                            return Ok(());
                        }
                    } else if let Err(e) = msg_result {
                        error!("[!] Connection lost with {}: {}", peer, e);
                        return Err(e);
                    }
                }
                Ok(Err(e)) => {
                    error!("[!] Stream error with {}: {}", peer, e);
                    return Err(e.into());
                }
                Err(_) => {
                    if !peer.status.choked {
                        let block = piece_picker.lock().unwrap().pick(&peer.status.bitfield);
                        if let Some(b) = block {
                            let _ = peer
                                .send_msg(Message::request(b.to_payload().as_slice().try_into()?))
                                .await;
                        }
                    }
                }
            }
        }
    }

    #[instrument(skip_all)]
    pub async fn handle_msg(
        peer: &mut Peer,
        piece_picker: &Arc<Mutex<PiecePicker>>,
        msg: Message,
        file: Arc<TorrentFile>,
        path: PathBuf,
    ) -> Result<usize> {
        let finished;
        match msg.id {
            MessageId::KeepAlive => Ok(0),
            MessageId::BitField => {
                peer.status.bitfield = BitField::from_payload(msg.payload);
                if let Ok(mut picker) = piece_picker.lock() {
                    picker.add_peer_bitfield(&peer.status.bitfield);
                } else {
                    error!("[!] Failed to lock the piece picker bitfield");
                    bail!("[!] Failed to lock the piece picker bitfield");
                }
                if peer.send_msg(Message::interested()).await.is_err() {
                    error!(
                        "[!] Failed to send interested message to the peer: {:?}",
                        peer.to_string()
                    );
                    bail!("[!] Failed to send interested message to the peer");
                } else {
                    Ok(0)
                }
            }
            MessageId::Choke => {
                peer.status.choked = true;
                Ok(0)
            }
            MessageId::Unchoke => {
                peer.status.choked = false;
                let block: Option<Block>;
                if let Ok(mut picker) = piece_picker.lock() {
                    block = picker.pick(&peer.status.bitfield);
                } else {
                    error!("[!] Failed to lock the piece picker bitfield");
                    bail!("[!] Failed to lock the piece picker bitfield");
                }

                if let Some(b) = block
                    && peer
                        .send_msg(Message::request(b.to_payload().as_slice().try_into()?))
                        .await
                        .is_err()
                {
                    error!(
                        "[!] Failed to send request message to the peer: {:?}",
                        peer.to_string()
                    );
                    bail!("[!] Failed to send request message to the peer");
                } else {
                    Ok(0)
                }
            }
            MessageId::Have => {
                if let Ok(mut picker) = piece_picker.lock() {
                    let index: u32 = u32::from_be_bytes(msg.payload[..4].try_into()?);
                    peer.status.bitfield.set_piece(index as usize);
                    picker.add_peer_have(index as usize);
                    Ok(0)
                } else {
                    error!("[!] Failed to lock the piece picker bitfield");
                    bail!("[!] Failed to lock the piece picker bitfield");
                }
            }
            MessageId::Piece => {
                let (next_block, piece_data) = if let Ok(mut picker) = piece_picker.lock() {
                    let index = u32::from_be_bytes(msg.payload[0..4].try_into()?) as usize;
                    let offset = u32::from_be_bytes(msg.payload[4..8].try_into()?);
                    let block_data = &msg.payload[8..];

                    let piece_ready = picker
                        .handle_piece(index, offset, block_data)
                        .context("[!] Failed to handle piece data received")?;
                    finished = picker.missing_pieces == 0;
                    let data_to_save = if piece_ready {
                        Some((index, std::mem::take(&mut picker.pieces[index].data)))
                    } else {
                        None
                    };
                    (picker.pick(&peer.status.bitfield), data_to_save)
                } else {
                    error!("[!] Failed to lock the piece picker bitfield");
                    bail!("[!] Failed to lock the piece picker bitfield");
                };

                if let Some((piece_index, data)) = piece_data {
                    Piece::write_to_disk(piece_index, &data, &file, path).await?;
                }

                if finished {
                    return Ok(1);
                }

                if let Some(b) = next_block {
                    peer.send_msg(Message::request(b.to_payload().as_slice().try_into()?))
                        .await?;
                }
                Ok(0)
            }
            _ => {
                warn!(
                    "[-] Unrecognized message id received: {:?}, closing connection",
                    msg.id
                );
                Ok(1) // 1 to indicate the connection should be closed
            }
        }
    }
}
