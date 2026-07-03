use crate::client::{ClientEvent, Config};
use crate::core::{
    bitfield::BitField,
    message::{Message, MessageId},
    peer::Peer,
    piece::{Piece, PiecePicker},
    torrent_file::TorrentFile,
    tracker::{complete_msg, get_peers},
};
use anyhow::{bail, Context, Result};
use std::{path::PathBuf, sync::Arc};
use tokio::sync::broadcast::Sender;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, error, info, instrument, warn};

pub type TorrentId = u64;

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

    pub fn spawn(
        self,
        id: TorrentId,
        config: &Config,
        event_tx: Sender<ClientEvent>,
    ) -> Result<TorrentHandle> {
        let (ctrl_tx, ctrl_rx) = mpsc::channel(16);

        let info = Arc::new(RwLock::new(TorrentInfo::new(id, &self)?));
        let info_clone = info.clone();
        let download_dir = config.download_dir.clone();

        tokio::spawn(async move {
            let _ = self.run(ctrl_rx, info_clone, event_tx, download_dir).await;
        });

        Ok(TorrentHandle { id, info, ctrl_tx })
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

    #[instrument(skip_all)]
    async fn run(
        mut self,
        mut ctrl_rx: Receiver<TorrentCommand>,
        info: Arc<RwLock<TorrentInfo>>,
        event_tx: Sender<ClientEvent>,
        download_dir: PathBuf,
    ) -> Result<()> {
        self.activate_peers().await;
        if self.peers.is_empty() {
            bail!("[!] The torrent has no active peers");
        }
        info!("Found {} active peers", self.peers.len());

        for peer in std::mem::take(&mut self.peers) {
            let piece_picker = Arc::clone(&self.piece_picker);
            let file = Arc::clone(&self.file);
            tokio::task::spawn(Self::peer_loop(
                peer,
                piece_picker,
                file,
                download_dir.clone(),
                event_tx.clone(),
                info.write().await.id,
            ));
        }

        let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut prev_downloaded: f64 = 0.0;
        loop {
            tokio::select! {
                cmd = ctrl_rx.recv() => {
                    match cmd {
                        Some(TorrentCommand::Pause) => {
                            let mut picker = self.piece_picker.lock().await;
                            let mut i = info.write().await;
                            i.state = TorrentState::Paused;
                            picker.paused = true;
                            event_tx.send(ClientEvent::StateChanged {
                                id: i.id,
                                state: i.state.clone(),
                            })?;
                        },
                        Some(TorrentCommand::Resume) => {
                            let mut picker = self.piece_picker.lock().await;
                            let mut i = info.write().await;
                            i.state = TorrentState::Downloading;
                            picker.paused = false;
                            event_tx.send(ClientEvent::StateChanged {
                                id: i.id,
                                state: i.state.clone(),
                            })?;
                        }
                        Some(TorrentCommand::Stop) => {
                            let mut i = info.write().await;
                            i.state = TorrentState::Completed;
                            event_tx.send(ClientEvent::StateChanged {
                                id: i.id,
                                state: i.state.clone(),
                            })?;

                            drop(i);
                            complete_msg(&self).await;
                            break;
                        },
                        Some(TorrentCommand::Cancel) => break,
                        None => break
                    }
                }
                _ = tick.tick() => {

                    let mut picker = self.piece_picker.lock().await;
                    let mut i = info.write().await;
                    if picker.missing_pieces == 0 {
                        info!("[*] Download complete");
                        i.state = TorrentState::Completed;
                        drop(i);
                        complete_msg(&self).await;
                        break
                    }
                    picker.tick_timeouts();

                    i.downloaded = i.total_size.saturating_sub((picker.missing_pieces * self.file.info.piece_length as usize) as u64);
                    let total_pieces = picker.piece_frequencies.len();
                    let downloaded_pieces = total_pieces - picker.missing_pieces;
                    i.progress = downloaded_pieces as f64 * 100.0 / total_pieces as f64;
                    i.download_rate = i.downloaded as f64 - prev_downloaded;
                    prev_downloaded = i.downloaded as f64;
                    event_tx.send(ClientEvent::Progress {
                        id: i.id,
                        progress: i.progress,
                        downloaded: i.downloaded,
                        download_rate: i.download_rate,
                        upload_rate: i.upload_rate,
                    })?;
                }
            }
        }
        Ok(())
    }

    #[instrument(skip_all)]
    async fn peer_loop(
        mut peer: Peer,
        piece_picker: Arc<Mutex<PiecePicker>>,
        file: Arc<TorrentFile>,
        path: PathBuf,
        event_tx: Sender<ClientEvent>,
        id: TorrentId,
    ) -> Result<()> {
        loop {
            let readable = if let Some(stream) = peer.status.stream.as_ref() {
                tokio::time::timeout(std::time::Duration::from_secs(5), stream.readable()).await
            } else {
                event_tx.send(ClientEvent::Error {
                    id,
                    message: String::from("No stream"),
                })?;
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
                            event_tx.clone(),
                            id,
                        )
                        .await?
                            == 1
                        {
                            return Ok(());
                        }
                    } else if let Err(e) = msg_result {
                        error!("[!] Connection lost with {}: {}", peer, e);
                        event_tx.send(ClientEvent::Error {
                            id,
                            message: format!("Connection lost with {}: {}", peer, e),
                        })?;
                        return Err(e);
                    }
                }
                Ok(Err(e)) => {
                    error!("[!] Stream error with {}: {}", peer, e);

                    event_tx.send(ClientEvent::Error {
                        id,
                        message: format!("[!] Stream error with {}: {}", peer, e),
                    })?;
                    return Err(e.into());
                }
                Err(_) => {
                    if !peer.status.choked {
                        let block = piece_picker.lock().await.pick(&peer.status.bitfield);
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
        event_tx: Sender<ClientEvent>,
        id: TorrentId,
    ) -> Result<usize> {
        let finished;
        match msg.id {
            MessageId::KeepAlive => Ok(0),
            MessageId::BitField => {
                peer.status.bitfield = BitField::from_payload(msg.payload);
                piece_picker
                    .lock()
                    .await
                    .add_peer_bitfield(&peer.status.bitfield);
                if peer.send_msg(Message::interested()).await.is_err() {
                    error!(
                        "[!] Failed to send interested message to the peer: {:?}",
                        peer.to_string()
                    );
                    event_tx.send(ClientEvent::Error {
                        id,
                        message: String::from("[!] Failed to send interested message to the peer"),
                    })?;
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
                let block = piece_picker.lock().await.pick(&peer.status.bitfield);
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
                    event_tx.send(ClientEvent::Error {
                        id,
                        message: String::from("[!] Failed to send request message to the peer"),
                    })?;
                    bail!("[!] Failed to send request message to the peer");
                } else {
                    Ok(0)
                }
            }
            MessageId::Have => {
                let index: u32 = u32::from_be_bytes(msg.payload[..4].try_into()?);
                peer.status.bitfield.set_piece(index as usize);
                piece_picker.lock().await.add_peer_have(index as usize);
                Ok(0)
            }
            MessageId::Piece => {
                let mut picker = piece_picker.lock().await;
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

                let (next_block, piece_data) = (picker.pick(&peer.status.bitfield), data_to_save);

                if let Some((piece_index, data)) = piece_data {
                    Piece::write_to_disk(piece_index, &data, &file, path).await?;
                }

                if finished {
                    event_tx.send(ClientEvent::PieceCompleted { id, index })?;
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

#[derive(Clone, Debug)]
pub enum TorrentState {
    Downloading,
    Seeding,
    Paused,
    Error,
    Completed,
}

#[derive(Clone)]
pub struct TorrentInfo {
    pub id: TorrentId,
    pub name: String,
    pub total_size: u64,
    pub downloaded: u64,
    pub uploaded: u64,
    pub state: TorrentState, // Downloading | Seeding | Paused | Error | Completed
    pub progress: f64,       // 0.0 - 100.0
    pub download_rate: f64,  // bytes/s (smoothed)
    pub upload_rate: f64,
    pub error: Option<String>,
}

impl TorrentInfo {
    pub fn new(id: TorrentId, torrent: &Torrent) -> Result<Self> {
        Ok(Self {
            id,
            name: torrent.file.info.name.clone(),
            total_size: torrent.file.info.total_len()? as u64,
            downloaded: 0,
            uploaded: 0,
            state: TorrentState::Downloading,
            progress: 0.0,
            download_rate: 0.0,
            upload_rate: 0.0,
            error: None,
        })
    }
}

pub enum TorrentCommand {
    Pause,
    Resume,
    Stop,
    Cancel,
}

pub struct TorrentHandle {
    pub id: TorrentId,
    pub info: Arc<RwLock<TorrentInfo>>,
    pub ctrl_tx: mpsc::Sender<TorrentCommand>,
}
