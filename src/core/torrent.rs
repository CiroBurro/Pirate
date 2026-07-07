use crate::core::peer::{PeerCommand, PeerHandshake, SharedPeerCtrl};
use crate::core::{
    bitfield::BitField,
    message::{Message, MessageId},
    peer::Peer,
    piece::{Piece, PiecePicker},
    torrent_file::TorrentFile,
    tracker::{complete_msg, get_peers},
};
use crate::persistence::resume_data::ResumeData;
use crate::persistence::Persistent;
use anyhow::{bail, Context, Result};
use std::{path::PathBuf, sync::Arc};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
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
        download_dir: PathBuf,
        torrent_path: PathBuf,
    ) -> Result<TorrentHandle> {
        let (ctrl_tx, ctrl_rx) = mpsc::channel(16);

        let info = Arc::new(RwLock::new(TorrentInfo::new(
            id,
            &self,
            download_dir.clone(),
            torrent_path.clone(),
        )?));
        let info_clone = info.clone();
        tokio::spawn(async move {
            let _ = self.run(ctrl_rx, info_clone, download_dir).await;
        });

        Ok(TorrentHandle { id, info, ctrl_tx })
    }

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
                        debug!("Handshake successfully completed with {}", peer);
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
        download_dir: PathBuf,
    ) -> Result<()> {
        self.activate_peers().await;
        if self.peers.is_empty() {
            bail!("[!] The torrent has no active peers");
        }
        info!("Found {} active peers", self.peers.len());

        let mut txs: Vec<mpsc::Sender<PeerCommand>> = Vec::with_capacity(self.peers.len());

        let control: Arc<Mutex<Vec<SharedPeerCtrl>>> = Arc::new(Mutex::new(vec![
                SharedPeerCtrl::default();
                self.peers.len()
            ]));

        for (i, peer) in std::mem::take(&mut self.peers).into_iter().enumerate() {
            let piece_picker = Arc::clone(&self.piece_picker);
            let file = Arc::clone(&self.file);
            let (tx, rx) = mpsc::channel::<PeerCommand>(16);
            txs.push(tx);
            tokio::task::spawn(Self::peer_loop(
                peer,
                control.clone(),
                i,
                piece_picker,
                file,
                download_dir.clone(),
                rx,
            ));
        }

        let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut timer = tokio::time::interval(std::time::Duration::from_secs(10));

        let mut prev_downloaded: f64 = 0.0;
        let mut prev_uploaded: f64 = 0.0;
        loop {
            tokio::select! {
                cmd = ctrl_rx.recv() => {
                    match cmd {
                        Some(TorrentCommand::Pause) => {
                            let mut picker = self.piece_picker.lock().await;
                            let mut i = info.write().await;
                            i.state = TorrentState::Paused;
                            picker.paused = true;
                        },
                        Some(TorrentCommand::Resume) => {
                            let mut picker = self.piece_picker.lock().await;
                            let mut i = info.write().await;
                            i.state = TorrentState::Downloading;
                            picker.paused = false;
                        }
                        Some(TorrentCommand::Stop) => {
                            let mut i = info.write().await;
                            i.state = TorrentState::Stopped;
                            let resume_data = ResumeData::new(i.downloaded, self.piece_picker.lock().await.bitfield.data.clone());
                            resume_data.save(&hex::encode(i.info_hash)).await?;

                            drop(i);
                            complete_msg(&self).await;
                            break;
                        },
                        Some(TorrentCommand::Cancel) => break,
                        Some(TorrentCommand::NewPeer(mut stream)) => {
                            let mut handshake = PeerHandshake::default();
                            handshake.info_hash = self.info_hash;
                            handshake.peer_id = self.peer_id;

                            stream.writable().await?;

                            stream
                                .write_all(&handshake.serialize())
                                .await
                                .context("[!] Failed to send the handshake to the peer")?;

                            let peer = Peer::from_stream(stream)?;
                            let (tx, rx) = mpsc::channel::<PeerCommand>(16);
                            txs.push(tx);

                            let mut ctrl = control.lock().await;
                            ctrl.push(SharedPeerCtrl::default());
                            let i = ctrl.len() - 1;
                            drop(ctrl);
                            tokio::task::spawn(Self::peer_loop(
                                peer,
                                control.clone(),
                                i,
                                self.piece_picker.clone(),
                                self.file.clone(),
                                download_dir.clone(),
                                rx,
                            ));
                        }
                        None => break
                    }
                }
                _ = tick.tick() => {

                    let mut picker = self.piece_picker.lock().await;
                    let mut i = info.write().await;
                    if picker.missing_pieces == 0 {
                        info!("[*] Download complete - now seeding");
                        i.state = TorrentState::Seeding;
                        complete_msg(&self).await;
                    }
                    picker.tick_timeouts();

                    i.downloaded = i.total_size.saturating_sub((picker.missing_pieces * self.file.info.piece_length as usize) as u64);
                    let total_pieces = picker.piece_frequencies.len();
                    let downloaded_pieces = total_pieces - picker.missing_pieces;
                    i.progress = downloaded_pieces as f64 * 100.0 / total_pieces as f64;
                    i.download_rate = i.downloaded as f64 - prev_downloaded;
                    i.uploaded = control.lock().await.iter().map(|p| p.uploaded).sum();
                    i.upload_rate = i.uploaded as f64 - prev_uploaded;

                    prev_downloaded = i.downloaded as f64;
                    prev_uploaded = i.uploaded as f64;

                }

                _ = timer.tick() => {
                    let unchokes = Self::recalc_unchoke(control.clone()).await;

                    for (u, tx) in unchokes.iter().zip(&txs) {
                        let cmd = if *u { PeerCommand::Unchoke } else { PeerCommand::Choke };
                        let _ = tx.send(cmd).await;
                    }
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn peer_loop(
        mut peer: Peer,
        peer_ctrl: Arc<Mutex<Vec<SharedPeerCtrl>>>,
        peer_index: usize,
        piece_picker: Arc<Mutex<PiecePicker>>,
        file: Arc<TorrentFile>,
        path: PathBuf,
        mut peer_cmd_rx: Receiver<PeerCommand>,
    ) -> Result<()> {
        loop {
            tokio::select! {
                msg_result = Self::read_msg(&mut peer, Arc::clone(&piece_picker)) => {
                    if let Ok(msg) = msg_result {
                        match Self::handle_msg(
                            &mut peer,
                            peer_ctrl.clone(),
                            peer_index,
                            &piece_picker,
                            msg,
                            file.clone(),
                            path.clone(),
                        ).await {
                            Ok(0) => continue,
                            Ok(_) => return Ok(()), // Connection should be closed if exit code != 0
                            Err(e) => {
                                error!("[!] Error in handle_msg with {}: {}", peer, e);
                                return Err(e);
                            }
                        }

                    } else if let Err(e) = msg_result {
                        error!("[!] Connection lost with {}: {}", peer, e);
                        return Err(e);
                    }
                }
                peer_cmd = peer_cmd_rx.recv() => {
                    match peer_cmd {
                        Some(PeerCommand::Choke) => peer.send_msg(Message::choke()),
                        Some(PeerCommand::Unchoke) => peer.send_msg(Message::unchoke()),
                        None => continue,
                    }.await?;
                }
            }
        }
    }

    async fn read_msg(peer: &mut Peer, piece_picker: Arc<Mutex<PiecePicker>>) -> Result<Message> {
        let readable = if let Some(stream) = peer.status.stream.as_ref() {
            tokio::time::timeout(std::time::Duration::from_secs(5), stream.readable()).await
        } else {
            debug!("No readable stream");
            return Err(anyhow::anyhow!("No readable stream"));
        };

        match readable {
            Ok(Ok(_)) => peer.read_msg().await,
            Ok(Err(e)) => {
                error!("[!] Stream error with {}: {}", peer, e);

                Err(e.into())
            }
            Err(e) => {
                if !peer.status.am_choked {
                    let block = piece_picker.lock().await.pick(&peer.status.bitfield);
                    if let Some(b) = block {
                        let _ = peer
                            .send_msg(Message::request(b.to_payload().as_slice().try_into()?))
                            .await;
                    }
                }
                Err(e.into())
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_msg(
        peer: &mut Peer,
        peer_ctrl: Arc<Mutex<Vec<SharedPeerCtrl>>>,
        peer_index: usize,
        piece_picker: &Arc<Mutex<PiecePicker>>,
        msg: Message,
        file: Arc<TorrentFile>,
        path: PathBuf,
    ) -> Result<usize> {
        match msg.id {
            MessageId::KeepAlive => Ok(0),
            MessageId::BitField => {
                peer.status.bitfield = BitField::from_payload(msg.payload);
                piece_picker
                    .lock()
                    .await
                    .add_peer_bitfield(&peer.status.bitfield);
                peer.send_msg(Message::interested())
                    .await
                    .context("[!] Failed to send interested message to the peer")?;
                Ok(0)
            }
            MessageId::Choke => {
                peer.status.am_choked = true;
                Ok(0)
            }
            MessageId::Unchoke => {
                peer.status.am_choked = false;
                let block = piece_picker.lock().await.pick(&peer.status.bitfield);
                if let Some(b) = block {
                    peer.send_msg(Message::request(b.to_payload().as_slice().try_into()?))
                        .await
                        .context("[!] Failed to send request message to the peer")?;
                }
                Ok(0)
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

                let data_to_save = if piece_ready {
                    Some((index, std::mem::take(&mut picker.pieces[index].data)))
                } else {
                    None
                };

                let (next_block, piece_data) = (picker.pick(&peer.status.bitfield), data_to_save);

                if let Some((piece_index, data)) = piece_data {
                    Piece::write_to_disk(piece_index, &data, &file, path)
                        .await
                        .context("[!] Failed to write received block to disk, retrying...")?;
                }

                if let Some(b) = next_block {
                    peer.send_msg(Message::request(b.to_payload().as_slice().try_into()?))
                        .await
                        .context("[!] Failed to send next block request message to the peer")?;
                }
                Ok(0)
            }
            MessageId::NotInterested => {
                peer_ctrl.lock().await[peer_index].peer_interested = false;
                Ok(0)
            }
            MessageId::Interested => {
                peer_ctrl.lock().await[peer_index].peer_interested = true;
                Ok(0)
            }
            MessageId::Request => {
                if peer_ctrl.lock().await[peer_index].am_choking {
                    return Ok(0);
                }

                let index = u32::from_be_bytes(msg.payload[0..4].try_into()?) as usize;
                let offset = u32::from_be_bytes(msg.payload[4..8].try_into()?);
                let length = u32::from_be_bytes(msg.payload[8..].try_into()?);

                if !piece_picker.lock().await.bitfield.has_piece(index) {
                    return Ok(0);
                }
                if offset + length > file.info.piece_length {
                    return Ok(0);
                }

                let mut piece_data = Piece::read_from_disk(index, offset, length, &file, path)
                    .await
                    .context("[!] Failed to read piece from disk")?;

                let mut data: Vec<u8> = msg.payload[0..8].to_vec();
                data.append(&mut piece_data);
                peer.send_msg(Message::piece(data))
                    .await
                    .context("[!] Failed to send piece to the peer")?;
                peer_ctrl.lock().await[peer_index].uploaded += piece_data.len() as u64;
                Ok(0)
            }
            _ => {
                warn!(
                    "[-] Unrecognized message id received: {:?}, closing connection",
                    msg.id
                );
                Ok(1) // 1 to indicate the connection should be closed silently
            }
        }
    }

    async fn recalc_unchoke(state: Arc<Mutex<Vec<SharedPeerCtrl>>>) -> Vec<bool> {
        let mut state = state.lock().await;
        let mut upload_rates: Vec<(usize, f64)> = state
            .iter_mut()
            .enumerate()
            .filter(|(_, p)| p.peer_interested)
            .map(|(i, p)| {
                let rate = (p.uploaded - p.uploaded_prev) as f64;
                p.uploaded_prev = p.uploaded;
                (i, rate)
            })
            .collect();
        upload_rates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        let mut result = vec![true; state.len()];

        for (i, _) in upload_rates.iter().skip(4) {
            result[*i] = false;
            if !state[*i].am_choking {
                state[*i].am_choking = true;
            }
        }

        result
    }
}

#[derive(Clone, Debug)]
pub enum TorrentState {
    Downloading,
    Seeding,
    Paused,
    Error,
    Stopped,
}

#[derive(Clone)]
pub struct TorrentInfo {
    pub id: TorrentId,
    pub name: String,
    pub info_hash: [u8; 20],
    pub download_dir: PathBuf,
    pub torrent_path: PathBuf,
    pub total_size: u64,
    pub downloaded: u64,
    pub uploaded: u64,
    pub state: TorrentState, // Downloading | Seeding | Paused | Error | Stopped
    pub progress: f64,       // 0.0 - 100.0
    pub download_rate: f64,  // bytes/s (smoothed)
    pub upload_rate: f64,
}

impl TorrentInfo {
    pub fn new(
        id: TorrentId,
        torrent: &Torrent,
        download_dir: PathBuf,
        torrent_path: PathBuf,
    ) -> Result<Self> {
        Ok(Self {
            id,
            name: torrent.file.info.name.clone(),
            info_hash: torrent.info_hash,
            download_dir,
            torrent_path,
            total_size: torrent.file.info.total_len()? as u64,
            downloaded: 0,
            uploaded: 0,
            state: TorrentState::Downloading,
            progress: 0.0,
            download_rate: 0.0,
            upload_rate: 0.0,
        })
    }
}

pub enum TorrentCommand {
    Pause,
    Resume,
    Stop,
    Cancel,
    NewPeer(TcpStream),
}

pub struct TorrentHandle {
    pub id: TorrentId,
    pub info: Arc<RwLock<TorrentInfo>>,
    pub ctrl_tx: mpsc::Sender<TorrentCommand>,
}
