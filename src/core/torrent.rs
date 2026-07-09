//! Core torrent download logic — the heart of the BitTorrent client.
//!
//! Manages the per-torrent state machine, peer connections, piece picking,
//! choking / unchoking, tracker announcements, and incoming peer routing.

use crate::core::{
    bitfield::BitField,
    message::{Message, MessageId},
    peer::{Peer, PeerCommand, PeerHandshake, SharedPeerCtrl},
    piece::{Piece, PiecePicker},
    torrent_file::TorrentFile,
    tracker::{complete_msg, get_peers},
};
use crate::persistence::{resume_data::ResumeData, Persistent};
use anyhow::{bail, Context, Result};
use std::{fmt::Display, path::PathBuf, sync::Arc};
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    sync::{mpsc::{self, Receiver}, Mutex, RwLock},
};
use tracing::{debug, error, info, instrument, warn};

/// Unique identifier for a torrent within the client session.
pub type TorrentId = u64;

/// A single torrent being downloaded (or seeded).
pub struct Torrent {
    pub peer_id: [u8; 20],
    pub file: Arc<TorrentFile>,
    /// SHA-1 info hash that uniquely identifies this torrent in the swarm.
    pub info_hash: [u8; 20],
    /// Peers obtained from tracker announce (pre-handshake).
    pub peers: Vec<Peer>,
    /// Shared piece picker — tracks which pieces we have and which to request.
    pub piece_picker: Arc<Mutex<PiecePicker>>,
}

impl Torrent {
    /// Build a [`Torrent`] from a parsed `.torrent` file.
    ///
    /// 1. Computes the info hash (SHA-1 of the bencoded `info` dict).
    /// 2. Splits the piece hash list into [`Piece`] objects with [`BLOCK_SIZE`] blocks.
    /// 3. Contacts all trackers to obtain the initial peer list.
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

        // Build each piece: extract its 20-byte SHA-1 hash and calculate
        // its actual byte length (the last piece is usually shorter).
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

        // Contact every tracker in the announce list concurrently
        // and merge the peer lists.
        let peers = get_peers(&torrent)
            .await
            .context("[!] Failed to get peers from tracker")?;

        torrent.peers = peers;
        info!("Torrent created successfully");

        Ok(torrent)
    }

    /// Spawn the torrent's async main loop as a background tokio task.
    ///
    /// Returns a [`TorrentHandle`] that allows the client to query state
    /// and send control commands (pause, resume, stop, cancel).
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

    /// Connect to all known peers and perform the BitTorrent handshake.
    ///
    /// Each peer is given a 3-second timeout. Only peers that complete
    /// the handshake successfully are kept; the rest are discarded.
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

    /// Main torrent event loop — runs as a background tokio task.
    ///
    /// This is the central coordination point for a single torrent. It:
    ///
    /// 1. **Activates peers** — performs the BitTorrent handshake with all
    ///    known peers from the tracker.
    /// 2. **Spawns peer tasks** — one async task per connected peer, each
    ///    running [`peer_loop`] to handle message I/O.
    /// 3. **Runs four concurrent branches** via `tokio::select!`:
    ///
    ///    | Branch | Frequency | Purpose |
    ///    |---|---|---|
    ///    | `ctrl_rx` | on-demand | Handle control commands (pause/resume/stop/cancel/incoming peer) |
    ///    | `tick` | 1 s | Update progress, download/upload rates, timeouts |
    ///    | `unchoke_timer` | 10 s | Recalculate which peers to unchoke (tit-for-tat) |
    ///    | `announce_timer` | 300 s | Re-announce to trackers for fresh peers |
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

        // Channels to send commands to each peer's dedicated task.
        let mut txs: Vec<mpsc::Sender<PeerCommand>> = Vec::with_capacity(self.peers.len());

        // Shared control state: one SharedPeerCtrl per peer, used by both
        // the main loop (unchoke calc) and the peer task (upload tracking).
        let control: Arc<Mutex<Vec<SharedPeerCtrl>>> = Arc::new(Mutex::new(vec![
                SharedPeerCtrl::default();
                self.peers.len()
            ]));

        // Spawn one task per peer: each runs peer_loop for message I/O.
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

        // --- Periodic timers ---
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut unchoke_timer = tokio::time::interval(std::time::Duration::from_secs(10));
        let mut announce_timer = tokio::time::interval(std::time::Duration::from_secs(300));
        announce_timer.tick().await; // Consume the immediate first tick
        let mut prev_downloaded: f64 = 0.0;
        let mut prev_uploaded: f64 = 0.0;

        // ────────────── Main event loop ──────────────
        loop {
            tokio::select! {
                // === Branch 1: Control commands from the client ===
                cmd = ctrl_rx.recv() => {
                    match cmd {
                        Some(TorrentCommand::Pause) => {
                            let mut picker = self.piece_picker.lock().await;
                            let mut i = info.write().await;
                            i.state = TorrentState::Paused;
                            picker.paused = true;
                        },
                        Some(TorrentCommand::Resume) => {
                            {
                                let mut picker = self.piece_picker.lock().await;
                                let mut i = info.write().await;
                                i.state = TorrentState::Downloading;
                                picker.paused = false;
                            }
                            // Unblock all peers so they start requesting again.
                            for tx in &txs {
                                let _ = tx.send(PeerCommand::RequestBlock).await;
                            }
                        }
                        Some(TorrentCommand::Stop) => {
                            let mut i = info.write().await;
                            i.state = TorrentState::Stopped;
                            // Persist progress for resume on next launch.
                            let resume_data = ResumeData::new(i.downloaded, self.piece_picker.lock().await.bitfield.data.clone());
                            resume_data.save(&hex::encode(i.info_hash)).await?;

                            drop(i);
                            // Notify trackers that we're going away (event=1, completed).
                            complete_msg(&self).await;
                            break;
                        },
                        Some(TorrentCommand::Cancel) => break,
                        // Incoming peer from the TCP listener — perform
                        // a responding handshake and spawn a peer task.
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

                // === Branch 2: Periodic 1-second tick — state updates ===
                _ = tick.tick() => {
                    let mut picker = self.piece_picker.lock().await;
                    let mut i = info.write().await;
                    // Detect download completion → switch to seeding.
                    let just_completed = picker.missing_pieces == 0 && i.state != TorrentState::Seeding;
                    if just_completed {
                        info!("[*] Download complete - now seeding");
                        i.state = TorrentState::Seeding;
                    }
                    // Expire block requests that have timed out.
                    picker.tick_timeouts();

                    // Estimate downloaded bytes from remaining missing pieces.
                    i.downloaded = i.total_size.saturating_sub((picker.missing_pieces * self.file.info.piece_length as usize) as u64);
                    let total_pieces = picker.piece_frequencies.len();
                    let downloaded_pieces = total_pieces - picker.missing_pieces;
                    i.progress = downloaded_pieces as f64 * 100.0 / total_pieces as f64;
                    i.download_rate = i.downloaded as f64 - prev_downloaded;
                    i.uploaded = control.lock().await.iter().map(|p| p.uploaded).sum();
                    i.upload_rate = i.uploaded as f64 - prev_uploaded;

                    prev_downloaded = i.downloaded as f64;
                    prev_uploaded = i.uploaded as f64;

                    drop(i);
                    drop(picker);

                    if just_completed {
                        complete_msg(&self).await;
                    }
                }

                // === Branch 3: Unchoke recalculation every 10 s ===
                _ = unchoke_timer.tick() => {
                    let unchokes = Self::recalc_unchoke(control.clone()).await;

                    for (u, tx) in unchokes.iter().zip(&txs) {
                        let cmd = if *u { PeerCommand::Unchoke } else { PeerCommand::Choke };
                        let _ = tx.send(cmd).await;
                    }
                }

                // === Branch 4: Tracker re-announce every 300 s ===
                _ = announce_timer.tick() => {
                    let peers = get_peers(&self).await;
                    if let Ok(peers) = peers {
                        let info_hash = self.info_hash;
                        let peer_id = self.peer_id;

                        for mut peer in peers {
                            // Handshake with 3-second timeout.
                            let success = matches!(tokio::time::timeout(
                                std::time::Duration::from_secs(3),
                                peer.handshake(&info_hash, &peer_id),
                            ).await, Ok(Ok(_)));

                            if success && peer.status.stream.is_some() {
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
                        }
                    } else {
                        warn!("Re-announce failed to get peers");
                    }
                }
            }
        }
        Ok(())
    }

    /// Per-peer I/O task — reads messages and handles control commands.
    ///
    /// Runs in a `select!` loop with two branches:
    /// - **Incoming message** via [`read_msg`] / [`handle_msg`] — processes
    ///   wire protocol messages (piece data, requests, bitfields, etc.).
    /// - **Control command** via `peer_cmd_rx` — choke/unchoke/request-block
    ///   commands from the main torrent loop.
    ///
    /// When a peer unchokes us and we have pipeline capacity, we pick the next
    /// block (via the rarest-first `PiecePicker`) and send a `request` message.
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
        if piece_picker.lock().await.missing_pieces <= 5 {
            peer_ctrl.lock().await[peer_index].max_pipeline = 1;
        }
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
                        Some(PeerCommand::Choke) => {
                            peer.send_msg(Message::choke()).await?;
                        }
                        Some(PeerCommand::Unchoke) => {
                            peer.send_msg(Message::unchoke()).await?;
                        }
                        Some(PeerCommand::RequestBlock) => {
                            if !peer.status.am_choked {
                                let needed = {
                                    let ctrl = peer_ctrl.lock().await;
                                    ctrl[peer_index].max_pipeline.saturating_sub(ctrl[peer_index].pending_requests)
                                };
                                for _ in 0..needed {
                                    let block = piece_picker.lock().await.pick(&peer.status.bitfield);
                                    if let Some(b) = block {
                                        peer.send_msg(
                                            Message::request(b.to_payload().as_slice().try_into()?),
                                        )
                                        .await?;
                                        peer_ctrl.lock().await[peer_index].pending_requests += 1;
                                    } else {
                                        break;
                                    }
                                }
                            }
                        }
                        None => continue,
                    }
                }
            }
        }
    }

    /// Read a single message from a peer with a 10-second timeout.
    ///
    /// On timeout (and if the peer hasn't choked us), it opportunistically
    /// requests a block — keeping the pipe busy while waiting.
    async fn read_msg(peer: &mut Peer, piece_picker: Arc<Mutex<PiecePicker>>) -> Result<Message> {
        let readable = if let Some(stream) = peer.status.stream.as_ref() {
            tokio::time::timeout(std::time::Duration::from_secs(10), stream.readable()).await
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

    /// Dispatch a single wire-protocol message from a peer.
    ///
    /// Returns `Ok(0)` to continue, `Ok(1)` to close the connection silently
    /// (unrecognized message), or `Err` to close with an error log.
    ///
    /// **Key message handlers:**
    ///
    /// | Message | Action |
    /// |---|---|
    /// | `BitField` / `Have` | Update peer bitfield, adjust piece freq |
    /// | `Choke` / `Unchoke` | Toggle `am_choked`, pipeline blocks on unchoke |
    /// | `Piece` | Store block data, assemble piece, write to disk |
    /// | `Request` | Serve an uploaded piece from disk (if unchoked) |
    /// | `Interested` / `NotInterested` | Track peer interest for choke algo |
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
                let needed = {
                    let ctrl = peer_ctrl.lock().await;
                    ctrl[peer_index]
                        .max_pipeline
                        .saturating_sub(ctrl[peer_index].pending_requests)
                };
                for _ in 0..needed {
                    let block = piece_picker.lock().await.pick(&peer.status.bitfield);
                    if let Some(b) = block {
                        peer.send_msg(Message::request(b.to_payload().as_slice().try_into()?))
                            .await
                            .context("[!] Failed to send request message to the peer")?;
                        peer_ctrl.lock().await[peer_index].pending_requests += 1;
                    } else {
                        break;
                    }
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
                // Decrement pending (peer_ctrl only, released immediately)
                peer_ctrl.lock().await[peer_index].pending_requests -= 1;

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

                if let Some((piece_index, data)) = data_to_save {
                    Piece::write_to_disk(piece_index, &data, &file, path)
                        .await
                        .context("[!] Failed to write received block to disk, retrying...")?;
                }

                // Get pipeline config (peer_ctrl only, released immediately)
                let needed = {
                    let ctrl = peer_ctrl.lock().await;
                    ctrl[peer_index]
                        .max_pipeline
                        .saturating_sub(ctrl[peer_index].pending_requests)
                };

                // Request blocks (only piece_picker held, no peer_ctrl)
                for _ in 0..needed {
                    let block = picker.pick(&peer.status.bitfield);
                    if let Some(b) = block {
                        peer.send_msg(Message::request(b.to_payload().as_slice().try_into()?))
                            .await
                            .context("[!] Failed to send next block request message to the peer")?;
                        peer_ctrl.lock().await[peer_index].pending_requests += 1;
                    } else {
                        break;
                    }
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

    /// Tit-for-tat unchoke calculation — runs every 10 s.
    ///
    /// Of the interested peers, the **top 4** by upload rate are unchoked;
    /// all others are choked. Returns a bool vector where `true` = unchoke
    /// so the caller can send `PeerCommand::Unchoke` / `Choke`.
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

/// Lifecycle state of a single torrent.
#[derive(Clone, Debug, PartialEq)]
pub enum TorrentState {
    /// Actively downloading pieces from peers.
    Downloading,
    /// All pieces downloaded — uploading to others.
    Seeding,
    /// User paused — no I/O, peers remain connected.
    Paused,
    /// An error occurred (e.g. tracker unreachable).
    Error,
    /// Stopped — progress saved, peers disconnected.
    Stopped,
}

impl Display for TorrentState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Observable state of a torrent — read by the TUI for display.
///
/// Wrapped in `Arc<RwLock<TorrentInfo>>` so the main loop, TUI, and client
/// can all access it concurrently. Updated every 1 s by the tick timer.
#[derive(Clone)]
pub struct TorrentInfo {
    pub id: TorrentId,
    pub name: String,
    pub info_hash: [u8; 20],
    pub download_dir: PathBuf,
    pub torrent_path: PathBuf,
    pub total_size: u64,
    /// Bytes downloaded so far (estimated from missing pieces).
    pub downloaded: u64,
    /// Bytes uploaded to peers (summed from all SharedPeerCtrl).
    pub uploaded: u64,
    pub state: TorrentState,
    /// Progress percentage 0.0 – 100.0.
    pub progress: f64,
    /// Smoothed download rate in bytes/s (delta from last tick).
    pub download_rate: f64,
    pub upload_rate: f64,
}

impl Display for TorrentInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}\nSize: {}\nStatus: {}\nDownloaded: {:.2} MB\nDownload speed: {:.2} MB/s\nUploaded: {:.2} MB\nUpload speed: {:.2} MB/s\nDownload directory: {}\nInfo hash: {}",
            self.name,
            self.total_size,
            self.state,
            self.downloaded as f64 / 1_000_000.0,
            self.download_rate / 1_000_000.0,
            self.uploaded as f64 / 1_000_000.0,
            self.upload_rate / 100_000.0, // Upload rates updates every 10s so to get MB/s must divide by 1_000_000 and multiply by 10
            self.download_dir.display(),
            hex::encode(self.info_hash),
        )
    }
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

/// Commands sent from [`TorrentHandle`] to the torrent's event loop.
pub enum TorrentCommand {
    /// Pause downloading (keep peers connected).
    Pause,
    /// Resume downloading after pause.
    Resume,
    /// Stop and persist progress, then notify tracker.
    Stop,
    /// Cancel immediately, no tracker notification.
    Cancel,
    /// Route a new incoming TCP connection to this torrent.
    NewPeer(TcpStream),
}

/// External handle to control a running torrent.
///
/// Created by [`Torrent::spawn`] and stored in the client's torrent map.
/// The TUI (or any caller) can read `info` for display and send commands
/// via `ctrl_tx` to pause/resume/stop/cancel.
pub struct TorrentHandle {
    pub id: TorrentId,
    /// Shared observable state — read-only from outside the torrent task.
    pub info: Arc<RwLock<TorrentInfo>>,
    /// Channel to send control commands to the torrent's event loop.
    pub ctrl_tx: mpsc::Sender<TorrentCommand>,
}
