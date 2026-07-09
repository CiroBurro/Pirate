//! High-level client — the central orchestrator.
//!
//! [`Client`] manages multiple torrents, persists config and session data,
//! and listens for incoming peer connections on a TCP socket.

use crate::core::torrent::{Torrent, TorrentCommand, TorrentHandle, TorrentId, TorrentInfo};
use crate::core::torrent_file::TorrentFile;
use crate::log::LogBuffer;
use crate::persistence::{resume_data::ResumeData, session::{Session, SessionEntry}, Persistent};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::{io::AsyncReadExt, net::TcpListener, sync::{mpsc, Mutex}};
use tracing::{error, info, instrument, warn};

/// Persistent application configuration.
#[derive(Serialize, Deserialize)]
pub struct Config {
    /// 20-byte peer ID sent in handshakes (must be exactly 20 bytes).
    pub peer_id: [u8; 20],
    /// Default directory for downloaded files.
    pub download_dir: PathBuf,
    /// TCP port for the incoming-peer listener.
    pub listen_port: u16,
}

impl Persistent for Config {}

impl Default for Config {
    fn default() -> Self {
        Self {
            peer_id: *b"-PR1337-012345678901",
            download_dir: dirs::download_dir().unwrap_or_default(),
            listen_port: 6881,
        }
    }
}

/// Central state manager for the application.
///
/// Owns all active torrents, a mapping from info hash to control channel
/// (used by the TCP listener to route incoming peers), and handles
/// persistence of config, session, and resume data.
pub struct Client {
    config: Config,
    session: Session,
    log_buffer: LogBuffer,
    torrents: Vec<TorrentHandle>,
    /// Maps info_hash → ctrl_tx so the TCP listener can route incoming
    /// peers to the correct torrent task.
    info_hash_map: Arc<Mutex<HashMap<[u8; 20], mpsc::Sender<TorrentCommand>>>>,
    next_id: TorrentId,
    listener_handle: Option<tokio::task::JoinHandle<()>>,
}

impl Client {
    /// Create a new client, restoring the previous session and starting the
    /// TCP listener for incoming peer connections.
    #[instrument(skip_all)]
    pub async fn new(config: Config, log_buffer: LogBuffer) -> Result<Self> {
        let session: Session = Session::load("session").await.unwrap_or_default();
        info!("Config loaded; listen_port={}", config.listen_port);
        info!("Session loaded with {} torrent(s)", session.inner.len());
        let mut result = Self {
            config,
            session: session.clone(),
            log_buffer,
            torrents: Vec::new(),
            info_hash_map: Arc::new(Mutex::new(HashMap::new())),
            next_id: 1,
            listener_handle: None,
        };

        // Re-add all torrents from the previous session.
        for entry in session.inner {
            let _ = result
                .add_torrent(entry.torrent_path, Some(entry.download_dir))
                .await?;
        }
        result.start_listener().await?;
        Ok(result)
    }

    /// Add a new torrent from a `.torrent` file path.
    ///
    /// Parses the metainfo, loads resume data if available, spawns the
    /// torrent's async task, and registers it in the session & routing map.
    #[instrument(skip(self, torrent_path))]
    pub async fn add_torrent(
        &mut self,
        torrent_path: PathBuf,
        download_dir: Option<PathBuf>,
    ) -> Result<TorrentId> {
        // Guard: reject if the same .torrent path was already added.
        for handle in self.torrents.iter() {
            let i = handle.info.read().await;
            if i.torrent_path == torrent_path {
                return Ok(i.id);
            }
        }

        // Parse the .torrent file and initialize the torrent state.
        let torrent_file = TorrentFile::from_file(torrent_path.clone()).await?;
        let torrent = Torrent::new(torrent_file, self.config.peer_id).await?;
        let info_hash = torrent.info_hash;

        // Attempt to restore partial progress from the resume data file.
        let info_hash_hex = hex::encode(info_hash);
        let mut downloaded = 0;
        if let Ok(resume_data) = ResumeData::load(&info_hash_hex).await {
            // Validate: the bitfield length must match the current piece count.
            if resume_data.bitfield_data.len()
                == torrent.piece_picker.lock().await.bitfield.data.len()
            {
                torrent
                    .piece_picker
                    .lock()
                    .await
                    .restore(&resume_data.bitfield_data);
                info!("Resume data restored for {}", info_hash_hex);
            } else {
                warn!(
                    "Resume data bitfield size mismatch for {}, ignoring",
                    info_hash_hex
                );
            }
            downloaded = resume_data.downloaded;
        }

        // Assign a unique ID and spawn the torrent background task.
        let id = self.next_id;
        self.next_id += 1;
        let download_dir = download_dir
            .unwrap_or(self.config.download_dir.clone())
            .canonicalize()?;
        let handle = torrent.spawn(id, download_dir.clone(), torrent_path.clone())?;
        handle.info.write().await.downloaded = downloaded;

        // Register the control channel in the info_hash → ctrl_tx map
        // so the TCP listener can route incoming peers.
        self.info_hash_map
            .lock()
            .await
            .insert(info_hash, handle.ctrl_tx.clone());

        // Persist the session entry if not already present.
        let entry = SessionEntry {
            torrent_path,
            download_dir,
        };

        if !self.session.inner.contains(&entry) {
            self.session.add(entry);
        }

        let name = handle.info.read().await.name.clone();
        info!("Torrent added: [{id}] {name}");
        self.torrents.push(handle);
        Ok(id)
    }

    /// Returns a snapshot of a single torrent's state.
    pub async fn torrent_info(&self, id: TorrentId) -> Option<TorrentInfo> {
        let info = self.torrents.iter().find(|h| h.id == id)?.info.clone();
        Some(info.read().await.clone())
    }

    /// Returns snapshots of all torrents.
    pub async fn all_torrents(&self) -> Vec<TorrentInfo> {
        let infos: Vec<_> = self.torrents.iter().map(|h| h.info.clone()).collect();
        let mut result = Vec::with_capacity(infos.len());
        for info in infos {
            result.push(info.read().await.clone());
        }
        result
    }

    pub async fn pause(&self, id: TorrentId) {
        self.send_cmd(id, TorrentCommand::Pause).await;
    }

    pub async fn resume(&self, id: TorrentId) {
        self.send_cmd(id, TorrentCommand::Resume).await;
    }

    pub async fn stop(&self, id: TorrentId) {
        self.send_cmd(id, TorrentCommand::Stop).await;
    }

    /// Remove a torrent: cancel its task, delete resume data, and
    /// clean up the session and routing map.
    #[instrument(skip(self))]
    pub async fn remove(&mut self, id: TorrentId) -> Result<()> {
        if let Some(pos) = self.torrents.iter().position(|h| h.id == id) {
            let handle = self.torrents.remove(pos);
            let info_hash = handle.info.read().await.info_hash;
            let file_name = hex::encode(info_hash);
            let _ = handle.ctrl_tx.send(TorrentCommand::Cancel).await;
            let _ = tokio::fs::remove_file(
                dirs::data_dir()
                    .unwrap_or_default()
                    .join("pirate")
                    .join(format!("{}.dat", file_name)),
            )
            .await;
            info!("Resume data deleted for [{id}]");
            let torrent_path_to_remove = handle.info.read().await.torrent_path.clone();
            self.session
                .inner
                .retain(|e| e.torrent_path.ne(&torrent_path_to_remove));
            self.info_hash_map.lock().await.remove(&info_hash);
            info!("Torrent [{id}] removed");
            Ok(())
        } else {
            warn!("Remove failed: torrent [{id}] not found");
            Err(anyhow::Error::msg("torrent not found"))
        }
    }

    /// Gracefully shut down: abort the listener, stop all torrents,
    /// and persist config + session.
    #[instrument(skip(self))]
    pub async fn shutdown(&mut self) {
        if let Some(handle) = self.listener_handle.take() {
            handle.abort();
        }
        let count = self.torrents.len();
        info!("Shutting down, stopping {count} torrent(s)...");
        for handle in self.torrents.drain(..) {
            let _ = handle.ctrl_tx.send(TorrentCommand::Stop).await;
        }
        self.config.save("config").await.unwrap();
        self.session.save("session").await.unwrap();
        info!("Config and session saved");
    }

    /// Send a control command to a specific torrent by ID.
    async fn send_cmd(&self, id: TorrentId, cmd: TorrentCommand) {
        if let Some(handle) = self.torrents.iter().find(|h| h.id == id) {
            let _ = handle.ctrl_tx.send(cmd).await;
        }
    }

    /// Bind a TCP listener on the configured port. For each incoming
    /// connection, read the 68-byte BitTorrent handshake, extract the
    /// info hash, and forward the stream to the matching torrent task.
    async fn start_listener(&mut self) -> Result<()> {
        let map = self.info_hash_map.clone();
        let listener = TcpListener::bind(("0.0.0.0", self.config.listen_port)).await?;

        self.listener_handle = Some(tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((mut stream, _addr)) => {
                        // Read the full 68-byte handshake to extract info_hash.
                        let mut buf = [0u8; 68];
                        if stream.read_exact(&mut buf).await.is_err() {
                            continue;
                        }
                        // Info hash is at bytes 28–47 of the handshake.
                        let info_hash: [u8; 20] = match buf[28..48].try_into() {
                            Ok(h) => h,
                            Err(_) => continue,
                        };
                        if let Some(tx) = map.lock().await.get(&info_hash) {
                            let _ = tx.send(TorrentCommand::NewPeer(stream)).await;
                        }
                    }
                    Err(e) => {
                        error!("Accept failed: {}", e);
                    }
                }
            }
        }));
        Ok(())
    }

    /// Drain the log buffer and return captured log text for the TUI.
    pub fn get_log(&self) -> String {
        let result = self.log_buffer.get_logs();
        self.log_buffer.clear();
        result
    }
}
