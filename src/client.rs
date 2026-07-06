use crate::core::torrent::{
    Torrent, TorrentCommand, TorrentHandle, TorrentId, TorrentInfo, TorrentState,
};
use crate::core::torrent_file::TorrentFile;
use crate::persistence::resume_data::ResumeData;
use crate::persistence::session::{Session, SessionEntry};
use crate::persistence::Persistent;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc, Mutex};
use tracing::{debug, error, info, instrument, warn};

#[derive(Clone, Debug)]
pub enum ClientEvent {
    TorrentAdded {
        id: TorrentId,
        name: String,
    },
    TorrentRemoved(TorrentId),
    StateChanged {
        id: TorrentId,
        state: TorrentState,
    },
    Progress {
        id: TorrentId,
        progress: f64,
        downloaded: u64,
        download_rate: f64,
        upload_rate: f64,
    },
    PieceCompleted {
        id: TorrentId,
        index: usize,
    },
    TorrentCompleted(TorrentId),
    PeerConnected {
        id: TorrentId,
        peer: String,
    },
    PeerDisconnected {
        id: TorrentId,
        peer: String,
    },
    Error {
        id: TorrentId,
        message: String,
    },
    ClientError(String),
}

#[derive(Serialize, Deserialize)]
pub struct Config {
    pub peer_id: [u8; 20],
    pub download_dir: PathBuf,
    pub listen_port: u16,
    pub max_peers_per_torrent: usize,
    pub max_uploads: usize,
}

impl Persistent for Config {}

impl Default for Config {
    fn default() -> Self {
        Self {
            peer_id: *b"-PR1337-012345678901",
            download_dir: dirs::download_dir().unwrap_or_default(),
            listen_port: 6881,
            max_peers_per_torrent: 50,
            max_uploads: 8,
        }
    }
}

pub struct Client {
    config: Config,
    session: Session,
    torrents: Vec<TorrentHandle>,
    info_hash_map: Arc<Mutex<HashMap<[u8; 20], mpsc::Sender<TorrentCommand>>>>,
    next_id: TorrentId,
    event_tx: broadcast::Sender<ClientEvent>,
}

impl Client {
    #[instrument(skip_all)]
    pub async fn new() -> Result<Self> {
        let config: Config = Config::load("config").await.unwrap_or_default();
        let session: Session = Session::load("session").await.unwrap_or_default();
        info!("Config loaded; listen_port={}", config.listen_port);
        info!(
            "Session loaded with {} torrent(s)",
            session.inner.len()
        );
        let (event_tx, _) = broadcast::channel(256);
        let mut result = Self {
            config,
            torrents: Vec::new(),
            session: session.clone(),
            info_hash_map: Arc::new(Mutex::new(HashMap::new())),
            next_id: 1,
            event_tx,
        };

        for entry in session.inner {
            let _ = result
                .add_torrent(entry.torrent_path, Some(entry.download_dir))
                .await?;
        }
        Ok(result)
    }

    pub fn events(&self) -> broadcast::Receiver<ClientEvent> {
        self.event_tx.subscribe()
    }

    #[instrument(skip(self, torrent_path))]
    pub async fn add_torrent(
        &mut self,
        torrent_path: PathBuf,
        download_dir: Option<PathBuf>,
    ) -> Result<TorrentId> {
        let torrent_file = TorrentFile::from_file(torrent_path.clone()).await?;
        let torrent = Torrent::new(torrent_file, self.config.peer_id).await?;
        let info_hash = torrent.info_hash;
        let info_hash_hex = hex::encode(info_hash);
        let mut downloaded = 0;
        if let Ok(resume_data) = ResumeData::load(&info_hash_hex).await {
            if resume_data.bitfield_data.len()
                == torrent.piece_picker.lock().await.bitfield.data.len()
            {
                torrent
                    .piece_picker
                    .lock()
                    .await
                    .bitfield
                    .data
                    .copy_from_slice(&resume_data.bitfield_data);
                info!("Resume data restored for {}", info_hash_hex);
            } else {
                warn!("Resume data bitfield size mismatch for {}, ignoring", info_hash_hex);
            }
            downloaded = resume_data.downloaded;
        }
        let id = self.next_id;
        self.next_id += 1;
        let download_dir = download_dir.unwrap_or(self.config.download_dir.clone());
        let handle = torrent.spawn(
            id,
            download_dir.clone(),
            torrent_path.clone(),
            self.event_tx.clone(),
        )?;
        handle.info.write().await.downloaded = downloaded;
        self.info_hash_map
            .lock()
            .await
            .insert(info_hash, handle.ctrl_tx.clone());

        let entry = SessionEntry {
            torrent_path,
            download_dir,
        };

        if !self.session.inner.contains(&entry) {
            self.session.add(entry);
        }

        let name = handle.info.read().await.name.clone();
        info!("Torrent added: [{id}] {name}");
        self.emit(ClientEvent::TorrentAdded { id, name });
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
        debug!("Pausing torrent [{id}]");
        self.send_cmd(id, TorrentCommand::Pause).await;
    }

    pub async fn resume(&self, id: TorrentId) {
        debug!("Resuming torrent [{id}]");
        self.send_cmd(id, TorrentCommand::Resume).await;
    }

    pub async fn stop(&self, id: TorrentId) {
        debug!("Stopping torrent [{id}]");
        self.send_cmd(id, TorrentCommand::Stop).await;
    }

    #[instrument(skip(self))]
    pub async fn remove(&mut self, id: TorrentId) -> Result<()> {
        if let Some(pos) = self.torrents.iter().position(|h| h.id == id) {
            let handle = self.torrents.remove(pos);
            let info_hash = handle.info.read().await.info_hash;
            let file_name = hex::encode(info_hash);
            let _ = handle.ctrl_tx.send(TorrentCommand::Cancel).await;
            tokio::fs::remove_file(
                dirs::data_dir()
                    .unwrap_or_default()
                    .join("pirate")
                    .join(format!("{}.dat", file_name)),
            )
            .await?;
            info!("Resume data deleted for [{id}]");
            let torrent_path_to_remove = handle.info.read().await.torrent_path.clone();
            self.session
                .inner
                .retain(|e| e.torrent_path.ne(&torrent_path_to_remove));
            self.info_hash_map.lock().await.remove(&info_hash);
            info!("Torrent [{id}] removed");
            self.emit(ClientEvent::TorrentRemoved(id));
            Ok(())
        } else {
            warn!("Remove failed: torrent [{id}] not found");
            Err(anyhow::Error::msg("torrent not found"))
        }
    }

    #[instrument(skip(self))]
    pub async fn shutdown(mut self) {
        let count = self.torrents.len();
        info!("Shutting down, stopping {count} torrent(s)...");
        for handle in self.torrents.drain(..) {
            let _ = handle.ctrl_tx.send(TorrentCommand::Stop).await;
        }
        self.config.save("config").await.unwrap();
        self.session.save("session").await.unwrap();
        info!("Config and session saved");
    }

    async fn send_cmd(&self, id: TorrentId, cmd: TorrentCommand) {
        if let Some(handle) = self.torrents.iter().find(|h| h.id == id) {
            let _ = handle.ctrl_tx.send(cmd).await;
        } else {
            debug!("Command dropped: torrent [{id}] not found");
        }
    }

    fn emit(&self, event: ClientEvent) {
        let _ = self.event_tx.send(event);
    }
    async fn start_listener(&self) -> Result<()> {
        let map = self.info_hash_map.clone();
        let listener = TcpListener::bind(("0.0.0.0", self.config.listen_port)).await?;

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((mut stream, addr)) => {
                        info!("Incoming connection from {}", addr);
                        let mut buf = [0u8; 68];
                        if stream.read_exact(&mut buf).await.is_err() {
                            continue;
                        }
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
        });
        Ok(())
    }
}
