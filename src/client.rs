use crate::core::torrent::{
    Torrent, TorrentCommand, TorrentHandle, TorrentId, TorrentInfo, TorrentState,
};
use crate::core::torrent_file::TorrentFile;
use anyhow::Result;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc, Mutex};
use tracing::{error, info};

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

pub struct Config {
    pub peer_id: [u8; 20],
    pub download_dir: PathBuf,
    pub listen_port: u16,
    pub max_peers_per_torrent: usize,
    pub max_uploads: usize,
}

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
    torrents: Vec<TorrentHandle>,
    info_hash_map: Arc<Mutex<HashMap<[u8; 20], mpsc::Sender<TorrentCommand>>>>,
    next_id: TorrentId,
    event_tx: broadcast::Sender<ClientEvent>,
}

impl Client {
    pub fn new(config: Config) -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            config,
            torrents: Vec::new(),
            info_hash_map: Arc::new(Mutex::new(HashMap::new())),
            next_id: 1,
            event_tx,
        }
    }

    pub fn events(&self) -> broadcast::Receiver<ClientEvent> {
        self.event_tx.subscribe()
    }

    pub async fn add_torrent(&mut self, path: PathBuf) -> Result<TorrentId> {
        let torrent_file = TorrentFile::from_file(path).await?;
        let torrent = Torrent::new(torrent_file, self.config.peer_id).await?;
        let info_hash = torrent.info_hash;
        let id = self.next_id;
        self.next_id += 1;
        let handle = torrent.spawn(id, &self.config, self.event_tx.clone())?;
        self.emit(ClientEvent::TorrentAdded {
            id,
            name: handle.info.read().await.name.clone(),
        });
        self.info_hash_map
            .lock()
            .await
            .insert(info_hash, handle.ctrl_tx.clone());
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

    pub async fn remove(&mut self, id: TorrentId) {
        if let Some(pos) = self.torrents.iter().position(|h| h.id == id) {
            let handle = self.torrents.remove(pos);
            let info_hash = handle.info.read().await.info_hash;
            let _ = handle.ctrl_tx.send(TorrentCommand::Cancel).await;
            self.info_hash_map.lock().await.remove(&info_hash);
            self.emit(ClientEvent::TorrentRemoved(id));
        }
    }

    pub async fn shutdown(&mut self) {
        for handle in self.torrents.drain(..) {
            let _ = handle.ctrl_tx.send(TorrentCommand::Stop).await;
        }
    }

    async fn send_cmd(&self, id: TorrentId, cmd: TorrentCommand) {
        if let Some(handle) = self.torrents.iter().find(|h| h.id == id) {
            let _ = handle.ctrl_tx.send(cmd).await;
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
