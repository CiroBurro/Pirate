use crate::core::torrent::Torrent;
use anyhow::{Context, Result};
use std::net::Ipv4Addr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpSocket, TcpStream},
};
use tracing::{info, instrument};

#[derive(Debug)]
pub struct Peer {
    pub ip: Ipv4Addr,
    pub port: u16,
    pub status: PeerStatus,
}

impl Peer {
    #[instrument(skip(torrent))]
    pub async fn handshake(&self, torrent: &Torrent) -> Result<TcpStream> {
        let mut handshake = PeerHandshake::default();
        handshake.info_hash = torrent.info_hash;

        let socket = TcpSocket::new_v4().context("[!] Failed to create a new TCP socket")?;

        let mut stream = socket
            .connect(self.to_string().parse()?)
            .await
            .with_context(|| {
                format!(
                    "[!] Failed to connect to the socket to the address: {}",
                    self.to_string()
                )
            })?;

        info!("Connection to the peer {} successful", self.to_string());

        stream
            .write_all(&handshake.serialize())
            .await
            .with_context(|| {
                format!(
                    "[!] Failed to send the handshale request to the peer: {}",
                    self.to_string()
                )
            })?;

        let mut buf = [0u8, 68];
        stream.read_exact(&mut buf).await.with_context(|| {
            format!(
                "Failed to read the handshake response from peer: {}",
                self.to_string()
            )
        })?;

        Ok(stream)
    }
}

impl ToString for Peer {
    fn to_string(&self) -> String {
        format!("{}:{}", self.ip, self.port)
    }
}

struct PeerHandshake {
    pstr: String,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
}

impl Default for PeerHandshake {
    fn default() -> Self {
        Self {
            pstr: String::from("BitTorrent protocol"),
            info_hash: [0_u8; 20],
            peer_id: *b"-PR0001-012345678901",
        }
    }
}

impl PeerHandshake {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(68);

        buf.extend_from_slice(&19_i32.to_be_bytes());
        buf.extend_from_slice(self.pstr.as_bytes());
        buf.extend_from_slice(&[0_u8; 8]);
        buf.extend_from_slice(&self.info_hash);
        buf.extend_from_slice(&self.peer_id);

        buf
    }
}

#[derive(Debug)]
pub struct PeerStatus {
    pub choked: bool,
    pub stream: Option<TcpStream>,
}

impl Default for PeerStatus {
    fn default() -> Self {
        Self {
            choked: false,
            stream: None,
        }
    }
}
