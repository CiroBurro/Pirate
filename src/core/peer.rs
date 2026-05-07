use crate::core::{bitfield::BitField, message::Message, torrent::Torrent};
use anyhow::{bail, Context, Result};
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
    #[instrument(skip(info_hash, peer_id))]
    pub async fn handshake(&mut self, info_hash: &[u8; 20], peer_id: &[u8; 20]) -> Result<()> {
        let mut handshake = PeerHandshake::default();
        handshake.info_hash = *info_hash;
        handshake.peer_id = *peer_id;

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

        self.status.stream = Some(stream);

        Ok(())
    }

    pub async fn send_msg(&mut self, message: Message) -> Result<()> {
        if let Some(stream) = self.status.stream.as_mut() {
            stream
                .write_all(&message.serialize())
                .await
                .with_context(|| format!("Failed to send message to peer: {}", self.to_string()))?;
        } else {
            bail!(
                "[!] Trying to send a message but there is no open stream with the peer: {}",
                self.to_string()
            );
        }

        Ok(())
    }

    pub async fn read_msg(&mut self) -> Result<Message> {
        if let Some(stream) = self.status.stream.as_mut() {
            let mut buf_len = [0u8; 4];
            stream
                .read_exact(&mut buf_len)
                .await
                .context("Failed to read message length from peer")?;

            let len = u32::from_be_bytes(buf_len);

            if len == 0 {
                return Ok(Message::keep_alive());
            }

            let mut data = vec![0u8; len as usize];
            stream.read_exact(&mut data).await.with_context(|| {
                format!("Failed to read message from peer: {}", self.to_string())
            })?;

            Message::parse(len, data)
        } else {
            bail!(
                "[!] Trying to read a message but there is no open stream with the peer: {}",
                self.to_string()
            );
        }
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
    pub bitfield: BitField,
}

impl Default for PeerStatus {
    fn default() -> Self {
        Self {
            choked: false,
            stream: None,
            bitfield: BitField::new(),
        }
    }
}
