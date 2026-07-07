use crate::core::{bitfield::BitField, message::Message};
use anyhow::{bail, Context, Result};
use std::net::IpAddr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tracing::{info, instrument};

#[derive(Debug)]
pub struct Peer {
    pub ip: IpAddr,
    pub port: u16,
    pub status: PeerStatus,
}

impl Peer {
    pub fn from_stream(stream: TcpStream) -> Result<Self> {
        let addr = stream.peer_addr()?;
        let status = PeerStatus {
            am_choked: true,
            stream: Some(stream),
            bitfield: BitField::default(),
        };
        Ok(Self {
            ip: addr.ip(),
            port: addr.port(),
            status,
        })
    }

    #[instrument(skip(info_hash, peer_id))]
    pub async fn handshake(&mut self, info_hash: &[u8; 20], peer_id: &[u8; 20]) -> Result<()> {
        let handshake = PeerHandshake {
            info_hash: *info_hash,
            peer_id: *peer_id,
            ..Default::default()
        };

        let mut stream = TcpStream::connect(self.to_string().parse::<std::net::SocketAddr>()?)
            .await
            .with_context(|| format!("Failed to connect to peer: {}", self))?;

        info!("Connection to the peer {} successful", self.to_string());

        stream
            .write_all(&handshake.serialize())
            .await
            .with_context(|| format!("Failed to send handshake to peer: {}", self))?;

        let mut buf = [0u8; 68];
        stream.read_exact(&mut buf).await.with_context(|| {
            format!("Failed to read handshake response from peer: {}", self)
        })?;

        self.status.stream = Some(stream);

        Ok(())
    }

    pub async fn send_msg(&mut self, message: Message) -> Result<()> {
        if let Some(stream) = self.status.stream.as_mut() {
            stream
                .write_all(&message.serialize())
                .await
                .with_context(|| format!("Failed to send message to peer: {}", self))?;
        } else {
            bail!("No open stream with peer: {}", self);
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
            stream
                .read_exact(&mut data)
                .await
                .with_context(|| format!("Failed to read message from peer: {}", self))?;

            Message::parse(len, data)
        } else {
            bail!("No open stream with peer: {}", self);
        }
    }
}

impl std::fmt::Display for Peer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.ip, self.port)
    }
}

pub struct PeerHandshake {
    pstr: String,
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
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
    pub(crate) fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(68);

        buf.push(19u8);
        buf.extend_from_slice(self.pstr.as_bytes());
        buf.extend_from_slice(&[0_u8; 8]);
        buf.extend_from_slice(&self.info_hash);
        buf.extend_from_slice(&self.peer_id);

        buf
    }
}

#[derive(Debug, Default)]
pub struct PeerStatus {
    pub am_choked: bool,
    pub stream: Option<TcpStream>,
    pub bitfield: BitField,
}

#[derive(Copy, Clone)]
pub struct SharedPeerCtrl {
    pub am_choking: bool,
    pub peer_interested: bool,
    pub uploaded: u64,
    pub uploaded_prev: u64,
}

impl Default for SharedPeerCtrl {
    fn default() -> Self {
        Self {
            am_choking: true,
            peer_interested: false,
            uploaded: 0,
            uploaded_prev: 0,
        }
    }
}

pub enum PeerCommand {
    Choke,
    Unchoke,
}
