//! Peer connection management — TCP handshake and message I/O.
//!
//! Defines the [`Peer`] struct that wraps a TCP connection to a remote peer,
//! the [`PeerHandshake`] for the BitTorrent handshake protocol, and shared
//! control state used by the choking algorithm.

use crate::core::{bitfield::BitField, message::Message};
use anyhow::{bail, Context, Result};
use std::net::IpAddr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tracing::{info, instrument};

/// A remote peer in the swarm.
#[derive(Debug)]
pub struct Peer {
    pub ip: IpAddr,
    pub port: u16,
    /// Mutable connection state (stream, bitfield, choke status).
    pub status: PeerStatus,
}

impl PartialEq for Peer {
    fn eq(&self, other: &Self) -> bool {
        self.ip.eq(&other.ip) && self.port.eq(&other.port)
    }
}

impl Peer {
    /// Create a [`Peer`] from an already-accepted TCP connection
    /// (used for incoming peers routed via the TCP listener).
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

    /// Perform the BitTorrent handshake over a new TCP connection.
    ///
    /// 1. Connects to the peer's IP:port.
    /// 2. Sends our 68-byte handshake (pstr="BitTorrent protocol", info_hash, peer_id).
    /// 3. Reads the peer's 68-byte handshake response.
    /// 4. Stores the connected stream in `self.status`.
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
        stream
            .read_exact(&mut buf)
            .await
            .with_context(|| format!("Failed to read handshake response from peer: {}", self))?;

        self.status.stream = Some(stream);

        Ok(())
    }

    /// Serialize and send a protocol message over the peer's TCP stream.
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

    /// Read one protocol message from the peer's TCP stream.
    ///
    /// First reads the 4-byte length prefix, then reads the payload.
    /// A length of 0 indicates a keep-alive message.
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

/// The 68-byte BitTorrent handshake payload.
///
/// Wire format:
/// - 1 byte: length of the protocol string (always 19)
/// - 19 bytes: `"BitTorrent protocol"`
/// - 8 bytes: reserved (zeroed)
/// - 20 bytes: info hash
/// - 20 bytes: peer ID
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
    /// Serialize the handshake into the standard 68-byte wire format.
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

/// Mutable state associated with a peer connection.
#[derive(Debug, Default)]
pub struct PeerStatus {
    /// Whether the remote peer is choking us (we cannot request blocks).
    pub am_choked: bool,
    /// The underlying TCP stream (None before handshake / after disconnect).
    pub stream: Option<TcpStream>,
    /// Bitfield received from the peer indicating which pieces they have.
    pub bitfield: BitField,
}

/// Per-peer control state shared between the torrent main loop and
/// the per-peer async task via `Arc<Mutex<Vec<SharedPeerCtrl>>>`.
///
/// Used by the unchoke/recalc algorithm and pipeline management.
#[derive(Clone)]
pub struct SharedPeerCtrl {
    /// Whether we are choking this peer.
    pub am_choking: bool,
    /// Whether the peer has expressed interest in our pieces.
    pub peer_interested: bool,
    /// Cumulative bytes uploaded to this peer.
    pub uploaded: u64,
    /// Snapshot of `uploaded` at the last unchoke recalculation.
    pub uploaded_prev: u64,
    /// Maximum number of concurrent in-flight block requests to this peer.
    pub max_pipeline: usize,
    /// Number of requests currently sent but not yet answered.
    pub pending_requests: usize,
}

impl Default for SharedPeerCtrl {
    fn default() -> Self {
        Self {
            am_choking: true,
            peer_interested: false,
            uploaded: 0,
            uploaded_prev: 0,
            max_pipeline: 20,
            pending_requests: 0,
        }
    }
}

/// Commands sent from the torrent main loop to a peer's dedicated task.
pub enum PeerCommand {
    Choke,
    Unchoke,
    /// Signal the peer to request more blocks from its piece picker.
    RequestBlock,
}
