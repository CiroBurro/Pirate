//! UDP tracker protocol implementation.
//!
//! Implements the UDP tracker protocol as defined in
//! [BEP 15](https://www.bittorrent.org/beps/bep_0015.html).
//!
//! The protocol is a two-phase request over UDP:
//! 1. **Connection request** — obtain a connection ID from the tracker.
//! 2. **Announce request** — send the info hash / peer ID to receive a peer list.

use crate::core::{peer::{Peer, PeerStatus}, torrent::Torrent};
use anyhow::{bail, Context, Result};
use std::{net::{IpAddr, Ipv4Addr}, time::Duration};
use tokio::{net::UdpSocket, task::JoinSet, time::timeout};
use tracing::{info, instrument};

/// Collect all unique tracker URLs from the torrent's announce list.
///
/// Merges the primary `announce` URL with any `announce-list` tiers (BEP 12).
fn read_trackers(torrent: &Torrent) -> Vec<String> {
    let mut trackers = vec![torrent.file.announce.clone()];

    if let Some(announce_list) = &torrent.file.announce_list {
        for tier in announce_list {
            for tracker in tier {
                if !trackers.contains(tracker) {
                    trackers.push(tracker.clone());
                }
            }
        }
    }
    trackers
}

/// Contact all trackers concurrently and collect the merged peer list.
///
/// Spawns a concurrent task per tracker URL. Only the peers from
/// successful responses are retained. Bails if every tracker fails.
#[instrument(skip(torrent))]
pub async fn get_peers(torrent: &Torrent) -> Result<Vec<Peer>> {
    let info_hash = torrent.info_hash;
    let peer_id = torrent.peer_id;
    let left_len = torrent
        .file
        .info
        .total_len()
        .context("Failed to get the total length of the files to download")?;

    let trackers = read_trackers(torrent);

    info!(
        "Found {} trackers inside the announce list.",
        trackers.len()
    );

    let mut set = JoinSet::new();
    for tracker_url in trackers {
        set.spawn(
            async move { tracker_handshake(tracker_url, info_hash, peer_id, left_len, 0).await },
        );
    }

    let peers = set
        .join_all()
        .await
        .into_iter()
        .filter(|r| r.is_ok())
        .flat_map(|r| r.unwrap().into_iter())
        .collect::<Vec<_>>();

    if peers.is_empty() {
        bail!("All trackers failed");
    }
    Ok(peers)
}

/// Send a "completed" event to all trackers (event=1, left=0).
///
/// Called when a download finishes so trackers can update swarm stats.
#[instrument(skip(torrent))]
pub async fn complete_msg(torrent: &Torrent) {
    let info_hash = torrent.info_hash;
    let peer_id = torrent.peer_id;
    let left_len = 0;

    let trackers = read_trackers(torrent);

    let mut set = JoinSet::new();
    for tracker_url in trackers {
        set.spawn(
            async move { tracker_handshake(tracker_url, info_hash, peer_id, left_len, 1).await },
        );
    }

    set.join_all().await;
}

/// Full UDP tracker handshake: connect → announce → parse peers.
///
/// # Errors
///
/// Returns an error if the URL is not `udp://`, if any network operation
/// times out (2 s per phase), or if the response is malformed.
#[instrument(skip(info_hash, peer_id, left_len))]
pub async fn tracker_handshake(
    url: String,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    left_len: usize,
    event: u32,
) -> Result<Vec<Peer>> {
    if !url.starts_with("udp://") {
        bail!("Tracker URL must start with 'udp://'");
    }

    let parsed_url = match url::Url::parse(&url) {
        Ok(url) => url,
        Err(e) => bail!("Failed to parse tracker URL: {e}"),
    };

    let host = match parsed_url.host_str() {
        Some(h) => h,
        None => bail!("Failed to extract host from tracker URL"),
    };

    let port = parsed_url.port().unwrap_or(80);
    let tracker_address = format!("{}:{}", host, port);

    let socket = UdpSocket::bind("0.0.0.0:0")
        .await
        .context("Failed to bind UDP socket")?;

    socket
        .connect(&tracker_address)
        .await
        .with_context(|| format!("Failed to connect to tracker: {url}"))?;

    // --- Phase 1: Connection request ---
    // Send the magic protocol ID and receive a connection ID.
    let connection_req = ConnectionRequest::default().serialize();

    socket
        .send(&connection_req)
        .await
        .with_context(|| format!("Failed to send connection request to tracker: {url}"))?;

    let mut res = [0u8; 16];
    timeout(Duration::from_secs(2), socket.recv(&mut res))
        .await
        .with_context(|| format!("Failed to receive connection ID from tracker: {url}"))?
        .with_context(|| format!("Tracker timeout: {url}"))?;

    let connection_id = &res[8..16];
    let connection_id = i64::from_be_bytes(connection_id.try_into().unwrap_or([0; 8]));

    // --- Phase 2: Announce request ---
    let announce_req =
        AnnounceRequest::new(connection_id, info_hash, peer_id, left_len as i64, event);

    socket
        .send(&announce_req.serialize())
        .await
        .with_context(|| format!("Failed to send announce request to tracker: {url}"))?;

    let mut peer_res = vec![0u8; 1024];
    let size = match tokio::time::timeout(Duration::from_secs(2), socket.recv(&mut peer_res)).await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            bail!("Failed to receive peers from {url}: {e}");
        }
        _ => {
            bail!("Announce connection timeout for {url}");
        }
    };

    peer_res.truncate(size);

    let res = AnnounceResponse::parse(peer_res)
        .with_context(|| format!("Failed to parse announce response from tracker: {url}"))?;

    Ok(res.peers)
}

/// UDP tracker connection request payload (16 bytes).
///
/// Sends the magic protocol ID (`0x41727101980`) to obtain
/// a 64-bit connection ID from the tracker.
#[derive(Debug)]
struct ConnectionRequest {
    protocol_id: i64,
    action: u32,
    transaction_id: u64,
}

impl Default for ConnectionRequest {
    fn default() -> Self {
        Self {
            protocol_id: 0x41727101980i64,
            action: 0, // 0 = connect
            transaction_id: 12345,
        }
    }
}

impl ConnectionRequest {
    fn serialize(&self) -> Vec<u8> {
        let mut req = Vec::with_capacity(16);
        req.extend_from_slice(&self.protocol_id.to_be_bytes());
        req.extend_from_slice(&self.action.to_be_bytes());
        req.extend_from_slice(&self.transaction_id.to_be_bytes());

        req
    }
}

/// UDP tracker announce request payload (98 bytes).
///
/// Sent after obtaining a connection ID. Contains the info hash, peer ID,
/// download state, and event type (0 = started, 1 = completed).
struct AnnounceRequest {
    connection_id: i64,
    action: u32,
    transaction_id: u32,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    downloaded: i64,
    left: i64,
    uploaded: i64,
    event: u32,
    ip_address: u32,
    key: u32,
    num_want: i32,
    port: u16,
}

impl AnnounceRequest {
    fn new(
        connection_id: i64,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
        left: i64,
        event: u32,
    ) -> Self {
        Self {
            connection_id,
            action: 1,
            transaction_id: 12345,
            info_hash,
            peer_id,
            downloaded: 0,
            left,
            uploaded: 0,
            event,
            ip_address: 0,
            key: 0,
            num_want: -1,
            port: 6881,
        }
    }
}

impl AnnounceRequest {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(98);

        buf.extend_from_slice(&self.connection_id.to_be_bytes());
        buf.extend_from_slice(&self.action.to_be_bytes());
        buf.extend_from_slice(&self.transaction_id.to_be_bytes());
        buf.extend_from_slice(&self.info_hash);
        buf.extend_from_slice(&self.peer_id);
        buf.extend_from_slice(&self.downloaded.to_be_bytes());
        buf.extend_from_slice(&self.left.to_be_bytes());
        buf.extend_from_slice(&self.uploaded.to_be_bytes());
        buf.extend_from_slice(&self.event.to_be_bytes());
        buf.extend_from_slice(&self.ip_address.to_be_bytes());
        buf.extend_from_slice(&self.key.to_be_bytes());
        buf.extend_from_slice(&self.num_want.to_be_bytes());
        buf.extend_from_slice(&self.port.to_be_bytes());

        buf
    }
}

/// Parsed announce response from the tracker (BEP 15).
///
/// Header is 20 bytes: action, transaction_id, interval, leechers, seeders.
/// Payload is a sequence of 6-byte peer entries: 4 bytes IPv4 + 2 bytes port.
#[derive(Debug)]
struct AnnounceResponse {
    _action: i32,
    _transaction_id: i32,
    _interval: i32,
    _leechers: i32,
    _seeders: i32,
    peers: Vec<Peer>,
}

impl AnnounceResponse {
    fn parse(res: Vec<u8>) -> Result<Self> {
        let slice = res.as_slice();

        let mut peers: Vec<Peer> = Vec::new();

        // Each peer entry is 6 bytes: 4 for IPv4 address, 2 for port.
        for chunk in slice[20..].chunks_exact(6) {
            peers.push(Peer {
                ip: IpAddr::V4(Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3])),
                port: u16::from_be_bytes(
                    chunk[4..]
                        .try_into()
                        .context("Failed to convert port bytes into an integer")?,
                ),
                status: PeerStatus::default(),
            });
        }

        Ok(Self {
            _action: i32::from_be_bytes(
                slice[0..4]
                    .try_into()
                    .context("Failed to convert action bytes into an integer")?,
            ),
            _transaction_id: i32::from_be_bytes(
                slice[4..8]
                    .try_into()
                    .context("Failed to convert transaction id bytes into an integer")?,
            ),
            _interval: i32::from_be_bytes(
                slice[8..12]
                    .try_into()
                    .context("Failed to convert interval bytes into an integer")?,
            ),
            _leechers: i32::from_be_bytes(
                slice[12..16]
                    .try_into()
                    .context("Failed to convert leechers bytes into an integer")?,
            ),
            _seeders: i32::from_be_bytes(
                slice[16..20]
                    .try_into()
                    .context("Failed to convert seeders bytes into an integer")?,
            ),
            peers,
        })
    }
}
