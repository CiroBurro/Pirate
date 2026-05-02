use crate::core::torrent::Torrent;
use anyhow::{bail, Context, Result};
use std::{net::Ipv4Addr, time::Duration};
use tokio::{net::UdpSocket, time::timeout};

#[derive(Debug)]
pub struct Peer {
    ip: Ipv4Addr,
    port: u16,
}

impl Peer {
    pub async fn get_peers(torrent: Torrent) -> Result<Vec<Peer>> {
        let info_hash = torrent
            .info
            .get_info_hash()
            .context("[!] Failed to calculate the info hash")?;
        let left_len = torrent
            .info
            .total_len()
            .context("[!] Failed to get the total length of the files to download")?;

        let mut trackers = vec![torrent.announce.clone()];

        if let Some(announce_list) = &torrent.announce_list {
            for tier in announce_list {
                for tracker in tier {
                    if !trackers.contains(tracker) {
                        trackers.push(tracker.clone());
                    }
                }
            }
        }

        let socket = UdpSocket::bind("0.0.0.0:6881")
            .await
            .context("[!] Failed to bind socket to the following address: '0.0.0.0:6881'")?;

        for tracker_url in trackers {
            if !tracker_url.starts_with("udp://") {
                continue;
            }

            let parsed_url = match url::Url::parse(&tracker_url) {
                Ok(url) => url,
                Err(_) => continue,
            };

            let host = match parsed_url.host_str() {
                Some(h) => h,
                None => continue,
            };

            let port = parsed_url.port().unwrap_or(80);
            let tracker_address = format!("{}:{}", host, port);

            println!("[*] Trying to connect to tracker: {}", tracker_url);

            if socket.connect(&tracker_address).await.is_err() {
                continue;
            }

            let connection_req = ConnectionRequest::default().serialize();

            if socket.send(&connection_req).await.is_err() {
                continue;
            }

            let mut res = [0u8; 16];
            if timeout(Duration::from_secs(2), socket.recv(&mut res))
                .await
                .is_err()
            {
                println!("[-] Connection timeout for {}", tracker_url);
                continue;
            }

            let connection_id = &res[8..16];

            let mut announce_req = AnnounceRequest::default();
            announce_req.connection_id =
                i64::from_be_bytes(connection_id.try_into().unwrap_or([0; 8]));

            announce_req.info_hash = info_hash;
            announce_req.left = left_len;

            if socket.send(&announce_req.serialize()).await.is_err() {
                continue;
            }

            let mut peer_res = vec![0u8; 1024];
            let size = match tokio::time::timeout(
                std::time::Duration::from_secs(2),
                socket.recv(&mut peer_res),
            )
            .await
            {
                Ok(Ok(s)) => s,
                Ok(Err(e)) => {
                    println!("[!] Failed to receive peers from {tracker_url}\nError: {e}");
                    continue;
                }
                _ => {
                    println!("[-] Announce connection timeout for {}", tracker_url);
                    continue;
                }
            };

            peer_res.truncate(size);

            if let Ok(parsed_res) = AnnounceResponse::parse(peer_res) {
                println!(
                    "[+] Received {} peer from {}",
                    parsed_res.peers.len(),
                    tracker_url
                );
                return Ok(parsed_res.peers);
            }
        }

        bail!("[!] All trackers failed");
    }
}

#[derive(Debug)]
pub struct ConnectionRequest {
    protocol_id: i64,
    action: u32,
    transaction_id: u64,
}

impl Default for ConnectionRequest {
    fn default() -> Self {
        Self {
            protocol_id: 0x41727101980i64,
            action: 0,
            transaction_id: 12345,
        }
    }
}

impl ConnectionRequest {
    pub fn serialize(&self) -> Vec<u8> {
        let mut req = Vec::with_capacity(16);
        req.extend_from_slice(&self.protocol_id.to_be_bytes());
        req.extend_from_slice(&self.action.to_be_bytes());
        req.extend_from_slice(&self.transaction_id.to_be_bytes());

        req
    }
}

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

impl Default for AnnounceRequest {
    fn default() -> Self {
        Self {
            connection_id: 0,
            action: 1,
            transaction_id: 12345,
            info_hash: [0; 20],
            peer_id: *b"-PR0001-012345678901",
            downloaded: 0,
            left: 0,
            uploaded: 0,
            event: 0,
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

#[derive(Debug)]
pub struct AnnounceResponse {
    action: i32,
    transaction_id: i32,
    interval: i32,
    leechers: i32,
    seeders: i32,
    peers: Vec<Peer>,
}

impl AnnounceResponse {
    pub fn parse(res: Vec<u8>) -> Result<Self> {
        let slice = res.as_slice();

        let mut peers: Vec<Peer> = Vec::new();

        for chunk in slice[20..].chunks_exact(6) {
            peers.push(Peer {
                ip: Ipv4Addr::from_octets(
                    chunk[0..4]
                        .try_into()
                        .context("[!] Failed to convert ip address bytes into Ipv4Addr struct")?,
                ),
                port: u16::from_be_bytes(
                    chunk[4..]
                        .try_into()
                        .context("[!] Failed to convert port bytes into an integer")?,
                ),
            });
        }

        Ok(Self {
            action: i32::from_be_bytes(
                slice[0..4]
                    .try_into()
                    .context("[!] Failed to convert action bytes into an integer")?,
            ),
            transaction_id: i32::from_be_bytes(
                slice[4..8]
                    .try_into()
                    .context("[!] Failed to convert transaction id bytes into an integer")?,
            ),
            interval: i32::from_be_bytes(
                slice[8..12]
                    .try_into()
                    .context("[!] Failed to convert interval bytes into an integer")?,
            ),
            leechers: i32::from_be_bytes(
                slice[12..16]
                    .try_into()
                    .context("[!] Failed to convert leechers bytes into an integer")?,
            ),
            seeders: i32::from_be_bytes(
                slice[16..20]
                    .try_into()
                    .context("[!] Failed to convert seeders bytes into an integer")?,
            ),
            peers,
        })
    }
}
