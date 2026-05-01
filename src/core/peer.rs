use crate::core::torrent::Torrent;
use std::net::Ipv4Addr;
use tokio::net::UdpSocket;

#[derive(Debug)]
pub struct Peer {
    ip: Ipv4Addr,
    port: u16,
}

impl Peer {
    pub async fn get_peers(torrent: Torrent) -> Result<Vec<Peer>, Box<dyn std::error::Error>> {
        let socket = UdpSocket::bind("0.0.0.0:6881").await?;

        socket.connect(torrent.announce).await?;

        let mut connection_req = ConnectionRequest::default().serialize();

        socket.send(&connection_req).await?;

        let mut res = [0u8; 16];
        socket.recv(&mut res).await?;

        let connection_id = &res[8..16];

        let mut announce_req = AnnounceRequest::default();
        announce_req.connection_id = i64::from_be_bytes(connection_id.try_into()?);
        announce_req.info_hash = torrent.info.get_info_hash()?;
        announce_req.left = torrent.info.total_len()?;

        socket.send(&announce_req.serialize()).await?;

        todo!()
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
