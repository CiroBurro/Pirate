use crate::core::torrent::Torrent;

pub struct Client {
    pub peer_id: [u8; 20],
    pub torrents: Vec<Torrent>,
}

impl Default for Client {
    fn default() -> Self {
        Self {
            peer_id: *b"-PR1337-012345678901",
            torrents: Vec::new(),
        }
    }
}
