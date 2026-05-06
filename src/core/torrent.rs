use crate::core::{
    peer::Peer,
    piece::{Piece, PiecePicker},
    torrent_file::TorrentFile,
    tracker::get_peers,
};
use anyhow::{Context, Result};
use tracing::{info, instrument};

pub struct Torrent {
    pub peer_id: [u8; 20],
    pub file: TorrentFile,
    pub info_hash: [u8; 20],
    pub peers: Vec<Peer>,
    pub piece_picker: PiecePicker,
}

impl Torrent {
    #[instrument(skip(file, peer_id))]
    pub async fn new(file: TorrentFile, peer_id: [u8; 20]) -> Result<Self> {
        let info_hash = &file
            .info
            .get_info_hash()
            .context("[!] Failed to calculate info hash")?;

        let num_pieces: usize = file.info.pieces.len() / 20;
        let piece_length: usize = file.info.piece_length as usize;
        let total_len = file.info.total_len()?;

        let mut pieces: Vec<Piece> = Vec::with_capacity(num_pieces);

        for (i, hash) in file.info.pieces.chunks_exact(20).enumerate() {
            let hash_array: [u8; 20] = hash.try_into().context("Hash size mismatch")?;

            let is_last_piece = i == num_pieces - 1;
            let this_piece_length = if is_last_piece {
                total_len.saturating_sub(i * piece_length)
            } else {
                piece_length
            };

            let piece = Piece::new(i, hash_array, this_piece_length);
            pieces.push(piece);
        }

        let piece_picker = PiecePicker::new(pieces);

        let mut torrent = Self {
            peer_id,
            file,
            info_hash: *info_hash,
            peers: Vec::new(),
            piece_picker,
        };

        let peers = get_peers(&torrent)
            .await
            .context("[!] Failed to get peers from tracker")?;

        torrent.peers = peers;
        info!("Torrent created succesfully");

        Ok(torrent)
    }
}
