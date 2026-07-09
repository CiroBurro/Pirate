//! Piece and block management — the lowest level of the download engine.
//!
//! A **piece** is a ~512 KB (configurable) chunk of the torrent's data.
//! Each piece is split into **blocks** of [`BLOCK_SIZE`] (16 KB) for wire
//! transfer. The [`PiecePicker`] implements the rarest-first selection
//! strategy to decide which block to request next.

use crate::core::{bitfield::BitField, torrent_file::TorrentFile};
use anyhow::Result;
use sha1::{Digest, Sha1};
use std::path::PathBuf;
use tokio::{fs::OpenOptions, io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom}};

/// Standard block size for wire transfer (16 KB).
const BLOCK_SIZE: usize = 16384;

/// Status of a single block within a piece.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum BlockStatus {
    /// Not yet requested from any peer.
    Free,
    /// Currently requested from a peer (awaiting response).
    Requested,
    /// Data received and stored.
    Downloaded,
}

/// A single block — the atomic unit of data transfer on the wire.
///
/// Blocks are 16 KB each (except possibly the last block of a piece).
#[derive(Debug, Clone, Copy)]
pub struct Block {
    pub piece_index: usize,
    /// Byte offset of this block within its parent piece.
    pub offset: usize,
    pub length: usize,
    pub status: BlockStatus,
    /// Countdown timer for request timeout (decremented every second).
    /// When it reaches 0, the block is freed for re-request.
    pub timer: usize,
}

impl Block {
    pub fn new(piece_index: usize, offset: usize, length: usize) -> Self {
        Self {
            piece_index,
            offset,
            length,
            status: BlockStatus::Free,
            timer: 30,
        }
    }

    /// Serialize this block into the 12-byte `request` message payload:
    /// `[piece_index: u32][offset: u32][length: u32]`.
    pub fn to_payload(&self) -> Vec<u8> {
        [
            (self.piece_index as u32).to_be_bytes(),
            (self.offset as u32).to_be_bytes(),
            (self.length as u32).to_be_bytes(),
        ]
        .concat()
    }
}

/// Lifecycle status of an entire piece.
#[derive(Debug, PartialEq)]
pub enum PieceStatus {
    /// No blocks downloaded yet; available for picking.
    Missing,
    /// At least one block requested or received, but not complete.
    Downloading,
    /// All blocks received; SHA-1 hash verification in progress.
    Verifying,
    /// SHA-1 hash verified; piece is ready.
    Completed,
}

/// A torrent piece — a contiguous range of the download.
#[derive(Debug)]
pub struct Piece {
    pub index: usize,
    /// Expected SHA-1 hash from the .torrent metainfo.
    pub hash: [u8; 20],
    pub status: PieceStatus,
    pub length: usize,
    /// Sub-divisions of this piece (blocks).
    pub blocks: Vec<Block>,
    pub missing_blocks: usize,
    /// Accumulated block data (filled as blocks arrive).
    pub data: Vec<u8>,
}

impl Piece {
    /// Create a new piece, splitting it into [`BLOCK_SIZE`] blocks.
    pub fn new(index: usize, hash: [u8; 20], length: usize) -> Self {
        let num_blocks = length.div_ceil(BLOCK_SIZE);
        let mut blocks = Vec::new();

        for i in 0..num_blocks {
            let offset = i * BLOCK_SIZE;
            let length = if i == num_blocks - 1 {
                length - offset
            } else {
                BLOCK_SIZE
            };
            blocks.push(Block::new(index, offset, length));
        }

        Self {
            index,
            hash,
            status: PieceStatus::Missing,
            length,
            blocks,
            missing_blocks: num_blocks,
            data: vec![0; length],
        }
    }

    /// Write a completed piece to disk.
    ///
    /// Handles both single-file and multi-file torrents by mapping the
    /// piece's global byte offset to the correct file + local offset.
    pub async fn write_to_disk(
        index: usize,
        data: &[u8],
        file: &TorrentFile,
        path: PathBuf,
    ) -> Result<()> {
        let piece_length = file.info.piece_length as usize;
        let mut global_offset = index * piece_length;
        let mut bytes_left = data.len();
        let mut data_offset = 0;

        let mut base_path = path.clone();
        base_path.push(&file.info.name);

        if let Some(files) = &file.info.files {
            // Multi-file torrent: distribute bytes across files.
            let mut current_file_start = 0;
            for file in files {
                let current_file_end = current_file_start + file.length;
                if global_offset >= current_file_start
                    && global_offset < current_file_end
                    && bytes_left > 0
                {
                    let local_offset = global_offset - current_file_start;
                    let bytes_to_write = bytes_left.min(file.length - local_offset);

                    let mut file_path = base_path.clone();
                    for d in &file.path {
                        file_path.push(d);
                    }

                    if let Some(parent) = file_path.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }

                    let mut file_handle = OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(false)
                        .open(&file_path)
                        .await?;

                    file_handle
                        .seek(SeekFrom::Start(local_offset as u64))
                        .await?;
                    file_handle
                        .write_all(&data[data_offset..data_offset + bytes_to_write])
                        .await?;

                    global_offset += bytes_to_write;
                    data_offset += bytes_to_write;
                    bytes_left -= bytes_to_write;
                }
                current_file_start = current_file_end;
                if bytes_left == 0 {
                    break;
                }
            }
        } else {
            // Single-file torrent: direct write at the piece offset.
            if let Some(parent) = base_path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }

            let mut file_handle = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&base_path)
                .await?;

            file_handle
                .seek(SeekFrom::Start(global_offset as u64))
                .await?;
            file_handle.write_all(data).await?
        }
        Ok(())
    }

    /// Read a specific byte range from a piece on disk (used for seeding).
    pub async fn read_from_disk(
        index: usize,
        offset: u32,
        length: u32,
        file: &TorrentFile,
        path: PathBuf,
    ) -> Result<Vec<u8>> {
        let piece_length = file.info.piece_length as usize;
        let mut global_offset = index * piece_length + offset as usize;
        let mut bytes_left = length as usize;

        let mut base_path = path.clone();
        base_path.push(&file.info.name);

        let mut result: Vec<u8> = Vec::with_capacity(length as usize);

        if let Some(files) = &file.info.files {
            let mut current_file_start = 0;
            for file in files {
                let current_file_end = current_file_start + file.length;
                if global_offset >= current_file_start
                    && global_offset < current_file_end
                    && bytes_left > 0
                {
                    let local_offset = global_offset - current_file_start;
                    let bytes_to_read = bytes_left.min(file.length - local_offset);
                    let mut buffer = vec![0; bytes_to_read];
                    let mut file_path = base_path.clone();
                    for d in &file.path {
                        file_path.push(d);
                    }

                    let mut file_handle = OpenOptions::new().read(true).open(&file_path).await?;
                    file_handle
                        .seek(SeekFrom::Start(local_offset as u64))
                        .await?;
                    file_handle.read_exact(&mut buffer).await?;
                    result.extend_from_slice(&buffer);

                    global_offset += bytes_to_read;
                    bytes_left -= bytes_to_read;
                }
                current_file_start = current_file_end;
            }
        } else {
            let mut buffer = vec![0; length as usize];
            let mut file_handle = OpenOptions::new().read(true).open(&base_path).await?;
            file_handle
                .seek(SeekFrom::Start(global_offset as u64))
                .await?;
            file_handle.read_exact(&mut buffer).await?;
            result.extend_from_slice(&buffer);
        }
        Ok(result)
    }

    /// Verify the piece data against the expected SHA-1 hash.
    pub fn verify(&mut self) -> bool {
        let hash = Sha1::digest(&self.data);
        hash == self.hash
    }
}

/// Piece selection logic implementing the **rarest-first** strategy.
///
/// Maintains:
/// - A local [`BitField`] of completed pieces (for upload / have messages).
/// - A frequency map counting how many peers have each piece.
/// - A count of missing (not yet completed) pieces.
#[derive(Debug)]
pub struct PiecePicker {
    pub pieces: Vec<Piece>,
    /// Our own bitfield — which pieces we've completed.
    pub bitfield: BitField,
    /// How many peers have reported having each piece index.
    pub piece_frequencies: Vec<usize>,
    /// Number of pieces still missing (not completed).
    pub missing_pieces: usize,
    /// When paused, `pick()` returns `None`.
    pub paused: bool,
}

impl PiecePicker {
    pub fn new(pieces: Vec<Piece>) -> Self {
        let num_pieces = pieces.len();
        Self {
            pieces,
            bitfield: BitField::with_pieces(num_pieces),
            // All pieces have frequency 0 until a peer bitfield arrives.
            piece_frequencies: vec![0; num_pieces],
            missing_pieces: num_pieces,
            paused: false,
        }
    }

    /// Update frequencies from a peer's bitfield (sent on connect).
    pub fn add_peer_bitfield(&mut self, bitfield: &BitField) {
        for i in 0..self.pieces.len() {
            if bitfield.has_piece(i) {
                self.piece_frequencies[i] += 1;
            }
        }
    }

    /// Increment frequency for a single piece (from a `have` message).
    pub fn add_peer_have(&mut self, index: usize) {
        if index < self.piece_frequencies.len() {
            self.piece_frequencies[index] += 1;
        }
    }

    /// Pick the best block to request next from a given peer.
    ///
    /// Strategy (two-phase):
    /// 1. **Endgame**: if any piece is already in `Downloading` state and
    ///    this peer has it, pick its first free block (keep pipelines full).
    /// 2. **Rarest-first**: among `Missing` pieces that this peer has,
    ///    pick the one with the lowest frequency across all peers.
    pub fn pick(&mut self, bitfield: &BitField) -> Option<Block> {
        if self.paused {
            return None;
        }
        // Phase 1: fill blocks already in progress.
        for piece in self.pieces.iter_mut() {
            if piece.status == PieceStatus::Downloading
                && bitfield.has_piece(piece.index)
                && let Some(block) = piece
                    .blocks
                    .iter_mut()
                    .find(|b| b.status == BlockStatus::Free)
            {
                block.status = BlockStatus::Requested;
                return Some(*block);
            }
        }

        // Phase 2: pick a new rarest missing piece.
        let mut missing_pieces: Vec<&mut Piece> = self
            .pieces
            .iter_mut()
            .filter(|p| p.status == PieceStatus::Missing && bitfield.has_piece(p.index))
            .collect();

        if missing_pieces.is_empty() {
            return None;
        }

        // Find the rarest: the piece with the lowest frequency.
        let frequencies: Vec<usize> = missing_pieces
            .iter()
            .map(|p| self.piece_frequencies[p.index])
            .collect();

        let (rarest_index, _) = frequencies
            .iter()
            .enumerate()
            .min_by_key(|&(_i, valore)| valore)
            .unwrap_or((0usize, &0));

        let rarest_piece = &mut missing_pieces[rarest_index];
        rarest_piece.status = PieceStatus::Downloading;

        if let Some(block) = rarest_piece
            .blocks
            .iter_mut()
            .find(|b| b.status == BlockStatus::Free)
        {
            block.status = BlockStatus::Requested;
            Some(*block)
        } else {
            None
        }
    }

    /// Process an incoming block of data.
    ///
    /// Returns `Ok(true)` if the piece is now complete and verified.
    /// Returns `Ok(false)` if more blocks are still needed.
    /// On hash mismatch, the entire piece is reset to `Missing` and all
    /// blocks are freed for re-download.
    pub fn handle_piece(&mut self, index: usize, offset: u32, data: &[u8]) -> Result<bool> {
        let piece: &mut Piece = self
            .pieces
            .get_mut(index)
            .ok_or(anyhow::anyhow!("Invalid piece index"))?;

        let block_idx = offset as usize / BLOCK_SIZE;
        if block_idx >= piece.blocks.len() {
            return Err(anyhow::anyhow!("Invalid block offset"));
        }
        // Guard: skip duplicate block data.
        if piece.blocks[block_idx].status == BlockStatus::Downloaded {
            return Ok(false);
        }

        let end_offset = offset as usize + data.len();
        if end_offset > piece.data.len() {
            return Err(anyhow::anyhow!("Piece data length overflow"));
        }

        piece.data[offset as usize..end_offset].copy_from_slice(data);

        piece.missing_blocks -= 1;

        piece.blocks[block_idx].status = BlockStatus::Downloaded;

        // If all blocks are in, verify the SHA-1 hash.
        if piece.missing_blocks == 0 {
            piece.status = PieceStatus::Verifying;

            return if piece.verify() {
                piece.status = PieceStatus::Completed;
                self.bitfield.set_piece(index);
                self.missing_pieces -= 1;
                Ok(true)
            } else {
                // Hash mismatch — reset the piece for re-download.
                piece.data.fill(0);
                piece.status = PieceStatus::Missing;
                piece.missing_blocks = piece.blocks.len();
                for block in &mut piece.blocks {
                    block.status = BlockStatus::Free;
                    block.timer = 30;
                }
                Ok(false)
            };
        }

        Ok(false)
    }

    /// Restore progress from saved resume data.
    ///
    /// Marks pieces as `Completed` based on the bitfield, and updates
    /// the `missing_pieces` count accordingly.
    pub fn restore(&mut self, bitfield_data: &[u8]) {
        let num_pieces = self.pieces.len();
        if bitfield_data.len() != self.bitfield.data.len() {
            return;
        }
        self.bitfield.data.copy_from_slice(bitfield_data);
        let mut restored = 0;
        for i in 0..num_pieces {
            if self.bitfield.has_piece(i) {
                self.pieces[i].status = PieceStatus::Completed;
                restored += 1;
            }
        }
        self.missing_pieces = num_pieces - restored;
    }

    /// Decrement block request timers every second.
    /// Blocks that have timed out (timer reached 0) revert to `Free`
    /// so they can be re-requested from another peer.
    pub fn tick_timeouts(&mut self) {
        for piece in &mut self.pieces {
            if piece.status == PieceStatus::Downloading {
                for block in &mut piece.blocks {
                    if block.status == BlockStatus::Requested {
                        if block.timer == 0 {
                            block.status = BlockStatus::Free;
                            block.timer = 30;
                        } else {
                            block.timer -= 1;
                        }
                    }
                }
            }
        }
    }
}
