use crate::core::{bitfield::BitField, torrent_file::TorrentFile};
use anyhow::Result;
use sha1::{Digest, Sha1};
use std::path::PathBuf;
use tokio::io::AsyncReadExt;
use tokio::{
    fs::OpenOptions,
    io::{AsyncSeekExt, AsyncWriteExt, SeekFrom},
};

const BLOCK_SIZE: usize = 16384;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum BlockStatus {
    Free,
    Requested,
    Downloaded,
}

#[derive(Debug, Clone, Copy)]
pub struct Block {
    pub piece_index: usize,
    pub offset: usize,
    pub length: usize,
    pub status: BlockStatus,
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

    pub fn to_payload(&self) -> Vec<u8> {
        [
            (self.piece_index as u32).to_be_bytes(),
            (self.offset as u32).to_be_bytes(),
            (self.length as u32).to_be_bytes(),
        ]
        .concat()
    }
}

#[derive(Debug, PartialEq)]
pub enum PieceStatus {
    Missing,
    Downloading,
    Verifying,
    Completed,
}

#[derive(Debug)]
pub struct Piece {
    pub index: usize,
    pub hash: [u8; 20],
    pub status: PieceStatus,
    pub length: usize,
    pub blocks: Vec<Block>,
    pub missing_blocks: usize,
    pub data: Vec<u8>,
}

impl Piece {
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

    pub fn verify(&mut self) -> bool {
        let hash = Sha1::digest(&self.data);
        hash == self.hash
    }
}

#[derive(Debug)]
pub struct PiecePicker {
    pub pieces: Vec<Piece>,
    pub bitfield: BitField,
    pub piece_frequencies: Vec<usize>,
    pub missing_pieces: usize,
    pub paused: bool,
}

impl PiecePicker {
    pub fn new(pieces: Vec<Piece>) -> Self {
        let num_pieces = pieces.len();
        Self {
            pieces,
            bitfield: BitField::with_pieces(num_pieces),
            piece_frequencies: vec![0; num_pieces],
            missing_pieces: num_pieces,
            paused: false,
        }
    }

    pub fn add_peer_bitfield(&mut self, bitfield: &BitField) {
        for i in 0..self.pieces.len() {
            if bitfield.has_piece(i) {
                self.piece_frequencies[i] += 1;
            }
        }
    }

    pub fn add_peer_have(&mut self, index: usize) {
        if index < self.piece_frequencies.len() {
            self.piece_frequencies[index] += 1;
        }
    }

    pub fn pick(&mut self, bitfield: &BitField) -> Option<Block> {
        if self.paused {
            return None;
        }
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

        let mut missing_pieces: Vec<&mut Piece> = self
            .pieces
            .iter_mut()
            .filter(|p| p.status == PieceStatus::Missing && bitfield.has_piece(p.index))
            .collect();

        if missing_pieces.is_empty() {
            return None;
        }

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

    pub fn handle_piece(&mut self, index: usize, offset: u32, data: &[u8]) -> Result<bool> {
        let piece: &mut Piece = self
            .pieces
            .get_mut(index)
            .ok_or(anyhow::anyhow!("Invalid piece index"))?;

        let block_idx = offset as usize / BLOCK_SIZE;
        if block_idx >= piece.blocks.len() {
            return Err(anyhow::anyhow!("Invalid block offset"));
        }
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

        if piece.missing_blocks == 0 {
            piece.status = PieceStatus::Verifying;

            return if piece.verify() {
                piece.status = PieceStatus::Completed;
                self.bitfield.set_piece(index);
                self.missing_pieces -= 1;
                Ok(true)
            } else {
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
