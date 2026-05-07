use crate::core::bitfield::BitField;

const BLOCK_SIZE: usize = 16384;

#[derive(Debug)]
pub enum BlockStatus {
    Free,
    Requested,
    Downloaded,
}

#[derive(Debug)]
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
            timer: 5,
        }
    }
}

#[derive(Debug)]
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
    pub bitfield: BitField,
    pub missing_blocks: usize,
    pub data: Vec<u8>,
}

impl Piece {
    pub fn new(index: usize, hash: [u8; 20], length: usize) -> Self {
        Self {
            index,
            hash,
            status: PieceStatus::Missing,
            length,
            bitfield: BitField::new(),
            missing_blocks: (length + BLOCK_SIZE - 1) / BLOCK_SIZE,
            data: Vec::with_capacity(length),
        }
    }
}

#[derive(Debug)]
pub struct PiecePicker {
    pub pieces: Vec<Piece>,
    pub bitfield: BitField,
    pub piece_frequencies: Vec<usize>,
    pub missing_pieces: usize,
}

impl PiecePicker {
    pub fn new(pieces: Vec<Piece>) -> Self {
        let num_pieces = pieces.len();
        Self {
            pieces,
            bitfield: BitField::with_pieces(num_pieces),
            piece_frequencies: vec![0; num_pieces],
            missing_pieces: num_pieces,
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
}
