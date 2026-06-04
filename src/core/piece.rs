use crate::core::bitfield::BitField;

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
            timer: 5,
        }
    }

    pub fn to_payload(&self) -> Vec<u8> {
        [
            self.piece_index.to_be_bytes(),
            self.offset.to_be_bytes(),
            self.length.to_be_bytes(),
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
    pub bitfield: BitField,
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
            bitfield: BitField::new(),
            blocks,
            missing_blocks: num_blocks,
            data: vec![0; length],
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

    pub fn pick(&mut self, bitfield: &BitField) -> Option<Block> {
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
}
