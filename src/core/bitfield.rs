//! BitTorrent bitfield data structure.
//!
//! A bitfield is a compact bitmap where each bit represents whether the
//! local client (or a remote peer) has completed a given piece.
//! Bits are stored big-endian within each byte (bit 7 = piece 0).

#[derive(Debug)]
pub struct BitField {
    pub data: Vec<u8>,
}

impl Default for BitField {
    fn default() -> Self {
        Self::new()
    }
}

impl BitField {
    /// Create an empty bitfield (zero length, no pieces tracked).
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    /// Create a bitfield large enough to track `num_pieces` pieces.
    /// All bits start cleared (piece not present).
    pub fn with_pieces(num_pieces: usize) -> Self {
        let num_bytes = num_pieces.div_ceil(8);
        Self {
            data: vec![0; num_bytes],
        }
    }

    /// Parse a bitfield from the raw payload of a `BitField` wire message.
    pub fn from_payload(payload: Vec<u8>) -> Self {
        Self { data: payload }
    }

    /// Check whether the bit for piece `index` is set.
    pub fn has_piece(&self, index: usize) -> bool {
        let byte_index = index / 8;
        let offset = index % 8;

        if byte_index >= self.data.len() {
            return false;
        }

        self.data[byte_index] >> (7 - offset) & 1 != 0
    }

    /// Mark piece `index` as completed (set its bit to 1).
    pub fn set_piece(&mut self, index: usize) {
        let byte_index = index / 8;
        let offset = index % 8;

        if byte_index >= self.data.len() {
            return;
        }

        self.data[byte_index] |= 1 << (7 - offset)
    }
}
