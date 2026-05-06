#[derive(Debug)]
pub struct BitField {
    pub data: Vec<u8>,
}

impl BitField {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn with_pieces(num_pieces: usize) -> Self {
        let num_bytes = (num_pieces + 7) / 8;
        Self {
            data: vec![0; num_bytes],
        }
    }

    pub fn from_payload(payload: Vec<u8>) -> Self {
        Self { data: payload }
    }

    pub fn has_piece(&self, index: usize) -> bool {
        let byte_index = index / 8;
        let offset = index % 8;

        if byte_index >= self.data.len() {
            return false;
        }

        self.data[byte_index] >> (7 - offset as usize) & 1 != 0
    }

    pub fn set_piece(&mut self, index: usize) {
        let byte_index = index / 8;
        let offset = index % 8;

        if byte_index >= self.data.len() {
            return;
        }

        self.data[byte_index] |= 1 << (7 - offset as usize)
    }
}
