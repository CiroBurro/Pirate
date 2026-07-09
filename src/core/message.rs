//! BitTorrent wire protocol messages.
//!
//! Implements the peer-wire protocol defined in
//! [BEP 3 § peer-wire-protocol](https://www.bittorrent.org/beps/bep_0003.html#peer-wire-protocol).
//!
//! Every message on the wire follows the format:
//! `<length prefix: u32><message ID: u8><payload ...>`
//! A length prefix of 0 designates a keep-alive message.

use anyhow::{Context, Result, anyhow};

/// Identifiers for every message type in the BitTorrent protocol.
#[derive(Debug)]
pub enum MessageId {
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have,
    BitField,
    Request,
    Piece,
    Cancel,
    KeepAlive,
}

impl TryFrom<u8> for MessageId {
    type Error = anyhow::Error;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(MessageId::Choke),
            1 => Ok(MessageId::Unchoke),
            2 => Ok(MessageId::Interested),
            3 => Ok(MessageId::NotInterested),
            4 => Ok(MessageId::Have),
            5 => Ok(MessageId::BitField),
            6 => Ok(MessageId::Request),
            7 => Ok(MessageId::Piece),
            8 => Ok(MessageId::Cancel),
            _ => Err(anyhow!("Unknown Message ID")),
        }
    }
}

impl From<&MessageId> for u8 {
    fn from(value: &MessageId) -> Self {
        match value {
            MessageId::Choke => 0,
            MessageId::Unchoke => 1,
            MessageId::Interested => 2,
            MessageId::NotInterested => 3,
            MessageId::Have => 4,
            MessageId::BitField => 5,
            MessageId::Request => 6,
            MessageId::Piece => 7,
            MessageId::Cancel => 8,
            MessageId::KeepAlive => 255,
        }
    }
}

/// A parsed or serialized peer-wire protocol message.
#[derive(Debug)]
pub struct Message {
    /// Total payload length (excluding the 4-byte length prefix itself).
    pub len: u32,
    /// The message type identifier.
    pub id: MessageId,
    /// Raw payload bytes (the part after the message ID byte).
    pub payload: Vec<u8>,
}

impl Message {
    /// Parse a message from its raw payload after reading the length prefix.
    ///
    /// `len` is the value of the 4-byte length prefix; `data` is the
    /// rest of the message (not including the length prefix).
    pub fn parse(len: u32, data: Vec<u8>) -> Result<Self> {
        let slice = data.as_slice();

        if len == 0 {
            return Ok(Self::keep_alive());
        }

        let id = MessageId::try_from(u8::from_be(slice[0]))
            .context("Failed to parse the message id")?;
        let payload = slice[1..].to_vec();

        Ok(Self { len, id, payload })
    }

    /// Serialize the message into its on-wire byte representation.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        buf.extend_from_slice(&self.len.to_be_bytes());
        if self.len == 0 {
            return buf;
        }

        buf.extend_from_slice(&u8::from(&self.id).to_be_bytes());
        buf.extend_from_slice(self.payload.as_slice());

        buf
    }

    // --- Constructor helpers for each message type ---

    pub fn keep_alive() -> Self {
        Self {
            len: 0,
            id: MessageId::KeepAlive,
            payload: Vec::with_capacity(0),
        }
    }

    pub fn choke() -> Self {
        Self {
            len: 1,
            id: MessageId::Choke,
            payload: Vec::with_capacity(0),
        }
    }

    pub fn unchoke() -> Self {
        Self {
            len: 1,
            id: MessageId::Unchoke,
            payload: Vec::with_capacity(0),
        }
    }

    pub fn interested() -> Self {
        Self {
            len: 1,
            id: MessageId::Interested,
            payload: Vec::with_capacity(0),
        }
    }

    pub fn uninterested() -> Self {
        Self {
            len: 1,
            id: MessageId::NotInterested,
            payload: Vec::with_capacity(0),
        }
    }

    /// `have` message: notifies a peer that we now own piece `index`.
    pub fn have(index: u32) -> Self {
        Self {
            len: 5,
            id: MessageId::Have,
            payload: index.to_be_bytes().to_vec(),
        }
    }

    /// `bitfield` message: initial bitmap of pieces we own.
    pub fn bitfield(payload: Vec<u8>) -> Self {
        Self {
            len: 1 + payload.len() as u32,
            id: MessageId::BitField,
            payload,
        }
    }

    /// `request` message: ask a peer for a block.
    /// Payload is 12 bytes: `[piece_index: u32][offset: u32][length: u32]`.
    pub fn request(payload: [u8; 12]) -> Self {
        Self {
            len: 13,
            id: MessageId::Request,
            payload: payload.to_vec(),
        }
    }

    /// `piece` message: response containing actual block data.
    /// Payload is 8 bytes header `[piece_index: u32][offset: u32]` + data.
    pub fn piece(payload: Vec<u8>) -> Self {
        Self {
            len: 9 + payload.len() as u32,
            id: MessageId::Piece,
            payload,
        }
    }

    /// `cancel` message: cancel a previously-requested block.
    pub fn cancel(payload: [u8; 12]) -> Self {
        Self {
            len: 13,
            id: MessageId::Cancel,
            payload: payload.to_vec(),
        }
    }
}
