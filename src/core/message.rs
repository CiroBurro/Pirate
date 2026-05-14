use anyhow::{Context, Result, anyhow};

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

#[derive(Debug)]
pub struct Message {
    pub len: u32,
    pub id: MessageId,
    pub payload: Vec<u8>,
}

impl Message {
    pub fn parse(len: u32, data: Vec<u8>) -> Result<Self> {
        let slice = data.as_slice();

        if len == 0 {
            return Ok(Self::keep_alive());
        }

        let id = MessageId::try_from(u8::from_be(slice[0]))
            .context("[!] Failed to parse the message id")?;
        let payload = slice[1..].to_vec();

        Ok(Self { len, id, payload })
    }

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

    pub fn have(index: u32) -> Self {
        Self {
            len: 5,
            id: MessageId::Have,
            payload: index.to_be_bytes().to_vec(),
        }
    }

    pub fn bitfield(payload: Vec<u8>) -> Self {
        Self {
            len: 1 + payload.len() as u32,
            id: MessageId::BitField,
            payload,
        }
    }

    pub fn request(payload: [u8; 12]) -> Self {
        Self {
            len: 13,
            id: MessageId::Request,
            payload: payload.to_vec(),
        }
    }

    pub fn piece(payload: Vec<u8>) -> Self {
        Self {
            len: 9 + payload.len() as u32,
            id: MessageId::Piece,
            payload,
        }
    }

    pub fn cancel(payload: [u8; 12]) -> Self {
        Self {
            len: 13,
            id: MessageId::Cancel,
            payload: payload.to_vec(),
        }
    }
}
