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

        let id = MessageId::try_from(u8::from_be(slice[0]))
            .context("[!] Failed to parse the message id")?;
        let payload = slice[1..].to_vec();

        Ok(Self { len, id, payload })
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        buf.extend_from_slice(&self.len.to_be_bytes());
        buf.extend_from_slice(&u8::from(&self.id).to_be_bytes());
        buf.extend_from_slice(self.payload.as_slice());

        buf
    }
}
