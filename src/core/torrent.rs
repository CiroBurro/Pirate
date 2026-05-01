use serde::{Deserialize, Serialize};
use serde_bencode::de;
use serde_bytes::ByteBuf;
use sha1::{Digest, Sha1};
use std::path::PathBuf;
use tokio::fs;

#[derive(Debug, Serialize, Deserialize)]
pub struct Torrent {
    pub announce: String,
    pub info: Info,
    pub comment: Option<String>,
    #[serde(rename = "creation date")]
    pub creation_date: Option<i64>,
}

impl Torrent {
    pub async fn from_file(path: PathBuf) -> Result<Torrent, Box<dyn std::error::Error>> {
        let data = fs::read(path).await?;
        let torrent: Torrent = de::from_bytes(&data)?;

        Ok(torrent)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Info {
    #[serde(rename = "piece length")]
    pub piece_date: i32,
    pub pieces: ByteBuf,
    pub name: String,
    pub length: Option<i64>,
    pub files: Option<Vec<File>>,
}

impl Info {
    pub fn get_info_hash(&self) -> Result<[u8; 20], Box<dyn std::error::Error>> {
        let bencoded_info = serde_bencode::to_bytes(self)?;

        let mut hasher = Sha1::new();
        hasher.update(&bencoded_info);
        let result = hasher.finalize();

        let mut hash = [0u8; 20];
        hash.copy_from_slice(&result);

        Ok(hash)
    }

    pub fn total_len(&self) -> Result<i64, Box<dyn std::error::Error>> {
        if self.length.is_some() {
            return Ok(self.length.unwrap());
        } else if self.files.is_some() {
            let mut len: i64 = 0;
            self.files.clone().unwrap().iter().map(|f| len += f.length);
            return Ok(len);
        } else {
            todo!()
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct File {
    pub length: i64,
    pub path: Vec<String>,
}
