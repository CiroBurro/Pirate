use serde::{Deserialize, Serialize};
use serde_bencode::de;
use serde_bytes::ByteBuf;
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

#[derive(Debug, Serialize, Deserialize)]
pub struct Info {
    #[serde(rename = "piece length")]
    pub creation_date: i32,
    pub pieces: ByteBuf,
    pub name: String,
    pub length: i32,
    pub files: Vec<File>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct File {
    pub length: i32,
    pub path: Vec<String>,
}

impl Torrent {
    pub async fn from_file(path: PathBuf) -> Result<Torrent, Box<dyn std::error::Error>> {
        let data = fs::read(path).await?;
        let torrent: Torrent = de::from_bytes(&data)?;

        Ok(torrent)
    }
}
