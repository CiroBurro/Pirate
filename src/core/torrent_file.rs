use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_bencode::de;
use serde_bytes::ByteBuf;
use sha1::{Digest, Sha1};
use std::path::PathBuf;
use tokio::fs;

#[derive(Debug, Serialize, Deserialize)]
pub struct TorrentFile {
    pub announce: String,
    #[serde(rename = "announce-list")]
    pub announce_list: Option<Vec<Vec<String>>>,
    pub info: Info,
    pub comment: Option<String>,
    #[serde(rename = "creation date")]
    pub creation_date: Option<i64>,
}

impl TorrentFile {
    pub async fn from_file(path: PathBuf) -> Result<TorrentFile> {
        let data = fs::read(path)
            .await
            .context("[!] Failed to read the torrent file")?;
        let torrent: TorrentFile =
            de::from_bytes(&data).context("[!] Failed to deserialize the torrent file content")?;

        Ok(torrent)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Info {
    #[serde(rename = "piece length")]
    pub piece_length: i32,
    pub pieces: ByteBuf,
    pub name: String,
    pub length: Option<usize>,
    pub files: Option<Vec<File>>,
}

impl Info {
    pub fn get_info_hash(&self) -> Result<[u8; 20]> {
        let bencoded_info = serde_bencode::to_bytes(self).context("[!] Failed to encode the Info data structure of the torrent file to calcultate the hash")?;

        let mut hasher = Sha1::new();
        hasher.update(&bencoded_info);
        let result = hasher.finalize();

        let mut hash = [0u8; 20];
        hash.copy_from_slice(&result);

        Ok(hash)
    }

    pub fn total_len(&self) -> Result<usize> {
        if self.length.is_some() {
            return Ok(self.length.unwrap());
        } else if self.files.is_some() {
            let mut len: usize = 0;
            let _ = self.files.clone().unwrap().iter().map(|f| len += f.length);
            return Ok(len);
        } else {
            return Err(anyhow!("[!] Both file length and files list are missing"));
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct File {
    pub length: usize,
    pub path: Vec<String>,
}
