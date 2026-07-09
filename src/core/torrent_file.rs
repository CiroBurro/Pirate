//! .torrent file parsing — bencode deserialization into Rust structs.
//!
//! The [`TorrentFile`] struct mirrors the bencoded metainfo file format
//! defined in [BEP 3](https://www.bittorrent.org/beps/bep_0003.html).

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use serde_bencode::de;
use serde_bytes::ByteBuf;
use sha1::{Digest, Sha1};
use std::path::PathBuf;
use tokio::fs;

/// Top-level metainfo structure parsed from a `.torrent` file.
#[derive(Debug, Serialize, Deserialize)]
pub struct TorrentFile {
    /// Primary tracker announce URL.
    pub announce: String,
    /// Optional list of tracker tiers (BEP 12).
    #[serde(rename = "announce-list")]
    pub announce_list: Option<Vec<Vec<String>>>,
    /// The info dictionary — the critical part that defines the torrent.
    pub info: Info,
    /// Free-text comment (optional).
    pub comment: Option<String>,
    /// Unix timestamp of when the torrent was created.
    #[serde(rename = "creation date")]
    pub creation_date: Option<i64>,
}

impl TorrentFile {
    /// Read and parse a `.torrent` file from disk.
    pub async fn from_file(path: PathBuf) -> Result<TorrentFile> {
        let data = fs::read(path)
            .await
            .context("Failed to read the torrent file")?;
        let torrent: TorrentFile =
            de::from_bytes(&data).context("Failed to deserialize the torrent file")?;

        Ok(torrent)
    }
}

/// The `info` dictionary from the metainfo file.
///
/// Contains piece layout, file names, and sizes. This dictionary is
/// hashed (SHA-1 of its bencoded form) to produce the torrent's
/// **info hash**, which uniquely identifies the torrent in the swarm.
#[derive(Debug, Serialize, Deserialize)]
pub struct Info {
    /// Length of each piece in bytes (except the last piece).
    #[serde(rename = "piece length")]
    pub piece_length: u32,
    /// Concatenated SHA-1 hashes of every piece (20 bytes per piece).
    pub pieces: ByteBuf,
    /// Suggested name for the download (file name or directory name).
    pub name: String,
    /// Total file length (single-file torrents only).
    pub length: Option<usize>,
    /// File list for multi-file torrents.
    pub files: Option<Vec<File>>,
}

impl Info {
    /// Compute the 20-byte SHA-1 info hash by re-bencoding the `info`
    /// dictionary exactly as-is. This hash is used to identify the torrent
    /// in tracker announces and peer handshakes.
    pub fn get_info_hash(&self) -> Result<[u8; 20]> {
        let bencoded_info = serde_bencode::to_bytes(self)
            .context("Failed to encode the Info structure to calculate hash")?;

        let mut hasher = Sha1::new();
        hasher.update(&bencoded_info);
        let result = hasher.finalize();

        let mut hash = [0u8; 20];
        hash.copy_from_slice(&result);

        Ok(hash)
    }

    /// Calculate the total download size by summing all file lengths.
    pub fn total_len(&self) -> Result<usize> {
        if let Some(length) = self.length {
            Ok(length)
        } else if let Some(files) = &self.files {
            let mut len: usize = 0;
            for file in files {
                len += file.length;
            }
            Ok(len)
        } else {
            Err(anyhow!("Both file length and files list are missing"))
        }
    }
}

/// A single file entry inside a multi-file torrent.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct File {
    /// File size in bytes.
    pub length: usize,
    /// Path components (directory + filename) relative to the download root.
    pub path: Vec<String>,
}
