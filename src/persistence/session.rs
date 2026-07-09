//! Session persistence — remembers which torrents were active across restarts.
//!
//! Stored as a JSON array of `SessionEntry` objects under
//! `{data_dir}/pirate/session.json`.

use crate::persistence::Persistent;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// A single entry in the session: path to the .torrent file and its
/// associated download directory.
#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub struct SessionEntry {
    pub torrent_path: PathBuf,
    pub download_dir: PathBuf,
}

/// The full session: a list of all active torrent entries.
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct Session {
    pub inner: Vec<SessionEntry>,
}

impl Session {
    /// Add a torrent to the session list.
    pub fn add(&mut self, entry: SessionEntry) {
        self.inner.push(entry)
    }
}

impl Persistent for Session {}
