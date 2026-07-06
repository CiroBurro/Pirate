use crate::persistence::Persistent;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub struct SessionEntry {
    pub torrent_path: PathBuf,
    pub download_dir: PathBuf,
}

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct Session {
    pub inner: Vec<SessionEntry>,
}

impl Session {
    pub fn add(&mut self, entry: SessionEntry) {
        self.inner.push(entry)
    }
}

impl Persistent for Session {}
