//! Generic persistence trait and implementations.
//!
//! Data is stored under the platform's data directory (e.g.
//! `~/.local/share/pirate/` on Linux).

#![allow(async_fn_in_trait)]

pub mod resume_data;
pub mod session;

use serde::{de::DeserializeOwned, Serialize};

/// A generic trait for saving/loading state to/from the filesystem as JSON.
///
/// # Default implementation
///
/// - `save`: serializes `self` to pretty-printed JSON and writes it to
///   `{data_dir}/pirate/{file_name}.json`.
/// - `load`: reads the JSON file from the same path and deserializes it.
///
/// Types that need a non-JSON format (e.g. binary resume data) override
/// one or both methods.
pub trait Persistent {
    async fn save(&self, file_name: &str) -> anyhow::Result<()>
    where
        Self: Sized,
        Self: Serialize,
    {
        let file_path = dirs::data_dir()
            .unwrap_or_default()
            .join("pirate")
            .join(file_name.to_owned() + ".json");

        tokio::fs::create_dir_all(file_path.parent().unwrap()).await?;

        let json = serde_json::to_string_pretty(self)?;
        tokio::fs::write(file_path, json).await?;
        Ok(())
    }
    async fn load(file_name: &str) -> anyhow::Result<Self>
    where
        Self: Sized,
        Self: DeserializeOwned,
    {
        let file_path = dirs::data_dir()
            .unwrap_or_default()
            .join("pirate")
            .join(file_name.to_owned() + ".json");

        let data = tokio::fs::read(file_path).await?;
        Ok(serde_json::from_slice(&data)?)
    }
}
