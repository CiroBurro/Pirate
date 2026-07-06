use crate::persistence::Persistent;
use anyhow::Result;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct ResumeData {
    pub downloaded: u64,
    pub bitfield_data: Vec<u8>,
}

impl ResumeData {
    pub fn new(downloaded: u64, bitfield_data: Vec<u8>) -> Self {
        Self {
            downloaded,
            bitfield_data,
        }
    }
}

impl Persistent for ResumeData {
    async fn save(&self, file_name: &str) -> Result<()>
    where
        Self: Sized,
        Self: Serialize,
    {
        let file_path = dirs::data_dir()
            .unwrap_or_default()
            .join("pirate")
            .join(file_name.to_owned() + ".dat");

        tokio::fs::create_dir_all(file_path.parent().unwrap()).await?;

        let mut data = self.downloaded.to_be_bytes().to_vec();
        data.extend_from_slice(&self.bitfield_data);

        tokio::fs::write(file_path, data).await?;
        Ok(())
    }

    async fn load(file_name: &str) -> Result<Self>
    where
        Self: Sized,
        Self: DeserializeOwned,
    {
        let file_path = dirs::data_dir()
            .unwrap_or_default()
            .join("pirate")
            .join(file_name.to_owned() + ".dat");

        let data = tokio::fs::read(file_path).await?;

        let downloaded = u64::from_be_bytes(data[..8].try_into()?);
        let bitfield_data = data[8..].to_vec();
        Ok(Self {
            downloaded,
            bitfield_data,
        })
    }
}
