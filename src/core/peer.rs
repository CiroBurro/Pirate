use crate::core::{torrent::Torrent, tracker::tracker_handshake};
use anyhow::{bail, Context, Result};
use std::net::Ipv4Addr;
use tokio::task::JoinSet;
use tracing::{info, instrument, warn};

#[derive(Debug)]
pub struct Peer {
    pub ip: Ipv4Addr,
    pub port: u16,
}

impl Peer {
    #[instrument(skip(torrent))]
    pub async fn get_peers(torrent: Torrent) -> Result<Vec<Peer>> {
        let info_hash = torrent
            .info
            .get_info_hash()
            .context("[!] Failed to calculate the info hash")?;
        let left_len = torrent
            .info
            .total_len()
            .context("[!] Failed to get the total length of the files to download")?;

        let mut trackers = vec![torrent.announce.clone()];

        if let Some(announce_list) = &torrent.announce_list {
            for tier in announce_list {
                for tracker in tier {
                    if !trackers.contains(tracker) {
                        trackers.push(tracker.clone());
                    }
                }
            }
        }

        info!(
            "Found {} trackers inside the announce list.",
            trackers.len()
        );

        let mut set = JoinSet::new();
        for tracker_url in trackers {
            set.spawn(async move { tracker_handshake(tracker_url, info_hash, left_len).await });
        }

        while let Some(res) = set.join_next().await {
            match res {
                Ok(Ok(peers)) => {
                    info!(peers_number = peers.len(), "Peers' list obtained.");
                    return Ok(peers);
                }
                Ok(Err(e)) => {
                    warn!(error = ?e, "A tracker failed, waiting for the others.");
                }
                Err(e) => {
                    warn!("A tracker task crashed: {}", e);
                }
            }
        }

        bail!("[!] All trackers failed");
    }

    #[instrument]
    pub async fn handshake(&self) -> Result<()> {
        todo!()
    }
}
