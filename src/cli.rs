//! CLI argument parsing via [clap].

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[clap(
    name = "Pirate",
    author,
    version,
    about = "A torrent client for tresure hunters, written in rust btw "
)]
pub struct Args {
    #[clap(subcommand)]
    pub command: Option<MainCommands>,
}

/// Top-level CLI subcommands.
#[derive(Subcommand)]
pub enum MainCommands {
    /// Add a .torrent file to the session.
    Add {
        torrent_path: PathBuf,
        #[clap(short, long)]
        download_dir: Option<PathBuf>,
    },
    /// Change persistent configuration values.
    Config {
        #[clap(subcommand)]
        action: ConfigCommands,
    },
}

/// Configuration subcommands — each modifies a single field in the config file.
#[derive(Subcommand)]
pub enum ConfigCommands {
    /// Set the peer ID (must be exactly 20 bytes/characters).
    PeerId { id: String },
    /// Set the default download directory.
    DownloadDir { dir: PathBuf },
    /// Set the TCP listen port for incoming peer connections.
    ListenPort { port: u16 },
}
