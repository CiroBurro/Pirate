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

#[derive(Subcommand)]
pub enum MainCommands {
    Add {
        torrent_path: PathBuf,
        download_dir: Option<PathBuf>,
    },
    Config {
        #[clap(subcommand)]
        action: ConfigCommands,
    },
}

#[derive(Subcommand)]
pub enum ConfigCommands {
    PeerId { id: String },
    DownloadDir { dir: PathBuf },
    ListenPort { port: u16 },
}
