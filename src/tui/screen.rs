use crate::core::torrent::TorrentId;

#[derive(Default)]
pub enum Screen {
    #[default]
    Main,
    Detail {
        id: TorrentId,
    },
    Log,
}
