//! TUI screen variants — each variant renders a different view.

/// The three screens of the TUI.
#[derive(Default)]
pub enum Screen {
    /// Torrent list overview (default).
    #[default]
    Main,
    /// Per-torrent detail: info, progress gauge, download speed chart.
    Detail {
        selected: usize,
    },
    /// Scrollable log output captured from `tracing`.
    Log,
}
