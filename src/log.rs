//! In-memory log capture for the TUI.
//!
//! [`LogBuffer`] implements [`MakeWriter`] so it can be plugged into
//! `tracing-subscriber` as a log sink. The TUI periodically drains the
//! buffer and renders the captured lines in the Log screen.

use std::sync::{Arc, Mutex};
use tracing_subscriber::fmt::MakeWriter;

/// A thread-safe, in-memory byte buffer that collects `tracing` output.
///
/// Cloning shares the same underlying `Arc<Mutex<Vec<u8>>>`, so the TUI and
/// the tracing pipeline can operate on the same buffer from different tasks.
#[derive(Default, Clone)]
pub struct LogBuffer {
    pub buffer: Arc<Mutex<Vec<u8>>>,
}

impl LogBuffer {
    /// Drain the accumulated log text and return it as a `String`.
    pub fn get_logs(&self) -> String {
        let guard = self.buffer.lock().unwrap();
        String::from_utf8(guard.clone()).unwrap_or(String::from("Failed to get logs"))
    }

    /// Clear the internal buffer (called after each TUI frame consumes the logs).
    pub fn clear(&self) {
        self.buffer.lock().unwrap().clear();
    }
}

impl std::io::Write for LogBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer
            .lock()
            .map_err(|e| std::io::Error::other(e.to_string()))?
            .write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.buffer
            .lock()
            .map_err(|e| std::io::Error::other(e.to_string()))?
            .flush()
    }
}

/// Required by `tracing-subscriber` so [`LogBuffer`] can be used as a
/// `MakeWriter` — every new log event gets a cloned handle to the same buffer.
impl<'a> MakeWriter<'a> for LogBuffer {
    type Writer = Self;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}
