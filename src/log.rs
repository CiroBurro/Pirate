use std::sync::{Arc, Mutex};
use tracing_subscriber::fmt::MakeWriter;

#[derive(Default, Clone)]
pub struct LogBuffer {
    pub buffer: Arc<Mutex<Vec<u8>>>,
}

impl LogBuffer {
    pub fn get_logs(&self) -> String {
        let guard = self.buffer.lock().unwrap();
        String::from_utf8(guard.clone()).unwrap_or(String::from("Failed to get logs"))
    }

    pub fn clear(&self) {
        self.buffer.lock().unwrap().clear();
    }
}

impl std::io::Write for LogBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer
            .lock()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?
            .write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.buffer
            .lock()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?
            .flush()
    }
}

impl<'a> MakeWriter<'a> for LogBuffer {
    type Writer = Self;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}
