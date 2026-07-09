# Pirate 🦜

> A torrent client for treasure hunters.

Pirate is an **async BitTorrent client** built in Rust with a terminal user interface (TUI). It supports multi-torrent
sessions, UDP tracker announce, rarest-first piece selection, and full upload/download bandwidth management — all from
the comfort of your terminal.

Note that AI was largely used as source to delve into the bittorrent protocol and to organize the development step by
step
or to catch bugs and help fixing them.
Apart from that the vast majority of the code is completely human written.

---

## Features

- **Multi-torrent management** — add, pause, resume, and remove torrents at runtime
- **Terminal UI** — built with [ratatui](https://github.com/ratatui-org/ratatui), featuring a torrent list, per-torrent
  detail views with live download charts, and a log viewer
- **UDP tracker protocol** — announces to all trackers in the announce list concurrently, supports tracker tiers
- **Rarest-first piece selection** — optimizes swarm availability by prioritizing the least replicated pieces
- **Piece pipelining** — maintains a configurable number of in-flight block requests per peer for maximum throughput
- **Choking / unchoking** — upload-rate-based tit-for-tat strategy (top 4 uploaders get unchoked)
- **Incoming peer connections** — listens for incoming TCP handshakes from other peers in the swarm
- **Resume support** — saves progress bitfield and resumes partial downloads across restarts
- **Session persistence** — remembers your torrent list between sessions
- **Configurable** — peer ID, download directory, and listen port via CLI commands

---

## Installation

### Prerequisites

- Rust toolchain (edition 2024). Install via [rustup](https://rustup.rs/).

### Build from source

```bash
git clone https://github.com/yourusername/pirate.git
cd pirate
cargo build --release
```

The binary will be at `target/release/pirate`.

---

## Usage

### First run

Just launch Pirate — it will start the TUI with an empty torrent list:

```bash
pirate
```

Default configuration:

- Peer ID: `-PR1337-012345678901`
- Download directory: your system `~/Downloads`
- Listen port: `6881`

### Adding a torrent

```bash
pirate add /path/to/your.torrent
# Optionally specify a download directory:
pirate add /path/to/your.torrent --download-dir /custom/path
```

### Configuration

```bash
# Set a custom peer ID (exactly 20 characters)
pirate config peer-id "-PR0001-012345678901"

# Set the default download directory
pirate config download-dir ~/torrents

# Set the listening port (>= 1024)
pirate config listen-port 6889
```

### TUI controls

| Key            | Action                           |
|----------------|----------------------------------|
| `↑` / `k`      | Move selection up                |
| `↓` / `j`      | Move selection down              |
| `Enter`        | View torrent details             |
| `p`            | Pause selected torrent           |
| `r`            | Resume selected torrent          |
| `c`            | Cancel / remove selected torrent |
| `Tab`          | Switch to log view               |
| `q` / `Ctrl+C` | Quit                             |

In detail / log view, press `Esc` or `Tab` to return to the main list.

---

## Project Structure

```
src/
├── main.rs                  # Entry point: tracing init, CLI parsing, TUI event loop
├── cli.rs                   # CLI argument definitions (clap)
├── client.rs                # High-level client: manages torrents, config, session, TCP listener
├── log.rs                   # LogBuffer: captures tracing output for the TUI
├── core/
│   ├── mod.rs
│   ├── torrent_file.rs      # .torrent file parsing (bencode → struct)
│   ├── torrent.rs           # Core torrent logic: state machine, peer loops, unchoke
│   ├── tracker.rs           # UDP tracker protocol (connect → announce)
│   ├── peer.rs              # TCP peer connection, handshake, message I/O
│   ├── piece.rs             # Piece/block management, PiecePicker, rarest-first
│   ├── message.rs           # BitTorrent wire protocol messages
│   └── bitfield.rs          # Bitfield data structure
├── tui/
│   ├── mod.rs
│   ├── app.rs               # TUI application: screens, drawing, key handling
│   └── screen.rs            # Screen enum (Main / Detail / Log)
└── persistence/
    ├── mod.rs               # Persistent trait (save/load JSON or binary)
    ├── resume_data.rs       # Resume data persistence (binary format)
    └── session.rs           # Session persistence (torrent list as JSON)
```

---

## Technical Details

### Async Runtime

Entirely built on **tokio**, the project uses async/await throughout. Each torrent spawns its own task, and within a
torrent every peer gets its own task — enabling concurrent downloads from multiple peers without blocking.

### Rarest-First Piece Selection

The `PiecePicker` maintains a frequency map of which pieces each peer has. When picking the next block to request:

- It first fills blocks of already-started pieces (Downloading state)
- If none are in progress, it picks the **rarest** missing piece (lowest frequency among peers)
- This ensures pieces don't become unavailable if the only peer hosting them disconnects

### Pipelining & Choking

Each peer maintains a pipeline of up to **20 in-flight requests** by default (reduced to 1 when only 5 pieces remain).
Every 10 seconds, the unchoke algorithm runs:

- Peers are sorted by upload rate
- The **top 4** interested peers are unchoked (tit-for-tat)
- Choked peers cannot request blocks

### Resume & Persistence

Progress is saved as a binary file (`.dat`) keyed by info hash hex. The format is simply `downloaded:u64` followed by
the raw bitfield bytes. The session (list of active torrents with paths) is stored as a JSON file. All data lives under
the platform's data directory (`~/.local/share/pirate` on Linux).

---

## Contributing

Contributions are welcome!

### GUI Frontend

The most impactful contribution would be a **proper graphical user interface**. The current TUI is functional but
minimal. A GUI could be built as:

- A desktop app using **egui**, **iced**, or **slint** (native Rust GUI frameworks)
- A web frontend using **Leptos** or **Yew** that communicates with the core library via a local HTTP/WebSocket API

The core logic in `src/core/` and `src/client.rs` is cleanly separated from the UI — the `Client` struct provides all
the primitives needed for a frontend:

- `all_torrents()` — snapshot of all torrent states
- `add_torrent()`, `pause()`, `resume()`, `remove()` — control operations
- `TorrentInfo` — contains progress, speeds, state, sizes
