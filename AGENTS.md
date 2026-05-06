# OpenCode Instructions for Pirate

This is an async BitTorrent client built in Rust.

## Build & Verify
- Standard Rust workflow: `cargo build` and `cargo check`.
- There are currently no tests (`cargo test`), but rely on `cargo clippy` to keep the codebase clean.
- Always resolve `cargo clippy` warnings before considering a task complete.

## Architecture
- `src/main.rs`: Application entrypoint and tracing initialization.
- `src/client.rs`: High-level client state manager.
- `src/core/`: Core BitTorrent protocol implementation (`peer`, `piece`, `torrent`, `tracker`, `message`, `bitfield`).

## Code Conventions
- **Error Handling**: Use `anyhow` (`anyhow::Result`, `Context`, `bail!`).
- **Logging**: Use `tracing` (`info!`, `warn!`, `error!`, `debug!`). Annotate async functions with `#[instrument]` where appropriate to trace execution flow. Avoid `println!`.
- **Async Runtime**: Built on `tokio`. Ensure proper use of async/await patterns without blocking the runtime.
