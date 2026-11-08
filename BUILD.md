# Building and developing

This project uses Cargo and the stable Rust toolchain. It has no external
runtime dependencies.

## Prerequisites

- Install the stable Rust toolchain using [rustup](https://rustup.rs/).
- Ensure `cargo` is available on your `PATH`.
- Use a Rust toolchain that supports edition 2024 and Rust 1.85 or newer.

## Build profiles

Cargo uses its faster, unoptimized development profile by default:

```bash
cargo build
```

The debug executable is written to `target/debug/rust-simple-http-server`.
For a smaller and faster executable, build with the optimized release profile:

```bash
cargo build --release
```

The release executable is written to
`target/release/rust-simple-http-server`. It can also be run through Cargo:

```bash
cargo run --release
```

## Validation

Run the same checks used by GitHub Actions:

```bash
cargo fmt --all -- --check
cargo check --all-targets
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
cargo doc --no-deps
```

To generate an LCOV coverage report locally, install `cargo-llvm-cov` and run:

```bash
cargo llvm-cov --all-features --workspace --lcov --output-path lcov.info
```

GitHub Actions uploads this report to Codecov after the coverage job completes.

To generate and open local API documentation:

```bash
cargo doc --no-deps --open
```

## Run focused checks

Format the code in place:

```bash
cargo fmt
```

Run only the concurrency integration test:

```bash
cargo test --test concurrency
```

The concurrency test starts the server on an ephemeral local port, sends a
slow `/sleep` request and a fast `/` request at the same time, and verifies
that the fast request is not blocked.

## Project layout

- `src/main.rs` — request parsing, routing, responses, worker pool, and server entry point.
- `public/` — static files served by `GET /static/<file>`.
- `tests/concurrency.rs` — integration coverage for concurrent connections.
- `.github/workflows/rust.yml` — formatting, compilation, tests, Clippy, and documentation checks.

## Run the server locally

Start the server with Cargo:

```bash
cargo run
```

The server listens on `127.0.0.1:7878` by default. Pass a different bind
address after `--` when needed:

```bash
cargo run -- 127.0.0.1:8080
```
