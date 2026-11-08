# Rust Simple HTTP Server

[![Author](https://img.shields.io/badge/Author-Vadim%20Starichkov-blue?style=for-the-badge)](https://github.com/starichkov)
[![GitHub License](https://img.shields.io/github/license/starichkov/rust-simple-http-server?style=for-the-badge)](https://github.com/starichkov/rust-simple-http-server/blob/main/LICENSE.md)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/starichkov/rust-simple-http-server/rust.yml?style=for-the-badge)](https://github.com/starichkov/rust-simple-http-server/actions/workflows/rust.yml)
[![Codecov](https://img.shields.io/codecov/c/github/starichkov/rust-simple-http-server?style=for-the-badge)](https://codecov.io/gh/starichkov/rust-simple-http-server)
![Rust](https://img.shields.io/badge/Rust-2024-orange?style=for-the-badge)

A small HTTP server built from scratch with Rust's standard library. It uses
`TcpListener` to accept connections, `TcpStream` and `BufReader` to process
requests, and a fixed thread pool to handle clients concurrently.

## Requirements

- Rust and Cargo with support for edition 2024

## Run the server

Start the server on the default address:

```bash
cargo run
```

The server listens on `127.0.0.1:7878`. You can provide a different bind
address as the first argument:

```bash
cargo run -- 127.0.0.1:8080
```

Then visit `http://127.0.0.1:7878/` in a browser or use `curl`:

```bash
curl http://127.0.0.1:7878/
```

## Routes

- `/` returns the home page.
- `/sleep` waits five seconds before returning, demonstrating that other
  connections can be served concurrently.
- `POST /echo` returns the request body, demonstrating `Content-Length` body
  parsing.
- `GET /static/<file>` serves a file from the `public` directory with a
  content type selected from its extension.
- Any other path returns `404 NOT FOUND`.

Each request is logged with its method, path, status, and elapsed time. Press
`Ctrl-C` to stop accepting new connections; the worker pool finishes queued
connections before its `Drop` implementation joins every worker.

## Build, test, and development

See [BUILD.md](BUILD.md) for prerequisites, build profiles, validation commands,
and the project layout.

## Attribution and license

This project is licensed under the [MIT License](LICENSE.md).

If you use this code in your own projects, attribution is required under the
MIT License:

```text
Based on rust-simple-http-server by Vadim Starichkov, TemplateTasks

https://github.com/starichkov/rust-simple-http-server
```

Copyright © 2026 Vadim Starichkov, TemplateTasks.
