//! Black-box coverage for command-line startup and bind-address handling.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant};

struct Server {
    process: Child,
    address: String,
}

impl Server {
    fn start() -> Self {
        let address = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .to_string();
        let mut process = Command::new(env!("CARGO_BIN_EXE_rust-simple-http-server"))
            .arg(&address)
            .spawn()
            .expect("failed to start server with a CLI address");
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if TcpStream::connect(&address).is_ok() {
                return Self { process, address };
            }
            thread::sleep(Duration::from_millis(10));
        }
        let _ = process.kill();
        let _ = process.wait();
        panic!("server did not start on the CLI-provided address");
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

#[test]
/// Confirms the executable accepts a bind address and serves on that address.
fn starts_on_command_line_bind_address() {
    let server = Server::start();
    let mut stream = TcpStream::connect(&server.address).unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();
    let response = String::from_utf8_lossy(&response);
    assert!(response.contains("HTTP/1.1 200 OK"));
    assert!(response.contains("Hello from Rust!"));
}

#[test]
/// Confirms invalid startup configuration is reported on stderr and exits nonzero.
fn rejects_invalid_command_line_bind_address() {
    let output = Command::new(env!("CARGO_BIN_EXE_rust-simple-http-server"))
        .arg("not-an-address")
        .output()
        .expect("failed to launch server with invalid address");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("startup error: could not bind to not-an-address"));
}
