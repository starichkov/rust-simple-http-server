//! Integration coverage for the server's fixed worker-pool design.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant};

/// A running server process and the address on which it accepts connections.
struct Server {
    /// The child process, terminated when this value is dropped.
    process: Child,
    /// The ephemeral loopback address selected for this test.
    address: String,
}

impl Server {
    /// Starts the server on an available local port and waits until it accepts connections.
    fn start() -> Self {
        let address = TcpListener::bind("127.0.0.1:0")
            .expect("failed to find an available port")
            .local_addr()
            .expect("failed to read the available port")
            .to_string();
        let mut process = Command::new(env!("CARGO_BIN_EXE_rust-simple-http-server"))
            .arg(&address)
            .spawn()
            .expect("failed to start server");

        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if TcpStream::connect(&address).is_ok() {
                return Self { process, address };
            }
            thread::sleep(Duration::from_millis(10));
        }

        let _ = process.kill();
        let _ = process.wait();
        panic!("server did not start in time");
    }
}

impl Drop for Server {
    /// Stops the child process so test failures do not leave a server running.
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

/// Sends a request and returns all bytes received from the server.
fn request(address: &str, path: &str) -> Vec<u8> {
    let mut stream = TcpStream::connect(address).expect("failed to connect to server");
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .expect("failed to write request");

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .expect("failed to read response");
    response
}

#[test]
/// Confirms a fast request is not blocked by another connection sleeping.
fn handles_requests_concurrently() {
    let server = Server::start();
    let slow_address = server.address.clone();

    let slow_request = thread::spawn(move || request(&slow_address, "/sleep"));
    thread::sleep(Duration::from_millis(100));

    let started = Instant::now();
    let fast_response = request(&server.address, "/");
    let fast_elapsed = started.elapsed();

    assert!(
        fast_elapsed < Duration::from_secs(2),
        "fast request took {fast_elapsed:?} while another request was sleeping"
    );
    assert!(String::from_utf8_lossy(&fast_response).contains("200 OK"));
    assert!(String::from_utf8_lossy(&fast_response).contains("Hello from Rust!"));

    let slow_response = slow_request.join().expect("slow request thread panicked");
    assert!(String::from_utf8_lossy(&slow_response).contains("Finished sleeping."));
}
