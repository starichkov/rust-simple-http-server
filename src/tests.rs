//! Unit tests for request parsing, routing, and response construction.

use super::{
    Response, ServerError, ThreadPool, content_type, handle_connection, parse_request,
    response_for, shutdown_requested,
};
#[cfg(unix)]
use super::{SHUTDOWN_REQUESTED, install_shutdown_handler, request_shutdown};
use std::io::{Cursor, Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;

#[test]
/// Parses all request components, including a body with `Content-Length`.
fn parses_request_line_headers_and_body() {
    let input = b"POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\r\nhello";
    let request = parse_request(&mut Cursor::new(input)).unwrap();

    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/echo");
    assert_eq!(request.version, "HTTP/1.1");
    assert_eq!(request.headers["host"], "localhost");
    assert_eq!(request.body, b"hello");
}

#[test]
/// Routes requests according to their method and path.
fn dispatches_by_method_and_path() {
    let get = parse_request(&mut Cursor::new(b"GET / HTTP/1.1\r\n\r\n")).unwrap();
    let post = parse_request(&mut Cursor::new(
        b"POST /echo HTTP/1.1\r\nContent-Length: 2\r\n\r\nok",
    ))
    .unwrap();
    let missing = parse_request(&mut Cursor::new(b"POST / HTTP/1.1\r\n\r\n")).unwrap();

    assert_eq!(
        response_for(&get, Path::new("public")).unwrap().status,
        "200 OK"
    );
    assert_eq!(
        response_for(&post, Path::new("public")).unwrap().body,
        b"ok"
    );
    assert_eq!(
        response_for(&missing, Path::new("public")).unwrap().status,
        "404 NOT FOUND"
    );
}

#[test]
/// Includes the status, standard headers, and body in a response.
fn response_contains_status_headers_and_body() {
    let request = parse_request(&mut Cursor::new(b"GET / HTTP/1.1\r\n\r\n")).unwrap();
    let response = response_for(&request, Path::new("public")).unwrap();

    assert_eq!(response.headers["Content-Length"], "25");
    assert_eq!(response.headers["Content-Type"], "text/html; charset=utf-8");
    assert_eq!(response.body, b"<h1>Hello from Rust!</h1>");
}

#[test]
/// Serves a public file and chooses its media type from the extension.
fn serves_static_files_with_content_type() {
    let response = super::static_response(Path::new("public"), "/static/index.html").unwrap();

    assert_eq!(response.status, "200 OK");
    assert_eq!(response.headers["Content-Type"], "text/html; charset=utf-8");
    assert!(String::from_utf8_lossy(&response.body).contains("Static files from Rust"));
}

#[test]
/// Rejects parent-directory components instead of escaping the public directory.
fn rejects_static_path_traversal() {
    let error = super::static_response(Path::new("public"), "/static/../Cargo.toml").unwrap_err();

    assert_eq!(error.status(), "404 NOT FOUND");
}

#[test]
/// Reports each server error in a readable form and maps it to HTTP status.
fn formats_errors_and_maps_statuses() {
    let errors = [
        (
            ServerError::Io(std::io::Error::other("disk")),
            "I/O error: disk",
            "500 INTERNAL SERVER ERROR",
        ),
        (
            ServerError::InvalidRequest("bad".into()),
            "invalid request: bad",
            "400 BAD REQUEST",
        ),
        (
            ServerError::UnsupportedVersion("HTTP/2".into()),
            "unsupported version: HTTP/2",
            "400 BAD REQUEST",
        ),
        (
            ServerError::NotFound("/missing".into()),
            "file not found: /missing",
            "404 NOT FOUND",
        ),
    ];

    for (error, display, status) in errors {
        assert_eq!(error.to_string(), display);
        assert_eq!(error.status(), status);
    }

    let io_error: ServerError = std::io::Error::other("converted").into();
    assert_eq!(io_error.to_string(), "I/O error: converted");
}

#[test]
/// Rejects incomplete request lines, headers, and invalid body lengths.
fn rejects_malformed_requests() {
    let cases = [
        (b"\r\n".as_slice(), "missing method"),
        (b"GET\r\n".as_slice(), "missing path"),
        (b"GET /\r\n".as_slice(), "missing HTTP version"),
        (
            b"GET / HTTP/1.1\r\nHost: localhost".as_slice(),
            "missing header separator",
        ),
        (
            b"GET / HTTP/1.1\r\nMalformed\r\n\r\n".as_slice(),
            "malformed header",
        ),
        (
            b"GET / HTTP/1.1\r\nContent-Length: nope\r\n\r\n".as_slice(),
            "invalid Content-Length",
        ),
    ];

    for (input, message) in cases {
        assert_eq!(
            parse_request(&mut Cursor::new(input))
                .unwrap_err()
                .to_string(),
            format!("invalid request: {message}")
        );
    }
}

#[test]
/// Executes queued jobs and joins workers when the pool is dropped.
fn thread_pool_runs_jobs_and_shuts_down() {
    let completed = Arc::new(Mutex::new(Vec::new()));
    let pool = ThreadPool::new(2);
    for value in 0..4 {
        let completed = Arc::clone(&completed);
        pool.execute(move || completed.lock().unwrap().push(value))
            .unwrap();
    }
    drop(pool);

    let mut values = completed.lock().unwrap().clone();
    values.sort_unstable();
    assert_eq!(values, vec![0, 1, 2, 3]);
}

#[test]
#[should_panic(expected = "thread pool must contain at least one worker")]
/// Rejects a pool with no workers because it could never execute jobs.
fn thread_pool_requires_a_worker() {
    let _ = ThreadPool::new(0);
}

#[test]
/// Returns a typed error when work is submitted after shutdown begins.
fn thread_pool_rejects_jobs_after_sender_is_taken() {
    let mut pool = ThreadPool::new(1);
    pool.sender.take();
    let error = pool.execute(|| {}).unwrap_err();
    assert_eq!(
        error.to_string(),
        "invalid request: thread pool is shut down"
    );
}

#[cfg(unix)]
#[test]
/// Installs the Ctrl-C handler and records a requested graceful shutdown.
fn shutdown_handler_sets_shutdown_flag() {
    SHUTDOWN_REQUESTED.store(false, std::sync::atomic::Ordering::SeqCst);
    install_shutdown_handler();
    assert!(!shutdown_requested());
    request_shutdown(2);
    assert!(shutdown_requested());
}

#[test]
/// Serializes a response over TCP with its status, headers, and body.
fn writes_response_to_tcp_stream() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let client = thread::spawn(move || {
        let mut stream = std::net::TcpStream::connect(address).unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).unwrap();
        response
    });
    let (mut stream, _) = listener.accept().unwrap();
    Response::new("204 NO CONTENT", "done")
        .write_to(&mut stream)
        .unwrap();
    drop(stream);

    let response = String::from_utf8(client.join().unwrap()).unwrap();
    assert!(response.starts_with("HTTP/1.1 204 NO CONTENT\r\n"));
    assert!(response.contains("Content-Length: 4\r\n"));
    assert!(response.ends_with("\r\ndone"));
}

#[test]
/// Returns every supported media type and the binary fallback.
fn detects_content_types() {
    let cases = [
        ("index.html", "text/html; charset=utf-8"),
        ("site.css", "text/css; charset=utf-8"),
        ("app.js", "text/javascript; charset=utf-8"),
        ("data.json", "application/json"),
        ("image.png", "image/png"),
        ("photo.jpg", "image/jpeg"),
        ("photo.jpeg", "image/jpeg"),
        ("icon.svg", "image/svg+xml"),
        ("notes.txt", "text/plain; charset=utf-8"),
        ("archive.bin", "application/octet-stream"),
        ("README", "application/octet-stream"),
    ];
    for (path, expected) in cases {
        assert_eq!(content_type(Path::new(path)), expected);
    }
}

#[test]
/// Handles successful and failing connections through the real TCP path.
fn handles_connection_responses_and_errors() {
    let cases = [
        ("GET / HTTP/1.1\r\n\r\n", "200 OK"),
        ("GET /missing HTTP/1.1\r\n\r\n", "404 NOT FOUND"),
        ("GET / HTTP/1.0\r\n\r\n", "400 BAD REQUEST"),
        ("GET\r\n", "400 BAD REQUEST"),
    ];
    for (request, status) in cases {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let request = request.to_string();
        let client = thread::spawn(move || {
            let mut stream = std::net::TcpStream::connect(address).unwrap();
            stream.write_all(request.as_bytes()).unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).unwrap();
            response
        });
        let (stream, _) = listener.accept().unwrap();
        handle_connection(stream, Path::new("public")).unwrap();
        let response = String::from_utf8(client.join().unwrap()).unwrap();
        assert!(response.contains(status), "response was {response}");
    }
}

#[test]
/// Returns a typed not-found error for missing static files.
fn missing_static_file_is_not_found() {
    let error =
        super::static_response(Path::new("public"), "/static/no-such-file.txt").unwrap_err();
    assert!(matches!(error, ServerError::NotFound(_)));
}

#[test]
/// Serves a file using a caller-provided temporary directory.
fn serves_static_file_from_directory() {
    let directory = PathBuf::from("target/test-public");
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join("data.json"), b"{}").unwrap();
    let response = super::static_response(&directory, "/static/data.json").unwrap();
    assert_eq!(response.body, b"{}");
    assert_eq!(response.headers["Content-Type"], "application/json");
}
