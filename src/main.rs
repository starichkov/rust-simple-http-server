//! A small educational HTTP/1.1 server built only with the Rust standard library.
//!
//! The server demonstrates request parsing, a mutex-protected `mpsc` thread pool,
//! graceful worker shutdown, typed errors, request logging, and static files.

use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(unix)]
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Errors produced while parsing, serving, or writing an HTTP request.
#[derive(Debug)]
enum ServerError {
    /// An operating-system I/O error.
    Io(std::io::Error),
    /// The client sent a malformed request.
    InvalidRequest(String),
    /// The client requested an unsupported HTTP version.
    UnsupportedVersion(String),
    /// A requested static file does not exist or is unsafe to access.
    NotFound(String),
}

impl fmt::Display for ServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "I/O error: {error}"),
            Self::InvalidRequest(message) => write!(formatter, "invalid request: {message}"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported version: {version}")
            }
            Self::NotFound(path) => write!(formatter, "file not found: {path}"),
        }
    }
}

impl std::error::Error for ServerError {}

impl From<std::io::Error> for ServerError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl ServerError {
    /// Returns the HTTP status that best describes this error.
    fn status(&self) -> &'static str {
        match self {
            Self::InvalidRequest(_) | Self::UnsupportedVersion(_) => "400 BAD REQUEST",
            Self::NotFound(_) => "404 NOT FOUND",
            Self::Io(_) => "500 INTERNAL SERVER ERROR",
        }
    }
}

/// A small fixed-size pool that distributes connections among worker threads.
struct ThreadPool {
    workers: Vec<Worker>,
    sender: Option<mpsc::Sender<Message>>,
}

type Job = Box<dyn FnOnce() + Send + 'static>;

enum Message {
    NewJob(Job),
    Terminate,
}

impl ThreadPool {
    /// Creates `size` workers sharing one receiving channel.
    fn new(size: usize) -> Self {
        assert!(size > 0, "thread pool must contain at least one worker");
        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        let workers = (0..size)
            .map(|_| Worker::new(Arc::clone(&receiver)))
            .collect();
        Self {
            workers,
            sender: Some(sender),
        }
    }

    /// Queues a connection handler for an available worker.
    fn execute<F>(&self, job: F) -> Result<(), ServerError>
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender
            .as_ref()
            .ok_or_else(|| ServerError::InvalidRequest("thread pool is shut down".into()))?
            .send(Message::NewJob(Box::new(job)))
            .map_err(|_| ServerError::InvalidRequest("worker channel is closed".into()))
    }
}

impl Drop for ThreadPool {
    /// Signals every worker and joins them, completing graceful shutdown.
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            for _ in &self.workers {
                let _ = sender.send(Message::Terminate);
            }
        }
        for worker in &mut self.workers {
            if let Some(thread) = worker.thread.take() {
                thread.join().expect("worker thread panicked");
            }
        }
    }
}

struct Worker {
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(receiver: Arc<Mutex<mpsc::Receiver<Message>>>) -> Self {
        let thread = thread::spawn(move || {
            loop {
                let message = receiver.lock().expect("worker receiver poisoned").recv();
                match message {
                    Ok(Message::NewJob(job)) => job(),
                    Ok(Message::Terminate) | Err(_) => break,
                }
            }
        });
        Self {
            thread: Some(thread),
        }
    }
}

/// The request data needed by the server's router.
struct Request {
    /// The HTTP method, such as `GET` or `POST`.
    method: String,
    /// The requested path, such as `/` or `/echo`.
    path: String,
    /// The HTTP protocol version from the request line.
    version: String,
    /// Header names normalized to lowercase and their trimmed values.
    #[allow(dead_code)]
    headers: HashMap<String, String>,
    /// The request body, read according to the `Content-Length` header.
    body: Vec<u8>,
}

/// An HTTP response assembled before it is written to a client.
#[derive(Debug)]
struct Response {
    /// The status code and reason phrase, such as `200 OK`.
    status: &'static str,
    /// Response headers written between the status line and body.
    headers: HashMap<String, String>,
    /// The response payload as bytes.
    body: Vec<u8>,
}

impl Response {
    /// Creates a response with standard headers for this server.
    fn new(status: &'static str, body: impl Into<Vec<u8>>) -> Self {
        Self::with_content_type(status, body, "text/html; charset=utf-8")
    }

    /// Creates a response with an explicitly selected media type.
    fn with_content_type(
        status: &'static str,
        body: impl Into<Vec<u8>>,
        content_type: &'static str,
    ) -> Self {
        let body = body.into();
        let mut headers = HashMap::new();
        headers.insert("Content-Length".to_string(), body.len().to_string());
        headers.insert("Content-Type".to_string(), content_type.to_string());
        headers.insert("Connection".to_string(), "close".to_string());
        Self {
            status,
            headers,
            body,
        }
    }

    /// Writes the complete HTTP response to `stream` and flushes it.
    fn write_to(&self, stream: &mut TcpStream) -> std::io::Result<()> {
        write!(stream, "HTTP/1.1 {}\r\n", self.status)?;
        for (name, value) in &self.headers {
            write!(stream, "{name}: {value}\r\n")?;
        }
        stream.write_all(b"\r\n")?;
        stream.write_all(&self.body)?;
        stream.flush()
    }
}

/// Starts the server and dispatches accepted connections to a fixed worker pool.
fn main() {
    let address = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:7878".to_string());
    let listener = TcpListener::bind(&address).expect("could not bind to server address");
    listener
        .set_nonblocking(true)
        .expect("could not make listener nonblocking");
    install_shutdown_handler();
    let pool = ThreadPool::new(4);
    let static_dir = PathBuf::from("public");

    eprintln!("listening on {address}; press Ctrl-C to stop");
    while !shutdown_requested() {
        match listener.accept() {
            Ok((stream, _)) => {
                let static_dir = static_dir.clone();
                if let Err(error) = pool.execute(move || {
                    if let Err(error) = handle_connection(stream, &static_dir) {
                        eprintln!("connection error: {error}");
                    }
                }) {
                    eprintln!("failed to queue connection: {error}");
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(25));
            }
            Err(error) => eprintln!("failed to accept connection: {error}"),
        }
    }
}

#[cfg(unix)]
extern "C" fn request_shutdown(_: i32) {
    SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
}

fn install_shutdown_handler() {
    #[cfg(unix)]
    unsafe {
        unsafe extern "C" {
            fn signal(signal: i32, handler: extern "C" fn(i32)) -> usize;
        }
        signal(2, request_shutdown);
    }
}

fn shutdown_requested() -> bool {
    #[cfg(unix)]
    {
        SHUTDOWN_REQUESTED.load(Ordering::SeqCst)
    }
    #[cfg(not(unix))]
    {
        false
    }
}

/// Parses and serves one client connection while logging method, path, status, and duration.
fn handle_connection(mut stream: TcpStream, static_dir: &Path) -> Result<(), ServerError> {
    let started = Instant::now();
    let mut reader = BufReader::new(&stream);
    let parsed = parse_request(&mut reader);
    drop(reader);
    let result = parsed.and_then(|request| {
        if request.version != "HTTP/1.1" {
            return Err(ServerError::UnsupportedVersion(request.version));
        }
        let response = response_for(&request, static_dir)?;
        response.write_to(&mut stream)?;
        Ok((request.method, request.path, response.status))
    });
    match result {
        Ok((method, path, status)) => {
            eprintln!("{method} {path} -> {status} ({:?})", started.elapsed());
            Ok(())
        }
        Err(error) => {
            eprintln!(
                "request failed -> {} ({:?}): {error}",
                error.status(),
                started.elapsed()
            );
            let response = Response::new(error.status(), format!("<h1>{error}</h1>"));
            response.write_to(&mut stream).map_err(ServerError::from)
        }
    }
}

/// Reads an HTTP request line, headers, and body from a buffered stream.
///
/// Header names are stored case-insensitively by normalizing them to lowercase.
/// If `Content-Length` is absent, the request body is empty.
fn parse_request(reader: &mut impl BufRead) -> Result<Request, ServerError> {
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;
    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| invalid_request("missing method"))?;
    let path = parts
        .next()
        .ok_or_else(|| invalid_request("missing path"))?;
    let version = parts
        .next()
        .ok_or_else(|| invalid_request("missing HTTP version"))?;

    let mut headers = HashMap::new();
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            return Err(invalid_request("missing header separator"));
        }
        if line == "\r\n" || line == "\n" {
            break;
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| invalid_request("malformed header"))?;
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }

    let content_length = headers
        .get("content-length")
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|_| invalid_request("invalid Content-Length"))
        })
        .transpose()?
        .unwrap_or(0);
    let mut body = vec![0; content_length];
    reader.read_exact(&mut body)?;

    Ok(Request {
        method: method.to_string(),
        path: path.to_string(),
        version: version.to_string(),
        headers,
        body,
    })
}

/// Creates an invalid-data error with a parser-specific message.
fn invalid_request(message: &str) -> ServerError {
    ServerError::InvalidRequest(message.to_string())
}

/// Selects a response by matching the request method and path as a tuple.
fn response_for(request: &Request, static_dir: &Path) -> Result<Response, ServerError> {
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/") => Ok(Response::new("200 OK", "<h1>Hello from Rust!</h1>")),
        ("GET", "/sleep") => {
            thread::sleep(Duration::from_secs(5));
            Ok(Response::new("200 OK", "<h1>Finished sleeping.</h1>"))
        }
        ("POST", "/echo") => Ok(Response::new("200 OK", request.body.clone())),
        ("GET", path) if path.starts_with("/static/") => static_response(static_dir, path),
        _ => Ok(Response::new("404 NOT FOUND", "<h1>Page not found.</h1>")),
    }
}

fn static_response(static_dir: &Path, url_path: &str) -> Result<Response, ServerError> {
    let relative = &url_path["/static/".len()..];
    let relative_path = Path::new(relative);
    if relative_path.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return Err(ServerError::NotFound(url_path.to_string()));
    }
    let path = static_dir.join(relative_path);
    let body = fs::read(&path).map_err(|_| ServerError::NotFound(url_path.to_string()))?;
    Ok(Response::with_content_type(
        "200 OK",
        body,
        content_type(&path),
    ))
}

fn content_type(path: &Path) -> &'static str {
    match path.extension().and_then(|extension| extension.to_str()) {
        Some("html") => "text/html; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("js") => "text/javascript; charset=utf-8",
        Some("json") => "application/json",
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("svg") => "image/svg+xml",
        Some("txt") => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
/// Unit tests for request parsing, routing, and response construction.
mod tests {
    use super::{parse_request, response_for};
    use std::io::Cursor;
    use std::path::Path;

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
        let error =
            super::static_response(Path::new("public"), "/static/../Cargo.toml").unwrap_err();

        assert_eq!(error.status(), "404 NOT FOUND");
    }
}
