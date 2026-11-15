//! Plumbing: newline-delimited JSON over TCP between cells and clients, and a minimal HTTP/1.1
//! server and client for the page, the control API and the MCP-style name service.

use serde::{Serialize, de::DeserializeOwned};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

/// One JSON line each way on a kept-open connection.
pub struct Line {
    r: BufReader<TcpStream>,
    w: TcpStream,
}

impl Line {
    pub fn connect(addr: &str, timeout: Duration) -> std::io::Result<Line> {
        let sa = addr
            .parse()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("bad address {addr}")))?;
        let s = TcpStream::connect_timeout(&sa, timeout)?;
        Line::from_stream(s, Some(timeout))
    }

    pub fn from_stream(s: TcpStream, timeout: Option<Duration>) -> std::io::Result<Line> {
        s.set_nodelay(true)?;
        s.set_read_timeout(timeout)?;
        Ok(Line { r: BufReader::new(s.try_clone()?), w: s })
    }

    pub fn send<T: Serialize>(&mut self, v: &T) -> std::io::Result<()> {
        let mut b = serde_json::to_vec(v)?;
        b.push(b'\n');
        self.w.write_all(&b)
    }

    /// `None` on a clean close.
    pub fn recv<T: DeserializeOwned>(&mut self) -> std::io::Result<Option<T>> {
        let mut s = String::new();
        if self.r.read_line(&mut s)? == 0 {
            return Ok(None);
        }
        serde_json::from_str(&s).map(Some).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    pub fn call<T: Serialize, R: DeserializeOwned>(&mut self, v: &T) -> std::io::Result<R> {
        self.send(v)?;
        self.recv()?.ok_or_else(|| std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "connection closed"))
    }
}

/// A parsed HTTP request.
pub struct Request {
    pub method: String,
    pub path: String,
    pub query: String,
    pub body: Vec<u8>,
}

/// Serve HTTP on `l`, one thread per connection, `Connection: close`.
pub fn serve_http<F>(l: TcpListener, handler: F)
where
    F: Fn(&Request) -> (u16, &'static str, Vec<u8>) + Send + Sync + 'static,
{
    let handler = std::sync::Arc::new(handler);
    std::thread::spawn(move || {
        for s in l.incoming().flatten() {
            let h = handler.clone();
            std::thread::spawn(move || {
                let _ = handle_http(s, &*h);
            });
        }
    });
}

fn handle_http<F>(mut s: TcpStream, handler: &F) -> std::io::Result<()>
where
    F: Fn(&Request) -> (u16, &'static str, Vec<u8>),
{
    s.set_read_timeout(Some(Duration::from_secs(10)))?;
    s.set_nodelay(true)?;
    let mut r = BufReader::new(s.try_clone()?);
    let mut first = String::new();
    r.read_line(&mut first)?;
    let mut parts = first.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("/").to_string();
    let mut len = 0usize;
    loop {
        let mut h = String::new();
        if r.read_line(&mut h)? == 0 || h == "\r\n" || h == "\n" {
            break;
        }
        if let Some((k, v)) = h.split_once(':')
            && k.trim().eq_ignore_ascii_case("content-length")
        {
            len = v.trim().parse().unwrap_or(0);
        }
    }
    let mut body = vec![0; len.min(64 << 20)];
    r.read_exact(&mut body)?;
    let (path, query) = target.split_once('?').map(|(a, b)| (a.to_string(), b.to_string())).unwrap_or((target, String::new()));
    let (code, ctype, out) = handler(&Request { method, path, query, body });
    let reason = match code {
        200 => "OK",
        404 => "Not Found",
        _ => "Error",
    };
    let mut resp = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        out.len()
    )
    .into_bytes();
    resp.extend(out);
    s.write_all(&resp)
}

/// POST `body` (JSON) to `url` (`http://host:port/path`) and return the response body.
pub fn http_post(url: &str, body: &[u8], timeout: Duration) -> Result<Vec<u8>, String> {
    let rest = url.strip_prefix("http://").ok_or("only http:// urls")?;
    let (host, path) = rest.split_once('/').map(|(h, p)| (h, format!("/{p}"))).unwrap_or((rest, "/".into()));
    let sa: std::net::SocketAddr = host.parse().map_err(|_| format!("bad host {host}"))?;
    let mut s = TcpStream::connect_timeout(&sa, timeout).map_err(|e| format!("{url}: {e}"))?;
    s.set_read_timeout(Some(timeout)).ok();
    s.set_nodelay(true).ok();
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let mut msg = req.into_bytes();
    msg.extend_from_slice(body);
    s.write_all(&msg).map_err(|e| e.to_string())?;
    let mut resp = Vec::new();
    s.read_to_end(&mut resp).map_err(|e| e.to_string())?;
    let at = resp.windows(4).position(|w| w == b"\r\n\r\n").ok_or("bad http response")?;
    let status = String::from_utf8_lossy(&resp[..at]).split_whitespace().nth(1).unwrap_or("").to_string();
    let out = resp[at + 4..].to_vec();
    if status != "200" {
        return Err(format!("{url}: http {status}: {}", String::from_utf8_lossy(&out)));
    }
    Ok(out)
}

/// Call an MCP tool over JSON-RPC (`tools/call`) and return its `structuredContent`.
pub fn mcp_call(url: &str, tool: &str, args: serde_json::Value) -> Result<serde_json::Value, String> {
    let msg = serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": tool, "arguments": args}});
    let out = http_post(url, msg.to_string().as_bytes(), Duration::from_secs(15))?;
    let v: serde_json::Value = serde_json::from_slice(&out).map_err(|e| e.to_string())?;
    let res = &v["result"];
    if res["isError"].as_bool() == Some(true) || res.is_null() {
        let text = res["content"][0]["text"].as_str().map(str::to_string).unwrap_or_else(|| v["error"].to_string());
        return Err(format!("{tool}: {text}"));
    }
    Ok(res["structuredContent"].clone())
}

/// Wrap a tool answer (`Ok`) or refusal (`Err`) as an MCP `tools/call` result.
pub fn mcp_result(id: serde_json::Value, out: Result<serde_json::Value, String>) -> Vec<u8> {
    let result = match out {
        Ok(v) => serde_json::json!({"content": [{"type": "text", "text": v.to_string()}], "structuredContent": v}),
        Err(e) => serde_json::json!({"content": [{"type": "text", "text": e}], "isError": true}),
    };
    serde_json::json!({"jsonrpc": "2.0", "id": id, "result": result}).to_string().into_bytes()
}
