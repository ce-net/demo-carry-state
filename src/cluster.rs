//! The control process: the name service (`name.resolve` over MCP JSON-RPC), cell processes A
//! and B, the load clients, moves, the audit and the page.

use crate::net::{Line, Request, http_post, mcp_call, mcp_result, serve_http};
use crate::wire::{MoveReport, Req, Resp};
use cell_migrate::{Genesis, HOLD_ABILITY, Handoff, chain_next, genesis_bytes, now_secs, sign_hex};
use ce_net_proto::{Caveats, Identity, Resource, SignedCapability};
use name_resolve::Names;
use notes_store::{Audit, Note};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

pub const NAME: &str = "notes";

/// A fresh demo key, generated per run.
fn fresh_identity(label: &str) -> Identity {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    let rs = std::hash::BuildHasher::hash_one(&std::collections::hash_map::RandomState::new(), label);
    let mut h = Sha256::new();
    h.update(b"carry-state demo key");
    h.update(nanos.to_le_bytes());
    h.update(std::process::id().to_le_bytes());
    h.update(rs.to_le_bytes());
    h.update(label.as_bytes());
    Identity::from_secret_bytes(&h.finalize().into())
}

pub struct CellHandle {
    pub label: String,
    pub addr: String,
    pub key: String,
    pub pid: u32,
    child: Child,
    _stdin: ChildStdin,
}

impl Drop for CellHandle {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[derive(Default)]
struct ClientStat {
    /// Distinct writes issued (first attempts).
    sent: AtomicU64,
    /// Writes answered "ok".
    acked: AtomicU64,
    /// Resends after "moved", "not holder" or a broken connection.
    retries: AtomicU64,
    /// "moved, epoch N" answers received.
    moved: AtomicU64,
    /// Longest wait for one write, start to ack, since the last reset (microseconds).
    max_wait_us: AtomicU64,
    idle: AtomicBool,
}

/// One move, as recorded for the page and the proof.
#[derive(Debug, Clone, Serialize)]
pub struct MoveRecord {
    pub n: usize,
    pub from: String,
    pub to: String,
    #[serde(flatten)]
    pub report: MoveReport,
    /// Longest any client waited for one write around this move (freeze, "moved", re-resolve, resend).
    pub client_max_wait_ms: f64,
    pub sent: u64,
    pub acked: u64,
    pub present: u64,
    pub duplicated: u64,
    pub lost: u64,
    /// The holder's chain head at the audit, and the same head recomputed from the first write
    /// over every note in the store. They must be equal.
    pub head_at_audit: String,
    pub chain_recomputed: String,
    /// What the old holder answered to a direct write after the move.
    pub stale_reply: String,
    pub ok: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Checkpoint {
    pub sent: u64,
    pub acked: u64,
    pub retries: u64,
    pub moved_replies: u64,
    #[serde(flatten)]
    pub audit: Audit,
    pub epoch: u64,
    pub index: u64,
    pub head: String,
    pub chain_recomputed: String,
    pub client_max_wait_ms: f64,
}

pub struct Cluster {
    pub url: String,
    pub resolver_url: String,
    /// Where clients found the name service: the resolver itself, or via the local-dev-sim.
    pub discovered_via: String,
    pub cells: Vec<CellHandle>,
    pub rate: f64,
    names: Arc<Mutex<Names>>,
    clients: Vec<(String, Arc<ClientStat>)>,
    pause: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    moves: Mutex<Vec<MoveRecord>>,
    moving: Mutex<()>,
    pub sim_note: Option<serde_json::Value>,
}

pub struct Opts {
    pub listen: String,
    pub rate: f64,
    pub clients: usize,
    pub sim: Option<String>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

impl Cluster {
    pub fn start(o: Opts) -> Result<Arc<Cluster>, String> {
        let root = fresh_identity("root");
        let names = Arc::new(Mutex::new(Names::new(root.node_id())));
        let l = TcpListener::bind(&o.listen).map_err(|e| format!("bind {}: {e}", o.listen))?;
        let url = format!("http://{}", l.local_addr().map_err(|e| e.to_string())?);
        let resolver_url = format!("{url}/mcp");
        let me: Arc<OnceLock<Arc<Cluster>>> = Arc::new(OnceLock::new());
        {
            let (names, me) = (names.clone(), me.clone());
            serve_http(l, move |r| route(r, &names, me.get()));
        }

        // Processes A and B, each with its own key and a hold capability from the root.
        let mut cells = Vec::new();
        for (i, label) in ["a", "b"].iter().enumerate() {
            let id = fresh_identity(label);
            let cap = SignedCapability::issue(
                &root,
                id.node_id(),
                vec![HOLD_ABILITY.into()],
                Resource::Any,
                Caveats { not_after: now_secs() + 7 * 86400, ..Default::default() },
                i as u64 + 1,
                None,
            );
            cells.push(spawn_cell(label, &id, &ce_net_proto::encode_chain(&[cap]), &root, &resolver_url)?);
        }

        // Bind the name to A under epoch 1, signed by the root.
        let g = Genesis { name: NAME.into(), epoch: 1, holder: cells[0].key.clone(), addr: cells[0].addr.clone() };
        let sig = sign_hex(&root, &genesis_bytes(&g));
        lock(&names).genesis(&g, &sig)?;
        match cell_call(&cells[0].addr, &Req::Activate { genesis: g, root_sig: sig })? {
            Resp::Ok { .. } => {}
            r => return Err(format!("activate: {r:?}")),
        }

        // Optional: list the name service on the local-dev-sim and let clients find it there.
        let (mut client_resolver, mut discovered_via, mut sim_note) = (resolver_url.clone(), "direct".to_string(), None);
        if let Some(sim) = &o.sim {
            let (found, note) = sim_publish_and_find(sim, &resolver_url)?;
            client_resolver = found;
            discovered_via = format!("local-dev-sim apps_search at {sim}");
            sim_note = Some(note);
        }

        let pause = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let mut clients = Vec::new();
        for i in 0..o.clients.max(1) {
            let name = format!("c{}", i + 1);
            let st = Arc::new(ClientStat::default());
            let (st2, p, s, url2, n2, rate) = (st.clone(), pause.clone(), stop.clone(), client_resolver.clone(), name.clone(), o.rate);
            std::thread::spawn(move || client_loop(&n2, &url2, rate, &st2, &p, &s));
            clients.push((name, st));
        }
        let c = Arc::new(Cluster {
            url,
            resolver_url,
            discovered_via,
            cells,
            rate: o.rate,
            names,
            clients,
            pause,
            stop,
            moves: Mutex::new(Vec::new()),
            moving: Mutex::new(()),
            sim_note,
        });
        let _ = me.set(c.clone());
        Ok(c)
    }

    pub fn holder(&self) -> Option<&CellHandle> {
        let b = lock(&self.names).resolve(NAME).cloned()?;
        self.cells.iter().find(|c| c.key == b.holder)
    }

    pub fn epoch(&self) -> u64 {
        lock(&self.names).resolve(NAME).map(|b| b.epoch).unwrap_or(0)
    }

    pub fn moves(&self) -> Vec<MoveRecord> {
        lock(&self.moves).clone()
    }

    pub fn shutdown(&self) {
        self.stop.store(true, SeqCst);
    }

    /// Live counters (no pause).
    pub fn live(&self) -> serde_json::Value {
        let (mut sent, mut acked, mut retries, mut moved) = (0, 0, 0, 0);
        for (_, s) in &self.clients {
            sent += s.sent.load(SeqCst);
            acked += s.acked.load(SeqCst);
            retries += s.retries.load(SeqCst);
            moved += s.moved.load(SeqCst);
        }
        let present = self
            .holder()
            .and_then(|h| match cell_call(&h.addr, &Req::Read) {
                Ok(Resp::Notes { notes, .. }) => Some(notes.notes.len() as u64),
                _ => None,
            })
            .unwrap_or(0);
        serde_json::json!({
            "name": NAME,
            "holder": self.holder().map(|h| h.label.clone()),
            "epoch": self.epoch(),
            "rate": self.rate * self.clients.len() as f64,
            "sent": sent, "acked": acked, "present": present,
            "retries": retries, "moved_replies": moved,
            "cells": self.cells.iter().map(|c| serde_json::json!({"label": c.label, "pid": c.pid, "addr": c.addr})).collect::<Vec<_>>(),
            "moves": self.moves(),
        })
    }

    /// Stop new writes, let in-flight ones finish, audit the store against every client's acks.
    pub fn checkpoint(&self) -> Result<Checkpoint, String> {
        self.pause.store(true, SeqCst);
        let res = self.audit_paused();
        for (_, s) in &self.clients {
            s.max_wait_us.store(0, SeqCst);
        }
        self.pause.store(false, SeqCst);
        res
    }

    fn audit_paused(&self) -> Result<Checkpoint, String> {
        let t = Instant::now();
        while !self.clients.iter().all(|(_, s)| s.idle.load(SeqCst)) {
            if t.elapsed() > Duration::from_secs(20) {
                return Err("clients did not go idle".into());
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        let mut c = Checkpoint::default();
        let mut acked = BTreeMap::new();
        for (name, s) in &self.clients {
            c.sent += s.sent.load(SeqCst);
            let a = s.acked.load(SeqCst);
            c.acked += a;
            c.retries += s.retries.load(SeqCst);
            c.moved_replies += s.moved.load(SeqCst);
            c.client_max_wait_ms = c.client_max_wait_ms.max(s.max_wait_us.load(SeqCst) as f64 / 1000.0);
            acked.insert(name.clone(), a);
        }
        let h = self.holder().ok_or("no holder")?;
        let Resp::Notes { epoch, index, head, notes } = cell_call(&h.addr, &Req::Read)? else {
            return Err("read failed".into());
        };
        c.audit = notes.audit(&acked);
        let mut chain = cell_migrate::EMPTY_HEAD;
        for (i, n) in notes.notes.iter().enumerate() {
            chain = chain_next(&chain, i as u64 + 1, n);
        }
        c.chain_recomputed = hex::encode(chain);
        (c.epoch, c.index, c.head) = (epoch, index, head);
        Ok(c)
    }

    /// Move the cell to `to` ("a" or "b") under load, then audit.
    pub fn move_to(&self, to: &str, settle: Duration) -> Result<MoveRecord, String> {
        let _one = lock(&self.moving);
        let from = self.holder().ok_or("no holder")?;
        let target = self.cells.iter().find(|c| c.label == to).ok_or_else(|| format!("no process {to}; use a or b"))?;
        if target.key == from.key {
            return Err(format!("the cell is already on {to}"));
        }
        let report = match cell_call(&from.addr, &Req::MoveTo { to_addr: target.addr.clone() })? {
            Resp::MoveDone(r) => r,
            Resp::Err { msg } => return Err(msg),
            r => return Err(format!("unexpected {r:?}")),
        };
        // A write sent straight to the old holder, as a client with a stale address would.
        let probe = Note { client: "stale-probe".into(), seq: 1, text: "stale".into() };
        let stale_reply = match cell_call(&from.addr, &Req::Write { note: probe })? {
            Resp::Moved { msg, .. } => msg,
            r => format!("{r:?}"),
        };
        std::thread::sleep(settle);
        let c = self.checkpoint()?;
        let ok = report.head_before == report.head_after
            && report.index_before == report.index_after
            && c.sent == c.acked
            && c.acked == c.audit.present
            && c.audit.duplicated == 0
            && c.audit.lost == 0
            && c.audit.unknown == 0
            && c.chain_recomputed == c.head
            && stale_reply == format!("moved, epoch {}", report.epoch);
        let mut moves = lock(&self.moves);
        let rec = MoveRecord {
            n: moves.len() + 1,
            from: from.label.clone(),
            to: to.to_string(),
            report,
            client_max_wait_ms: c.client_max_wait_ms,
            sent: c.sent,
            acked: c.acked,
            present: c.audit.present,
            duplicated: c.audit.duplicated,
            lost: c.audit.lost,
            head_at_audit: c.head,
            chain_recomputed: c.chain_recomputed,
            stale_reply,
            ok,
        };
        moves.push(rec.clone());
        Ok(rec)
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        self.stop.store(true, SeqCst);
    }
}

fn spawn_cell(label: &str, id: &Identity, chain: &str, root: &Identity, resolver: &str) -> Result<CellHandle, String> {
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    let mut child = Command::new(exe)
        .args(["cell", "--listen", "127.0.0.1:0", "--resolver", resolver, "--root", &root.node_id_hex()])
        .env("CARRY_SECRET", hex::encode(id.secret_bytes()))
        .env("CARRY_CHAIN", chain)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn {label}: {e}"))?;
    let mut line = String::new();
    BufReader::new(child.stdout.take().ok_or("no stdout")?).read_line(&mut line).map_err(|e| e.to_string())?;
    let addr = line.trim().strip_prefix("listening ").ok_or_else(|| format!("cell {label} said {line:?}"))?.to_string();
    Ok(CellHandle {
        label: label.into(),
        addr,
        key: id.node_id_hex(),
        pid: child.id(),
        _stdin: child.stdin.take().ok_or("no stdin")?,
        child,
    })
}

pub fn cell_call(addr: &str, req: &Req) -> Result<Resp, String> {
    let what = serde_json::to_value(req).ok().and_then(|v| v["op"].as_str().map(str::to_string)).unwrap_or_default();
    let mut l = Line::connect(addr, Duration::from_secs(20)).map_err(|e| format!("{what} to {addr}: {e}"))?;
    l.call(req).map_err(|e| format!("{what} to {addr}: {e}"))
}

fn resolve_addr(resolver: &str) -> Option<String> {
    mcp_call(resolver, "name.resolve", serde_json::json!({"name": NAME})).ok()?["addr"].as_str().map(str::to_string)
}

fn client_loop(name: &str, resolver: &str, rate: f64, st: &ClientStat, pause: &AtomicBool, stop: &AtomicBool) {
    let interval = Duration::from_secs_f64(1.0 / rate.max(0.1));
    let mut next = Instant::now();
    let mut addr = resolve_addr(resolver);
    let mut conn: Option<Line> = None;
    let mut seq = 0u64;
    while !stop.load(SeqCst) {
        // Order matters: mark busy, then check pause; the checkpoint sets pause, then reads idle.
        st.idle.store(false, SeqCst);
        if pause.load(SeqCst) {
            st.idle.store(true, SeqCst);
            std::thread::sleep(Duration::from_millis(1));
            next = Instant::now();
            continue;
        }
        let now = Instant::now();
        if next > now {
            std::thread::sleep(next - now);
        }
        next = (next + interval).max(Instant::now() - interval);
        seq += 1;
        st.sent.fetch_add(1, SeqCst);
        let note = Note { client: name.to_string(), seq, text: format!("note {seq} from {name}") };
        let t = Instant::now();
        let mut backoff = 0u64;
        loop {
            if conn.is_none() {
                conn = addr.as_deref().and_then(|a| Line::connect(a, Duration::from_secs(5)).ok());
            }
            let r = match conn.as_mut() {
                Some(c) => c.call::<_, Resp>(&Req::Write { note: note.clone() }).ok(),
                None => None,
            };
            match r {
                Some(Resp::Ok { .. }) => {
                    st.acked.fetch_add(1, SeqCst);
                    break;
                }
                Some(Resp::Moved { .. }) => {
                    st.moved.fetch_add(1, SeqCst);
                }
                _ => {}
            }
            // The name never changes: ask the name service where it lives now, and resend.
            st.retries.fetch_add(1, SeqCst);
            conn = None;
            let fresh = resolve_addr(resolver);
            if fresh.is_none() || fresh == addr {
                // Name service not moved yet (or unreachable): back off a little before resending.
                backoff = (backoff * 2).clamp(1, 50);
                std::thread::sleep(Duration::from_millis(backoff));
            } else {
                backoff = 0;
            }
            addr = fresh.or(addr);
        }
        st.max_wait_us.fetch_max(t.elapsed().as_micros() as u64, SeqCst);
    }
    st.idle.store(true, SeqCst);
}

fn sim_publish_and_find(sim: &str, resolver_url: &str) -> Result<(String, serde_json::Value), String> {
    let j = mcp_call(sim, "agent_join", serde_json::json!({"name": "carry-state", "about": "demo: a cell that takes its data with it"}))?;
    let key = j["key"].as_str().ok_or("agent_join: no key")?.to_string();
    // One listing per run: the mock keeps names across runs and a name belongs to one agent.
    let app = format!("carry-state-names-{}", resolver_url.rsplit(':').next().unwrap_or("0").trim_end_matches("/mcp"));
    mcp_call(
        sim,
        "app_publish",
        serde_json::json!({"key": key, "name": app, "url": resolver_url, "kind": "mcp",
            "what": "name.resolve: a cell name to its current holder and epoch"}),
    )?;
    let found = mcp_call(sim, "apps_search", serde_json::json!({"q": "name.resolve"}))?;
    let hit = found["apps"]
        .as_array()
        .and_then(|a| a.iter().find(|x| x["name"].as_str() == Some(app.as_str())))
        .cloned()
        .ok_or("apps_search did not find the name service")?;
    let url = hit["url"].as_str().ok_or("listing has no url")?.to_string();
    let via = mcp_call(sim, "mcp_use", serde_json::json!({"key": key, "url": url, "tool": "name.resolve", "arguments": {"name": NAME}}))?;
    Ok((url, serde_json::json!({"found": hit, "name_resolve_via_mcp_use": via["answer"]["structuredContent"]})))
}

const PAGE: &str = include_str!("page.html");

fn route(r: &Request, names: &Mutex<Names>, me: Option<&Arc<Cluster>>) -> (u16, &'static str, Vec<u8>) {
    let json = |v: serde_json::Value| (200, "application/json", v.to_string().into_bytes());
    match (r.method.as_str(), r.path.as_str()) {
        ("POST", "/mcp") => (200, "application/json", resolver_rpc(&r.body, names)),
        ("GET", "/") => (200, "text/html; charset=utf-8", PAGE.as_bytes().to_vec()),
        ("GET", "/stats") => match me {
            Some(c) => json(c.live()),
            None => json(serde_json::json!({"starting": true})),
        },
        ("POST", "/move") => {
            let Some(c) = me else { return json(serde_json::json!({"error": "starting"})) };
            let to = r.query.split('&').find_map(|kv| kv.strip_prefix("to=")).unwrap_or("").to_string();
            let settle = r.query.split('&').find_map(|kv| kv.strip_prefix("settle_ms=")).and_then(|v| v.parse().ok()).unwrap_or(1000);
            match c.move_to(&to, Duration::from_millis(settle)) {
                Ok(m) => json(serde_json::to_value(m).unwrap_or_default()),
                Err(e) => json(serde_json::json!({"error": e})),
            }
        }
        _ => (404, "text/plain", b"not found".to_vec()),
    }
}

/// The name service as an MCP server: `name.resolve`, plus `name.handoff` for new holders.
fn resolver_rpc(body: &[u8], names: &Mutex<Names>) -> Vec<u8> {
    let msg: serde_json::Value = serde_json::from_slice(body).unwrap_or_default();
    let id = msg["id"].clone();
    let rpc = |result: serde_json::Value| serde_json::json!({"jsonrpc": "2.0", "id": id, "result": result}).to_string().into_bytes();
    match msg["method"].as_str().unwrap_or("") {
        "initialize" => rpc(serde_json::json!({"protocolVersion": "2025-06-18", "capabilities": {"tools": {}},
            "serverInfo": {"name": "name.resolve", "version": env!("CARGO_PKG_VERSION")}})),
        "tools/list" => rpc(serde_json::json!({"tools": [
            {"name": "name.resolve", "description": "A cell name to its current holder, address and epoch.",
             "inputSchema": {"type": "object", "required": ["name"], "properties": {"name": {"type": "string"}}}},
            {"name": "name.handoff", "description": "Move a name to the next epoch: old holder's signed handoff, new holder's countersignature and hold capability.",
             "inputSchema": {"type": "object", "required": ["handoff", "from_sig", "to_sig", "to_chain"]}}]})),
        "tools/call" => {
            let p = &msg["params"];
            let a = &p["arguments"];
            let out = match p["name"].as_str().unwrap_or("") {
                "name.resolve" => lock(names)
                    .resolve(a["name"].as_str().unwrap_or(""))
                    .map(|b| serde_json::to_value(b).unwrap_or_default())
                    .ok_or_else(|| "no such name".to_string()),
                "name.handoff" => serde_json::from_value::<Handoff>(a["handoff"].clone())
                    .map_err(|e| e.to_string())
                    .and_then(|h| {
                        let s = |k: &str| a[k].as_str().unwrap_or("").to_string();
                        lock(names).handoff(&h, &s("from_sig"), &s("to_sig"), &s("to_chain"), now_secs())
                    })
                    .map(|b| serde_json::to_value(b).unwrap_or_default()),
                t => Err(format!("unknown tool {t}")),
            };
            mcp_result(id, out)
        }
        _ => serde_json::json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32601, "message": "no such method"}}).to_string().into_bytes(),
    }
}

/// `carry-state move --to b`: ask a running `up` to move the cell.
pub fn post_move(base: &str, to: &str) -> Result<serde_json::Value, String> {
    let out = http_post(&format!("{base}/move?to={to}"), b"", Duration::from_secs(60))?;
    let v: serde_json::Value = serde_json::from_slice(&out).map_err(|e| e.to_string())?;
    if let Some(e) = v["error"].as_str() {
        return Err(e.to_string());
    }
    Ok(v)
}
