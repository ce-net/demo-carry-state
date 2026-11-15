//! One cell process (A or B). It serves `notes.store` when it holds the name, and moves it with
//! `cell.migrate`: snapshot while serving, freeze, ship the tail and a signed handoff, answer
//! every later write with "moved, epoch N".

use crate::net::{Line, mcp_call};
use crate::wire::{MoveReport, Req, Resp};
use cell_migrate::{
    Handoff, Journal, Prepared, accept_bytes, check_handoff, genesis_bytes, handoff_bytes, now_secs, sign_hex, verify_hex,
};
use ce_net_proto::{Identity, NodeId};
use notes_store::Notes;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq)]
enum Role {
    Idle,
    Holding,
    Moved { addr: String },
}

struct Inner {
    role: Role,
    name: String,
    epoch: u64,
    journal: Journal<Notes>,
    /// A snapshot (and the sender's hold chain) received in `Prepare`, waiting for `Commit`.
    pending: Option<(Prepared<Notes>, String)>,
}

pub struct CellProc {
    me: Identity,
    key: String,
    addr: String,
    chain: String,
    root: NodeId,
    resolver: String,
    inner: Mutex<Inner>,
}

/// `carry-state cell --listen ADDR --resolver URL --root HEX`, with the key in `CARRY_SECRET` and
/// the hold capability chain in `CARRY_CHAIN`. Prints `listening ADDR` and serves until stdin closes.
pub fn main(args: &[String]) -> Result<(), String> {
    let get = |k: &str| args.iter().position(|a| a == k).and_then(|i| args.get(i + 1)).cloned();
    let listen = get("--listen").unwrap_or_else(|| "127.0.0.1:0".into());
    let resolver = get("--resolver").ok_or("need --resolver")?;
    let root = cell_migrate::hex32(&get("--root").ok_or("need --root")?).ok_or("bad --root")?;
    let secret = cell_migrate::hex32(&std::env::var("CARRY_SECRET").map_err(|_| "need CARRY_SECRET")?).ok_or("bad CARRY_SECRET")?;
    let chain = std::env::var("CARRY_CHAIN").map_err(|_| "need CARRY_CHAIN")?;
    let l = TcpListener::bind(&listen).map_err(|e| format!("bind {listen}: {e}"))?;
    let addr = l.local_addr().map_err(|e| e.to_string())?.to_string();
    let me = Identity::from_secret_bytes(&secret);
    let cell = Arc::new(CellProc {
        key: me.node_id_hex(),
        me,
        addr: addr.clone(),
        chain,
        root,
        resolver,
        inner: Mutex::new(Inner { role: Role::Idle, name: String::new(), epoch: 0, journal: Journal::default(), pending: None }),
    });
    println!("listening {addr}");
    // Exit when the parent goes away.
    std::thread::spawn(|| {
        let mut sink = String::new();
        while std::io::stdin().read_line(&mut sink).map(|n| n > 0).unwrap_or(false) {}
        std::process::exit(0);
    });
    for s in l.incoming().flatten() {
        let c = cell.clone();
        std::thread::spawn(move || {
            let Ok(mut line) = Line::from_stream(s, None) else { return };
            while let Ok(Some(req)) = line.recv::<Req>() {
                let resp = c.handle(req);
                if line.send(&resp).is_err() {
                    break;
                }
            }
        });
    }
    Ok(())
}

impl CellProc {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn handle(&self, req: Req) -> Resp {
        match req {
            Req::Write { note } => {
                let mut g = self.lock();
                match &g.role {
                    Role::Holding => {
                        let epoch = g.epoch;
                        match g.journal.append(note.clone()) {
                            Some(index) => Resp::Ok { epoch, index, dup: false },
                            None => Resp::Ok { epoch, index: g.journal.index, dup: true },
                        }
                    }
                    Role::Moved { addr } => Resp::Moved { epoch: g.epoch, addr: addr.clone(), msg: format!("moved, epoch {}", g.epoch) },
                    Role::Idle => Resp::NotHolder { msg: "not the holder".into() },
                }
            }
            Req::Read => {
                let g = self.lock();
                Resp::Notes { epoch: g.epoch, index: g.journal.index, head: hex::encode(g.journal.head), notes: g.journal.state.clone() }
            }
            Req::Status => {
                let g = self.lock();
                let role = match &g.role {
                    Role::Idle => "idle".to_string(),
                    Role::Holding => "holding".to_string(),
                    Role::Moved { .. } => "moved".to_string(),
                };
                Resp::Status {
                    key: self.key.clone(),
                    addr: self.addr.clone(),
                    role,
                    epoch: g.epoch,
                    index: g.journal.index,
                    head: hex::encode(g.journal.head),
                }
            }
            Req::Activate { genesis, root_sig } => {
                if let Err(e) = verify_hex(&hex::encode(self.root), &genesis_bytes(&genesis), &root_sig) {
                    return Resp::Err { msg: e };
                }
                if genesis.holder != self.key {
                    return Resp::Err { msg: "genesis names another holder".into() };
                }
                let mut g = self.lock();
                g.role = Role::Holding;
                g.name = genesis.name;
                g.epoch = genesis.epoch;
                Resp::Ok { epoch: g.epoch, index: g.journal.index, dup: false }
            }
            Req::Prepare { snapshot, from_chain } => {
                // Unpack and digest the snapshot now, while the old holder still serves writes.
                let prepared = match Prepared::new(&snapshot) {
                    Ok(p) => p,
                    Err(e) => return Resp::Err { msg: e },
                };
                let mut g = self.lock();
                if g.role == Role::Holding {
                    return Resp::Err { msg: "already holding".into() };
                }
                g.pending = Some((prepared, from_chain));
                Resp::Prepared
            }
            Req::Catchup { tail } => {
                let mut g = self.lock();
                match g.pending.as_mut().map(|(p, _)| p.extend(&tail)) {
                    Some(Ok(())) => Resp::Prepared,
                    Some(Err(e)) => {
                        g.pending = None;
                        Resp::Err { msg: e }
                    }
                    None => Resp::Err { msg: "catchup without prepare".into() },
                }
            }
            Req::Commit { handoff, from_sig, tail } => self.commit(handoff, from_sig, tail),
            Req::MoveTo { to_addr } => match self.move_to(&to_addr) {
                Ok(r) => Resp::MoveDone(r),
                Err(e) => Resp::Err { msg: e },
            },
        }
    }

    /// New-holder side: check everything, countersign, move the name, start serving.
    fn commit(&self, h: Handoff, from_sig: String, tail: Vec<(u64, notes_store::Note)>) -> Resp {
        let mut g = self.lock();
        let Some((prepared, from_chain)) = g.pending.take() else {
            return Resp::Err { msg: "commit without prepare".into() };
        };
        if h.epoch <= g.epoch {
            return Resp::Err { msg: format!("stale handoff: I have seen epoch {}", g.epoch) };
        }
        let journal = match check_handoff(&h, &from_sig, &from_chain, &self.root, &self.key, prepared, &tail, now_secs()) {
            Ok(j) => j,
            Err(e) => return Resp::Err { msg: e },
        };
        let to_sig = sign_hex(&self.me, &accept_bytes(&h));
        let args = serde_json::json!({"handoff": h, "from_sig": from_sig, "to_sig": to_sig, "to_chain": self.chain});
        if let Err(e) = mcp_call(&self.resolver, "name.handoff", args) {
            return Resp::Err { msg: format!("name service refused: {e}") };
        }
        g.journal = journal;
        g.epoch = h.epoch;
        g.name = h.name.clone();
        g.role = Role::Holding;
        Resp::Committed { epoch: g.epoch, index: g.journal.index, head: hex::encode(g.journal.head) }
    }

    /// Old-holder side.
    fn move_to(&self, to_addr: &str) -> Result<MoveReport, String> {
        let timeout = Duration::from_secs(5);
        let mut b = Line::connect(to_addr, timeout).map_err(|e| format!("connect {to_addr}: {e}"))?;
        let to_key = match b.call::<_, Resp>(&Req::Status).map_err(|e| e.to_string())? {
            Resp::Status { key, .. } => key,
            r => return Err(format!("unexpected {r:?}")),
        };
        // 1. Snapshot while still serving: only a clone under the lock; digest outside it.
        let snap = {
            let g = self.lock();
            if g.role != Role::Holding {
                return Err("this process does not hold the cell".into());
            }
            g.journal.snapshot()
        };
        let digest = snap.digest();
        let snap_index = snap.index;
        match b.call::<_, Resp>(&Req::Prepare { snapshot: snap, from_chain: self.chain.clone() }).map_err(|e| e.to_string())? {
            Resp::Prepared => {}
            Resp::Err { msg } => return Err(msg),
            r => return Err(format!("unexpected {r:?}")),
        }
        // 2. Pre-copy: ship what was written meanwhile, still serving, until the rest is small.
        let (mut sent_to, mut precopied, mut rounds) = (snap_index, 0u64, 0u64);
        while rounds < 5 {
            let tail = self.lock().journal.tail_since(sent_to);
            if tail.len() <= 8 {
                break;
            }
            sent_to = tail.last().map(|(i, _)| *i).unwrap_or(sent_to);
            precopied += tail.len() as u64;
            rounds += 1;
            match b.call::<_, Resp>(&Req::Catchup { tail }).map_err(|e| e.to_string())? {
                Resp::Prepared => {}
                Resp::Err { msg } => return Err(msg),
                r => return Err(format!("unexpected {r:?}")),
            }
        }
        // 3. Freeze: hold the write lock until the new holder has taken over.
        let mut g = self.lock();
        let t0 = Instant::now();
        if g.role != Role::Holding {
            return Err("lost the cell while preparing".into());
        }
        let tail = g.journal.tail_since(sent_to);
        let h = Handoff {
            name: g.name.clone(),
            epoch: g.epoch + 1,
            from: self.key.clone(),
            to: to_key,
            to_addr: to_addr.to_string(),
            snapshot_index: snap_index,
            snapshot_digest: digest,
            index: g.journal.index,
            head: hex::encode(g.journal.head),
        };
        let from_sig = sign_hex(&self.me, &handoff_bytes(&h));
        let mut report = MoveReport {
            epoch: h.epoch,
            snapshot_index: snap_index,
            precopied,
            precopy_rounds: rounds,
            tail_len: tail.len() as u64,
            head_before: h.head.clone(),
            index_before: h.index,
            ..Default::default()
        };
        let res = b.call::<_, Resp>(&Req::Commit { handoff: h.clone(), from_sig, tail });
        let mut old = Journal::default();
        let out = match res {
            Ok(Resp::Committed { epoch, index, head }) if epoch == h.epoch => {
                g.epoch = epoch;
                g.role = Role::Moved { addr: to_addr.to_string() };
                report.head_after = head;
                report.index_after = index;
                // The log now lives on the new holder; free the old copy after unfreezing.
                std::mem::swap(&mut g.journal, &mut old);
                report.pause_ms = t0.elapsed().as_secs_f64() * 1000.0;
                Ok(report)
            }
            other => {
                // Did the name move anyway (answer lost)? Ask the name service before serving again.
                let cur = mcp_call(&self.resolver, "name.resolve", serde_json::json!({"name": h.name}))
                    .ok()
                    .and_then(|v| v["epoch"].as_u64())
                    .unwrap_or(u64::MAX);
                // Fail closed: if the name service cannot say, stop serving rather than risk two holders.
                if cur >= h.epoch {
                    g.epoch = if cur == u64::MAX { h.epoch } else { cur };
                    g.role = Role::Moved { addr: to_addr.to_string() };
                }
                Err(format!("handoff failed: {other:?}"))
            }
        };
        drop(g);
        drop(old);
        out
    }
}
