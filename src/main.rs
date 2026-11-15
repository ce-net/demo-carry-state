//! carry-state: a cell that takes its data with it.
//!
//! ```text
//! cargo run -- up                  start processes A and B, the name service, a client and the page
//! cargo run -- move --to b         move the notes cell to process B (while `up` runs)
//! cargo run -- prove --moves 10    N moves under load, a table, a JSON record, exit 1 on any loss
//! ```

mod cellproc;
mod cluster;
mod net;
mod wire;

use cluster::{Cluster, MoveRecord, Opts};
use std::time::Duration;

fn flag(args: &[String], k: &str) -> Option<String> {
    args.iter().position(|a| a == k).and_then(|i| args.get(i + 1)).cloned()
}

fn num<T: std::str::FromStr>(args: &[String], k: &str, d: T) -> Result<T, String> {
    match flag(args, k) {
        Some(v) => v.parse().map_err(|_| format!("{k}: not a number: {v}")),
        None => Ok(d),
    }
}

fn short(h: &str) -> &str {
    &h[..h.len().min(12)]
}

fn print_move(m: &MoveRecord) {
    let r = &m.report;
    println!(
        "move {}: {} -> {} under epoch {}: writes frozen {:.2} ms (snapshot at #{}, pre-copied {} ops, frozen tail {} ops)",
        m.n, m.from, m.to, r.epoch, r.pause_ms, r.snapshot_index, r.precopied, r.tail_len
    );
    println!("  hash chain before {} #{}  after {} #{}", short(&r.head_before), r.index_before, short(&r.head_after), r.index_after);
    println!("  old holder now answers: {:?}", m.stale_reply);
    println!(
        "  writes sent {}  acknowledged {}  present {}  (lost {}, duplicated {})  longest client wait {:.1} ms  {}",
        m.sent,
        m.acked,
        m.present,
        m.lost,
        m.duplicated,
        m.client_max_wait_ms,
        if m.ok { "OK" } else { "MISMATCH" }
    );
}

const USAGE: &str = "usage:
  carry-state up [--port 7400] [--rate 20] [--clients 1] [--sim URL]
  carry-state move --to a|b [--port 7400]
  carry-state prove [--moves 10] [--rate 20] [--clients 1] [--settle-ms 500] [--out FILE] [--sim URL]";

fn run(args: &[String]) -> Result<(), String> {
    let cmd = args.first().map(String::as_str).unwrap_or("");
    let rest = &args[args.len().min(1)..];
    match cmd {
        "cell" => cellproc::main(rest),
        "up" => {
            let port: u16 = num(rest, "--port", 7400)?;
            let c = Cluster::start(Opts {
                listen: format!("127.0.0.1:{port}"),
                rate: num(rest, "--rate", 20.0)?,
                clients: num(rest, "--clients", 1)?,
                sim: flag(rest, "--sim"),
            })?;
            for cell in &c.cells {
                println!("process {} pid {} on {} key {}", cell.label.to_uppercase(), cell.pid, cell.addr, short(&cell.key));
            }
            println!("the notes cell runs in process A under epoch 1");
            println!("a client writes to the name \"notes\" {} times a second (name service found: {})", c.rate, c.discovered_via);
            println!("page:    {}   name service: {}", c.url, c.resolver_url);
            println!("move it: cargo run -- move --to b{}", if port == 7400 { String::new() } else { format!(" --port {port}") });
            let mut seen = 0;
            loop {
                std::thread::sleep(Duration::from_millis(250));
                let moves = c.moves();
                for m in &moves[seen..] {
                    print_move(m);
                }
                seen = moves.len();
            }
        }
        "move" => {
            let to = flag(rest, "--to").ok_or("need --to a or --to b")?;
            let port: u16 = num(rest, "--port", 7400)?;
            let v = cluster::post_move(&format!("http://127.0.0.1:{port}"), &to)
                .map_err(|e| if e.starts_with("http://") { format!("{e}\n(is `cargo run -- up` running on port {port}?)") } else { e })?;
            let m: serde_json::Value = v;
            let r = |k: &str| m[k].clone();
            println!(
                "moved {} -> {} under epoch {}: writes frozen {:.2} ms, tail {} ops",
                r("from").as_str().unwrap_or(""),
                r("to").as_str().unwrap_or(""),
                r("epoch"),
                r("pause_ms").as_f64().unwrap_or(0.0),
                r("tail_len")
            );
            println!("old holder now answers: {}", r("stale_reply"));
            println!(
                "writes sent {}  acknowledged {}  present {}  lost {}  duplicated {}  {}",
                r("sent"),
                r("acked"),
                r("present"),
                r("lost"),
                r("duplicated"),
                if r("ok").as_bool() == Some(true) { "all match" } else { "MISMATCH" }
            );
            if r("ok").as_bool() != Some(true) {
                return Err("the move did not check out".into());
            }
            Ok(())
        }
        "prove" => {
            let moves: usize = num(rest, "--moves", 10)?;
            let settle = Duration::from_millis(num(rest, "--settle-ms", 500)?);
            let c = Cluster::start(Opts {
                listen: "127.0.0.1:0".into(),
                rate: num(rest, "--rate", 20.0)?,
                clients: num(rest, "--clients", 1)?,
                sim: flag(rest, "--sim"),
            })?;
            println!(
                "{} moves, {} clients at {} writes/s each, name service found: {}",
                moves,
                num(rest, "--clients", 1usize)?,
                c.rate,
                c.discovered_via
            );
            std::thread::sleep(settle);
            let mut failed = 0;
            for i in 0..moves {
                let to = if i % 2 == 0 { "b" } else { "a" };
                let m = c.move_to(to, settle)?;
                print_move(&m);
                if !m.ok {
                    failed += 1;
                }
            }
            c.shutdown();
            let all = c.moves();
            let pauses: Vec<f64> = all.iter().map(|m| m.report.pause_ms).collect();
            let mut sorted = pauses.clone();
            sorted.sort_by(|a, b| a.total_cmp(b));
            let pct = |p: f64| sorted.get(((sorted.len() as f64 - 1.0) * p).round() as usize).copied().unwrap_or(0.0);
            let last = all.last();
            let summary = serde_json::json!({
                "moves": all.len(),
                "failed": failed,
                "clients": num(rest, "--clients", 1usize)?,
                "rate_per_client": c.rate,
                "pause_ms": {"min": pct(0.0), "median": pct(0.5), "p95": pct(0.95), "max": pct(1.0)},
                "final": last.map(|m| serde_json::json!({"sent": m.sent, "acked": m.acked, "present": m.present,
                    "lost": m.lost, "duplicated": m.duplicated, "head_at_audit": m.head_at_audit, "chain_recomputed": m.chain_recomputed})),
                "name_service": c.discovered_via,
                "sim": c.sim_note,
            });
            println!(
                "pause ms: min {:.2}  median {:.2}  p95 {:.2}  max {:.2}",
                pct(0.0),
                pct(0.5),
                pct(0.95),
                pct(1.0)
            );
            if let Some(m) = last {
                println!(
                    "final: sent {}  acknowledged {}  present {}  lost {}  duplicated {}",
                    m.sent, m.acked, m.present, m.lost, m.duplicated
                );
            }
            if let Some(out) = flag(rest, "--out") {
                let doc = serde_json::json!({"summary": summary, "moves": all});
                std::fs::write(&out, serde_json::to_string_pretty(&doc).unwrap_or_default() + "\n").map_err(|e| format!("{out}: {e}"))?;
                println!("wrote {out}");
            }
            if failed > 0 {
                return Err(format!("{failed} of {moves} moves lost, duplicated or mismatched a write"));
            }
            println!("all {moves} moves: no write lost, none duplicated");
            Ok(())
        }
        _ => Err(USAGE.into()),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let Err(e) = run(&args) {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
