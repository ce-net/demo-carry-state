//! Messages between clients, cells and the control process.

use cell_migrate::{Genesis, Handoff, Snapshot};
use notes_store::{Note, Notes};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Req {
    /// A client write.
    Write { note: Note },
    /// Every note, for the audit.
    Read,
    /// Become the first holder (root-signed).
    Activate { genesis: Genesis, root_sig: String },
    /// Control: move the cell you hold to the cell at `to_addr`.
    MoveTo { to_addr: String },
    /// Old holder to new holder: the snapshot, sent while writes continue.
    Prepare { snapshot: Snapshot<Notes>, from_chain: String },
    /// Old holder to new holder, still serving: ops written since the snapshot (pre-copy).
    Catchup { tail: Vec<(u64, Note)> },
    /// Old holder to new holder, while writes are frozen: the tail and the signed handoff.
    Commit { handoff: Handoff, from_sig: String, tail: Vec<(u64, Note)> },
    /// Who are you and what do you hold.
    Status,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "r", rename_all = "snake_case")]
pub enum Resp {
    Ok { epoch: u64, index: u64, dup: bool },
    /// The cell left this process. `msg` is the human line, e.g. "moved, epoch 2".
    Moved { epoch: u64, addr: String, msg: String },
    /// This process does not hold the cell (yet).
    NotHolder { msg: String },
    Notes { epoch: u64, index: u64, head: String, notes: Notes },
    Status { key: String, addr: String, role: String, epoch: u64, index: u64, head: String },
    Prepared,
    /// New holder installed the state and the name service moved the name.
    Committed { epoch: u64, index: u64, head: String },
    /// Old holder's report on a finished move.
    MoveDone(MoveReport),
    Err { msg: String },
}

/// What one move did, as measured by the old holder.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MoveReport {
    pub epoch: u64,
    /// Time writes were frozen, from taking the write lock to answering "moved".
    pub pause_ms: f64,
    pub snapshot_index: u64,
    /// Ops sent while still serving, after the snapshot, and in how many rounds.
    pub precopied: u64,
    pub precopy_rounds: u64,
    /// Ops sent while frozen.
    pub tail_len: u64,
    /// Chain head and index on the old holder when it froze.
    pub head_before: String,
    pub index_before: u64,
    /// Chain head and index on the new holder after it replayed the tail.
    pub head_after: String,
    pub index_after: u64,
}
