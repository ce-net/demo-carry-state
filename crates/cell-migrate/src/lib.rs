//! `cell.migrate`: move a stateful cell from one process to another without losing or
//! duplicating a write.
//!
//! The pieces:
//!
//! - [`Journal`]: the cell's state plus an append-only op log whose entries form a SHA-256 hash
//!   chain (`head_i = sha256(domain || head_{i-1} || i || op)`).
//! - [`Snapshot`]: the state at some index, with the chain head at that index. Taken while the
//!   cell keeps serving, so the copy does not pause writes.
//! - the tail: the ops after the snapshot, shipped while writes are frozen. It is short, so the
//!   freeze is short.
//! - [`Handoff`]: the old holder's signed statement "name N moves from me to you under epoch E;
//!   the snapshot digest is D; after the tail the chain head is H at index I". The new holder
//!   checks the signature, that the old holder is allowed to hold the cell (a ce-net-proto
//!   capability chain from the operator's root), that the snapshot matches D and that replaying
//!   the tail lands exactly on H. Then it countersigns ([`accept_bytes`]) and presents both
//!   signatures to the name service, which moves the name only to the next epoch.
//!
//! Signatures are plain Ed25519 from [`ce_net_proto::Identity`]; authority to hold a cell is a
//! [`ce_net_proto::SignedCapability`] chain verified with [`ce_net_proto::verify_chain`].

use ce_net_proto::{Identity, NodeId, SignedCapability};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

/// The ability a capability must grant for its holder to serve a migratable cell.
pub const HOLD_ABILITY: &str = "cell.hold";
const CHAIN_DOMAIN: &[u8] = b"ce-cell-log-v1";
const HANDOFF_DOMAIN: &[u8] = b"ce-cell-handoff-v1";
const ACCEPT_DOMAIN: &[u8] = b"ce-cell-accept-v1";
const GENESIS_DOMAIN: &[u8] = b"ce-cell-genesis-v1";

/// A cell whose state can be carried: it applies ops deterministically.
pub trait Cell: Clone + Serialize + DeserializeOwned + Default {
    /// One write.
    type Op: Clone + Serialize + DeserializeOwned;
    /// Apply `op`. Returns `false` (and changes nothing) when `op` is a duplicate of a write
    /// already applied; such an op must not be appended to the log.
    fn apply(&mut self, op: &Self::Op) -> bool;
}

/// 32 zero bytes: the chain head of an empty log.
pub const EMPTY_HEAD: [u8; 32] = [0; 32];

/// The next chain head after appending `op` at `index`.
pub fn chain_next<O: Serialize>(head: &[u8; 32], index: u64, op: &O) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(CHAIN_DOMAIN);
    h.update(head);
    h.update(index.to_le_bytes());
    h.update(serde_json::to_vec(op).unwrap_or_default());
    h.finalize().into()
}

/// The state at `index` and the chain head there.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct Snapshot<C: Cell> {
    pub index: u64,
    pub head: String,
    pub state: C,
}

impl<C: Cell> Snapshot<C> {
    /// `sha256` of the snapshot's canonical JSON, hex.
    pub fn digest(&self) -> String {
        hex::encode(Sha256::digest(serde_json::to_vec(self).unwrap_or_default()))
    }
}

/// State plus the op log since the journal was created or installed.
#[derive(Debug, Clone)]
pub struct Journal<C: Cell> {
    pub state: C,
    /// Index of the last applied op (0 = none).
    pub index: u64,
    pub head: [u8; 32],
    /// `(index, op)` for every op applied here, oldest first.
    pub ops: Vec<(u64, C::Op)>,
}

impl<C: Cell> Default for Journal<C> {
    fn default() -> Self {
        Journal { state: C::default(), index: 0, head: EMPTY_HEAD, ops: Vec::new() }
    }
}

impl<C: Cell> Journal<C> {
    /// Apply and log `op`. Returns the op's index, or `None` when it was a duplicate.
    pub fn append(&mut self, op: C::Op) -> Option<u64> {
        if !self.state.apply(&op) {
            return None;
        }
        self.index += 1;
        self.head = chain_next(&self.head, self.index, &op);
        self.ops.push((self.index, op));
        Some(self.index)
    }

    /// The state now, without stopping writes for longer than a clone.
    pub fn snapshot(&self) -> Snapshot<C> {
        Snapshot { index: self.index, head: hex::encode(self.head), state: self.state.clone() }
    }

    /// The ops after `index`.
    pub fn tail_since(&self, index: u64) -> Vec<(u64, C::Op)> {
        let at = self.ops.partition_point(|(i, _)| *i <= index);
        self.ops[at..].to_vec()
    }

    /// Rebuild from a snapshot plus its tail, checking the chain lands on `head` at `index`.
    pub fn install(snap: &Snapshot<C>, tail: &[(u64, C::Op)], index: u64, head: &str) -> Result<Self, String> {
        Prepared::new(snap)?.finish(tail, index, head)
    }
}

/// A received snapshot, already unpacked, digested and turned into a journal, so the frozen part
/// of a move only replays the tail.
#[derive(Clone)]
pub struct Prepared<C: Cell> {
    pub snapshot_index: u64,
    pub digest: String,
    base: Journal<C>,
}

impl<C: Cell> Prepared<C> {
    pub fn new(snap: &Snapshot<C>) -> Result<Self, String> {
        let base = Journal {
            state: snap.state.clone(),
            index: snap.index,
            head: hex32(&snap.head).ok_or("snapshot head is not 32 hex bytes")?,
            ops: Vec::new(),
        };
        Ok(Prepared { snapshot_index: snap.index, digest: snap.digest(), base })
    }

    /// Index the prepared state has reached (snapshot plus any pre-copied ops).
    pub fn index(&self) -> u64 {
        self.base.index
    }

    /// Pre-copy: apply ops the old holder sent while it kept serving. Nothing is trusted yet; the
    /// signed head checked in [`Prepared::finish`] covers every op, pre-copied or not.
    pub fn extend(&mut self, tail: &[(u64, C::Op)]) -> Result<(), String> {
        for (i, op) in tail {
            if *i != self.base.index + 1 {
                return Err(format!("tail gap: expected index {}, got {i}", self.base.index + 1));
            }
            if self.base.append(op.clone()).is_none() {
                return Err(format!("tail op {i} is a duplicate write"));
            }
        }
        Ok(())
    }

    /// Replay the final `tail` and check it ends exactly at (`index`, `head`).
    pub fn finish(mut self, tail: &[(u64, C::Op)], index: u64, head: &str) -> Result<Journal<C>, String> {
        self.extend(tail)?;
        let j = self.base;
        if j.index != index {
            return Err(format!("tail ends at index {}, handoff says {index}", j.index));
        }
        if hex::encode(j.head) != head {
            return Err("hash chain after the tail does not match the signed head".into());
        }
        Ok(j)
    }
}

/// Parse 64 hex chars into 32 bytes.
pub fn hex32(s: &str) -> Option<[u8; 32]> {
    hex::decode(s).ok()?.try_into().ok()
}

fn hex64(s: &str) -> Option<[u8; 64]> {
    hex::decode(s).ok()?.try_into().ok()
}

/// The old holder's statement that a cell moves to a new holder under the next epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Handoff {
    /// The name clients call. It does not change.
    pub name: String,
    /// The new epoch (old epoch + 1).
    pub epoch: u64,
    /// Old holder's public key, hex.
    pub from: String,
    /// New holder's public key, hex.
    pub to: String,
    /// Where the new holder answers.
    pub to_addr: String,
    pub snapshot_index: u64,
    pub snapshot_digest: String,
    /// Index and chain head after the tail: the exact state the new holder must start from.
    pub index: u64,
    pub head: String,
}

fn domain_bytes<T: Serialize>(domain: &[u8], body: &T) -> Vec<u8> {
    let mut b = domain.to_vec();
    b.extend(serde_json::to_vec(body).unwrap_or_default());
    b
}

/// Bytes the old holder signs.
pub fn handoff_bytes(h: &Handoff) -> Vec<u8> {
    domain_bytes(HANDOFF_DOMAIN, h)
}

/// Bytes the new holder signs to say it installed the state and takes over.
pub fn accept_bytes(h: &Handoff) -> Vec<u8> {
    domain_bytes(ACCEPT_DOMAIN, h)
}

/// The first binding of a name, signed by the operator's root key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Genesis {
    pub name: String,
    pub epoch: u64,
    pub holder: String,
    pub addr: String,
}

/// Bytes the root signs for a [`Genesis`].
pub fn genesis_bytes(g: &Genesis) -> Vec<u8> {
    domain_bytes(GENESIS_DOMAIN, g)
}

/// Sign `bytes`, hex.
pub fn sign_hex(id: &Identity, bytes: &[u8]) -> String {
    hex::encode(id.sign(bytes))
}

/// Verify a hex signature by a hex public key.
pub fn verify_hex(key_hex: &str, bytes: &[u8], sig_hex: &str) -> Result<NodeId, String> {
    let key = hex32(key_hex).ok_or("public key is not 32 hex bytes")?;
    let sig = hex64(sig_hex).ok_or("signature is not 64 hex bytes")?;
    ce_net_proto::identity::verify(&key, bytes, &sig).map_err(|e| format!("bad signature: {e}"))?;
    Ok(key)
}

/// Check that `chain` (a ce-net-proto capability chain, encoded) roots at `root`, is valid at
/// `now`, ends at `who` and grants [`HOLD_ABILITY`].
pub fn verify_hold(chain: &str, root: &NodeId, who: &str, now: u64) -> Result<(), String> {
    let chain: Vec<SignedCapability> = ce_net_proto::decode_chain(chain).map_err(|e| format!("hold chain: {e}"))?;
    let v = ce_net_proto::verify_chain(&chain, root, now, &|_, _| false).map_err(|e| format!("hold chain: {e}"))?;
    if hex::encode(v.holder) != who {
        return Err("hold chain is for another key".into());
    }
    if !v.abilities.iter().any(|a| ce_net_proto::ability_covers(a, HOLD_ABILITY)) {
        return Err(format!("hold chain does not grant {HOLD_ABILITY}"));
    }
    Ok(())
}

/// What the new holder checks before taking over. Returns the installed journal.
#[allow(clippy::too_many_arguments)]
pub fn check_handoff<C: Cell>(
    h: &Handoff,
    from_sig: &str,
    from_chain: &str,
    root: &NodeId,
    me: &str,
    prepared: Prepared<C>,
    tail: &[(u64, C::Op)],
    now: u64,
) -> Result<Journal<C>, String> {
    if h.to != me {
        return Err("handoff names another new holder".into());
    }
    verify_hex(&h.from, &handoff_bytes(h), from_sig)?;
    verify_hold(from_chain, root, &h.from, now)?;
    if prepared.snapshot_index != h.snapshot_index || prepared.digest != h.snapshot_digest {
        return Err("snapshot does not match the signed digest".into());
    }
    prepared.finish(tail, h.index, &h.head)
}

/// Unix seconds now.
pub fn now_secs() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ce_net_proto::{Caveats, Resource};

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct Counter {
        seen: Vec<u32>,
    }
    impl Cell for Counter {
        type Op = u32;
        fn apply(&mut self, op: &u32) -> bool {
            if self.seen.contains(op) {
                return false;
            }
            self.seen.push(*op);
            true
        }
    }

    fn id(n: u8) -> Identity {
        Identity::from_secret_bytes(&[n; 32])
    }

    fn hold(root: &Identity, who: &Identity) -> String {
        let c = SignedCapability::issue(
            root,
            who.node_id(),
            vec![HOLD_ABILITY.into()],
            Resource::Any,
            Caveats::default(),
            1,
            None,
        );
        ce_net_proto::encode_chain(&[c])
    }

    fn setup() -> (Identity, Identity, Identity, Journal<Counter>, Snapshot<Counter>, Handoff) {
        let (root, a, b) = (id(1), id(2), id(3));
        let mut j = Journal::<Counter>::default();
        for n in 0..5 {
            j.append(n);
        }
        let snap = j.snapshot();
        for n in 5..8 {
            j.append(n);
        }
        let h = Handoff {
            name: "notes".into(),
            epoch: 2,
            from: a.node_id_hex(),
            to: b.node_id_hex(),
            to_addr: "b".into(),
            snapshot_index: snap.index,
            snapshot_digest: snap.digest(),
            index: j.index,
            head: hex::encode(j.head),
        };
        (root, a, b, j, snap, h)
    }

    #[test]
    fn good_handoff_installs_same_head() {
        let (root, a, b, j, snap, h) = setup();
        let sig = sign_hex(&a, &handoff_bytes(&h));
        let tail = j.tail_since(snap.index);
        assert_eq!(tail.len(), 3);
        let got = check_handoff(&h, &sig, &hold(&root, &a), &root.node_id(), &b.node_id_hex(), Prepared::new(&snap).unwrap(), &tail, now_secs())
            .unwrap();
        assert_eq!(got.head, j.head);
        assert_eq!(got.state.seen, j.state.seen);
    }

    #[test]
    fn duplicate_write_is_not_logged() {
        let mut j = Journal::<Counter>::default();
        assert_eq!(j.append(7), Some(1));
        assert_eq!(j.append(7), None);
        assert_eq!(j.index, 1);
    }

    #[test]
    fn refuses_forged_signature_tampered_tail_and_unheld_sender() {
        let (root, a, b, j, snap, h) = setup();
        let me = b.node_id_hex();
        let tail = j.tail_since(snap.index);
        // Signed by B, claiming to be from A.
        let forged = sign_hex(&b, &handoff_bytes(&h));
        assert!(check_handoff(&h, &forged, &hold(&root, &a), &root.node_id(), &me, Prepared::new(&snap).unwrap(), &tail, now_secs()).is_err());
        let sig = sign_hex(&a, &handoff_bytes(&h));
        // One op dropped from the tail.
        let short = &tail[..2];
        let e = check_handoff(&h, &sig, &hold(&root, &a), &root.node_id(), &me, Prepared::new(&snap).unwrap(), short, now_secs()).unwrap_err();
        assert!(e.contains("index"), "{e}");
        // One op changed.
        let mut bad = tail.clone();
        bad[2].1 = 99;
        let e = check_handoff(&h, &sig, &hold(&root, &a), &root.node_id(), &me, Prepared::new(&snap).unwrap(), &bad, now_secs()).unwrap_err();
        assert!(e.contains("hash chain"), "{e}");
        // A duplicated op.
        let mut dup = tail.clone();
        dup[2].1 = dup[1].1;
        assert!(check_handoff(&h, &sig, &hold(&root, &a), &root.node_id(), &me, Prepared::new(&snap).unwrap(), &dup, now_secs()).is_err());
        // A's hold chain is from another root.
        let e = check_handoff(&h, &sig, &hold(&id(9), &a), &root.node_id(), &me, Prepared::new(&snap).unwrap(), &tail, now_secs()).unwrap_err();
        assert!(e.contains("hold chain"), "{e}");
    }
}
