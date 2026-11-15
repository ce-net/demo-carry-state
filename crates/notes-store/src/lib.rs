//! `notes.store`: the sample stateful cell. An append-only list of notes; each client numbers
//! its writes 1, 2, 3, ... and a write whose number was already applied is a duplicate, so a
//! client can safely resend after a timeout or a "moved" answer.

use cell_migrate::Cell;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// One write.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Note {
    pub client: String,
    pub seq: u64,
    pub text: String,
}

/// The notes and, per client, the highest write number applied.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Notes {
    pub notes: Vec<Note>,
    pub last_seq: BTreeMap<String, u64>,
}

impl Cell for Notes {
    type Op = Note;
    fn apply(&mut self, op: &Note) -> bool {
        let last = self.last_seq.entry(op.client.clone()).or_insert(0);
        if op.seq <= *last {
            return false;
        }
        *last = op.seq;
        self.notes.push(op.clone());
        true
    }
}

/// What a check of "every acknowledged write is present exactly once" found.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Audit {
    /// Notes in the store.
    pub present: u64,
    /// `(client, seq)` pairs seen more than once.
    pub duplicated: u64,
    /// Acknowledged writes missing from the store.
    pub lost: u64,
    /// Notes in the store that no client sent (should never happen).
    pub unknown: u64,
}

impl Notes {
    /// Compare the store with each client's acknowledged count (writes 1..=n).
    pub fn audit(&self, acked: &BTreeMap<String, u64>) -> Audit {
        let mut seen: BTreeMap<(&str, u64), u64> = BTreeMap::new();
        for n in &self.notes {
            *seen.entry((n.client.as_str(), n.seq)).or_insert(0) += 1;
        }
        let mut a = Audit { present: self.notes.len() as u64, ..Default::default() };
        a.duplicated = seen.values().filter(|c| **c > 1).count() as u64;
        for (c, n) in acked {
            a.lost += (1..=*n).filter(|s| !seen.contains_key(&(c.as_str(), *s))).count() as u64;
        }
        a.unknown = seen.keys().filter(|(c, s)| acked.get(*c).is_none_or(|n| s > n)).count() as u64;
        a
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn note(c: &str, s: u64) -> Note {
        Note { client: c.into(), seq: s, text: format!("{c}-{s}") }
    }

    #[test]
    fn resend_is_a_duplicate() {
        let mut n = Notes::default();
        assert!(n.apply(&note("c1", 1)));
        assert!(n.apply(&note("c1", 2)));
        assert!(!n.apply(&note("c1", 2)));
        assert!(!n.apply(&note("c1", 1)));
        assert!(n.apply(&note("c2", 1)));
        assert_eq!(n.notes.len(), 3);
    }

    #[test]
    fn audit_counts_lost_and_duplicated() {
        let mut n = Notes::default();
        n.notes = vec![note("c1", 1), note("c1", 1), note("c1", 3)];
        let acked = BTreeMap::from([("c1".to_string(), 3)]);
        let a = n.audit(&acked);
        assert_eq!((a.present, a.duplicated, a.lost, a.unknown), (3, 1, 1, 0));
    }
}
