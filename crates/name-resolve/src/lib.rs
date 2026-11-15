//! `name.resolve`: a name maps to its current holder and epoch. The binding changes only two ways:
//!
//! - a [`Genesis`] signed by the operator's root key, for a name not bound yet;
//! - a [`Handoff`] to exactly the next epoch, signed by the current holder and countersigned by
//!   the new holder, whose capability chain from the root grants `cell.hold`.
//!
//! Anything else (a replayed handoff, a stale epoch, a handoff from a key that no longer holds the
//! name) is refused, so at most one holder is ever current for an epoch.

use cell_migrate::{Genesis, Handoff, accept_bytes, genesis_bytes, handoff_bytes, verify_hex, verify_hold};
use ce_net_proto::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Where a name lives now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Binding {
    pub name: String,
    pub epoch: u64,
    pub holder: String,
    pub addr: String,
}

/// The name table, trusting one root key.
#[derive(Debug, Clone)]
pub struct Names {
    root: NodeId,
    map: BTreeMap<String, Binding>,
}

impl Names {
    pub fn new(root: NodeId) -> Self {
        Names { root, map: BTreeMap::new() }
    }

    pub fn resolve(&self, name: &str) -> Option<&Binding> {
        self.map.get(name)
    }

    /// Bind a new name. Refused if the name is already bound.
    pub fn genesis(&mut self, g: &Genesis, root_sig: &str) -> Result<Binding, String> {
        verify_hex(&hex::encode(self.root), &genesis_bytes(g), root_sig)?;
        if self.map.contains_key(&g.name) {
            return Err(format!("{} is already bound", g.name));
        }
        let b = Binding { name: g.name.clone(), epoch: g.epoch, holder: g.holder.clone(), addr: g.addr.clone() };
        self.map.insert(g.name.clone(), b.clone());
        Ok(b)
    }

    /// Move a name to the next epoch.
    pub fn handoff(&mut self, h: &Handoff, from_sig: &str, to_sig: &str, to_chain: &str, now: u64) -> Result<Binding, String> {
        let cur = self.map.get(&h.name).ok_or_else(|| format!("{} is not bound", h.name))?;
        if h.epoch != cur.epoch + 1 {
            return Err(format!("stale handoff: {} is at epoch {}, handoff is for epoch {}", h.name, cur.epoch, h.epoch));
        }
        if h.from != cur.holder {
            return Err(format!("handoff is from a key that does not hold {} at epoch {}", h.name, cur.epoch));
        }
        verify_hex(&h.from, &handoff_bytes(h), from_sig)?;
        verify_hex(&h.to, &accept_bytes(h), to_sig)?;
        verify_hold(to_chain, &self.root, &h.to, now)?;
        let b = Binding { name: h.name.clone(), epoch: h.epoch, holder: h.to.clone(), addr: h.to_addr.clone() };
        self.map.insert(h.name.clone(), b.clone());
        Ok(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cell_migrate::{HOLD_ABILITY, now_secs, sign_hex};
    use ce_net_proto::{Caveats, Identity, Resource, SignedCapability};

    fn id(n: u8) -> Identity {
        Identity::from_secret_bytes(&[n; 32])
    }

    fn hold(root: &Identity, who: &Identity) -> String {
        let c = SignedCapability::issue(root, who.node_id(), vec![HOLD_ABILITY.into()], Resource::Any, Caveats::default(), 1, None);
        ce_net_proto::encode_chain(&[c])
    }

    fn handoff(from: &Identity, to: &Identity, epoch: u64) -> Handoff {
        Handoff {
            name: "notes".into(),
            epoch,
            from: from.node_id_hex(),
            to: to.node_id_hex(),
            to_addr: "x".into(),
            snapshot_index: 0,
            snapshot_digest: String::new(),
            index: 0,
            head: String::new(),
        }
    }

    #[test]
    fn moves_only_forward_and_only_from_the_holder() {
        let (root, a, b, c) = (id(1), id(2), id(3), id(4));
        let mut n = Names::new(root.node_id());
        let g = Genesis { name: "notes".into(), epoch: 1, holder: a.node_id_hex(), addr: "a".into() };
        // Genesis must be root-signed.
        assert!(n.genesis(&g, &sign_hex(&a, &genesis_bytes(&g))).is_err());
        n.genesis(&g, &sign_hex(&root, &genesis_bytes(&g))).unwrap();

        let h = handoff(&a, &b, 2);
        let (fs, ts) = (sign_hex(&a, &handoff_bytes(&h)), sign_hex(&b, &accept_bytes(&h)));
        // Missing countersignature.
        assert!(n.handoff(&h, &fs, &fs, &hold(&root, &b), now_secs()).is_err());
        // New holder without a hold capability from this root.
        assert!(n.handoff(&h, &fs, &ts, &hold(&c, &b), now_secs()).is_err());
        assert_eq!(n.handoff(&h, &fs, &ts, &hold(&root, &b), now_secs()).unwrap().epoch, 2);
        // Replay of the same handoff: stale.
        let e = n.handoff(&h, &fs, &ts, &hold(&root, &b), now_secs()).unwrap_err();
        assert!(e.contains("stale"), "{e}");
        // A no longer holds the name: it cannot hand epoch 3 to C.
        let h3 = handoff(&a, &c, 3);
        let e = n
            .handoff(&h3, &sign_hex(&a, &handoff_bytes(&h3)), &sign_hex(&c, &accept_bytes(&h3)), &hold(&root, &c), now_secs())
            .unwrap_err();
        assert!(e.contains("does not hold"), "{e}");
        assert_eq!(n.resolve("notes").unwrap().holder, b.node_id_hex());
    }
}
