//! Round-robin write sets and fence coverage.

use std::collections::HashSet;

use crate::error::{Error, Result};
use crate::meta::LedgerMeta;
use crate::types::{BookieId, EntryId, Fragment, Quorum};

/// Write quorum for `entry_id`: `Qw` bookies starting at `entry_id % E`.
pub fn write_set(ensemble: &[BookieId], qw: u32, entry_id: EntryId) -> Result<Vec<BookieId>> {
    let e = ensemble.len();
    if e == 0 || qw == 0 || qw as usize > e {
        return Err(Error::InvalidArgument(
            "write set requires 1 <= Qw <= ensemble size".into(),
        ));
    }
    let start = (entry_id as usize) % e;
    Ok((0..qw as usize)
        .map(|i| ensemble[(start + i) % e].clone())
        .collect())
}

/// Fragment that owns `entry_id` (greatest `first_entry_id` that is `<= entry_id`).
pub fn fragment_for(meta: &LedgerMeta, entry_id: EntryId) -> Result<&Fragment> {
    if meta.fragments.is_empty() {
        return Err(Error::InvalidArgument("ledger has no fragments".into()));
    }
    if entry_id < meta.fragments[0].first_entry_id {
        return Err(Error::InvalidArgument(
            "entry id precedes first fragment".into(),
        ));
    }
    let mut chosen = &meta.fragments[0];
    for frag in &meta.fragments {
        if frag.first_entry_id <= entry_id {
            chosen = frag;
        } else {
            break;
        }
    }
    Ok(chosen)
}

/// `true` when every write-quorum window contains at least `(Qw - Qa) + 1` fenced bookies.
///
/// That leaves the old writer at most `Qa - 1` unfenced acks in every window.
pub fn write_quorum_covered(
    ensemble: &[BookieId],
    quorum: Quorum,
    fenced: &HashSet<BookieId>,
) -> bool {
    if quorum.validate().is_err() || ensemble.len() != quorum.e as usize {
        return false;
    }
    let need = (quorum.qw - quorum.qa + 1) as usize;
    let e = ensemble.len();
    for start in 0..e {
        let hits = (0..quorum.qw as usize)
            .filter(|i| fenced.contains(&ensemble[(start + i) % e]))
            .count();
        if hits < need {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ens(n: usize) -> Vec<BookieId> {
        (0..n).map(|i| format!("b{i}")).collect()
    }

    fn names(ids: &[&str]) -> Vec<BookieId> {
        ids.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn stripes_rotate_write_set() {
        let ensemble = ens(5);
        assert_eq!(
            write_set(&ensemble, 3, 0).unwrap(),
            names(&["b0", "b1", "b2"])
        );
        assert_eq!(
            write_set(&ensemble, 3, 1).unwrap(),
            names(&["b1", "b2", "b3"])
        );
        assert_eq!(
            write_set(&ensemble, 3, 4).unwrap(),
            names(&["b4", "b0", "b1"])
        );
    }

    #[test]
    fn fence_coverage_qw3_qa2() {
        let ensemble = ens(3);
        let q = Quorum { e: 3, qw: 3, qa: 2 };
        let two: HashSet<_> = ["b0".into(), "b1".into()].into_iter().collect();
        let one: HashSet<_> = ["b0".into()].into_iter().collect();
        assert!(write_quorum_covered(&ensemble, q, &two));
        assert!(!write_quorum_covered(&ensemble, q, &one));

        // Qa = Qw ⇒ one fence in the only window is enough.
        let q3 = Quorum { e: 3, qw: 3, qa: 3 };
        assert!(write_quorum_covered(&ensemble, q3, &one));
    }

    #[test]
    fn striped_fence_can_miss_one_bookie() {
        let ensemble = ens(5);
        let q = Quorum { e: 5, qw: 3, qa: 2 };
        let four: HashSet<_> = ["b0", "b1", "b2", "b3"]
            .into_iter()
            .map(str::to_string)
            .collect();
        assert!(write_quorum_covered(&ensemble, q, &four));
        let two: HashSet<_> = ["b0".into(), "b1".into()].into_iter().collect();
        assert!(!write_quorum_covered(&ensemble, q, &two));
    }
}
