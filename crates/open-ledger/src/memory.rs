//! In-memory Bookie for unit tests and M0 smoke (no disk).

use std::collections::BTreeMap;
use std::sync::Mutex;

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::store::LedgerStore;
use crate::types::{BookieId, Ensemble, Entry, EntryId, LedgerId, LedgerState, Quorum};

struct LedgerMem {
    state: LedgerState,
    next_entry_id: EntryId,
    last_entry_id: i64,
    entries: BTreeMap<EntryId, Entry>,
    #[allow(dead_code)]
    quorum: Quorum,
    #[allow(dead_code)]
    ensemble: Ensemble,
}

/// Process-local bookie. Not durable across restart — tests / embed only.
#[derive(Default)]
pub struct MemoryBookie {
    inner: Mutex<BTreeMap<LedgerId, LedgerMem>>,
}

impl MemoryBookie {
    /// Create an empty in-memory bookie.
    pub fn new() -> Self {
        Self::default()
    }
}

impl LedgerStore for MemoryBookie {
    fn create(
        &self,
        ledger_id: LedgerId,
        quorum: Quorum,
        ensemble: Ensemble,
        this_bookie: &BookieId,
    ) -> Result<()> {
        quorum
            .validate()
            .map_err(|e| Error::InvalidArgument(e.into()))?;
        if !ensemble.iter().any(|b| b == this_bookie) {
            return Err(Error::InvalidArgument(
                "this_bookie not in ensemble".into(),
            ));
        }
        if ensemble.len() != quorum.e as usize {
            return Err(Error::InvalidArgument(
                "ensemble len must equal E".into(),
            ));
        }

        let mut g = self.inner.lock().expect("memory bookie lock");
        if g.contains_key(&ledger_id) {
            return Err(Error::AlreadyExists(ledger_id));
        }
        g.insert(
            ledger_id,
            LedgerMem {
                state: LedgerState::Open,
                next_entry_id: 0,
                last_entry_id: -1,
                entries: BTreeMap::new(),
                quorum,
                ensemble,
            },
        );
        Ok(())
    }

    fn append(
        &self,
        ledger_id: LedgerId,
        entry_id: EntryId,
        lac: i64,
        payload: Bytes,
    ) -> Result<()> {
        let mut g = self.inner.lock().expect("memory bookie lock");
        let led = g.get_mut(&ledger_id).ok_or(Error::NotFound(ledger_id))?;
        match led.state {
            LedgerState::Fenced => return Err(Error::Fenced(ledger_id)),
            LedgerState::Closed => return Err(Error::Sealed(ledger_id)),
            LedgerState::Open => {}
        }
        if entry_id != led.next_entry_id {
            return Err(Error::BadEntryId {
                ledger_id,
                expected: led.next_entry_id,
                got: entry_id,
            });
        }
        led.entries.insert(
            entry_id,
            Entry::new(ledger_id, entry_id, lac, payload),
        );
        led.next_entry_id = entry_id + 1;
        led.last_entry_id = entry_id as i64;
        Ok(())
    }

    fn fence(&self, ledger_id: LedgerId) -> Result<()> {
        let mut g = self.inner.lock().expect("memory bookie lock");
        let led = g.get_mut(&ledger_id).ok_or(Error::NotFound(ledger_id))?;
        if led.state == LedgerState::Closed {
            return Ok(());
        }
        led.state = LedgerState::Fenced;
        Ok(())
    }

    fn recover(&self, ledger_id: LedgerId) -> Result<i64> {
        let mut g = self.inner.lock().expect("memory bookie lock");
        let led = g.get_mut(&ledger_id).ok_or(Error::NotFound(ledger_id))?;
        if led.state == LedgerState::Closed {
            return Ok(led.last_entry_id);
        }
        led.state = LedgerState::Closed;
        Ok(led.last_entry_id)
    }

    fn close(&self, ledger_id: LedgerId, last_entry_id: i64) -> Result<()> {
        let mut g = self.inner.lock().expect("memory bookie lock");
        let led = g.get_mut(&ledger_id).ok_or(Error::NotFound(ledger_id))?;
        if last_entry_id < -1 || last_entry_id > led.last_entry_id {
            return Err(Error::InvalidArgument(
                "last_entry_id out of range".into(),
            ));
        }
        led.last_entry_id = last_entry_id;
        led.state = LedgerState::Closed;
        Ok(())
    }

    fn read(&self, ledger_id: LedgerId, from_entry_id: EntryId, max: usize) -> Result<Vec<Entry>> {
        let g = self.inner.lock().expect("memory bookie lock");
        let led = g.get(&ledger_id).ok_or(Error::NotFound(ledger_id))?;
        let out: Vec<_> = led
            .entries
            .range(from_entry_id..)
            .take(max)
            .map(|(_, e)| e.clone())
            .collect();
        Ok(out)
    }

    fn delete(&self, ledger_id: LedgerId) -> Result<()> {
        let mut g = self.inner.lock().expect("memory bookie lock");
        if g.remove(&ledger_id).is_none() {
            return Err(Error::NotFound(ledger_id));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_read_fence() {
        let b = MemoryBookie::new();
        let id = "bookie-0".to_string();
        b.create(1, Quorum::ONE, vec![id.clone()], &id).unwrap();
        b.append(1, 0, -1, Bytes::from_static(b"a")).unwrap();
        b.append(1, 1, 0, Bytes::from_static(b"b")).unwrap();
        let got = b.read(1, 0, 10).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[1].payload.as_ref(), b"b");

        b.fence(1).unwrap();
        assert!(matches!(
            b.append(1, 2, 1, Bytes::from_static(b"c")),
            Err(Error::Fenced(1))
        ));
        assert_eq!(b.recover(1).unwrap(), 1);
        assert!(matches!(
            b.append(1, 2, 1, Bytes::from_static(b"c")),
            Err(Error::Sealed(1))
        ));
    }
}
