//! In-memory bookie. Same add / fence / recovery-add rules as [`crate::LocalBookie`], without a journal.

use std::sync::Mutex;

use bytes::Bytes;

use crate::error::Result;
use crate::state::{BookieState, Rec};
use crate::store::Bookie;
use crate::types::{Entry, EntryId, LedgerId};

/// Process-local bookie. State does not survive a restart.
#[derive(Debug, Default)]
pub struct MemoryBookie {
    inner: Mutex<BookieState>,
}

impl MemoryBookie {
    /// Empty bookie.
    pub fn new() -> Self {
        Self::default()
    }

    fn submit(&self, rec: Rec) -> Result<()> {
        let mut g = self.inner.lock().expect("memory bookie");
        if g.prepare(&rec)? {
            g.apply(rec)?;
        }
        Ok(())
    }
}

impl Bookie for MemoryBookie {
    fn create_ledger(&self, ledger_id: LedgerId, master_key: &[u8]) -> Result<()> {
        self.submit(Rec::Create {
            ledger_id,
            master_key: master_key.to_vec(),
        })
    }

    fn add_entry(
        &self,
        ledger_id: LedgerId,
        entry_id: EntryId,
        lac: i64,
        payload: Bytes,
        master_key: &[u8],
    ) -> Result<()> {
        self.submit(Rec::Add {
            ledger_id,
            entry_id,
            lac,
            payload,
            recovery: false,
            master_key: master_key.to_vec(),
        })
    }

    fn recovery_add_entry(
        &self,
        ledger_id: LedgerId,
        entry_id: EntryId,
        lac: i64,
        payload: Bytes,
        master_key: &[u8],
    ) -> Result<()> {
        self.submit(Rec::Add {
            ledger_id,
            entry_id,
            lac,
            payload,
            recovery: true,
            master_key: master_key.to_vec(),
        })
    }

    fn read_entry(&self, ledger_id: LedgerId, entry_id: EntryId) -> Result<Entry> {
        self.inner
            .lock()
            .expect("memory bookie")
            .read_entry(ledger_id, entry_id)
    }

    fn read_entries(
        &self,
        ledger_id: LedgerId,
        from_entry_id: EntryId,
        max: usize,
    ) -> Result<Vec<Entry>> {
        self.inner
            .lock()
            .expect("memory bookie")
            .read_entries(ledger_id, from_entry_id, max)
    }

    fn list_entries(&self, ledger_id: LedgerId) -> Result<Vec<EntryId>> {
        self.inner
            .lock()
            .expect("memory bookie")
            .list_entries(ledger_id)
    }

    fn fence_ledger(&self, ledger_id: LedgerId, master_key: &[u8]) -> Result<()> {
        self.submit(Rec::Fence {
            ledger_id,
            master_key: master_key.to_vec(),
        })
    }

    fn set_explicit_lac(&self, ledger_id: LedgerId, lac: i64, master_key: &[u8]) -> Result<()> {
        self.submit(Rec::ExplicitLac {
            ledger_id,
            lac,
            master_key: master_key.to_vec(),
        })
    }

    fn read_last_confirmed(&self, ledger_id: LedgerId) -> Result<i64> {
        self.inner
            .lock()
            .expect("memory bookie")
            .read_last_confirmed(ledger_id)
    }

    fn last_entry_id(&self, ledger_id: LedgerId) -> Result<i64> {
        self.inner
            .lock()
            .expect("memory bookie")
            .last_entry_id(ledger_id)
    }

    fn is_fenced(&self, ledger_id: LedgerId) -> Result<bool> {
        self.inner
            .lock()
            .expect("memory bookie")
            .is_fenced(ledger_id)
    }

    fn delete_ledger(&self, ledger_id: LedgerId) -> Result<()> {
        self.submit(Rec::Delete { ledger_id })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    const KEY: &[u8] = b"k";

    #[test]
    fn gaps_fence_and_explicit_lac() {
        let b = MemoryBookie::new();
        b.create_ledger(1, KEY).unwrap();
        b.add_entry(1, 0, -1, Bytes::from_static(b"a"), KEY)
            .unwrap();
        b.add_entry(1, 2, 0, Bytes::from_static(b"c"), KEY).unwrap();
        assert!(matches!(
            b.read_entry(1, 1),
            Err(Error::NoSuchEntry { entry_id: 1, .. })
        ));
        assert_eq!(b.list_entries(1).unwrap(), vec![0, 2]);
        assert_eq!(b.read_last_confirmed(1).unwrap(), 0);
        assert_eq!(b.last_entry_id(1).unwrap(), 2);

        b.set_explicit_lac(1, 2, KEY).unwrap();
        assert_eq!(b.read_last_confirmed(1).unwrap(), 2);

        b.add_entry(1, 0, -1, Bytes::from_static(b"a"), KEY)
            .unwrap();
        assert!(matches!(
            b.add_entry(1, 0, -1, Bytes::from_static(b"nope"), KEY),
            Err(Error::Conflict { entry_id: 0, .. })
        ));

        b.fence_ledger(1, KEY).unwrap();
        assert!(matches!(
            b.add_entry(1, 3, 2, Bytes::from_static(b"x"), KEY),
            Err(Error::Fenced(1))
        ));
        assert!(matches!(
            b.set_explicit_lac(1, 3, KEY),
            Err(Error::Fenced(1))
        ));
        b.recovery_add_entry(1, 4, 2, Bytes::from_static(b"r"), KEY)
            .unwrap();
        assert_eq!(b.read_entry(1, 4).unwrap().payload.as_ref(), b"r");
        b.recovery_add_entry(1, 4, 2, Bytes::from_static(b"r"), KEY)
            .unwrap();

        assert!(matches!(
            b.fence_ledger(1, b"other"),
            Err(Error::Unauthorized(1))
        ));
        assert!(b.is_fenced(1).unwrap());
    }

    #[test]
    fn fence_unknown_ledger_blocks_later_add() {
        let b = MemoryBookie::new();
        b.fence_ledger(9, KEY).unwrap();
        b.create_ledger(9, KEY).unwrap();
        assert!(b.is_fenced(9).unwrap());
        assert!(matches!(
            b.add_entry(9, 0, -1, Bytes::from_static(b"a"), KEY),
            Err(Error::Fenced(9))
        ));
    }
}
