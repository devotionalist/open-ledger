//! In-memory bookie ledger state.
//!
//! `prepare` is the user-path check. `apply` is the committed mutation and is
//! what journal replay runs. A normal add is journaled only while the ledger
//! is unfenced; replay still applies that record after a later fence record.

use std::collections::BTreeMap;

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::types::{Entry, EntryId, LedgerId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LedgerMem {
    pub master_key: Vec<u8>,
    pub fenced: bool,
    /// `-1` when unset.
    pub explicit_lac: i64,
    pub entries: BTreeMap<EntryId, Entry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Rec {
    Create {
        ledger_id: LedgerId,
        master_key: Vec<u8>,
    },
    Add {
        ledger_id: LedgerId,
        entry_id: EntryId,
        lac: i64,
        payload: Bytes,
        recovery: bool,
        master_key: Vec<u8>,
    },
    Fence {
        ledger_id: LedgerId,
        master_key: Vec<u8>,
    },
    ExplicitLac {
        ledger_id: LedgerId,
        lac: i64,
        master_key: Vec<u8>,
    },
    Delete {
        ledger_id: LedgerId,
    },
}

impl Rec {
    pub(crate) fn ledger_id(&self) -> LedgerId {
        match *self {
            Rec::Create { ledger_id, .. }
            | Rec::Add { ledger_id, .. }
            | Rec::Fence { ledger_id, .. }
            | Rec::ExplicitLac { ledger_id, .. }
            | Rec::Delete { ledger_id } => ledger_id,
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct BookieState {
    ledgers: BTreeMap<LedgerId, LedgerMem>,
}

impl BookieState {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn get(&self, ledger_id: LedgerId) -> Option<&LedgerMem> {
        self.ledgers.get(&ledger_id)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&LedgerId, &LedgerMem)> {
        self.ledgers.iter()
    }

    pub(crate) fn insert_loaded(&mut self, ledger_id: LedgerId, ledger: LedgerMem) {
        self.ledgers.insert(ledger_id, ledger);
    }

    /// `Ok(true)` means the caller must persist `rec`. `Ok(false)` is an idempotent no-op.
    pub(crate) fn prepare(&self, rec: &Rec) -> Result<bool> {
        match rec {
            Rec::Create { master_key, .. } => {
                require_key(master_key)?;
                match self.ledgers.get(&rec.ledger_id()) {
                    None => Ok(true),
                    Some(led) if led.master_key == *master_key => Ok(false),
                    Some(_) => Err(Error::Unauthorized(rec.ledger_id())),
                }
            }
            Rec::Add {
                ledger_id,
                entry_id,
                lac,
                payload,
                recovery,
                master_key,
            } => {
                require_key(master_key)?;
                require_lac(*lac)?;
                let led = self
                    .ledgers
                    .get(ledger_id)
                    .ok_or(Error::NotFound(*ledger_id))?;
                if led.master_key != *master_key {
                    return Err(Error::Unauthorized(*ledger_id));
                }
                if !*recovery && led.fenced {
                    return Err(Error::Fenced(*ledger_id));
                }
                if let Some(existing) = led.entries.get(entry_id) {
                    if existing.payload.as_ref() != payload.as_ref() {
                        return Err(Error::Conflict {
                            ledger_id: *ledger_id,
                            entry_id: *entry_id,
                        });
                    }
                    return Ok(existing.lac != *lac);
                }
                Ok(true)
            }
            Rec::Fence {
                ledger_id,
                master_key,
            } => {
                require_key(master_key)?;
                match self.ledgers.get(ledger_id) {
                    None => Ok(true),
                    Some(led) if led.master_key != *master_key => {
                        Err(Error::Unauthorized(*ledger_id))
                    }
                    Some(led) if led.fenced => Ok(false),
                    Some(_) => Ok(true),
                }
            }
            Rec::ExplicitLac {
                ledger_id,
                lac,
                master_key,
            } => {
                require_key(master_key)?;
                require_lac(*lac)?;
                let led = self
                    .ledgers
                    .get(ledger_id)
                    .ok_or(Error::NotFound(*ledger_id))?;
                if led.master_key != *master_key {
                    return Err(Error::Unauthorized(*ledger_id));
                }
                if led.fenced {
                    return Err(Error::Fenced(*ledger_id));
                }
                Ok(led.explicit_lac != *lac)
            }
            Rec::Delete { ledger_id } => {
                if self.ledgers.contains_key(ledger_id) {
                    Ok(true)
                } else {
                    Err(Error::NotFound(*ledger_id))
                }
            }
        }
    }

    pub(crate) fn apply(&mut self, rec: Rec) -> Result<()> {
        match rec {
            Rec::Create {
                ledger_id,
                master_key,
            } => {
                if let Some(led) = self.ledgers.get(&ledger_id) {
                    if led.master_key != master_key {
                        return Err(Error::Unauthorized(ledger_id));
                    }
                    return Ok(());
                }
                self.ledgers
                    .insert(ledger_id, empty_ledger(master_key, false));
                Ok(())
            }
            Rec::Add {
                ledger_id,
                entry_id,
                lac,
                payload,
                ..
            } => {
                let led = self
                    .ledgers
                    .get_mut(&ledger_id)
                    .ok_or(Error::NotFound(ledger_id))?;
                if let Some(existing) = led.entries.get(&entry_id) {
                    if existing.payload.as_ref() != payload.as_ref() {
                        return Err(Error::Conflict {
                            ledger_id,
                            entry_id,
                        });
                    }
                }
                led.entries
                    .insert(entry_id, Entry::new(ledger_id, entry_id, lac, payload));
                Ok(())
            }
            Rec::Fence {
                ledger_id,
                master_key,
            } => {
                match self.ledgers.get_mut(&ledger_id) {
                    Some(led) => {
                        if led.master_key != master_key {
                            return Err(Error::Unauthorized(ledger_id));
                        }
                        led.fenced = true;
                    }
                    None => {
                        self.ledgers
                            .insert(ledger_id, empty_ledger(master_key, true));
                    }
                }
                Ok(())
            }
            Rec::ExplicitLac { ledger_id, lac, .. } => {
                let led = self
                    .ledgers
                    .get_mut(&ledger_id)
                    .ok_or(Error::NotFound(ledger_id))?;
                led.explicit_lac = lac;
                Ok(())
            }
            Rec::Delete { ledger_id } => {
                self.ledgers.remove(&ledger_id);
                Ok(())
            }
        }
    }

    pub(crate) fn read_entry(&self, ledger_id: LedgerId, entry_id: EntryId) -> Result<Entry> {
        let led = self
            .ledgers
            .get(&ledger_id)
            .ok_or(Error::NotFound(ledger_id))?;
        led.entries
            .get(&entry_id)
            .cloned()
            .ok_or(Error::NoSuchEntry {
                ledger_id,
                entry_id,
            })
    }

    pub(crate) fn read_entries(
        &self,
        ledger_id: LedgerId,
        from_entry_id: EntryId,
        max: usize,
    ) -> Result<Vec<Entry>> {
        let led = self
            .ledgers
            .get(&ledger_id)
            .ok_or(Error::NotFound(ledger_id))?;
        Ok(led
            .entries
            .range(from_entry_id..)
            .take(max)
            .map(|(_, e)| e.clone())
            .collect())
    }

    pub(crate) fn list_entries(&self, ledger_id: LedgerId) -> Result<Vec<EntryId>> {
        let led = self
            .ledgers
            .get(&ledger_id)
            .ok_or(Error::NotFound(ledger_id))?;
        Ok(led.entries.keys().copied().collect())
    }

    pub(crate) fn read_last_confirmed(&self, ledger_id: LedgerId) -> Result<i64> {
        let led = self
            .ledgers
            .get(&ledger_id)
            .ok_or(Error::NotFound(ledger_id))?;
        let piggy = led
            .entries
            .iter()
            .next_back()
            .map(|(_, e)| e.lac)
            .unwrap_or(-1);
        Ok(piggy.max(led.explicit_lac))
    }

    pub(crate) fn last_entry_id(&self, ledger_id: LedgerId) -> Result<i64> {
        let led = self
            .ledgers
            .get(&ledger_id)
            .ok_or(Error::NotFound(ledger_id))?;
        Ok(led
            .entries
            .iter()
            .next_back()
            .map(|(id, _)| *id as i64)
            .unwrap_or(-1))
    }

    pub(crate) fn is_fenced(&self, ledger_id: LedgerId) -> Result<bool> {
        let led = self
            .ledgers
            .get(&ledger_id)
            .ok_or(Error::NotFound(ledger_id))?;
        Ok(led.fenced)
    }
}

fn empty_ledger(master_key: Vec<u8>, fenced: bool) -> LedgerMem {
    LedgerMem {
        master_key,
        fenced,
        explicit_lac: -1,
        entries: BTreeMap::new(),
    }
}

fn require_key(key: &[u8]) -> Result<()> {
    if key.is_empty() {
        Err(Error::InvalidArgument(
            "master key must not be empty".into(),
        ))
    } else {
        Ok(())
    }
}

fn require_lac(lac: i64) -> Result<()> {
    if lac < -1 {
        Err(Error::InvalidArgument("lac must be >= -1".into()))
    } else {
        Ok(())
    }
}
