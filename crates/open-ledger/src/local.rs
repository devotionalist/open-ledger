//! Local directory Bookie: Journal (WAL) + ledger entry files.
//!
//! Layout:
//! ```text
//! {root}/
//!   bookie_id
//!   journal/journal.log      # append-only WAL, fsynced on Append
//!   ledgers/{ledgerId}/
//!     meta.json
//!     entries.bin            # length-prefixed records after journal ack
//! ```

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::store::LedgerStore;
use crate::types::{BookieId, Ensemble, Entry, EntryId, LedgerId, LedgerState, Quorum};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LedgerMetaFile {
    ledger_id: LedgerId,
    quorum: Quorum,
    ensemble: Ensemble,
    state: LedgerState,
    next_entry_id: EntryId,
    last_entry_id: i64,
}

struct LedgerHandle {
    meta: LedgerMetaFile,
    entries_path: PathBuf,
    meta_path: PathBuf,
}

/// Single-node bookie with durable Journal.
pub struct LocalBookie {
    root: PathBuf,
    bookie_id: BookieId,
    journal: Mutex<File>,
    ledgers: Mutex<std::collections::HashMap<LedgerId, LedgerHandle>>,
}

impl LocalBookie {
    /// Open or create a bookie data directory.
    pub fn open(root: impl AsRef<Path>, bookie_id: impl Into<BookieId>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let bookie_id = bookie_id.into();
        fs::create_dir_all(root.join("journal"))?;
        fs::create_dir_all(root.join("ledgers"))?;
        fs::write(root.join("bookie_id"), bookie_id.as_bytes())?;

        let journal_path = root.join("journal").join("journal.log");
        let journal = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&journal_path)?;

        let mut bookie = Self {
            root,
            bookie_id,
            journal: Mutex::new(journal),
            ledgers: Mutex::new(std::collections::HashMap::new()),
        };
        bookie.load_existing()?;
        Ok(bookie)
    }

    /// This bookie's id.
    pub fn bookie_id(&self) -> &str {
        &self.bookie_id
    }

    fn load_existing(&mut self) -> Result<()> {
        let ledgers_dir = self.root.join("ledgers");
        for ent in fs::read_dir(&ledgers_dir)? {
            let ent = ent?;
            if !ent.file_type()?.is_dir() {
                continue;
            }
            let name = ent.file_name();
            let Some(id_str) = name.to_str() else {
                continue;
            };
            let Ok(ledger_id) = id_str.parse::<LedgerId>() else {
                continue;
            };
            let meta_path = ent.path().join("meta.json");
            let entries_path = ent.path().join("entries.bin");
            let raw = fs::read_to_string(&meta_path)?;
            let meta: LedgerMetaFile =
                serde_json::from_str(&raw).map_err(|e| Error::Codec(e.to_string()))?;
            self.ledgers.lock().expect("ledgers").insert(
                ledger_id,
                LedgerHandle {
                    meta,
                    entries_path,
                    meta_path,
                },
            );
        }
        Ok(())
    }

    fn persist_meta(handle: &LedgerHandle) -> Result<()> {
        let raw =
            serde_json::to_string_pretty(&handle.meta).map_err(|e| Error::Codec(e.to_string()))?;
        let tmp = handle.meta_path.with_extension("json.tmp");
        fs::write(&tmp, raw)?;
        fs::rename(&tmp, &handle.meta_path)?;
        Ok(())
    }

    /// Journal record: magic + ledger_id + entry_id + lac + payload_len + payload
    fn journal_append(&self, entry: &Entry) -> Result<()> {
        let mut j = self.journal.lock().expect("journal");
        let payload_len = entry.payload.len() as u32;
        j.write_all(b"OJNL")?;
        j.write_all(&entry.ledger_id.to_le_bytes())?;
        j.write_all(&entry.entry_id.to_le_bytes())?;
        j.write_all(&entry.lac.to_le_bytes())?;
        j.write_all(&payload_len.to_le_bytes())?;
        j.write_all(&entry.payload)?;
        j.sync_all()?;
        Ok(())
    }

    fn entries_append(path: &Path, entry: &Entry) -> Result<()> {
        let mut f = OpenOptions::new().create(true).append(true).open(path)?;
        let payload_len = entry.payload.len() as u32;
        f.write_all(&entry.entry_id.to_le_bytes())?;
        f.write_all(&entry.lac.to_le_bytes())?;
        f.write_all(&payload_len.to_le_bytes())?;
        f.write_all(&entry.payload)?;
        f.sync_all()?;
        Ok(())
    }

    fn entries_read(path: &Path, ledger_id: LedgerId, from: EntryId, max: usize) -> Result<Vec<Entry>> {
        if !path.exists() {
            return Ok(Vec::new());
        }
        let mut f = File::open(path)?;
        let mut out = Vec::new();
        loop {
            let mut hdr = [0u8; 20]; // entry_id u64 + lac i64 + len u32 = 20
            match f.read_exact(&mut hdr) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let entry_id = u64::from_le_bytes(hdr[0..8].try_into().unwrap());
            let lac = i64::from_le_bytes(hdr[8..16].try_into().unwrap());
            let len = u32::from_le_bytes(hdr[16..20].try_into().unwrap()) as usize;
            let mut payload = vec![0u8; len];
            f.read_exact(&mut payload)?;
            if entry_id >= from {
                out.push(Entry::new(ledger_id, entry_id, lac, Bytes::from(payload)));
                if out.len() >= max {
                    break;
                }
            }
        }
        Ok(out)
    }
}

impl LedgerStore for LocalBookie {
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
        if this_bookie != &self.bookie_id {
            return Err(Error::InvalidArgument(
                "this_bookie mismatch".into(),
            ));
        }
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

        let mut g = self.ledgers.lock().expect("ledgers");
        if g.contains_key(&ledger_id) {
            return Err(Error::AlreadyExists(ledger_id));
        }

        let dir = self.root.join("ledgers").join(ledger_id.to_string());
        fs::create_dir_all(&dir)?;
        let meta_path = dir.join("meta.json");
        let entries_path = dir.join("entries.bin");
        let meta = LedgerMetaFile {
            ledger_id,
            quorum,
            ensemble,
            state: LedgerState::Open,
            next_entry_id: 0,
            last_entry_id: -1,
        };
        let handle = LedgerHandle {
            meta,
            entries_path,
            meta_path,
        };
        Self::persist_meta(&handle)?;
        g.insert(ledger_id, handle);
        Ok(())
    }

    fn append(
        &self,
        ledger_id: LedgerId,
        entry_id: EntryId,
        lac: i64,
        payload: Bytes,
    ) -> Result<()> {
        let entry = Entry::new(ledger_id, entry_id, lac, payload);

        // Validate under lock, then Journal fsync (durability barrier), then ledger file.
        {
            let g = self.ledgers.lock().expect("ledgers");
            let handle = g.get(&ledger_id).ok_or(Error::NotFound(ledger_id))?;
            match handle.meta.state {
                LedgerState::Fenced => return Err(Error::Fenced(ledger_id)),
                LedgerState::Closed => return Err(Error::Sealed(ledger_id)),
                LedgerState::Open => {}
            }
            if entry_id != handle.meta.next_entry_id {
                return Err(Error::BadEntryId {
                    ledger_id,
                    expected: handle.meta.next_entry_id,
                    got: entry_id,
                });
            }
        }

        self.journal_append(&entry)?;

        let mut g = self.ledgers.lock().expect("ledgers");
        let handle = g.get_mut(&ledger_id).ok_or(Error::NotFound(ledger_id))?;
        // Re-check after journal (fence may have raced).
        match handle.meta.state {
            LedgerState::Fenced => return Err(Error::Fenced(ledger_id)),
            LedgerState::Closed => return Err(Error::Sealed(ledger_id)),
            LedgerState::Open => {}
        }
        if entry_id != handle.meta.next_entry_id {
            return Err(Error::BadEntryId {
                ledger_id,
                expected: handle.meta.next_entry_id,
                got: entry_id,
            });
        }

        Self::entries_append(&handle.entries_path, &entry)?;
        handle.meta.next_entry_id = entry_id + 1;
        handle.meta.last_entry_id = entry_id as i64;
        Self::persist_meta(handle)?;
        Ok(())
    }

    fn fence(&self, ledger_id: LedgerId) -> Result<()> {
        let mut g = self.ledgers.lock().expect("ledgers");
        let handle = g.get_mut(&ledger_id).ok_or(Error::NotFound(ledger_id))?;
        if handle.meta.state == LedgerState::Closed {
            return Ok(());
        }
        handle.meta.state = LedgerState::Fenced;
        Self::persist_meta(handle)?;
        Ok(())
    }

    fn recover(&self, ledger_id: LedgerId) -> Result<i64> {
        let mut g = self.ledgers.lock().expect("ledgers");
        let handle = g.get_mut(&ledger_id).ok_or(Error::NotFound(ledger_id))?;
        if handle.meta.state != LedgerState::Closed {
            handle.meta.state = LedgerState::Closed;
            Self::persist_meta(handle)?;
        }
        Ok(handle.meta.last_entry_id)
    }

    fn close(&self, ledger_id: LedgerId, last_entry_id: i64) -> Result<()> {
        let mut g = self.ledgers.lock().expect("ledgers");
        let handle = g.get_mut(&ledger_id).ok_or(Error::NotFound(ledger_id))?;
        if last_entry_id < -1 || last_entry_id > handle.meta.last_entry_id {
            return Err(Error::InvalidArgument(
                "last_entry_id out of range".into(),
            ));
        }
        handle.meta.last_entry_id = last_entry_id;
        handle.meta.state = LedgerState::Closed;
        Self::persist_meta(handle)?;
        Ok(())
    }

    fn read(&self, ledger_id: LedgerId, from_entry_id: EntryId, max: usize) -> Result<Vec<Entry>> {
        let g = self.ledgers.lock().expect("ledgers");
        let handle = g.get(&ledger_id).ok_or(Error::NotFound(ledger_id))?;
        Self::entries_read(&handle.entries_path, ledger_id, from_entry_id, max)
    }

    fn delete(&self, ledger_id: LedgerId) -> Result<()> {
        let mut g = self.ledgers.lock().expect("ledgers");
        if g.remove(&ledger_id).is_none() {
            return Err(Error::NotFound(ledger_id));
        }
        let dir = self.root.join("ledgers").join(ledger_id.to_string());
        if dir.exists() {
            fs::remove_dir_all(dir)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn journal_durable_append_and_reopen() {
        let dir = tempdir().unwrap();
        let id = "b0".to_string();
        {
            let b = LocalBookie::open(dir.path(), id.clone()).unwrap();
            b.create(7, Quorum::ONE, vec![id.clone()], &id).unwrap();
            b.append(7, 0, -1, Bytes::from_static(b"hello")).unwrap();
            b.append(7, 1, 0, Bytes::from_static(b"world")).unwrap();
        }
        let b = LocalBookie::open(dir.path(), id).unwrap();
        let got = b.read(7, 0, 10).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].payload.as_ref(), b"hello");
        assert_eq!(b.recover(7).unwrap(), 1);
    }
}
