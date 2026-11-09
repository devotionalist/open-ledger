//! Directory bookie: Journal (WAL) plus checkpointed ledger storage.
//!
//! ```text
//! {root}/
//!   bookie_id
//!   journal/journal.log     # fsynced before add/fence returns Ok
//!   journal/checkpoint      # ledger storage covers bytes [0, checkpoint)
//!   ledgers/{ledgerId}/meta.json
//!   ledgers/{ledgerId}/entries.bin   # append-only entry log (OLE2)
//! ```
//!
//! Restart loads ledger storage, then replays journal records at or after the
//! checkpoint. A torn journal tail is truncated. Hot-path adds append one
//! record to `entries.bin`; open/replay may rebuild the file once.

use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use bytes::Bytes;
use tracing::info;

use crate::disk::{self, append_entry, persist_meta, read_checkpoint, rebuild_ledgers, write_checkpoint};
use crate::error::{Error, Result};
use crate::journal::{self, encode};
use crate::state::{BookieState, Rec};
use crate::store::Bookie;
use crate::types::{BookieId, Entry, EntryId, LedgerId};

struct Inner {
    state: BookieState,
    journal: File,
    journal_len: u64,
}

/// Single-node bookie with a durable journal.
pub struct LocalBookie {
    root: PathBuf,
    bookie_id: BookieId,
    inner: Mutex<Inner>,
}

impl LocalBookie {
    /// Open or create a bookie directory. Replays the journal before returning.
    pub fn open(root: impl AsRef<Path>, bookie_id: impl Into<BookieId>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let bookie_id = bookie_id.into();
        if bookie_id.is_empty() {
            return Err(Error::InvalidArgument("bookie id must not be empty".into()));
        }
        fs::create_dir_all(root.join("journal"))?;
        fs::create_dir_all(root.join("ledgers"))?;
        check_cookie(&root, &bookie_id)?;

        let journal_path = root.join("journal").join("journal.log");
        let mut journal = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&journal_path)?;
        let bytes = fs::read(&journal_path)?;
        let scanned = journal::scan(&bytes)?;
        if scanned.torn_tail {
            journal.set_len(scanned.good_len)?;
            journal.sync_all()?;
            info!(good_len = scanned.good_len, "truncated torn journal tail");
        }
        journal.seek(SeekFrom::Start(scanned.good_len))?;

        let checkpoint = read_checkpoint(&root)?;
        if checkpoint > scanned.good_len {
            return Err(Error::Codec(
                "checkpoint is past the end of the journal".into(),
            ));
        }
        let on_boundary = checkpoint == 0
            || checkpoint == scanned.good_len
            || scanned.records.iter().any(|(off, _)| *off == checkpoint);
        if !on_boundary {
            return Err(Error::Codec(
                "checkpoint is not on a journal record boundary".into(),
            ));
        }

        let mut state = disk::load_ledgers(&root)?;
        let mut replayed = 0u64;
        for (offset, rec) in scanned.records {
            if offset < checkpoint {
                continue;
            }
            state.apply(rec)?;
            replayed += 1;
        }
        if replayed > 0 || scanned.torn_tail || checkpoint != scanned.good_len {
            info!(replayed, "replayed journal into ledger storage");
            rebuild_ledgers(&root, &state)?;
            write_checkpoint(&root, scanned.good_len)?;
        }

        Ok(Self {
            root,
            bookie_id,
            inner: Mutex::new(Inner {
                state,
                journal,
                journal_len: scanned.good_len,
            }),
        })
    }

    /// This bookie's id (cookie).
    pub fn bookie_id(&self) -> &str {
        &self.bookie_id
    }

    fn commit(&self, rec: Rec) -> Result<()> {
        let mut g = self.inner.lock().expect("local bookie");
        if !g.state.prepare(&rec)? {
            return Ok(());
        }
        let bytes = encode(&rec)?;
        let start = g.journal_len;
        if let Err(err) = write_record(&mut g, &bytes) {
            let _ = g.journal.set_len(start);
            g.journal_len = start;
            return Err(err);
        }
        g.state.apply(rec.clone())?;
        persist_commit(&self.root, &g.state, &rec)?;
        write_checkpoint(&self.root, g.journal_len)?;
        Ok(())
    }
}

fn persist_commit(root: &Path, state: &BookieState, rec: &Rec) -> Result<()> {
    let ledger_id = rec.ledger_id();
    match rec {
        Rec::Delete { .. } => disk::remove_ledger_dir(root, ledger_id),
        Rec::Create { .. } | Rec::Fence { .. } | Rec::ExplicitLac { .. } => {
            let led = state
                .get(ledger_id)
                .ok_or(Error::NotFound(ledger_id))?;
            persist_meta(root, ledger_id, led)
        }
        Rec::Add { entry_id, .. } => {
            let led = state
                .get(ledger_id)
                .ok_or(Error::NotFound(ledger_id))?;
            let entry = led
                .entries
                .get(entry_id)
                .ok_or(Error::NoSuchEntry {
                    ledger_id,
                    entry_id: *entry_id,
                })?;
            append_entry(root, ledger_id, entry)
        }
    }
}

fn write_record(inner: &mut Inner, bytes: &[u8]) -> Result<()> {
    inner.journal.seek(SeekFrom::Start(inner.journal_len))?;
    inner.journal.write_all(bytes)?;
    inner.journal.sync_all()?;
    inner.journal_len += bytes.len() as u64;
    Ok(())
}

fn check_cookie(root: &Path, bookie_id: &str) -> Result<()> {
    let path = root.join("bookie_id");
    if path.exists() {
        let existing = fs::read(&path)?;
        if existing.as_slice() != bookie_id.as_bytes() {
            return Err(Error::InvalidArgument(
                "bookie id does not match directory cookie".into(),
            ));
        }
    } else {
        fs::write(&path, bookie_id.as_bytes())?;
    }
    Ok(())
}

impl Bookie for LocalBookie {
    fn create_ledger(&self, ledger_id: LedgerId, master_key: &[u8]) -> Result<()> {
        self.commit(Rec::Create {
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
        self.commit(Rec::Add {
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
        self.commit(Rec::Add {
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
            .expect("local bookie")
            .state
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
            .expect("local bookie")
            .state
            .read_entries(ledger_id, from_entry_id, max)
    }

    fn list_entries(&self, ledger_id: LedgerId) -> Result<Vec<EntryId>> {
        self.inner
            .lock()
            .expect("local bookie")
            .state
            .list_entries(ledger_id)
    }

    fn fence_ledger(&self, ledger_id: LedgerId, master_key: &[u8]) -> Result<()> {
        self.commit(Rec::Fence {
            ledger_id,
            master_key: master_key.to_vec(),
        })
    }

    fn set_explicit_lac(&self, ledger_id: LedgerId, lac: i64, master_key: &[u8]) -> Result<()> {
        self.commit(Rec::ExplicitLac {
            ledger_id,
            lac,
            master_key: master_key.to_vec(),
        })
    }

    fn read_last_confirmed(&self, ledger_id: LedgerId) -> Result<i64> {
        self.inner
            .lock()
            .expect("local bookie")
            .state
            .read_last_confirmed(ledger_id)
    }

    fn last_entry_id(&self, ledger_id: LedgerId) -> Result<i64> {
        self.inner
            .lock()
            .expect("local bookie")
            .state
            .last_entry_id(ledger_id)
    }

    fn is_fenced(&self, ledger_id: LedgerId) -> Result<bool> {
        self.inner
            .lock()
            .expect("local bookie")
            .state
            .is_fenced(ledger_id)
    }

    fn delete_ledger(&self, ledger_id: LedgerId) -> Result<()> {
        self.commit(Rec::Delete { ledger_id })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const KEY: &[u8] = b"k";

    #[test]
    fn reopen_and_replay_from_journal_alone() {
        let dir = tempdir().unwrap();
        {
            let b = LocalBookie::open(dir.path(), "b0").unwrap();
            b.create_ledger(7, KEY).unwrap();
            b.add_entry(7, 0, -1, Bytes::from_static(b"hello"), KEY)
                .unwrap();
            b.add_entry(7, 2, 0, Bytes::from_static(b"world"), KEY)
                .unwrap();
            b.set_explicit_lac(7, 0, KEY).unwrap();
            b.fence_ledger(7, KEY).unwrap();
            b.recovery_add_entry(7, 4, 0, Bytes::from_static(b"tail"), KEY)
                .unwrap();
        }

        let b = LocalBookie::open(dir.path(), "b0").unwrap();
        assert_eq!(b.read_entry(7, 0).unwrap().payload.as_ref(), b"hello");
        assert!(matches!(
            b.read_entry(7, 1),
            Err(Error::NoSuchEntry { entry_id: 1, .. })
        ));
        assert!(b.is_fenced(7).unwrap());
        assert_eq!(b.read_last_confirmed(7).unwrap(), 0);
        assert!(matches!(
            b.add_entry(7, 5, 0, Bytes::from_static(b"no"), KEY),
            Err(Error::Fenced(7))
        ));

        drop(b);
        fs::remove_dir_all(dir.path().join("ledgers")).unwrap();
        fs::write(
            dir.path().join("journal").join("checkpoint"),
            0u64.to_le_bytes(),
        )
        .unwrap();
        let b = LocalBookie::open(dir.path(), "b0").unwrap();
        assert_eq!(b.read_entry(7, 2).unwrap().payload.as_ref(), b"world");
        assert_eq!(b.read_entry(7, 4).unwrap().payload.as_ref(), b"tail");
        assert!(b.is_fenced(7).unwrap());
        assert_eq!(b.read_last_confirmed(7).unwrap(), 0);
    }

    #[test]
    fn torn_journal_tail_is_truncated() {
        let dir = tempdir().unwrap();
        {
            let b = LocalBookie::open(dir.path(), "b0").unwrap();
            b.create_ledger(1, KEY).unwrap();
            b.add_entry(1, 0, -1, Bytes::from_static(b"a"), KEY)
                .unwrap();
        }
        let journal = dir.path().join("journal").join("journal.log");
        let mut f = OpenOptions::new().append(true).open(&journal).unwrap();
        f.write_all(&[9, 9, 9]).unwrap();
        f.sync_all().unwrap();
        drop(f);

        let b = LocalBookie::open(dir.path(), "b0").unwrap();
        b.add_entry(1, 3, -1, Bytes::from_static(b"b"), KEY)
            .unwrap();
        assert_eq!(b.list_entries(1).unwrap(), vec![0, 3]);
        let raw = fs::read(&journal).unwrap();
        let scanned = journal::scan(&raw).unwrap();
        assert!(!scanned.torn_tail);
        assert_eq!(scanned.good_len, raw.len() as u64);
    }

    #[test]
    fn cookie_mismatch() {
        let dir = tempdir().unwrap();
        let _ = LocalBookie::open(dir.path(), "b0").unwrap();
        match LocalBookie::open(dir.path(), "b1") {
            Err(Error::InvalidArgument(_)) => {}
            Ok(_) => panic!("cookie mismatch must fail"),
            Err(err) => panic!("unexpected error: {err}"),
        }
    }

    #[test]
    fn delete_survives_replay() {
        let dir = tempdir().unwrap();
        {
            let b = LocalBookie::open(dir.path(), "b0").unwrap();
            b.create_ledger(3, KEY).unwrap();
            b.add_entry(3, 0, -1, Bytes::from_static(b"z"), KEY)
                .unwrap();
            b.delete_ledger(3).unwrap();
        }
        fs::remove_dir_all(dir.path().join("ledgers")).unwrap();
        fs::write(
            dir.path().join("journal").join("checkpoint"),
            0u64.to_le_bytes(),
        )
        .unwrap();
        let b = LocalBookie::open(dir.path(), "b0").unwrap();
        assert!(matches!(b.read_entry(3, 0), Err(Error::NotFound(3))));
    }

    #[test]
    fn entries_bin_grows_by_append() {
        let dir = tempdir().unwrap();
        let b = LocalBookie::open(dir.path(), "b0").unwrap();
        b.create_ledger(1, KEY).unwrap();
        let path = dir.path().join("ledgers/1/entries.bin");
        let after_create = fs::metadata(&path).unwrap().len();
        assert_eq!(after_create, 4, "OLE2 header only");

        b.add_entry(1, 0, -1, Bytes::from_static(b"aa"), KEY)
            .unwrap();
        let after_0 = fs::metadata(&path).unwrap().len();
        assert_eq!(after_0, after_create + 20 + 2);

        b.add_entry(1, 2, 0, Bytes::from_static(b"bbbb"), KEY)
            .unwrap();
        let after_2 = fs::metadata(&path).unwrap().len();
        assert_eq!(after_2, after_0 + 20 + 4);

        // Same payload + new lac appends another record; does not rewrite.
        b.add_entry(1, 2, 1, Bytes::from_static(b"bbbb"), KEY)
            .unwrap();
        let after_lac = fs::metadata(&path).unwrap().len();
        assert_eq!(after_lac, after_2 + 20 + 4);

        drop(b);
        let b = LocalBookie::open(dir.path(), "b0").unwrap();
        assert_eq!(b.read_entry(1, 2).unwrap().lac, 1);
        assert_eq!(b.read_entry(1, 2).unwrap().payload.as_ref(), b"bbbb");
    }

    #[test]
    fn torn_entries_bin_tail_is_truncated() {
        let dir = tempdir().unwrap();
        {
            let b = LocalBookie::open(dir.path(), "b0").unwrap();
            b.create_ledger(1, KEY).unwrap();
            b.add_entry(1, 0, -1, Bytes::from_static(b"ok"), KEY)
                .unwrap();
        }
        let path = dir.path().join("ledgers/1/entries.bin");
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&[1, 2, 3]).unwrap();
        f.sync_all().unwrap();
        drop(f);

        let b = LocalBookie::open(dir.path(), "b0").unwrap();
        assert_eq!(b.read_entry(1, 0).unwrap().payload.as_ref(), b"ok");
        let len = fs::metadata(&path).unwrap().len();
        assert_eq!(len, 4 + 20 + 2);
    }
}
