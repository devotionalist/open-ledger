//! Ledger directory on disk.
//!
//! `meta.json` is small and rewritten on create / fence / explicit LAC.
//! `entries.bin` is an **append-only** entry log (OLE2). Hot-path adds never
//! rewrite the whole file. The same `entryId` may appear more than once (LAC
//! update); load keeps the last complete record. A torn tail is truncated.
//!
//! After journal replay on open, `rebuild_ledgers` may rewrite `entries.bin`
//! once so the file matches memory. That path is recovery, not the write path.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::state::{BookieState, LedgerMem};
use crate::types::{Entry, EntryId, LedgerId};

const ENTRIES_MAGIC: &[u8; 4] = b"OLE2";

#[derive(Serialize, Deserialize)]
struct DiskMeta {
    master_key: Vec<u8>,
    fenced: bool,
    explicit_lac: i64,
}

pub(crate) fn load_ledgers(root: &Path) -> Result<BookieState> {
    let mut state = BookieState::new();
    let dir = root.join("ledgers");
    if !dir.exists() {
        return Ok(state);
    }
    for ent in fs::read_dir(&dir)? {
        let ent = ent?;
        if !ent.file_type()?.is_dir() {
            continue;
        }
        let name = ent.file_name();
        let id_str = name
            .to_str()
            .ok_or_else(|| Error::Codec("ledger directory name is not utf-8".into()))?;
        let ledger_id: LedgerId = id_str
            .parse()
            .map_err(|_| Error::Codec(format!("bad ledger directory {id_str}")))?;
        state.insert_loaded(ledger_id, load_one(&ent.path(), ledger_id)?);
    }
    Ok(state)
}

/// Persist meta and create an empty append log if needed.
pub(crate) fn persist_meta(root: &Path, ledger_id: LedgerId, led: &LedgerMem) -> Result<()> {
    let dir = ledger_dir(root, ledger_id);
    fs::create_dir_all(&dir)?;
    write_meta(&dir, led)?;
    ensure_entries_header(&dir.join("entries.bin"))?;
    Ok(())
}

/// Append one entry record and fsync. Does not rewrite prior records.
pub(crate) fn append_entry(root: &Path, ledger_id: LedgerId, entry: &Entry) -> Result<()> {
    let dir = ledger_dir(root, ledger_id);
    fs::create_dir_all(&dir)?;
    let path = dir.join("entries.bin");
    ensure_entries_header(&path)?;
    let mut f = OpenOptions::new()
        .create(true)
        .truncate(false)
        .append(true)
        .open(&path)?;
    let rec = encode_entry_record(entry)?;
    f.write_all(&rec)?;
    f.sync_all()?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

pub(crate) fn remove_ledger_dir(root: &Path, ledger_id: LedgerId) -> Result<()> {
    let dir = ledger_dir(root, ledger_id);
    if dir.exists() {
        fs::remove_dir_all(dir)?;
    }
    Ok(())
}

/// Rebuild every ledger directory from memory (open / replay only).
pub(crate) fn rebuild_ledgers(root: &Path, state: &BookieState) -> Result<()> {
    let mut keep = Vec::new();
    for (id, led) in state.iter() {
        rebuild_ledger(root, *id, led)?;
        keep.push(*id);
    }
    let dir = root.join("ledgers");
    if !dir.exists() {
        return Ok(());
    }
    for ent in fs::read_dir(&dir)? {
        let ent = ent?;
        if !ent.file_type()?.is_dir() {
            continue;
        }
        let Some(name) = ent.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let Ok(id) = name.parse::<LedgerId>() else {
            continue;
        };
        if !keep.contains(&id) {
            fs::remove_dir_all(ent.path())?;
        }
    }
    Ok(())
}

pub(crate) fn read_checkpoint(root: &Path) -> Result<u64> {
    let path = root.join("journal").join("checkpoint");
    if !path.exists() {
        return Ok(0);
    }
    let bytes = fs::read(path)?;
    if bytes.len() != 8 {
        return Err(Error::Codec("checkpoint file must be 8 bytes".into()));
    }
    Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
}

pub(crate) fn write_checkpoint(root: &Path, offset: u64) -> Result<()> {
    atomic_write(
        &root.join("journal").join("checkpoint"),
        &offset.to_le_bytes(),
    )
}

fn rebuild_ledger(root: &Path, ledger_id: LedgerId, led: &LedgerMem) -> Result<()> {
    let dir = ledger_dir(root, ledger_id);
    fs::create_dir_all(&dir)?;
    write_meta(&dir, led)?;
    let mut buf = Vec::new();
    buf.extend_from_slice(ENTRIES_MAGIC);
    for entry in led.entries.values() {
        buf.extend_from_slice(&encode_entry_record(entry)?);
    }
    atomic_write(&dir.join("entries.bin"), &buf)?;
    Ok(())
}

fn write_meta(dir: &Path, led: &LedgerMem) -> Result<()> {
    let meta = DiskMeta {
        master_key: led.master_key.clone(),
        fenced: led.fenced,
        explicit_lac: led.explicit_lac,
    };
    let raw = serde_json::to_vec_pretty(&meta).map_err(|e| Error::Codec(e.to_string()))?;
    atomic_write(&dir.join("meta.json"), &raw)
}

fn ensure_entries_header(path: &Path) -> Result<()> {
    if path.exists() {
        let len = fs::metadata(path)?.len();
        if len == 0 {
            let mut f = OpenOptions::new().write(true).open(path)?;
            f.write_all(ENTRIES_MAGIC)?;
            f.sync_all()?;
        }
        return Ok(());
    }
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;
    f.write_all(ENTRIES_MAGIC)?;
    f.sync_all()?;
    Ok(())
}

fn load_one(dir: &Path, ledger_id: LedgerId) -> Result<LedgerMem> {
    let raw = fs::read_to_string(dir.join("meta.json"))?;
    let meta: DiskMeta = serde_json::from_str(&raw).map_err(|e| Error::Codec(e.to_string()))?;
    let entries_path = dir.join("entries.bin");
    let entries = if entries_path.exists() {
        load_entries_file(ledger_id, &entries_path)?
    } else {
        std::collections::BTreeMap::new()
    };
    Ok(LedgerMem {
        master_key: meta.master_key,
        fenced: meta.fenced,
        explicit_lac: meta.explicit_lac,
        entries,
    })
}

fn load_entries_file(
    ledger_id: LedgerId,
    path: &Path,
) -> Result<std::collections::BTreeMap<EntryId, Entry>> {
    let mut buf = fs::read(path)?;
    let scanned = scan_entries(ledger_id, &buf)?;
    if scanned.torn_tail {
        let f = OpenOptions::new().write(true).open(path)?;
        f.set_len(scanned.good_len)?;
        f.sync_all()?;
        buf.truncate(scanned.good_len as usize);
    }
    decode_entries(ledger_id, &buf)
}

struct EntriesScan {
    good_len: u64,
    torn_tail: bool,
}

fn scan_entries(ledger_id: LedgerId, buf: &[u8]) -> Result<EntriesScan> {
    if buf.is_empty() {
        return Ok(EntriesScan {
            good_len: 0,
            torn_tail: false,
        });
    }
    if buf.len() < 4 {
        return Ok(EntriesScan {
            good_len: 0,
            torn_tail: true,
        });
    }
    if &buf[..4] != ENTRIES_MAGIC {
        return Err(Error::Codec(format!(
            "ledger {ledger_id} entries.bin magic"
        )));
    }
    let mut i = 4usize;
    let mut good = 4usize;
    while i < buf.len() {
        if buf.len() - i < 20 {
            return Ok(EntriesScan {
                good_len: good as u64,
                torn_tail: true,
            });
        }
        let n = u32::from_le_bytes(buf[i + 16..i + 20].try_into().unwrap()) as usize;
        if buf.len() - (i + 20) < n {
            return Ok(EntriesScan {
                good_len: good as u64,
                torn_tail: true,
            });
        }
        i += 20 + n;
        good = i;
    }
    Ok(EntriesScan {
        good_len: good as u64,
        torn_tail: false,
    })
}

fn encode_entry_record(entry: &Entry) -> Result<Vec<u8>> {
    let n = u32::try_from(entry.payload.len())
        .map_err(|_| Error::InvalidArgument("entry too large".into()))?;
    let mut buf = Vec::with_capacity(20 + entry.payload.len());
    buf.extend_from_slice(&entry.entry_id.to_le_bytes());
    buf.extend_from_slice(&entry.lac.to_le_bytes());
    buf.extend_from_slice(&n.to_le_bytes());
    buf.extend_from_slice(&entry.payload);
    Ok(buf)
}

fn decode_entries(
    ledger_id: LedgerId,
    buf: &[u8],
) -> Result<std::collections::BTreeMap<EntryId, Entry>> {
    if buf.len() < 4 || &buf[..4] != ENTRIES_MAGIC {
        return Err(Error::Codec(format!(
            "ledger {ledger_id} entries.bin magic"
        )));
    }
    let mut i = 4;
    let mut out = std::collections::BTreeMap::new();
    while i < buf.len() {
        if buf.len() - i < 20 {
            return Err(Error::Codec(format!("ledger {ledger_id} torn entries.bin")));
        }
        let entry_id = u64::from_le_bytes(buf[i..i + 8].try_into().unwrap());
        let lac = i64::from_le_bytes(buf[i + 8..i + 16].try_into().unwrap());
        let n = u32::from_le_bytes(buf[i + 16..i + 20].try_into().unwrap()) as usize;
        i += 20;
        if buf.len() - i < n {
            return Err(Error::Codec(format!(
                "ledger {ledger_id} torn entry payload"
            )));
        }
        let payload = Bytes::copy_from_slice(&buf[i..i + n]);
        i += n;
        // Later record for the same entryId wins (LAC refresh).
        out.insert(entry_id, Entry::new(ledger_id, entry_id, lac, payload));
    }
    Ok(out)
}

fn ledger_dir(root: &Path, ledger_id: LedgerId) -> PathBuf {
    root.join("ledgers").join(ledger_id.to_string())
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}
