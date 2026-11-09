//! Append-only journal encoding.
//!
//! A short torn tail (crash during `write`) is truncated. A bad magic on a
//! complete header is corruption and refuses open.

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::state::Rec;
use crate::types::LedgerId;

const MAGIC: [u8; 4] = *b"OLJ1";
const MAX_BODY: u32 = 32 * 1024 * 1024;

const TAG_CREATE: u8 = 1;
const TAG_ADD: u8 = 2;
const TAG_FENCE: u8 = 3;
const TAG_LAC: u8 = 4;
const TAG_DELETE: u8 = 5;

pub(crate) struct Scanned {
    pub records: Vec<(u64, Rec)>,
    /// Bytes that form complete records. A torn suffix starts here.
    pub good_len: u64,
    pub torn_tail: bool,
}

pub(crate) fn encode(rec: &Rec) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    match rec {
        Rec::Create {
            ledger_id,
            master_key,
        } => {
            body.push(TAG_CREATE);
            put_u64(&mut body, *ledger_id);
            put_bytes(&mut body, master_key)?;
        }
        Rec::Add {
            ledger_id,
            entry_id,
            lac,
            payload,
            recovery,
            master_key,
        } => {
            body.push(TAG_ADD);
            body.push(u8::from(*recovery));
            put_u64(&mut body, *ledger_id);
            put_u64(&mut body, *entry_id);
            put_i64(&mut body, *lac);
            put_bytes(&mut body, master_key)?;
            put_bytes(&mut body, payload)?;
        }
        Rec::Fence {
            ledger_id,
            master_key,
        } => {
            body.push(TAG_FENCE);
            put_u64(&mut body, *ledger_id);
            put_bytes(&mut body, master_key)?;
        }
        Rec::ExplicitLac {
            ledger_id,
            lac,
            master_key,
        } => {
            body.push(TAG_LAC);
            put_u64(&mut body, *ledger_id);
            put_i64(&mut body, *lac);
            put_bytes(&mut body, master_key)?;
        }
        Rec::Delete { ledger_id } => {
            body.push(TAG_DELETE);
            put_u64(&mut body, *ledger_id);
        }
    }
    if body.len() > MAX_BODY as usize {
        return Err(Error::InvalidArgument("journal record too large".into()));
    }
    let mut out = Vec::with_capacity(8 + body.len());
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

pub(crate) fn scan(buf: &[u8]) -> Result<Scanned> {
    let mut i = 0usize;
    let mut records = Vec::new();
    while i < buf.len() {
        if buf.len() - i < 8 {
            return Ok(Scanned {
                records,
                good_len: i as u64,
                torn_tail: true,
            });
        }
        if buf[i..i + 4] != MAGIC {
            return Err(Error::Codec(format!("bad journal magic at offset {i}")));
        }
        let body_len = u32::from_le_bytes(buf[i + 4..i + 8].try_into().unwrap());
        if body_len > MAX_BODY {
            return Err(Error::Codec(format!(
                "journal record at {i} exceeds max length"
            )));
        }
        let total = 8 + body_len as usize;
        if buf.len() - i < total {
            return Ok(Scanned {
                records,
                good_len: i as u64,
                torn_tail: true,
            });
        }
        let body = &buf[i + 8..i + total];
        let rec = decode_body(body)?;
        records.push((i as u64, rec));
        i += total;
    }
    Ok(Scanned {
        records,
        good_len: i as u64,
        torn_tail: false,
    })
}

fn decode_body(body: &[u8]) -> Result<Rec> {
    if body.is_empty() {
        return Err(Error::Codec("empty journal body".into()));
    }
    let mut r = Reader { s: body, i: 1 };
    let rec = match body[0] {
        TAG_CREATE => Rec::Create {
            ledger_id: r.u64()?,
            master_key: r.bytes()?,
        },
        TAG_ADD => {
            let flags = r.u8()?;
            Rec::Add {
                recovery: flags & 1 == 1,
                ledger_id: r.u64()?,
                entry_id: r.u64()?,
                lac: r.i64()?,
                master_key: r.bytes()?,
                payload: Bytes::from(r.bytes()?),
            }
        }
        TAG_FENCE => Rec::Fence {
            ledger_id: r.u64()?,
            master_key: r.bytes()?,
        },
        TAG_LAC => Rec::ExplicitLac {
            ledger_id: r.u64()?,
            lac: r.i64()?,
            master_key: r.bytes()?,
        },
        TAG_DELETE => Rec::Delete {
            ledger_id: r.u64()?,
        },
        tag => return Err(Error::Codec(format!("unknown journal tag {tag}"))),
    };
    if r.i != r.s.len() {
        return Err(Error::Codec("trailing bytes in journal record".into()));
    }
    Ok(rec)
}

struct Reader<'a> {
    s: &'a [u8],
    i: usize,
}

impl Reader<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8]> {
        if self.s.len() - self.i < n {
            return Err(Error::Codec("truncated journal field".into()));
        }
        let out = &self.s[self.i..self.i + n];
        self.i += n;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes(b.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<LedgerId> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes(b.try_into().unwrap()))
    }

    fn i64(&mut self) -> Result<i64> {
        let b = self.take(8)?;
        Ok(i64::from_le_bytes(b.try_into().unwrap()))
    }

    fn bytes(&mut self) -> Result<Vec<u8>> {
        let n = self.u32()? as usize;
        Ok(self.take(n)?.to_vec())
    }
}

fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn put_i64(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn put_bytes(buf: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    let n = u32::try_from(bytes.len())
        .map_err(|_| Error::InvalidArgument("field exceeds u32 length".into()))?;
    buf.extend_from_slice(&n.to_le_bytes());
    buf.extend_from_slice(bytes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn add(recovery: bool) -> Rec {
        Rec::Add {
            ledger_id: 7,
            entry_id: 3,
            lac: 1,
            payload: Bytes::from_static(b"xyz"),
            recovery,
            master_key: b"k".to_vec(),
        }
    }

    #[test]
    fn roundtrip_and_torn_tail() {
        let records = [
            Rec::Create {
                ledger_id: 7,
                master_key: b"k".to_vec(),
            },
            add(false),
            Rec::Fence {
                ledger_id: 7,
                master_key: b"k".to_vec(),
            },
            add(true),
            Rec::ExplicitLac {
                ledger_id: 7,
                lac: 3,
                master_key: b"k".to_vec(),
            },
            Rec::Delete { ledger_id: 7 },
        ];
        let mut buf = Vec::new();
        for rec in &records {
            buf.extend(encode(rec).unwrap());
        }
        let good = buf.len();
        buf.extend_from_slice(&[1, 2, 3]);
        let scanned = scan(&buf).unwrap();
        assert!(scanned.torn_tail);
        assert_eq!(scanned.good_len, good as u64);
        assert_eq!(scanned.records.len(), records.len());
        for ((_, got), want) in scanned.records.iter().zip(records.iter()) {
            assert_eq!(got, want);
        }
    }
}
