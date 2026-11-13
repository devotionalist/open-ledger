# open-ledger Design

[English](./design.md) | [中文](./zh-CN/design.md)

Entry summary: [architecture.md](./architecture.md).

This document describes **open-ledger**: how a Bookie stores entries, and how the client completes quorum writes and tail recovery across multiple Bookies.

## 1. Boundaries

```text
Client (this repo's client module, or an external library with the same semantics)
        │  Bookie API (in-process / gRPC)
        ▼
Bookie (this repo)     stores only local entry bytes, fenced, explicit LAC
Metadata (external)    E/Qw/Qa, fragment, CLOSED.lastEntryId, master key
```

| In scope | Out of scope |
|---|---|
| Bookie: Journal + ledger storage | Topic, subscription, cursor, ownership |
| Single-bookie add / read / fence / recoveryAdd | Treating "local max entryId" as the ledger tail |
| Quorum write, fence coverage, recover, graceful close | Topic-layer ledger-rolling policy |
| AutoRecovery (Auditor + ReplicationWorker) | Compatibility layers for other systems' storage protocols |
| `MemoryBookie` (test / embed) | |

This repo's `client` module is tested in-process with `LocalBookie` / `MemoryBookie`. `openledgerd` exposes **Callback-model** gRPC on a local directory (receive schedules WorkPool I/O; see [async-threading.md](./async-threading.md)); `openledger-autorecovery` connects as a sidecar to Oxia and the local bookie. External clients implement `Bookie` / `BookieAsync`; the algorithms stay the same.

## 2. Replication parameters

Constraint: `E >= Qw >= Qa >= 1`.

| Symbol | Meaning |
|---|---|
| E | Number of bookies in a fragment |
| Qw | Number of bookies each entry is written to |
| Qa | Number of journal acks required before confirmation |

Write set for entry `i`: on the current fragment's ensemble, `Qw` consecutive bookies starting at index `i % E` (wrap-around). Therefore when `E > Qw`, entryIds on a single bookie **have holes**. The Bookie does not require `entryId == last + 1`.

The Bookie does not know E/Qw/Qa and does not store the ensemble. Those live only in metadata.

## 3. Two kinds of state

### 3.1 Metadata (authoritative, not on bookie disk)

```text
LedgerMeta {
  ledgerId
  E, Qw, Qa
  state: OPEN | IN_RECOVERY | CLOSED
  lastEntryId          // pinned after CLOSED; -1 for an empty ledger
  fragments[]: { firstEntryId, ensemble[] }
  masterKey
  metaVersion          // CAS
  offloadLocation?     // optional URI; only meaningful when CLOSED (M3 upload)
  createdAtMs?         // for orphan GC aging
  orphanMarked?        // upper layer abandoned unindexed Create
}
```

The first fragment's `firstEntryId` is 0. Changing the ensemble only appends a fragment; `firstEntryId` must be greater than the previous segment's, and historical fragments are not modified.

**ledgerId allocation (authoritative in this repo):** Client obtains a unique id from OL Metadata (sequence key / snowflake under `/open-ledger/…`) before writing `LedgerMeta` OPEN. Upstream brokers (e.g. open-message) **must not** mint conflicting ids. Prefer setting `createdAtMs` on Create; `MarkOrphan` sets `orphanMarked`.
`MemoryMetaStore` is an in-process CAS for tests. Production uses Oxia (`OxiaMetaStore`): `LedgerMeta.meta_version` ↔ Oxia's `version_id`. Key layout is in `open_ledger::keys` (`/open-ledger/ledgers/...`, bookie registry, underreplicated, election).

### 3.2 Bookie-local

```text
LedgerLocal {
  masterKey
  fenced: bool
  explicitLac: i64     // unset is -1
  entries: map<entryId, { lac, payload }>
}
```

The Bookie has no OPEN / CLOSED. CLOSED exists only in metadata. After fence, ordinary add fails; `recoveryAdd` can still write, for read-repair.

`readLastConfirmed` = `max(explicitLac, piggyback lac on the entry with the highest local entryId)`. If there are no entries and explicit LAC is unset, it is -1. This value is not the local highest entryId.

## 4. Bookie API

```text
createLedger(ledgerId, masterKey)
addEntry(ledgerId, entryId, lac, payload, masterKey)
recoveryAdd(ledgerId, entryId, lac, payload, masterKey)
readEntry(ledgerId, entryId)
readEntries(ledgerId, from, max)          // entries present locally, skipping holes
listEntries(ledgerId)
fenceLedger(ledgerId, masterKey)
setExplicitLac(ledgerId, lac, masterKey)
readLastConfirmed(ledgerId) -> i64
lastEntryId(ledgerId) -> i64
deleteLedger(ledgerId)
```

Rules:

1. `masterKey` must be non-empty. For an existing ledger, a mismatched key is rejected.
2. `createLedger` is idempotent for the same key, and does not reopen an already-fenced ledger.
3. When `fenceLedger` hits a ledger that does not exist locally, it creates an **already-fenced empty ledger**, blocking a late write from an old writer.
4. The same `(ledgerId, entryId, payload)` is idempotent. Same entryId with a different payload is a conflict.
5. `lac < -1` is rejected.
6. `setExplicitLac` is rejected after fence.
7. When `addEntry` / `recoveryAdd` / `fenceLedger` return success, the corresponding journal record has already been `fsync`ed.

## 5. Journal and checkpoint

Directory layout:

```text
{data_dir}/
  bookie_id                         # cookie; must match the process bookie id
  journal/journal.log
  journal/checkpoint                # u64 little-endian; ledger storage has covered [0, checkpoint)
  ledgers/{ledgerId}/meta.json      # master_key, fenced, explicit_lac
  ledgers/{ledgerId}/entries.bin    # local entry append log in order; holes allowed
```

Journal record (little-endian):

```text
magic "OLJ1" | body_len u32 | body
body.tag:
  1 create        ledgerId, masterKey
  2 add           recovery flag, ledgerId, entryId, lac, masterKey, payload
  3 fence         ledgerId, masterKey
  4 explicit LAC  ledgerId, lac, masterKey
  5 delete        ledgerId
```

Single-body size limit is 32MiB. An incomplete record at end of file is treated as a tear and truncated on open. A bad magic on a complete header refuses open. The checkpoint must fall on a record boundary and must not exceed the journal's valid length.

A successful mutation is persisted in this order:

```text
prepare (in-memory validation)
→ append to journal and fsync
→ apply to memory
→ update the ledger directory by record type:
     create / fence / explicit LAC → write meta.json (create empty entries.bin if needed)
     add → append one record to entries.bin and fsync (do not rewrite the whole file)
     delete → remove the directory
→ advance checkpoint to the end of the journal
```

Therefore on a normal return, the ledger directory has already caught up with the journal, and restart need not replay that record. If the process crashes after journal fsync but before checkpoint, restart replays from the old checkpoint; replay of add / fence / delete is idempotent. Deleting `ledgers/` and setting checkpoint to 0 can rebuild from the journal alone; on open, if replay needs it, a **one-shot** rewrite of `entries.bin` from memory is allowed (compaction); the hot path must not do that.

The current implementation does **one fsync per journal record**, and each entry append is also fsync'd. Group commit can come later, but the journal must already be fsync'd before ack; that must not be relaxed.

`entries.bin` starts with `OLE2`, then appends in order `entryId u64 | lac i64 | len u32 | payload`. The same `entryId` may appear multiple times (e.g. lac-only updates); on load, later writes override earlier ones. A tear at the end is truncated. It is not a second WAL: durable authority remains the journal; `entries.bin` is checkpointed local entry storage and must be written sequentially.

## 6. Client

### 6.1 Writes

Single writer. Metadata must be OPEN.

```text
entryId = next++
send in parallel to the write set: addEntry(ledgerId, entryId, lac, payload, masterKey)
acks >= Qa  →  this entry can be confirmed
else if any replica returns fenced  →  ErrFenced
else  →  NotEnoughBookies
```

The `lac` sent to bookies is the largest entryId **already confirmed before writing this entry**; the first entry uses -1. Only after this entry reaches Qa does the writer's LAC become this entryId.

Under pipelining, LAC advances only along a contiguous prefix: if entry 3 reaches Qa first while 2 has not, LAC stays at 1. External Receipts are released according to that LAC.

`LedgerWriter` / `Writer` are synchronous handles: the next entry is sent only after the previous reaches Qa, so LAC increments by one each time. For pipelining, call `append_entry` directly and merge completions with `advance_lac`.

Entries that did not reach Qa may already be written on a minority of bookies. Retrying the same entryId with the same payload is idempotent.

Pipelined writes use `PipelineWriter`: `submit` records the entry as pending; only after `confirm` is it removed from pending. `change_ensemble` **must** run when pending is empty, with `next` as the new fragment start; otherwise it is rejected (BK #4194 / #4924, avoiding entries already sent to the old write set being hung onto a new ensemble in metadata). Synchronous `append` = submit+confirm, so it has no such race by nature.

### 6.2 Ensemble change

Append a fragment only while OPEN. New bookies need `createLedger` (idempotent for the same key). `change_ensemble` only updates metadata; `ChangeEnsemble` on the writer also creates on the new ensemble.

An entry lands on the last fragment whose `firstEntryId <= entryId`.

### 6.3 Graceful close

After the writer stops writing, CAS metadata to `CLOSED(lastEntryId = writer LAC)`. Do not fence bookies. Already CLOSED with the same `lastEntryId` is idempotent; a different value is a conflict. `IN_RECOVERY` cannot take this path; call `recover` first.

### 6.4 Recover

```text
already CLOSED → return the existing lastEntryId

CAS OPEN → IN_RECOVERY
send fenceLedger to every bookie that appears in any fragment
every fragment must satisfy fence coverage; otherwise stay in IN_RECOVERY

read readLastConfirmed from the last fragment's ensemble
  NotFound is treated as LAC = -1, counting as one valid response
  if valid responses >= Qa, take the maximum among them as maxLac

from entryId = maxLac + 1 forward (**fenced recovery read**, BK #2614 / RecoveryReadsFence):
  for each reachable bookie in the write set: fenceLedger first, then readEntry
  if one complete entry exists → recoveryAdd to the write set; if successes >= Qa, last = that id, continue
  if the entire write set explicitly answers "missing" → stop (those replicas are already fenced, so an old writer cannot flip "missing" into a successful add)
  if some bookie does not respond and no copy of the entry exists → NotEnoughBookies, do not CLOSED

CAS IN_RECOVERY → CLOSED(lastEntryId = last)
```

Fence coverage: at least `(Qw - Qa) + 1` bookies in each write-set window are already fenced. An old writer can still get at most `Qa - 1` valid acks in each window. Coverage allows a minority of replicas to remain unfenced; fence-on-read during the forward scan also pins those uncovered members, avoiding the race of "answer NoSuchEntry first, then get written by an old writer."

For `E = Qw = 3, Qa = 2`, 2 fences are required. When `Qa = Qw`, 1 fence per window is enough. For `E = 5, Qw = 3, Qa = 2`, one missing bookie can still cover all windows.

The forward scan stops at the first entryId that does not exist on the write set. Even if later entries exist (e.g. 1 is missing but 5 is on some bookie), it does not skip over that hole. Bytes after the hole remain on bookies; metadata `lastEntryId` does not include them. Readers only trust the CLOSED tail.

A read entry must be repaired to at least Qa replicas before it counts in the tail. If repair cannot reach Qa, stay in `IN_RECOVERY`.

Concurrent recover: CAS, one wins and one loses. The loser seeing CLOSED adopts the other's `lastEntryId`. fence and recoveryAdd are idempotent.

## 7. Failures

| Situation | Outcome |
|---|---|
| Bookie crashes after journal fsync, before checkpoint | Restart replays the journal; the entry is still there |
| Tear at end of journal | Truncate, then open on complete records |
| cookie mismatches `--bookie-id` | Refuse to open that directory |
| Confirmed entry (once reached Qa) | recover can read it from surviving replicas and repair |
| Written to only 1 replica, crash before Qa | If that entry can still be read, it is repaired into the CLOSED tail; otherwise it is truncated. The caller never got success |
| Old writer keeps adding after coverage is met | Cannot gather Qa; returns fenced. The one unfenced replica may retain a dirty entry |
| A hole in the middle, with later bytes present | Scan stops at the hole; CLOSED tail excludes entries after the hole |
| Not enough bookies during recover | Metadata stays `IN_RECOVERY`; retry when bookies return |
| Same entryId, two different payloads | Conflict; recover fails; do not close a fork as CLOSED |

Entries confirmed to the caller were, at confirmation time, in at least Qa bookies' journals. After recover, every entry in the CLOSED tail has been recoveryAdd'ed to at least Qa replicas.

## 8. Milestones

| Milestone | Status |
|---|---|
| M0 | Bookie semantics in this doc, journal replay, in-process quorum client. Done |
| M1 | gRPC for `openledgerd` (done); k8s bookie STS |
| M2 | Multi-process ensemble; fault injection on fragment changes |
| M3 | Oxia metadata + AutoRecovery (Auditor / ReplicationWorker sidecar). Cluster gRPC e2e covered |

## 9. AutoRecovery (M3)

Isomorphic to BookKeeper; all coordination state lives in Oxia:

| Component | Behavior |
|---|---|
| Bookie registry | Each sidecar writes ephemeral `/open-ledger/bookies/{id}` |
| Auditor election | ephemeral `/open-ledger/election/auditor`; only the leader scans for lost bookies |
| Under-replicated queue | `/open-ledger/underreplicated/{ledgerId}`; process after `lostBookieRecoveryDelay` |
| ReplicationWorker | Grab ephemeral lock, `readEntry` from surviving ensemble members, `recoveryAdd` to the local bookie, CAS replace missing with local id |
| OPEN tail segment | Wait for grace first, then fence, then replicate; do not change entryId / payload |
| Backpressure | `max_tasks_per_tick` caps tasks per round; `entry_interval` throttles `recoveryAdd`; `max_copy_bytes_per_tick` caps bytes copied per round (when budget is exhausted, leave the task for the next round; prevents #3408-class storms) |

The Worker **only repairs the local bookie** (same Pod). k8s: Oxia STS×1 + bookie STS×8 (each Pod runs `openledgerd` + `openledger-autorecovery`).

M0 allows callers to use `E = Qw = Qa = 1`; the API and metadata still carry E/Qw/Qa.

## 10. Orphan ledger GC

Upper layers (e.g. open-message `LedgerIndex`) may fail after `Create` succeeds, leaving a ledger in OL Metadata that is **never referenced**. Orphan GC lives **in open-ledger** (not the broker):

1. **Primary path:** Client `MarkOrphan(ledgerId)` → `orphanMarked=true` → after grace, Fence/Close/Delete.  
2. **Reconciliation path:** age via `createdAtMs`; delete only against a **complete** upper-layer reference snapshot (no sampling) or after MarkOrphan.  
3. **Default:** do **not** reclaim orphans back into an Index; upper layer Creates a new ledger.  
4. Never delete a ledger still listed in a same-generation full Index export.

Detail: `open-message/docs/design/22-orphan-gc.md`.
