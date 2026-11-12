# open-ledger Architecture

[English](./architecture.md) | [中文](./zh-CN/architecture.md)

Full design: [design.md](./design.md).

open-ledger is a Bookie: Journal + ledger storage, keyed only by `ledgerId + entryId`.

```text
Client
    add / fence / read / recoveryAdd
        │
        ▼
Bookie (this repo)        entry bytes, fenced, explicit LAC
Oxia (LedgerMeta + AR)    E/Qw/Qa, fragments, CLOSED, offloadLocation?, registry, recovery queue
AutoRecovery sidecar      Auditor (leader) + ReplicationWorker (rereplicate to local bookie)
```

`ledgerId` is allocated by the open-ledger client via OL Metadata. Orphan GC for unreferenced ledgers: [design.md §10](./design.md#10-orphan-ledger-gc).

A single bookie does not know the ensemble. When `E > Qw`, local entry ids have gaps. The ledger tail is written by the client into Oxia metadata — not derived from the highest local entry id.

## Repository

| Path | Role |
|---|---|
| `crates/open-ledger` | `Bookie`, `LocalBookie`, `MemoryBookie`, in-process quorum client, Oxia key layout |
| `crates/open-ledger-oxia` | `OxiaMetaStore`, registry, election, Auditor, ReplicationWorker ([oxia-client](https://github.com/oxia-db/oxia-client-rust)) |
| `crates/openledgerd` | Bookie process: local directory + gRPC `:3181` |
| `crates/openledger-autorecovery` | AutoRecovery sidecar colocated with the bookie |
| External clients | [open-ledger-client-go](https://github.com/devotionalist/open-ledger-client-go), [rust](https://github.com/devotionalist/open-ledger-client-rust), [java](https://github.com/devotionalist/open-ledger-client-java), [ts](https://github.com/devotionalist/open-ledger-client-ts) — same quorum / recover / PipelineWriter semantics |

## Reading guide

| Question | Section |
|---|---|
| Who stores what; where the client lives | [design.md §1 Boundaries](./design.md#1-boundaries) |
| E / Qw / Qa and write sets | [§2](./design.md#2-replication-parameters) |
| Metadata vs bookie-local state | [§3](./design.md#3-two-kinds-of-state) |
| add, fence, recoveryAdd, idempotency, conflicts | [§4](./design.md#4-bookie-api) |
| Journal, checkpoint, crash replay, disk layout | [§5](./design.md#5-journal-and-checkpoint) |
| Quorum write, LAC, ensemble change, close, recover | [§6](./design.md#6-client) |
| Crashes, dirty tails, holes, old writers | [§7](./design.md#7-failures) |
| M0–M3 | [§8](./design.md#8-milestones) |
| Dev rules, test layers | [development.md](./development.md) |
| AutoRecovery / Oxia keys | [design.md §9](./design.md#9-autorecovery-m3) |
| Local k3s deploy | [deploy/local-k8s.md](./deploy/local-k8s.md) |
| Fault-test matrix and scripts | [deploy/failure-tests.md](./deploy/failure-tests.md) |

## Invariants

1. An OPEN ledger has at most one writer (enforced by upper-layer ownership).
2. Successful `addEntry` / `recoveryAdd` ⇒ that replica’s journal has been fsynced.
3. After fence is durable, ordinary adds must fail. Once coverage is met, an old writer cannot gather Qa.
4. `CLOSED.lastEntryId` is written only by recover or graceful close, and only once.
5. Same `(ledgerId, entryId)` with different payloads is a conflict; same payload is idempotent.
6. MessageID `(ledgerId, entryId)` is never reused.
