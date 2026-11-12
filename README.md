# open-ledger

Rust **primary store** for [open-message](../open-message): BookKeeper-style ledgers with **Journal (WAL) + ledger storage**.

Broker / Metadata / subscriptions stay in open-message. This repo is only the **Bookie / LedgerStore** role — no topic concept, only `ledgerId + entryId`.

## Design alignment

| open-message | open-ledger |
|---|---|
| `LedgerStore` API | `open_ledger::LedgerStore` trait |
| Journal before Receipt | `Append` fsyncs Journal before `Ok` |
| E / Qw / Qa | `Quorum { e, qw, qa }` |
| Fence + Recover | `fence` / `recover` |
| Metadata holds LedgerMeta/Index | **Not** stored here (cluster meta is open-message / OpenKV) |

Canonical design: `open-message/docs/design/01-ledger.md`, `17-storage-vs-bookkeeper.md`.

## Crates

| Crate | Role |
|---|---|
| `open-ledger` | Library: types, `MemoryBookie`, `LocalBookie` |
| `openledgerd` | Bookie process (M0: local dir; RPC later) |

## Quick start

```bash
cargo test -p open-ledger
cargo run -p openledgerd -- --data-dir /tmp/ol-data --bookie-id bookie-0 --smoke
```

## Layout (LocalBookie)

```text
{data_dir}/
  bookie_id
  journal/journal.log
  ledgers/{ledgerId}/meta.json
  ledgers/{ledgerId}/entries.bin
```

## Roadmap

| Milestone | Scope |
|---|---|
| **M0** | Trait + Memory + Local Journal fsync + unit tests |
| **M1** | gRPC Bookie service; open-message Go/Rust client adapter |
| **M2** | True Qw/Qa multi-bookie; fragment change |
| **M3** | AutoRecovery hooks / Auditor worker |

## License

Apache-2.0
