# Development

[English](./development.md) | [中文](./zh-CN/development.md)

Design semantics are defined by [design.md](./design.md). Docs live only under this repo’s `docs/` (English default; Chinese under `docs/zh-CN/`).

## 1. Boundaries

| In scope | Out of scope for this repo |
|---|---|
| Bookie-local state, Journal, checkpoint, disk layout | Topics / subscriptions / ownership |
| In-process quorum client (`append_entry` / `recover_ledger`) | Production Metadata service implementation |
| `openledgerd` process and deploy manifests | Compatibility layers for other storage protocols |

A single bookie **does not know** the ensemble. `E/Qw/Qa`, fragments, `CLOSED.lastEntryId`, and the master key are authoritative in Metadata. Do not treat “highest local entry id” as the ledger tail.

Payloads use `bytes::Bytes`. In-process client tests against `LocalBookie` must share the same semantics.

## 2. Durability and errors

1. Before `addEntry` / `recoveryAdd` / `fence` returns Ok, that replica’s journal **must be fsynced**. Future group commit must not weaken this.
2. Same `(ledgerId, entryId)`: same payload → idempotent; different payload → `Conflict`.
3. After fence, ordinary add fails; `recoveryAdd` is still allowed.
4. Cookie (`bookie_id`) mismatch with `--bookie-id` → refuse to open.
5. MessageID `(ledgerId, entryId)` is never reused.
6. Use existing `Error` variants; never swallow `Fenced` / `NotEnoughBookies` / `Conflict`.

## 3. Style

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

- Public APIs keep `#![deny(missing_docs)]`.
- `OpenOptions::create(true)` must set `.truncate(false)` explicitly (or document intentional truncate).
- Prefer `match` on error variants over `unwrap_err()` that depends on `Debug`.
- Do not `git init` / commit / push unless the user asks.

## 4. Disk layout

`LocalBookie` puts journal and ledgers under one `--data-dir`. In k8s one PVC mounts `/data`. Splitting journal/ledgers into two mounts requires code and design changes first.

```text
{data_dir}/
  bookie_id
  journal/journal.log
  journal/checkpoint
  ledgers/{ledgerId}/meta.json
  ledgers/{ledgerId}/entries.bin   # sequential append; never rewrite whole file on hot path
```

## 5. Local process and images

```bash
cargo run -p openledgerd -- --data-dir /tmp/ol-data --bookie-id bookie-0 --smoke
```

`--ready` only checks the cookie (for probes); it does not open the journal.

Building Linux images on macOS: static musl + `scratch`, link with `rust-lld`. Steps in [deploy/local-k8s.md](./deploy/local-k8s.md).

## 6. Test layers

| Layer | Entry | Coverage |
|---|---|---|
| Unit | `cargo test --workspace` | Journal, fence, gaps, recover, striping |
| Process smoke | `openledgerd --smoke` | Open dir, write one entry, exit |
| K8s process faults | [deploy/failure-tests.md](./deploy/failure-tests.md) | Kill Pod, PVC retained, cookie still matches |
| Ensemble-change faults | M2 | Covered by unit U17 + cluster scripts |

Every failure mode in design §7 must appear in a unit test or the fault matrix. Change tests before changing semantics.

## 7. Milestone gates

| Milestone | Merge criteria |
|---|---|
| M0 | Units green; clippy `-D warnings`; design matches code |
| M1 | Callback gRPC against multiple `openledgerd` ([async-threading.md](./async-threading.md)); kill 1 bookie, Qa writes still succeed (Qa≥2) |
| M2 | Ensemble-change fault scenarios green |
| M3 | AutoRecovery does not alter entryId/payload; OPEN-tail rereplicate does not bypass fence |
