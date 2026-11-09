# open-ledger

BookKeeper-style **ledger primary store**: Journal (WAL) + ledger storage. Addresses only `ledgerId + entryId` — no topics, subscriptions, or cursors.

[English](README.md) | [中文](README.zh-CN.md)

```text
Client (Go / Rust / Java / TS / in-repo client module)
        │  add / fence / read / recoveryAdd
        ▼
Bookie (openledgerd)      local entries, fenced bit, explicit LAC
Oxia                      LedgerMeta, registry, underreplicated queue
AutoRecovery sidecar      Auditor (leader) + Worker (rereplicate to local bookie)
```

A single bookie **does not know** the ensemble. When `E > Qw`, entry ids on one bookie may have gaps. The ledger tail lives only in metadata as `CLOSED.lastEntryId`.

## Quick start

```bash
# Units (semantics / journal / recover / in-process AR)
cargo test --workspace

# Single-node smoke: cookie + journal + one entry
cargo run -p openledgerd -- --data-dir /tmp/ol-data --bookie-id bookie-0 --smoke
cargo run -p openledgerd -- --data-dir /tmp/ol-data --bookie-id bookie-0 --ready
```

Local Colima k3s (8 bookies + Oxia + AR sidecar): [docs/deploy/local-k8s.md](docs/deploy/local-k8s.md).  
Fault matrix and scripts: [docs/deploy/failure-tests.md](docs/deploy/failure-tests.md):

```bash
./scripts/k8s-fault.sh          # Pod-kill durability
./scripts/k8s-traffic-fault.sh  # Continuous write + kill bookie (C06)
./scripts/k8s-recover-fault.sh  # E/Qw/Qa recover (R01–R06)
./scripts/k8s-netem-fault.sh    # delay / partition (C07 / C12)
./scripts/k8s-ar-e2e.sh         # Auditor → Worker recoveryAdd (M3)
```

## Repository

| Path | Role |
|---|---|
| `crates/open-ledger` | `Bookie`, `LocalBookie` / `MemoryBookie`, in-process quorum client, Oxia key layout |
| `crates/open-ledger-oxia` | `OxiaMetaStore`, registry, election, Auditor, ReplicationWorker |
| `crates/openledgerd` | Bookie process: local dir + gRPC `:3181` |
| `crates/openledger-autorecovery` | Colocated AutoRecovery sidecar |
| `proto/bookie/v1` | Bookie gRPC definition |
| `deploy/k8s` | Local k3s manifests |
| `scripts/` | Cluster fault and e2e scripts |

### Language clients

Same E/Qw/Qa, fence coverage, `RecoveryReadsFence`, and `PipelineWriter` semantics as the in-repo client:

| Language | Repository |
|---|---|
| Go | [open-ledger-client-go](https://github.com/devotionalist/open-ledger-client-go) |
| Rust | [open-ledger-client-rust](https://github.com/devotionalist/open-ledger-client-rust) |
| Java | [open-ledger-client-java](https://github.com/devotionalist/open-ledger-client-java) |
| TypeScript | [open-ledger-client-ts](https://github.com/devotionalist/open-ledger-client-ts) |

## Capabilities

| Area | Behavior |
|---|---|
| Single-replica API | `createLedger` / `addEntry` / `recoveryAdd` / `readEntry` / `fence` / LAC |
| Durability | Journal fsync before ack; append-only `entries.bin`; checkpoint replay |
| Quorum write | Round-robin write set; wait for Qa acks |
| Recover | Fence coverage `(Qw−Qa)+1`, then forward repair → CAS `CLOSED` |
| Metadata | Oxia in production; `MemoryMetaStore` for tests only |
| AutoRecovery | Lost bookie → underreplicated → local `recoveryAdd` + ensemble replace |

## Disk layout (`LocalBookie`)

```text
{data_dir}/
  bookie_id                      # cookie; must match --bookie-id
  journal/journal.log            # WAL; fsynced before add returns
  journal/checkpoint             # ledger storage covers [0, checkpoint)
  ledgers/{ledgerId}/meta.json
  ledgers/{ledgerId}/entries.bin # append-only entry log
```

## Documentation

English is the default. Chinese mirrors live under [`docs/zh-CN/`](docs/zh-CN/).

| Doc | Contents |
|---|---|
| [docs/architecture.md](docs/architecture.md) | Boundaries, repo map, invariants, reading guide |
| [docs/design.md](docs/design.md) | E/Qw/Qa, two state planes, Bookie API, recover, AR |
| [docs/development.md](docs/development.md) | Dev rules and test layers |
| [docs/deploy/local-k8s.md](docs/deploy/local-k8s.md) | Build images, import into k3s, deploy |
| [docs/deploy/failure-tests.md](docs/deploy/failure-tests.md) | Unit / k8s fault matrix and pass criteria |

## License

Apache-2.0
