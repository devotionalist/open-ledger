# Fault tests

[English](./failure-tests.md) | [中文](../zh-CN/deploy/failure-tests.md)

Aligned with [design.md §7](../design.md#7-failures). Covers journal durability, E/Qw/Qa recover, and continuous writes under k8s process faults.

## 0. Quick run

```bash
# Host: semantics and journal
cargo test --workspace

# Deployed Colima k3s (namespace open-ledger)
./scripts/k8s-fault.sh

# Go client continuous R/W + kill bookie (C06)
./scripts/k8s-traffic-fault.sh

# Go client E/Qw/Qa recover matrix (R01–R06 + existing integration)
./scripts/k8s-recover-fault.sh

# Cluster netem / iptables (C07 delay + C12 partition)
./scripts/k8s-netem-fault.sh

# Cluster gRPC AutoRecovery (M3: Auditor enqueue → Worker recoveryAdd)
./scripts/k8s-ar-e2e.sh
```

Prereq: follow [local-k8s.md](./local-k8s.md) until `oxia-0` and bookie Pods (with sidecars) are Ready.  
Go client repo: `open-ledger-client-go` (override with `GO_CLIENT=`).  
Default **8 bookies**, primary quorum `E=8,Qw=3,Qa=2` (override with `BOOKIE_COUNT=`).

## 1. Unit matrix (host)

Command: `cargo test --workspace`.

| ID | Test | Fault | Expect |
|---|---|---|---|
| U01 | `local::reopen_and_replay_from_journal_alone` | Crash after fsync, before checkpoint; wipe ledgers | After replay, entry / fence / LAC remain |
| U02 | `local::torn_journal_tail_is_truncated` | Torn journal tail | Truncate, then continue writing |
| U03 | `local::cookie_mismatch` | Directory opened with another bookie-id | `InvalidArgument` |
| U04 | `local::delete_survives_replay` | Delete then only journal left | After replay, ledger absent |
| U05 | `memory::gaps` / striping | `E > Qw` local gaps | List skips holes; missing entry → `NoSuchEntry` |
| U06 | `memory::fence_unknown_ledger_blocks_later_add` | Fence unknown ledger first | Later create stays fenced; ordinary add fails |
| U07 | `client::confirmed_entries_survive_recovery` | Recover / second recover after Qa | CLOSED tail unchanged; all fenced |
| U08 | `client::dirty_tail_on_one_bookie_is_repaired` | Dirty tail below Qa still readable | recoveryAdd fills write set; CLOSED includes id |
| U09 | `client::hole_stops_the_scan_even_if_a_later_entry_exists` | Missing middle entry | Scan stops at hole; later bytes remain on disk |
| U10 | `client::fenced_quorum_blocks_old_writer_from_qa` | Old writer after fence coverage | `Fenced`; unfenced replica may be dirty |
| U11 | `client::missing_bookies_leave_recovery_open` | Not enough bookies during recover | Metadata stays `IN_RECOVERY`; CLOSED after return |
| U12 | `distribution::fence_coverage_qw3_qa2` | Coverage count | `E=Qw=3,Qa=2` needs 2 fences |
| U13 | `client::recovery_reads_fence_uncovered_bookies` | BK #2614: unfenced after coverage | After recover scan, replica fenced; late add → `Fenced` |
| U14 | `writer::ensemble_change_blocked_while_pending` | BK #4924: ensemble change while pending | `InvalidArgument`; succeeds after drain |
| U15 | `client::slow_bookie_add_still_reaches_qa` | C07 slow disk / delay | Append succeeds; latency shows delay |
| U16 | `client::partitioned_bookie_still_reaches_qa` | C12 drop one bookie | Qa=2 still confirms; absent replica has no entry |
| U17 | `client::ensemble_change_then_drop_old_member_recovers` | M2 drop old member after fragment change | Continue write + recover → CLOSED readable |
| U18 | `rereplicate::auditor_style_rereplicate_replaces_missing_bookie` | M3 Auditor→Worker in-process | Missing replaced by local; entry recoveryAdd’d |
| U19 | `rereplicate::byte_budget_stops_mid_copy` | AR #3408 byte budget | Budget exhaust → no CAS; next tick resumes |

**Pass:** exit code 0.

## 2. Process smoke (host)

```bash
rm -rf /tmp/ol-smoke && mkdir -p /tmp/ol-smoke
cargo run -p openledgerd -- --data-dir /tmp/ol-smoke --bookie-id bookie-0 --smoke
test -f /tmp/ol-smoke/bookie_id
cargo run -p openledgerd -- --data-dir /tmp/ol-smoke --bookie-id bookie-0 --ready
```

| ID | Step | Expect |
|---|---|---|
| S01 | `--smoke` | Exit 0; cookie, journal, ledger 1 written |
| S02 | `--ready` on same dir | Exit 0 |
| S03 | Open same dir with `--bookie-id bookie-1` (not `--ready`) | Fail |

## 3. K8s process faults

Script: [`scripts/k8s-fault.sh`](../../scripts/k8s-fault.sh). Namespace default `open-ledger`.

| ID | Inject | Verify | Expect |
|---|---|---|---|
| F01 | No fault | Oxia + bookie Pods Ready (2/2); `--ready` | Baseline passes |
| F02 | `kubectl delete pod bookie-0` | STS Ready; `--ready`; PVC cookie=`bookie-0`, `journal/journal.log` present | Process durability |
| F03 | `kubectl delete pod bookie-1` | Same as F02, id=`bookie-1` | Same |
| F04 | Delete two bookie Pods in a row | Both back to 2/2 Ready | No stuck rollout |
| F05 | Read PVC dir on node | `bookie_id` matches ordinal | Identity pinned to disk, not Pod UID |

## 4. K8s continuous write (C06)

Script: [`scripts/k8s-traffic-fault.sh`](../../scripts/k8s-traffic-fault.sh).

| ID | Scenario | Expect |
|---|---|---|
| C06 | Go traffic `E=Qw=3,Qa=2` continuous append/read while deleting bookies | `writes_ok` keeps rising; traffic exit 0; Pods Ready |

## 5. K8s E/Qw/Qa recover (production matrix)

Script: [`scripts/k8s-recover-fault.sh`](../../scripts/k8s-recover-fault.sh)  
Impl: `open-ledger-client-go/integration` (`-tags=integration`, default 8 bookies).

| ID | Test | Quorum | Inject | Expect |
|---|---|---|---|---|
| R01 | `TestR01_RecoverConfirmedTriple` | `E=N,Qw=3,Qa=2` | None | Recover + second recover; CLOSED; all fenced; readable |
| R02 | `TestR02_RecoverAfterBookiePodKill` | `E=N,Qw=3,Qa=2` | Kill `bookie-0` after write; recover when Ready | `lastEntryId` unchanged; fully readable |
| R03 | `TestR03_WriteDuringOutageThenRecover` | `E=N,Qw=3,Qa=2` | Write without one member (Qa=2); kill Pod; recover | Extended tail CLOSED and readable |
| R04 | `TestR04_RecoverBlockedUntilEnsembleReturns` | dense `3/3/2` | Recover missing one member | `NotEnoughBookies` + `IN_RECOVERY`; CLOSED when full |
| R05 | `TestR05_DirtyTailRepairTriple` | dense `3/3/2` | Dirty tail on one replica | After recover, write set has dirty payload |
| R06 | `TestR06_StripeRecoverAfterKill` | `E=N,Qw=3,Qa=2` | Kill last bookie after stripe write; recover | All entries readable |
| — | `TestStripeWriteAndRecover` etc. | `E=N,Qw=3,Qa=2` | Baseline | Striping not full-set; recover seals |

Same package also covers graceful close, old writer fenced, concurrent recover, hole truncation, `Qa=1` dual dirty tail.

**Pass:** `go test` exit 0.

## 6. K8s netem / partition (C07 / C12)

Script: [`scripts/k8s-netem-fault.sh`](../../scripts/k8s-netem-fault.sh)  
Inject via Colima `crictl` + `nsenter` into the bookie netns (`tc netem` / `iptables eth0 DROP`).

| ID | Inject | Expect |
|---|---|---|
| C07 | `tc netem delay` on `bookie-0` while Go traffic runs | `writes_ok` still rises; traffic exit 0 |
| C12 | `iptables` DROP `eth0` isolating `bookie-0` | Writes continue at Qa=2; Pod Ready after heal |

Unit counterparts: U15 / U16.

## 7. K8s gRPC AutoRecovery (M3)

Script: [`scripts/k8s-ar-e2e.sh`](../../scripts/k8s-ar-e2e.sh)  
Helper: `open-ledger-client-go/cmd/ar-e2e` (seed CLOSED entries + verify recoveryAdd).

| ID | Steps | Expect |
|---|---|---|
| M3 | Put CLOSED `LedgerMeta` in Oxia; gRPC write; partition lost bookie + delete registry; wait `lostBookieRecoveryDelay` | underreplicated enqueued; ensemble replaces missing; new member has all entries; task deleted |

Unit counterpart: U18.

## 8. Promoted (unit-covered)

| ID | Scenario | Status |
|---|---|---|
| M2 | Drop old ensemble member after fragment change | ✅ unit U17 |

## 9. Pass criteria

1. **Safety**: journal-fsynced data survives Pod kill under the same cookie (F02/F03 + U01).
2. **Identity**: PVC `bookie_id` does not drift with Pod UID (U03 + F05).
3. **Quorum recover**: confirmed entries readable after recover; missing members leave `IN_RECOVERY`; after coverage old writer cannot reach Qa; holes are not crossed; RecoveryReadsFence; pending ensemble accounting; slow/partition/ensemble-change (U07–U19 + R01–R06 + C07/C12).
4. **Liveness**: STS Ready within timeout after inject; C06 writes uninterrupted; M3 AR replaces ensemble and recoveryAdds.

## 10. Results log

| Batch | Date | Cluster | Unit | Smoke | K8s Fault | Notes |
|---|---|---|---|---|---|---|
| 20261109-133235 | 2026-11-09 | colima / k3s v1.35.0+k3s1 | ✅ 19 | — | ✅ F01–F05 | Process durability |
| 20261109-152410 | 2026-11-09 | colima / k3s | — | — | ✅ C06 | Go traffic 3076 ok; kill bookie-0/1 uninterrupted |
| 20261109-153000 | 2026-11-09 | colima / k3s | — | — | ✅ R01–R06 + integration | 17 tests / ~109s; pod-kill recover |
| 20261109-153900 | 2026-11-09 | colima / k3s ×8 | — | — | ✅ R01–R06 @ E=8/Qw=3/Qa=2 | 17 tests / ~115s |
| 20261109-154050 | 2026-11-09 | colima / k3s ×8 | — | — | ✅ C06 @ E=8/Qw=3/Qa=2 | traffic 2964 ok; kill bookie-0/7 uninterrupted |
| 20261109-162845 | 2026-11-09 | colima / k3s ×8 | — | — | ✅ C07+C12 netem | delay 80ms + eth0 DROP; writes continue |
| 20261109-163815 | 2026-11-09 | colima / k3s ×8 | — | — | ✅ M3 gRPC AR | bookie-0→bookie-4; 5 entries recoveryAdd |
