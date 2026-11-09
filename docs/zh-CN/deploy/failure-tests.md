# 故障测试

[English](../../deploy/failure-tests.md) | [中文](./failure-tests.md)


对齐 [design.md §7](../design.md#7-故障)。覆盖 journal 耐久、E/Qw/Qa recover、以及 k8s 进程故障下的持续写。

## 0. 快速执行

```bash
# 主机：语义与 journal
cargo test --workspace

# 已部署的 Colima k3s（namespace open-ledger）
./scripts/k8s-fault.sh

# Go client 持续读写 + 杀 bookie（C06）
./scripts/k8s-traffic-fault.sh

# Go client E/Qw/Qa recover 生产矩阵（R01–R06 + 既有 integration）
./scripts/k8s-recover-fault.sh

# 集群 netem / iptables（C07 delay + C12 partition）
./scripts/k8s-netem-fault.sh

# 集群 gRPC AutoRecovery（M3：Auditor enqueue → Worker recoveryAdd）
./scripts/k8s-ar-e2e.sh
```

前置：按 [local-k8s.md](./local-k8s.md) 把 `oxia-0` 与 `bookie-0`…`bookie-2`（含 sidecar）跑到 Ready。
Go 客户端仓库：`open-ledger-client-go`（可用 `GO_CLIENT=` 覆盖路径）。
默认 **8 bookie**，主 quorum `E=8,Qw=3,Qa=2`（可用 `BOOKIE_COUNT=` 覆盖）。

## 1. 单元矩阵（主机）

命令：`cargo test --workspace`。

| ID | 用例 | 对应故障 | 期望 |
|---|---|---|---|
| U01 | `local::reopen_and_replay_from_journal_alone` | fsync 后、checkpoint 前崩溃；wipe ledgers | 回放后 entry / fence / LAC 仍在 |
| U02 | `local::torn_journal_tail_is_truncated` | journal 末尾撕裂 | 截断后可继续写 |
| U03 | `local::cookie_mismatch` | 目录被另一个 bookie-id 打开 | `InvalidArgument` |
| U04 | `local::delete_survives_replay` | 删除后仅留 journal | 回放后 ledger 不存在 |
| U05 | `memory::gaps` / striping | `E > Qw` 本机空洞 | 列表可跳洞；读缺 entry → `NoSuchEntry` |
| U06 | `memory::fence_unknown_ledger_blocks_later_add` | 先 fence 未知 ledger | 之后 create 仍 fenced，普通 add 失败 |
| U07 | `client::confirmed_entries_survive_recovery` | 已达 Qa 后 recover / 二次 recover | CLOSED 尾部不变；全体 fenced |
| U08 | `client::dirty_tail_on_one_bookie_is_repaired` | 未达 Qa 的脏尾仍可读 | recoveryAdd 补齐到全体，CLOSED 含该 id |
| U09 | `client::hole_stops_the_scan_even_if_a_later_entry_exists` | 中间缺 entry | 扫描停洞上；洞后字节仍留在盘上 |
| U10 | `client::fenced_quorum_blocks_old_writer_from_qa` | fence coverage 后旧 writer | `Fenced`；未 fence 副本可能脏 |
| U11 | `client::missing_bookies_leave_recovery_open` | recover 时 bookie 不够 | metadata 留 `IN_RECOVERY`；回来后可 CLOSED |
| U12 | `distribution::fence_coverage_qw3_qa2` | coverage 计数 | `E=Qw=3,Qa=2` 需 2 个 fence |
| U13 | `client::recovery_reads_fence_uncovered_bookies` | BK #2614：coverage 后未 fence 副本 | recover 扫描后该副本已 fenced；晚到 add → `Fenced` |
| U14 | `writer::ensemble_change_blocked_while_pending` | BK #4924：pending add 时换 ensemble | `InvalidArgument`；drain 后换 ensemble 成功 |
| U15 | `client::slow_bookie_add_still_reaches_qa` | C07 慢盘 / 延迟注入 | append 成功；耗时体现 delay |
| U16 | `client::partitioned_bookie_still_reaches_qa` | C12 分区掉 1 个 bookie | Qa=2 仍确认；缺席副本无该 entry |
| U17 | `client::ensemble_change_then_drop_old_member_recovers` | M2 换 fragment 后丢旧成员 | 继续写 + recover → CLOSED 可读 |
| U18 | `rereplicate::auditor_style_rereplicate_replaces_missing_bookie` | M3 Auditor→Worker 进程内 e2e | missing 换成 local；entry 已 recoveryAdd |
| U19 | `rereplicate::byte_budget_stops_mid_copy` | AR #3408 字节预算 | 预算耗尽不 CAS；下轮可续 |

**判定：** 退出码 0。

## 2. 进程 smoke（主机）

```bash
rm -rf /tmp/ol-smoke && mkdir -p /tmp/ol-smoke
cargo run -p openledgerd -- --data-dir /tmp/ol-smoke --bookie-id bookie-0 --smoke
test -f /tmp/ol-smoke/bookie_id
cargo run -p openledgerd -- --data-dir /tmp/ol-smoke --bookie-id bookie-0 --ready
```

| ID | 步骤 | 期望 |
|---|---|---|
| S01 | `--smoke` | 退出 0；写出 cookie、journal、ledger 1 |
| S02 | 同目录再 `--ready` | 退出 0 |
| S03 | 换 `--bookie-id bookie-1` 打开同目录（非 `--ready`） | 失败 |

## 3. K8s 进程故障

脚本：[`scripts/k8s-fault.sh`](../../scripts/k8s-fault.sh)。命名空间默认 `open-ledger`。

| ID | 注入 | 验证 | 期望 |
|---|---|---|---|
| F01 | 无故障 | Oxia + 三 bookie Pod Ready（各 2/2）；`--ready` | baseline 过 |
| F02 | `kubectl delete pod bookie-0` | STS 拉起后 Ready；`--ready`；节点 PVC 上 cookie=`bookie-0`、`journal/journal.log` 仍在 | 进程级耐久 |
| F03 | `kubectl delete pod bookie-1` | 同 F02，id=`bookie-1` | 同上 |
| F04 | 连续删两个 bookie Pod | 都回到 2/2 Ready | 无卡死 |
| F05 | 读节点上 PVC 目录 | `bookie_id` 与 ordinal 一致 | 身份钉在盘上，不跟临时 Pod UID |

## 4. K8s 持续写（C06）

脚本：[`scripts/k8s-traffic-fault.sh`](../../scripts/k8s-traffic-fault.sh)。

| ID | 场景 | 期望 |
|---|---|---|
| C06 | Go traffic `E=Qw=3,Qa=2` 持续 append/read，期间删 `bookie-0`/`bookie-1` | `writes_ok` 持续上涨；traffic 退出 0；Pod Ready |

## 5. K8s E/Qw/Qa recover（生产矩阵）

脚本：[`scripts/k8s-recover-fault.sh`](../../scripts/k8s-recover-fault.sh)  
实现：`open-ledger-client-go/integration`（`-tags=integration`，默认 8 bookie）。

| ID | 用例 | Quorum | 注入 | 期望 |
|---|---|---|---|---|
| R01 | `TestR01_RecoverConfirmedTriple` | `E=N,Qw=3,Qa=2` | 无 | recover + 二次 recover；CLOSED；全员 fenced；条目可读 |
| R02 | `TestR02_RecoverAfterBookiePodKill` | `E=N,Qw=3,Qa=2` | 写完删 `bookie-0`，Ready 后 recover | `lastEntryId` 不变；全量可读 |
| R03 | `TestR03_WriteDuringOutageThenRecover` | `E=N,Qw=3,Qa=2` | 排除 1 个成员继续写（Qa=2）；再删 Pod；recover | 扩展尾部 CLOSED 且可读 |
| R04 | `TestR04_RecoverBlockedUntilEnsembleReturns` | dense `3/3/2` | recover 时缺 1 个成员 | `NotEnoughBookies` + `IN_RECOVERY`；全集回来后 CLOSED |
| R05 | `TestR05_DirtyTailRepairTriple` | dense `3/3/2` | 单副本脏尾 | recover 后 write set 都有脏尾 payload |
| R06 | `TestR06_StripeRecoverAfterKill` | `E=N,Qw=3,Qa=2` | striping 写完删最后一个 bookie，Ready 后 recover | 全 entry 可读 |
| — | `TestStripeWriteAndRecover` 等 | `E=N,Qw=3,Qa=2` | 无故障基线 | striping 落盘非全集；recover 密封 |

另有同包基线：优雅 close、旧 writer 被 fence、并发 recover、洞截断、`Qa=1` 双副本脏尾。

**判定：** `go test` 退出 0。

## 6. K8s netem / 分区（C07 / C12）

脚本：[`scripts/k8s-netem-fault.sh`](../../scripts/k8s-netem-fault.sh)  
注入：Colima 上 `crictl` + `nsenter` 进 bookie netns（`tc netem` / `iptables eth0 DROP`）。

| ID | 注入 | 期望 |
|---|---|---|
| C07 | `tc netem delay` 打在 `bookie-0`，Go traffic 持续跑 | `writes_ok` 在 delay 下仍上涨；traffic 退出 0 |
| C12 | `iptables` DROP `eth0` 隔离 `bookie-0` | Qa=2 下写继续；heal 后 Pod Ready |

单元对照：U15 / U16。

## 7. K8s gRPC AutoRecovery（M3）

脚本：[`scripts/k8s-ar-e2e.sh`](../../scripts/k8s-ar-e2e.sh)  
辅助：`open-ledger-client-go/cmd/ar-e2e`（seed CLOSED entries + verify recoveryAdd）。

| ID | 步骤 | 期望 |
|---|---|---|
| M3 | Oxia 写入 CLOSED `LedgerMeta`；gRPC 落盘；分区丢失 bookie + 删 registry；等 `lostBookieRecoveryDelay` | underreplicated 入队；ensemble 换掉 missing；新成员可读全部 entry；task 删除 |

单元对照：U18。

## 8. 已升格（单元已覆盖）

| ID | 场景 | 状态 |
|---|---|---|
| M2 | 换 fragment 后丢旧 ensemble 成员 | ✅ 单元 U17 |

## 9. 通过标准

1. **Safety**：journal fsync 过的数据，杀 Pod 后仍能被同一 cookie 打开（F02/F03 + U01）。
2. **Identity**：PVC 上 `bookie_id` 不跟临时 Pod UID 漂移（U03 + F05）。
3. **Quorum recover**：已确认 entry recover 后可读；缺成员时 `IN_RECOVERY`；coverage 后旧 writer 凑不齐 Qa；洞不跨过；RecoveryReadsFence；pending ensemble 对账；慢盘/分区/换 ensemble（U07–U19 + R01–R06 + C07/C12）。
4. **Liveness**：故障注入后 STS 超时内 Ready；C06 写不中断；M3 AR 能换 ensemble 并 recoveryAdd。

## 10. 结果记录

| 批次 | 日期 | 集群 | Unit | Smoke | K8s Fault | 备注 |
|---|---|---|---|---|---|---|
| 20261109-133235 | 2026-11-09 | colima / k3s v1.35.0+k3s1 | ✅ 19 | — | ✅ F01–F05 | 进程级耐久 |
| 20261109-152410 | 2026-11-09 | colima / k3s | — | — | ✅ C06 | Go traffic 3076 ok；杀 bookie-0/1 写不中断 |
| 20261109-153000 | 2026-11-09 | colima / k3s | — | — | ✅ R01–R06 + integration | 17 tests / ~109s；含 pod-kill recover |
| 20261109-153900 | 2026-11-09 | colima / k3s ×8 | — | — | ✅ R01–R06 @ E=8/Qw=3/Qa=2 | 17 tests / ~115s |
| 20261109-154050 | 2026-11-09 | colima / k3s ×8 | — | — | ✅ C06 @ E=8/Qw=3/Qa=2 | traffic 2964 ok；杀 bookie-0/7 写不中断 |
| 20261109-162845 | 2026-11-09 | colima / k3s ×8 | — | — | ✅ C07+C12 netem | delay 80ms + eth0 DROP；writes 不中断 |
| 20261109-163815 | 2026-11-09 | colima / k3s ×8 | — | — | ✅ M3 gRPC AR | bookie-0→bookie-4；5 entries recoveryAdd |
