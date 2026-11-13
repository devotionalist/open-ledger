# open-ledger 架构

[English](../architecture.md) | [中文](./architecture.md)


完整设计：[design.md](./design.md)。

open-ledger 是 Bookie：Journal + ledger storage，只认 `ledgerId + entryId`。

```text
Client
    add / fence / read / recoveryAdd
        │
        ▼
Bookie（本仓库）          entry 字节、fenced、explicit LAC
Oxia（LedgerMeta + AR）   E/Qw/Qa、fragment、CLOSED、offloadLocation?、registry、recovery 队列
AutoRecovery sidecar      Auditor（选主）+ ReplicationWorker（补到本机 bookie）
```

`ledgerId` 由 open-ledger Client 经 OL Metadata 发号。未挂链 orphan GC 见 [design.md §10](./design.md#10-orphan-ledger-gc)。

单个 bookie 不知道 ensemble。`E > Qw` 时本机 entryId 有空洞。账本尾部由 client 写入 Oxia metadata，不由本机最大 entryId 决定。

## 仓库

| 路径 | 角色 |
|---|---|
| `crates/open-ledger` | `Bookie`、`LocalBookie`、`MemoryBookie`，同进程 quorum client，Oxia key 布局 |
| `crates/open-ledger-oxia` | `OxiaMetaStore`、registry、election、Auditor、ReplicationWorker（[oxia-client](https://github.com/oxia-db/oxia-client-rust)） |
| `crates/openledgerd` | Bookie 进程：本地目录 + **Callback** gRPC `:3181`（`spawn_blocking` WorkPool） |
| 线程模型 | [async-threading.md](./async-threading.md) — 接收 → WorkPool → Finish |
| `crates/openledger-autorecovery` | 与 bookie 同 Pod 的 AutoRecovery sidecar |
| 外部客户端 | [open-ledger-client-go](https://github.com/devotionalist/open-ledger-client-go)、[open-ledger-client-rust](https://github.com/devotionalist/open-ledger-client-rust)、[open-ledger-client-java](https://github.com/devotionalist/open-ledger-client-java)、[open-ledger-client-ts](https://github.com/devotionalist/open-ledger-client-ts)（同 quorum / recover / PipelineWriter 语义） |

## 读哪一节

| 问题 | 章节 |
|---|---|
| Callback / 接收 vs WorkPool | [async-threading.md](./async-threading.md) |
| 谁存什么、client 放哪 | [design.md §1 边界](./design.md#1-边界) |
| E / Qw / Qa 与 write set | [§2](./design.md#2-复制参数) |
| metadata 与 bookie 本地状态 | [§3](./design.md#3-两套状态) |
| add、fence、recoveryAdd、幂等与冲突 | [§4](./design.md#4-bookie-api) |
| Journal、checkpoint、崩溃回放、磁盘布局 | [§5](./design.md#5-journal-与-checkpoint) |
| quorum 写、LAC、换 ensemble、close、recover | [§6](./design.md#6-client) |
| 宕机、脏尾、空洞、旧 writer | [§7](./design.md#7-故障) |
| M0–M3 | [§8](./design.md#8-阶段) |
| 开发规范、测试分层 | [development.md](./development.md) |
| AutoRecovery / Oxia 键 | [design.md §9](./design.md#9-autorecovery-m3) |
| 本地 k3s 部署 | [deploy/local-k8s.md](./deploy/local-k8s.md) |
| 故障测试矩阵与脚本 | [deploy/failure-tests.md](./deploy/failure-tests.md) |

## 不变量

1. OPEN ledger 至多一个 writer（由上层 ownership 保证）。
2. `addEntry` / `recoveryAdd` 成功 ⇒ 该副本 journal 已 fsync。
3. Fence 落盘后，普通 add 不能再成功。coverage 达成后，旧 writer 凑不齐 Qa。
4. CLOSED 的 `lastEntryId` 只由 recover 或优雅 close 写入 metadata，只定一次。
5. 同一 `(ledgerId, entryId)` 不同 payload 是冲突；相同 payload 幂等。
6. MessageID `(ledgerId, entryId)` 不复用。
