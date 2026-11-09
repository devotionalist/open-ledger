# open-ledger 设计

[English](../design.md) | [中文](./design.md)


入口摘要：[architecture.md](./architecture.md)。

本文写 **open-ledger**：一个 Bookie 怎么存 entry，以及 client 如何在多个 Bookie 上完成 quorum 写和尾部恢复。

## 1. 边界

```text
Client（本仓库 client 模块，或外部同语义库）
        │  Bookie API（进程内 / gRPC）
        ▼
Bookie（本仓库）     只存本机 entry 字节、fenced、explicit LAC
Metadata（外部）     E/Qw/Qa、fragment、CLOSED.lastEntryId、master key
```

| 在这里 | 不在这里 |
|---|---|
| Bookie：Journal + ledger storage | Topic、订阅、cursor、ownership |
| 单 bookie 的 add / read / fence / recoveryAdd | 把「本机最大 entryId」当成账本尾部 |
| Quorum 写、fence coverage、recover、优雅 close | Topic 层滚本策略 |
| AutoRecovery（Auditor + ReplicationWorker） | 其它系统的存储协议兼容层 |
| `MemoryBookie`（测试 / 嵌入） | |

本仓库 `client` 模块与 `LocalBookie` / `MemoryBookie` 同进程测试。`openledgerd` 在本地目录上提供 gRPC；`openledger-autorecovery` 以 sidecar 连 Oxia 与本机 bookie。外部 client 只要实现同一 `Bookie` 接口，算法不变。

## 2. 复制参数

约束：`E >= Qw >= Qa >= 1`。

| 符号 | 含义 |
|---|---|
| E | 一个 fragment 里的 bookie 数 |
| Qw | 每条 entry 写入的 bookie 数 |
| Qa | 确认前需要的 journal ack 数 |

Entry `i` 的 write set：当前 fragment 的 ensemble 上，从下标 `i % E` 起连续 `Qw` 个 bookie（环状）。因此 `E > Qw` 时，单个 bookie 上的 entryId **有空洞**。Bookie 不要求 `entryId == last + 1`。

Bookie 不知道 E/Qw/Qa，也不保存 ensemble。这些只在 metadata。

## 3. 两套状态

### 3.1 Metadata（权威，不在 Bookie 盘上）

```text
LedgerMeta {
  ledgerId
  E, Qw, Qa
  state: OPEN | IN_RECOVERY | CLOSED
  lastEntryId          // CLOSED 后钉死；空账本为 -1
  fragments[]: { firstEntryId, ensemble[] }
  masterKey
  metaVersion          // CAS
  offloadLocation?     // 可选冷层 URI；仅 CLOSED 有意义（M3 上传）
  createdAtMs?         // orphan GC 老化
  orphanMarked?        // 上层放弃未挂链 Create
}
```

第一个 fragment 的 `firstEntryId` 为 0。换 ensemble 只追加 fragment，`firstEntryId` 必须大于上一段，历史 fragment 不改。

**ledgerId 发号（本仓权威）**：Client 先从 OL Metadata（`/open-ledger/…` 下序列键 / snowflake）取唯一 id，再写 `LedgerMeta` OPEN。上游 Broker（如 open-message）**不得**另发冲突号。Create 宜写 `createdAtMs`；`MarkOrphan` 置 `orphanMarked`。

`MemoryMetaStore` 是测试用的进程内 CAS。生产走可插拔协调存储（`CoordStore` / `OxiaMetaStore`）：

| 后端 | 开关 | 版本 | Ephemeral | 列举 |
|---|---|---|---|---|
| **Oxia**（默认） | `COORD_BACKEND=oxia` | `version_id` | session ephemeral | 原生 `range_scan` |
| **OpenKV** | `COORD_BACKEND=openkv` | Gateway MVCC `revision` | `Put` + `ttl_ms`（心跳刷新） | DS Set 索引（`/open-ledger/idx/...`；Gateway 无 range RPC） |

`LedgerMeta.meta_version` ↔ 存储版本。键布局见 `open_ledger::keys`。OpenKV create-only = `PutCAS(expected_revision=0)`。

### 3.2 Bookie 本地

```text
LedgerLocal {
  masterKey
  fenced: bool
  explicitLac: i64     // 未设置为 -1
  entries: map<entryId, { lac, payload }>
}
```

Bookie 没有 OPEN / CLOSED。CLOSED 只存在于 metadata。Fence 之后普通 add 失败；`recoveryAdd` 仍可写，供读修。

`readLastConfirmed` = `max(explicitLac, 本机最高 entryId 那条 entry 上的 piggyback lac)`。没有 entry 且未设置 explicit LAC 时为 -1。这个值不是本机最高 entryId。

## 4. Bookie API

```text
createLedger(ledgerId, masterKey)
addEntry(ledgerId, entryId, lac, payload, masterKey)
recoveryAdd(ledgerId, entryId, lac, payload, masterKey)
readEntry(ledgerId, entryId)
readEntries(ledgerId, from, max)          // 本机已有的 entry，跳过空洞
listEntries(ledgerId)
fenceLedger(ledgerId, masterKey)
setExplicitLac(ledgerId, lac, masterKey)
readLastConfirmed(ledgerId) -> i64
lastEntryId(ledgerId) -> i64
deleteLedger(ledgerId)
```

规则：

1. `masterKey` 非空。已存在的账本，key 不一致则拒绝。
2. `createLedger` 对相同 key 幂等，且不会把已 fence 的账本重新打开。
3. `fenceLedger` 遇到本机没有的账本时，创建一份 **已经 fence 的空账本**，挡住旧 writer 事后补写。
4. 相同 `(ledgerId, entryId, payload)` 幂等。相同 entryId、不同 payload 是冲突。
5. `lac < -1` 拒绝。
6. `setExplicitLac` 在 fence 之后拒绝。
7. `addEntry` / `recoveryAdd` / `fenceLedger` 返回成功时，对应 journal 记录已经 `fsync`。

## 5. Journal 与 checkpoint

目录：

```text
{data_dir}/
  bookie_id                         # cookie，必须与进程 bookie id 一致
  journal/journal.log
  journal/checkpoint                # u64 小端，ledger storage 已覆盖 [0, checkpoint)
  ledgers/{ledgerId}/meta.json      # master_key, fenced, explicit_lac
  ledgers/{ledgerId}/entries.bin    # 本机 entry 顺序追加日志，允许空洞
```

Journal 记录（小端）：

```text
magic "OLJ1" | body_len u32 | body
body.tag:
  1 create        ledgerId, masterKey
  2 add           recovery 标志, ledgerId, entryId, lac, masterKey, payload
  3 fence         ledgerId, masterKey
  4 explicit LAC  ledgerId, lac, masterKey
  5 delete        ledgerId
```

单条 body 上限 32MiB。文件末尾不完整的记录视为撕裂，打开时截掉。完整头部上的 magic 错误则拒绝打开。checkpoint 必须落在记录边界上，且不能超过 journal 有效长度。

一次成功的变更按这个顺序落盘：

```text
prepare（内存校验）
→ journal 追加并 fsync
→ 应用到内存
→ 按记录类型更新 ledger 目录：
     create / fence / explicit LAC → 写 meta.json（必要时建空 entries.bin）
     add → 向 entries.bin 追加一条记录并 fsync（不重写整文件）
     delete → 删目录
→ 把 checkpoint 推进到 journal 末尾
```

因此正常返回时，ledger 目录已经追上 journal，重启不必再回放这条记录。若在 journal fsync 之后、checkpoint 之前崩溃，重启从旧 checkpoint 回放；add / fence / delete 的回放是幂等的。删掉 `ledgers/` 并把 checkpoint 置 0，只靠 journal 可以重建；打开时回放若需要，允许 **一次性** 按内存重写 `entries.bin`（整理），热路径禁止这么做。

当前实现是 **每条 journal 记录一次 fsync**，entry 追加也是每条 fsync。以后可以做 group commit，但 ack 之前 journal 必须已经 fsync，这条不能放宽。

`entries.bin` 以 `OLE2` 开头，后面顺序追加 `entryId u64 | lac i64 | len u32 | payload`。同一 `entryId` 可出现多次（例如只更新 lac），加载时后写覆盖先写。末尾撕裂截断。它不是第二份 WAL：权威耐久仍是 journal；`entries.bin` 是已 checkpoint 的本机 entry 存储，必须顺序写。

## 6. Client

### 6.1 写

单 writer。Metadata 必须是 OPEN。

```text
entryId = next++
并行发给 write set：addEntry(ledgerId, entryId, lac, payload, masterKey)
acks >= Qa  →  该 entry 可确认
否则若任一副本返回 fenced  →  ErrFenced
否则  →  NotEnoughBookies
```

发给 bookie 的 `lac` 是 **写这条之前** 已经确认的最大 entryId，首条为 -1。本条达到 Qa 之后，writer 的 LAC 才变成这条 entryId。

Pipeline 时，LAC 只沿连续前缀前进：entry 3 先达到 Qa，而 2 还没有，则 LAC 停在 1。对外 Receipt 按这个 LAC 释放。

`LedgerWriter` / `Writer` 是同步句柄：上一条达到 Qa 才发下一条，因此 LAC 每次加一。需要 pipeline 时直接调用 `append_entry`，并用 `advance_lac` 合并完成集合。

未达 Qa 的 entry 可能已经写进少数 bookie。用相同 payload 重试同一 entryId 是幂等的。

Pipeline 写走 `PipelineWriter`：`submit` 把 entry 记入 pending，`confirm` 后才从 pending 去掉。`change_ensemble` **必须**在 pending 为空时、以 `next` 为新 fragment 起点；否则拒绝（BK #4194 / #4924，避免已发往旧 write set 的 entry 被 metadata 挂到新 ensemble）。同步 `append` = submit+confirm，天然无此竞态。

### 6.2 换 ensemble

只在 OPEN 下追加 fragment。新 bookie 需要 `createLedger`（相同 key 幂等）。`change_ensemble` 只改 metadata；writer 上的 `ChangeEnsemble` 会顺便在新 ensemble 上 create。

Entry 落在 `firstEntryId <= entryId` 的最后一段 fragment 上。

### 6.3 优雅 close

Writer 停写之后，把 metadata CAS 成 `CLOSED(lastEntryId = writer LAC)`。不 fence bookie。已经 CLOSED 且 `lastEntryId` 相同则幂等；不同则冲突。`IN_RECOVERY` 不能走这条路径，要先 `recover`。

### 6.4 Recover

```text
已是 CLOSED → 返回已有 lastEntryId

CAS OPEN → IN_RECOVERY
对所有 fragment 里出现过的 bookie 发 fenceLedger
每个 fragment 都要满足 fence coverage，否则停在 IN_RECOVERY

向最后一个 fragment 的 ensemble 读 readLastConfirmed
  NotFound 视为 LAC = -1，算一次有效响应
  有效响应 >= Qa，取其中最大值 maxLac

从 entryId = maxLac + 1 向前（**fenced recovery read**，BK #2614 / RecoveryReadsFence）：
  对 write set 每个可达 bookie：先 fenceLedger，再 readEntry
  有一份完整 entry → recoveryAdd 到 write set，成功数 >= Qa，last = 该 id，继续
  write set 全部明确回答「没有」→ 停止（此时这些副本已 fenced，旧 writer 不能再把「没有」翻成成功 add）
  有 bookie 无响应，且没有任何一份 entry → NotEnoughBookies，不 CLOSED

CAS IN_RECOVERY → CLOSED(lastEntryId = last)
```

Fence coverage：每个 write set 窗口里至少 `(Qw - Qa) + 1` 个 bookie 已经 fence。旧 writer 在每个窗口里最多还能拿到 `Qa - 1` 个有效 ack。Coverage 允许少数副本仍未 fence；向前扫描的 fence-on-read 把这些未覆盖成员也钉死，避免「先答 NoSuchEntry、后被旧 writer 写入」的竞态。

`E = Qw = 3, Qa = 2` 时需要 2 个 fence。`Qa = Qw` 时每个窗口 1 个 fence 就够。`E = 5, Qw = 3, Qa = 2` 时可以缺 1 个 bookie 仍然盖住全部窗口。

向前扫描停在第一个写集上不存在的 entryId。后面即使还有 entry（例如缺了 1，但 5 在某个 bookie 上），也不跨过这个洞。洞后面的字节留在 bookie 上，metadata 的 `lastEntryId` 不包含它们。读者只认 CLOSED 的尾部。

读到的 entry 要补到至少 Qa 个副本才把它算进尾部。补不齐则保持 `IN_RECOVERY`。

并发 recover：CAS 一胜一败。败者看到 CLOSED 就采用对方的 `lastEntryId`。fence 和 recoveryAdd 幂等。

## 7. 故障

| 情况 | 结果 |
|---|---|
| Bookie 在 journal fsync 之后、checkpoint 之前崩溃 | 重启回放 journal，entry 还在 |
| Journal 末尾撕裂 | 截断后按完整记录打开 |
| cookie 与 `--bookie-id` 不一致 | 拒绝打开该目录 |
| 已确认 entry（达到过 Qa） | recover 能从仍存活的副本读到并补齐 |
| 只写到 1 个副本、未达 Qa 就崩溃 | 该 entry 若还能读到，会被读修进 CLOSED 尾部；读不到则截掉。调用方未拿到成功 |
| 旧 writer 在 coverage 达成后继续 add | 凑不齐 Qa，返回 fenced。未 fence 的那一个副本可能留下脏 entry |
| 中间缺 entry、更后面还有字节 | 扫描停在洞上，CLOSED 尾部不含洞后的 entry |
| recover 时 bookie 不够 | metadata 留在 `IN_RECOVERY`，bookie 回来后可重试 |
| 同一 entryId 两份不同 payload | 冲突，recover 失败，不把分叉收成 CLOSED |

已向调用方确认的 entry，在确认当时至少在 Qa 个 bookie 的 journal 里。Recover 之后，CLOSED 尾部里的每条 entry 至少被 recoveryAdd 到 Qa 个副本。

## 8. 阶段

| 阶段 | 状态 |
|---|---|
| M0 | 本文的 Bookie 语义、journal 回放、进程内 quorum client。已落地 |
| M1 | `openledgerd` 的 gRPC（已落地）；k8s bookie STS |
| M2 | 多进程 ensemble，对换 fragment 做故障注入 |
| M3 | Oxia metadata + AutoRecovery（Auditor / ReplicationWorker sidecar）。集群 gRPC e2e 已覆盖 |

## 9. AutoRecovery（M3）

与 BookKeeper 同构，协调状态全部在 Oxia：

| 组件 | 行为 |
|---|---|
| Bookie registry | 每个 sidecar 写 ephemeral `/open-ledger/bookies/{id}` |
| Auditor election | ephemeral `/open-ledger/election/auditor`；仅 leader 扫丢 bookie |
| Under-replicated queue | `/open-ledger/underreplicated/{ledgerId}`；`lostBookieRecoveryDelay` 后再处理 |
| ReplicationWorker | 抢 ephemeral lock，从仍存活的 ensemble 成员 `readEntry`，对本机 bookie `recoveryAdd`，CAS 把 missing 换成 local id |
| OPEN 尾段 | 先等 grace，再 fence 再复制；不改 entryId / payload |
| 背压 | `max_tasks_per_tick` 限制每轮任务数；`entry_interval` 节流 `recoveryAdd`；`max_copy_bytes_per_tick` 限制每轮复制字节（预算耗尽则留下 task 下轮继续，防 #3408 类风暴） |

Worker **只补本机 bookie**（同 Pod）。k8s 默认：Oxia STS×1 + bookie STS×8（每 Pod `openledgerd` + `openledger-autorecovery`）。可选 OpenKV：`kubectl apply -k deploy/k8s/overlays/openkv`（`COORD_BACKEND=openkv`，Gateway `:19000`）。

M0 允许调用方使用 `E = Qw = Qa = 1`，API 和 metadata 仍然携带 E/Qw/Qa。

## 11. 可观测性与 Web 管理

### 11.1 Bookie（`openledgerd` `:3182`）

在 gRPC `:3181` 之外：

| 路径 | 用途 |
|---|---|
| `GET /` | 单 bookie 状态页（id、journal 大小、ledger 表） |
| `GET /api/v1/status` | 同上，JSON |
| `GET /metrics` | Prometheus 文本 |
| `GET /ready` / `GET /live` | 探针 |

| 指标 | 类型 | Labels |
|---|---|---|
| `openledger_bookie_rpc_total` | counter | `method`, `result` |
| `openledger_bookie_rpc_errors_total` | counter | `method`, `error_kind` |
| `openledger_bookie_rpc_duration_seconds` | histogram | `method` |
| `openledger_bookie_rpc_bytes_total` | counter | `method`, `direction`（`in`/`out`） |
| `openledger_bookie_journal_bytes` | gauge | — |
| `openledger_bookie_ledgers` | gauge | — |
| `openledger_bookie_fenced_ledgers` | gauge | — |

gRPC 打 `bookie.rpc` span（`rpc` / `ledger_id` / `entry_id`）；失败日志带 `error_kind`。

### 11.2 AutoRecovery（`openledger-autorecovery` `:3183`）

| 路径 | 用途 |
|---|---|
| `GET /` | sidecar 状态 |
| `GET /metrics` | Prometheus |
| `GET /ready` `/live` | 探针 |

系列前缀 `openledger_ar_*`（Auditor 领导、心跳、队列、复制、CAS、budget、耗时）。

### 11.3 集群管理（`openledger-admin` `:8080`）

独立进程，定期扫 Oxia：`/` 集群页、`/api/v1/cluster`、协调面 gauges（alive bookies / ledger metas / underreplicated / waiting）。

### 11.4 日志与 tracing

- `RUST_LOG`；k8s 默认 `RUST_LOG_FORMAT=json`。
- 字段：`bookie_id`、`ledger_id`、`entry_id`、`method`/`rpc`、`error_kind`。
- span 目前是 `tracing` 原生（可再挂 OTLP）；采样建议写路径约 1%、错误全采。

### 11.5 部署 scrape / 告警

- Service annotation 便于注解式 scrape。
- 可选 Prometheus Operator：`deploy/k8s/monitoring`（`ServiceMonitor`、`PrometheusRule`、Grafana dashboard ConfigMap）。
- 初版告警：bookie scrape 失败、RPC 错误率/p99、alive bookies 过低、UR 队列积压且 AR 无进展。
