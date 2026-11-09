# open-ledger

BookKeeper 风格的 **ledger 主存**：Journal（WAL）+ ledger storage。只认 `ledgerId + entryId`，不做 topic / 订阅 / cursor。

[English](README.md) | [中文](README.zh-CN.md)

```text
Client（Go / Rust / Java / TS / 本仓库 client 模块）
        │  add / fence / read / recoveryAdd
        ▼
Bookie（openledgerd）     本机 entry、fenced、explicit LAC
Oxia                      LedgerMeta、registry、underreplicated 队列
AutoRecovery sidecar      Auditor 选主 + Worker 对本机补副本
```

单个 bookie **不知道** ensemble。`E > Qw` 时本机 entryId 可以有空洞；账本尾部只在 metadata 的 `CLOSED.lastEntryId`。

## Quick start

```bash
# 单元（语义 / journal / recover / AR 进程内）
cargo test --workspace

# 单机 smoke：写 cookie + journal + 一条 entry
cargo run -p openledgerd -- --data-dir /tmp/ol-data --bookie-id bookie-0 --smoke
cargo run -p openledgerd -- --data-dir /tmp/ol-data --bookie-id bookie-0 --ready
```

本地 Colima k3s（8 bookie + Oxia + AR sidecar）见 [docs/zh-CN/deploy/local-k8s.md](docs/zh-CN/deploy/local-k8s.md)。  
故障矩阵与脚本见 [docs/zh-CN/deploy/failure-tests.md](docs/zh-CN/deploy/failure-tests.md)：

```bash
./scripts/k8s-fault.sh          # Pod 杀进程耐久
./scripts/k8s-traffic-fault.sh  # 持续写 + 杀 bookie（C06）
./scripts/k8s-recover-fault.sh  # E/Qw/Qa recover（R01–R06）
./scripts/k8s-netem-fault.sh    # delay / 分区（C07 / C12）
./scripts/k8s-ar-e2e.sh         # Auditor → Worker recoveryAdd（M3）
```

## Repository

| 路径 | 角色 |
|---|---|
| `crates/open-ledger` | `Bookie`、`LocalBookie` / `MemoryBookie`、进程内 quorum client、Oxia key 布局 |
| `crates/open-ledger-oxia` | `OxiaMetaStore`、registry、election、Auditor、ReplicationWorker |
| `crates/openledgerd` | Bookie 进程：本地目录 + gRPC `:3181` |
| `crates/openledger-autorecovery` | 同 Pod AutoRecovery sidecar |
| `proto/bookie/v1` | Bookie gRPC 定义 |
| `deploy/k8s` | 本地 k3s 清单 |
| `scripts/` | 集群故障与 e2e 脚本 |

### Language clients

与本仓库 client 同一套 E/Qw/Qa、fence coverage、`RecoveryReadsFence`、`PipelineWriter` 语义：

| 语言 | 仓库 |
|---|---|
| Go | [open-ledger-client-go](https://github.com/devotionalist/open-ledger-client-go) |
| Rust | [open-ledger-client-rust](https://github.com/devotionalist/open-ledger-client-rust) |
| Java | [open-ledger-client-java](https://github.com/devotionalist/open-ledger-client-java) |
| TypeScript | [open-ledger-client-ts](https://github.com/devotionalist/open-ledger-client-ts) |

## Capabilities

| 能力 | 行为 |
|---|---|
| 单副本 API | `createLedger` / `addEntry` / `recoveryAdd` / `readEntry` / `fence` / LAC |
| 耐久 | ack 前 journal fsync；`entries.bin` 追加；checkpoint 回放 |
| Quorum 写 | round-robin write set，等 Qa ack |
| Recover | fence coverage `(Qw−Qa)+1`，再向前读修 → CAS `CLOSED` |
| Metadata | 生产用 Oxia；`MemoryMetaStore` 仅测试 |
| AutoRecovery | 丢 bookie → underreplicated → 本机 `recoveryAdd` + 换 ensemble |

## Disk layout（LocalBookie）

```text
{data_dir}/
  bookie_id                      # cookie；必须与 --bookie-id 一致
  journal/journal.log            # WAL；add 返回前 fsync
  journal/checkpoint             # ledger storage 已覆盖 [0, checkpoint)
  ledgers/{ledgerId}/meta.json
  ledgers/{ledgerId}/entries.bin # 追加 entry log
```

## Documentation

默认英文在 [`docs/`](docs/)；中文镜像在 [`docs/zh-CN/`](docs/zh-CN/)。

| 文档 | 内容 |
|---|---|
| [docs/zh-CN/architecture.md](docs/zh-CN/architecture.md) | 边界、仓库地图、不变量、读哪一节 |
| [docs/zh-CN/design.md](docs/zh-CN/design.md) | E/Qw/Qa、两套状态、Bookie API、recover、AR |
| [docs/zh-CN/development.md](docs/zh-CN/development.md) | 开发规范与测试分层 |
| [docs/zh-CN/deploy/local-k8s.md](docs/zh-CN/deploy/local-k8s.md) | 构建镜像、导入 k3s、部署 |
| [docs/zh-CN/deploy/failure-tests.md](docs/zh-CN/deploy/failure-tests.md) | 单元 / k8s 故障矩阵与通过标准 |

## License

Apache-2.0
