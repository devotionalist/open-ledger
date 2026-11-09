# 开发规范

[English](../development.md) | [中文](./development.md)


设计语义以 [design.md](./design.md) 为准。文档默认英文在 `docs/`，中文镜像在 `docs/zh-CN/`。设计语义以 [design.md](./design.md) 为准。

## 1. 边界

| 可以改 | 不要改进本仓库 |
|---|---|
| Bookie 本地状态、Journal、checkpoint、磁盘布局 | Topic / 订阅 / ownership |
| 进程内 quorum client（`append_entry` / `recover_ledger`） | 生产 Metadata 服务实现 |
| `openledgerd` 进程与部署清单 | 其它系统的存储协议兼容层 |

单个 bookie **不知道** ensemble。`E/Qw/Qa`、fragment、`CLOSED.lastEntryId`、master key 权威在 Metadata。不要在 bookie 上塞「本机最大 entryId 即账本尾部」。

本仓库 payload 用 `bytes::Bytes`；进程内 client 与 `LocalBookie` 测试必须同语义。

## 2. 耐久与错误

1. `addEntry` / `recoveryAdd` / `fence` 返回 Ok 前，该副本 journal **必须已 fsync**。以后做 group commit 也不能弱化这条。
2. 同一 `(ledgerId, entryId)`：同 payload 幂等；不同 payload → `Conflict`。
3. Fence 之后普通 add 失败；`recoveryAdd` 允许。
4. cookie（`bookie_id`）与 `--bookie-id` 不一致 → 拒绝打开。
5. MessageID `(ledgerId, entryId)` 不复用。
6. 错误用现有 `Error` 变体；不要吞掉 `Fenced` / `NotEnoughBookies` / `Conflict`。

## 3. 代码风格

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

- 公共 API 保持 `#![deny(missing_docs)]`。
- `OpenOptions::create(true)` 必须显式 `.truncate(false)`（或真要截断时写明）。
- 不要为「看起来更干净」引入 `unwrap_err()` 依赖 `Debug`；用 `match` 对错误变体。
- 不擅自 `git init` / commit / push；用户明确要求再提交。

## 4. 磁盘布局

`LocalBookie` 一个 `--data-dir` 同时放 journal 与 ledgers。K8s 一个 PVC 挂 `/data`。拆 journal/ledgers 成两个挂载点前必须先改代码与设计。

```text
{data_dir}/
  bookie_id
  journal/journal.log
  journal/checkpoint
  ledgers/{ledgerId}/meta.json
  ledgers/{ledgerId}/entries.bin   # 顺序追加，热路径禁止整文件重写
```

## 5. 本地进程与镜像

```bash
cargo run -p openledgerd -- --data-dir /tmp/ol-data --bookie-id bookie-0 --smoke
```

`--ready`：只检查 cookie，供探针使用，不打开 journal。

macOS 打 Linux 镜像：静态 musl + `scratch`，链接用 `rust-lld`。步骤见 [deploy/local-k8s.md](./deploy/local-k8s.md)。

## 6. 测试分层

| 层 | 入口 | 覆盖 |
|---|---|---|
| 单元 | `cargo test --workspace` | Journal、fence、空洞、recover、striping |
| 进程 smoke | `openledgerd --smoke` | 打开目录、写一条、退出 |
| K8s 进程故障 | [deploy/failure-tests.md](./deploy/failure-tests.md) | 杀 Pod、PVC 保留、cookie 仍匹配 |
| 换 fragment 故障注入 | M2 | 见故障测试「未覆盖」 |

设计 §7 里的每一种故障，至少落在单元测试或故障矩阵的一行。改语义先改测试再改实现。

## 7. 阶段门禁

| 阶段 | 合入条件 |
|---|---|
| M0 | 单测绿；clippy `-D warnings`；设计文档与代码一致 |
| M1 | gRPC 可对多 `openledgerd` 跑；杀 1 bookie 仍能 Qa 写（Qa≥2） |
| M2 | 换 fragment 故障注入场景绿 |
| M3 | AutoRecovery 不改 entryId/payload；OPEN 账本补副本不绕过 fence |
