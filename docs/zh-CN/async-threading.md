# 异步线程模型（Callback API）

[English](../async-threading.md) | [中文](./async-threading.md)

open-ledger Bookie gRPC 与 OpenKV 相同，采用 **Callback / 异步 reaction** 纪律（`CallbackService` + WorkPool）：接收路径只投递；Journal fsync / 盘 I/O 在工作池；完成后再 Finish RPC。

gRPC C++ 背景：[Callback API Tutorial](https://grpc.io/docs/languages/cpp/callback/)。

## 角色

| 角色 | 谁 | 允许 | 禁止 |
|---|---|---|---|
| **接收 / reaction** | tonic poll（Rust）；gRPC callback CQ（C++ OpenKV） | 解析请求、`spawn_blocking` / 投递 WorkPool、返回 future / reactor | Journal fsync、睡眠、在接收线程同步等下游 RPC |
| **工作池** | `tokio::task::spawn_blocking`（openledgerd）；OpenKV `WorkPool` | Journal + ledger I/O、填响应 | 阻塞 reactor |
| **Finish / 回调** | future 完成 → tonic 回包；客户端 `Done` | 发送 status、续跑状态机 | 在接收线程嵌套同步 RPC |

```text
Client Async* / Sync 适配
        │  unary gRPC
        ▼
openledgerd BookieService (tonic async_trait)
  receive → spawn_blocking(LocalBookie) → Finish response
```

## 服务端（`openledgerd`）

- 实现：`crates/openledgerd/src/grpc.rs` — 每个 unary 经 `react(...)` → `spawn_blocking` 调 `LocalBookie`。
- 默认监听 `:3181`；日志 `model=callback/async-reaction+spawn_blocking`。
- 热路径禁止在接收线程上同步 stub / 睡眠。
- **优雅关机**：SIGTERM / SIGINT → `serve_with_shutdown` 停接新连接并排空进行中 RPC（最长 `--shutdown-grace-secs`，默认 25）→ `LocalBookie::flush` → 退出。k8s `terminationGracePeriodSeconds` 应大于该值。

## Go 客户端（`open-ledger-client-go`）

| 表面 | 用途 |
|---|---|
| `BookieAsync` + `*Async` | Callback API：投递即返回，完成走 `Done` / 类型化回调 |
| `GRPCBookie` | 实现 `BookieAsync`；阻塞 `Bookie` 方法为薄 sync 适配 |
| `SyncBookie` / `NewSyncBookie` | 显式 async→sync |
| `AsyncFromSync` | 把 `MemoryBookie`（或任意 sync `Bookie`）包成 `BookieAsync` |

完成回调内禁止再同步 RPC 或睡眠。

## 对照

| OpenKV / gRPC C++ | open-ledger |
|---|---|
| `CallbackService` + `ServerUnaryReactor` | tonic `async_trait` + `spawn_blocking` + await Finish |
| Client async / reactor `OnDone` | Go `BookieAsync` 回调 |
| Sync stub（仅边缘） | `Bookie` / `SyncBookie` |

Java / TypeScript 在接入 gRPC stub 前仍用进程内 `Bookie`；接入后优先 async stub / `StreamObserver`，同样区分接收与工作路径。
