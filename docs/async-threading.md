# Async threading model (Callback API)

[English](./async-threading.md) | [中文](./zh-CN/async-threading.md)

open-ledger Bookie gRPC follows the same **Callback / async reaction** discipline as OpenKV (`CallbackService` + WorkPool): the receive path only schedules work; disk I/O and journal fsync run off the reactor; completion finishes the RPC.

Official gRPC C++ background: [Callback API Tutorial](https://grpc.io/docs/languages/cpp/callback/).

## Roles

| Role | Who | Allowed | Forbidden |
|---|---|---|---|
| **Receive / reaction** | tonic poll thread (Rust); gRPC callback CQ (C++ OpenKV) | Parse request, `spawn_blocking` / post WorkPool, return future / reactor | Journal fsync, sleep, sync downstream RPC wait |
| **Work pool** | `tokio::task::spawn_blocking` (openledgerd); OpenKV `WorkPool` | Journal + ledger I/O, fill response | Blocking the reactor |
| **Finish / callback** | future completion → tonic response; client `Done` callback | Send status, continue state machine | Nested sync RPC on the receive thread |

```text
Client Async* / Sync adapter
        │  unary gRPC
        ▼
openledgerd BookieService (tonic async_trait)
  receive → spawn_blocking(LocalBookie) → Finish response
```

## Server (`openledgerd`)

- Service: `crates/openledgerd/src/grpc.rs` — every unary handler calls `react(...)` which is `spawn_blocking` over `LocalBookie`.
- Listen default `:3181`. Log field `model=callback/async-reaction+spawn_blocking`.
- Hot path must not call blocking stubs or wait on the receive future beyond awaiting the work-pool join.
- **Graceful shutdown**: SIGTERM / SIGINT → `serve_with_shutdown` stops accept and drains in-flight RPCs (up to `--shutdown-grace-secs`, default 25) → `LocalBookie::flush` → exit. Align k8s `terminationGracePeriodSeconds` above this value.

## Go client (`open-ledger-client-go`)

| Surface | Use |
|---|---|
| `BookieAsync` + `*Async` methods | Callback API: schedule RPC, return immediately; completion on `Done` / typed callbacks |
| `GRPCBookie` | Implements `BookieAsync`; blocking `Bookie` methods are thin sync adapters |
| `SyncBookie` / `NewSyncBookie` | Explicit async→sync adapter |
| `AsyncFromSync` | Wrap `MemoryBookie` (or any sync `Bookie`) as `BookieAsync` |

Do not perform further synchronous RPCs or sleep inside a completion callback.

## Equivalence

| OpenKV / gRPC C++ | open-ledger |
|---|---|
| `CallbackService` + `ServerUnaryReactor` | tonic `async_trait` + `spawn_blocking` + await Finish |
| Client async / reactor `OnDone` | Go `BookieAsync` callbacks |
| Sync stub (edge only) | `Bookie` / `SyncBookie` |

Java / TypeScript clients remain in-process `Bookie` until a gRPC stub lands; then prefer async stubs / `StreamObserver` with the same receive-vs-work split.
