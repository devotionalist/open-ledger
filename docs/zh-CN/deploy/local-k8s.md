# 本地 Kubernetes

[English](../../deploy/local-k8s.md) | [中文](./local-k8s.md)


把 `openledgerd` + `openledger-autorecovery` 跑在本机 k3s 上，并用 Oxia 存 ledger metadata / AutoRecovery 协调状态。

- `openledgerd`：gRPC `:3181`；HTTP 管理/指标 `:3182`（`/`、`/metrics`、`/ready`、`/live`）。
- sidecar（`openledger-autorecovery`）：Oxia 注册 + Auditor/Worker；HTTP 指标/探针 `:3183`。
- `openledger-admin`：集群 UI + 协调面 gauges，`:8080`。
- 协调（默认）：Oxia standalone `:6648`（`COORD_BACKEND=oxia`）。
- 可选：OpenKV Gateway `:19000`，用 `deploy/k8s/overlays/openkv`（`COORD_BACKEND=openkv`）。
- 日志：集群内 `RUST_LOG_FORMAT=json`。

集群是 Colima 的 `docker+k3s`（`kubectl` context `colima`）。磁盘用默认 `local-path` StorageClass。

## 构建并导入

### Bookie（静态 musl → scratch）

macOS 上的 `cc` 链不出 musl，链接器要用工具链自带的 `rust-lld`。

```bash
rustup target add x86_64-unknown-linux-musl
export PATH="$(rustc --print sysroot)/lib/rustlib/x86_64-apple-darwin/bin:$PATH"
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=rust-lld
export RUSTFLAGS="-C linker-flavor=ld.lld"
cargo build --release --target x86_64-unknown-linux-musl -p openledgerd
docker build -t openledgerd:local .
docker save openledgerd:local | colima ssh -- sudo k3s ctr images import -
```

### AutoRecovery（Docker 内编译 → debian slim）

`oxia-client` 0.2 需要 Linux/glibc；macOS 上直接用多阶段 Dockerfile 在容器里编：

```bash
docker build -f Dockerfile.autorecovery -t openledger-autorecovery:local .
docker save openledger-autorecovery:local | colima ssh -- sudo k3s ctr images import -
```

### 集群管理（Docker → debian slim）

```bash
docker build -f Dockerfile.admin -t openledger-admin:local .
docker save openledger-admin:local | colima ssh -- sudo k3s ctr images import -
```

节点镜像名：`openledgerd:local`、`openledger-autorecovery:local`、`openledger-admin:local`。工作负载使用 `imagePullPolicy: Never`。

## 部署

```bash
kubectl apply -k deploy/k8s
kubectl -n open-ledger rollout status statefulset/oxia
kubectl -n open-ledger rollout status statefulset/bookie
kubectl -n open-ledger rollout status deployment/openledger-admin
kubectl -n open-ledger get pods -o wide
```

可选 OpenKV 协调后端（Gateway 需已可达 `openkv:19000`）：

```bash
kubectl apply -k deploy/k8s/overlays/openkv
```

可选 Prometheus Operator 包（ServiceMonitor / 告警 / Grafana dashboard）：

```bash
kubectl apply -k deploy/k8s/monitoring
```

- Oxia：`oxia-0`
- Bookie×8：`bookie-0`…`bookie-7`，每 Pod 两容器；`--bookie-id` / registry 用 Pod 名；每副本一块 1Gi PVC。主测试 quorum `E=8,Qw=3,Qa=2`。
- Admin：`openledger-admin` Deployment。

## 确认

```bash
kubectl -n open-ledger exec bookie-0 -c openledgerd -- /usr/local/bin/openledgerd --data-dir=/data --ready
kubectl -n open-ledger logs bookie-0 -c openledgerd
kubectl -n open-ledger logs bookie-0 -c autorecovery
kubectl -n open-ledger port-forward pod/bookie-0 13181:3181 13182:3182 13183:3183
kubectl -n open-ledger port-forward svc/openledger-admin 18080:8080
# 然后打开 http://127.0.0.1:18080/ 与 http://127.0.0.1:13182/
```

## 故障测试

进程级杀 Pod / PVC 保留：[failure-tests.md](./failure-tests.md)，脚本 `./scripts/k8s-fault.sh`。

Go client 持续读写 + 杀 bookie（`E=Qw=3,Qa=2`）：

```bash
./scripts/k8s-traffic-fault.sh
# 可选：GO_CLIENT=/path/to/open-ledger-client-go TRAFFIC_DURATION=90s
```

Go client E/Qw/Qa recover 生产矩阵（写 → 杀 Pod → `RecoverLedger` → CLOSED/可读）：

```bash
./scripts/k8s-recover-fault.sh
```

AutoRecovery 集群 e2e：`./scripts/k8s-ar-e2e.sh`（见 [failure-tests.md](./failure-tests.md) §7）。
netem / 分区：`./scripts/k8s-netem-fault.sh`（§6）。
