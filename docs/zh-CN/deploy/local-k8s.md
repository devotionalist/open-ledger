# 本地 Kubernetes

[English](../../deploy/local-k8s.md) | [中文](./local-k8s.md)


把 `openledgerd` + `openledger-autorecovery` 跑在本机 k3s 上，并用 Oxia 存 ledger metadata / AutoRecovery 协调状态。

- `openledgerd`：`0.0.0.0:3181` Bookie gRPC；Pod Ready = TCP `3181` 在听。
- sidecar：连 Oxia，注册本机 bookie，选 Auditor，对本机补副本。
- Oxia：standalone，`:6648`。

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

节点上的名字是 `docker.io/library/openledgerd:local` 与 `docker.io/library/openledger-autorecovery:local`。StatefulSet 使用 `imagePullPolicy: Never`。

## 部署

```bash
kubectl apply -k deploy/k8s
kubectl -n open-ledger rollout status statefulset/oxia
kubectl -n open-ledger rollout status statefulset/bookie
kubectl -n open-ledger get pods -o wide
```

- Oxia：`oxia-0`
- Bookie×8：`bookie-0`…`bookie-7`，每 Pod 两容器；`--bookie-id` / registry 用 Pod 名；每副本一块 1Gi PVC。主测试 quorum `E=8,Qw=3,Qa=2`。

## 确认

```bash
kubectl -n open-ledger exec bookie-0 -c openledgerd -- /usr/local/bin/openledgerd --data-dir=/data --ready
kubectl -n open-ledger logs bookie-0 -c openledgerd
kubectl -n open-ledger logs bookie-0 -c autorecovery
kubectl -n open-ledger port-forward pod/bookie-0 13181:3181
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
