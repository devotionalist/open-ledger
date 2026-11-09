# Local Kubernetes

[English](./local-k8s.md) | [中文](../zh-CN/deploy/local-k8s.md)

Run `openledgerd` + `openledger-autorecovery` on local k3s, with Oxia holding ledger metadata and AutoRecovery coordination state.

- `openledgerd`: Bookie gRPC on `0.0.0.0:3181`; Pod Ready = TCP `3181` listening.
- Sidecar: connects to Oxia, registers this bookie, elects Auditor, rereplicates onto the local bookie.
- Oxia: standalone on `:6648`.

Cluster is Colima `docker+k3s` (`kubectl` context `colima`). Disks use the default `local-path` StorageClass.

## Build and import

### Bookie (static musl → scratch)

macOS `cc` cannot link musl; use the toolchain’s `rust-lld`.

```bash
rustup target add x86_64-unknown-linux-musl
export PATH="$(rustc --print sysroot)/lib/rustlib/x86_64-apple-darwin/bin:$PATH"
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=rust-lld
export RUSTFLAGS="-C linker-flavor=ld.lld"
cargo build --release --target x86_64-unknown-linux-musl -p openledgerd
docker build -t openledgerd:local .
docker save openledgerd:local | colima ssh -- sudo k3s ctr images import -
```

### AutoRecovery (build inside Docker → debian slim)

`oxia-client` 0.2 needs Linux/glibc; on macOS build with the multi-stage Dockerfile:

```bash
docker build -f Dockerfile.autorecovery -t openledger-autorecovery:local .
docker save openledger-autorecovery:local | colima ssh -- sudo k3s ctr images import -
```

Node image names are `docker.io/library/openledgerd:local` and `docker.io/library/openledger-autorecovery:local`. The StatefulSet uses `imagePullPolicy: Never`.

## Deploy

```bash
kubectl apply -k deploy/k8s
kubectl -n open-ledger rollout status statefulset/oxia
kubectl -n open-ledger rollout status statefulset/bookie
kubectl -n open-ledger get pods -o wide
```

- Oxia: `oxia-0`
- Bookies×8: `bookie-0`…`bookie-7`, two containers per Pod; `--bookie-id` / registry use the Pod name; 1Gi PVC per replica. Primary test quorum is `E=8,Qw=3,Qa=2`.

## Verify

```bash
kubectl -n open-ledger exec bookie-0 -c openledgerd -- /usr/local/bin/openledgerd --data-dir=/data --ready
kubectl -n open-ledger logs bookie-0 -c openledgerd
kubectl -n open-ledger logs bookie-0 -c autorecovery
kubectl -n open-ledger port-forward pod/bookie-0 13181:3181
```

## Fault tests

Process-level Pod kill / PVC retention: [failure-tests.md](./failure-tests.md), script `./scripts/k8s-fault.sh`.

Go client continuous read/write + kill bookie (`E=Qw=3,Qa=2`):

```bash
./scripts/k8s-traffic-fault.sh
# optional: GO_CLIENT=/path/to/open-ledger-client-go TRAFFIC_DURATION=90s
```

Go client E/Qw/Qa recover production matrix (write → kill Pod → `RecoverLedger` → CLOSED/readable):

```bash
./scripts/k8s-recover-fault.sh
```

AutoRecovery cluster e2e: `./scripts/k8s-ar-e2e.sh` (see [failure-tests.md](./failure-tests.md) §7).  
netem / partition: `./scripts/k8s-netem-fault.sh` (§6).
