#!/usr/bin/env bash
# Cluster-level netem / iptables faults against bookie pods (C07 delay, C12 partition).
# Uses Colima host tc/iptables via nsenter into the openledgerd netns.
set -euo pipefail

NS="${NS:-open-ledger}"
TIMEOUT="${TIMEOUT:-180s}"
TRAFFIC_DURATION="${TRAFFIC_DURATION:-50s}"
BOOKIE_COUNT="${BOOKIE_COUNT:-8}"
GO_CLIENT="${GO_CLIENT:-$HOME/workspace/open-ledger-client-go}"
DELAY="${DELAY:-80ms}"
TARGET="${TARGET:-bookie-0}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=lib/k8s-netem.sh
source "$ROOT/scripts/lib/k8s-netem.sh"

pass=0
fail=0
PFS=()
PORTS=()
IDS=()

log() { printf '%s\n' "$*"; }
ok() { pass=$((pass + 1)); log "PASS  $*"; }
bad() { fail=$((fail + 1)); log "FAIL  $*"; }

cleanup() {
  netem_clear "$NS" "$TARGET" 2>/dev/null || true
  if [[ -n "${TRAFFIC_PID:-}" ]] && kill -0 "$TRAFFIC_PID" 2>/dev/null; then
    kill "$TRAFFIC_PID" 2>/dev/null || true
    wait "$TRAFFIC_PID" 2>/dev/null || true
  fi
  for pid in "${PFS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

need() {
  command -v "$1" >/dev/null 2>&1 || {
    log "missing command: $1"
    exit 2
  }
}

ready_pod() {
  kubectl -n "$NS" exec "$1" -c openledgerd -- /usr/local/bin/openledgerd --data-dir=/data --ready
}

wait_sts() {
  kubectl -n "$NS" rollout status statefulset/bookie --timeout="$TIMEOUT"
}

free_port() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

wait_tcp() {
  local addr=$1
  local deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    if python3 - "$addr" <<'PY'
import socket, sys
host, port = sys.argv[1].rsplit(":", 1)
s = socket.socket()
s.settimeout(0.5)
try:
    s.connect((host, int(port)))
except Exception:
    sys.exit(1)
else:
    s.close()
PY
    then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

start_pf() {
  local id=$1
  local port=$2
  local idx=$3
  if [[ -n "${PFS[$idx]:-}" ]] && kill -0 "${PFS[$idx]}" 2>/dev/null; then
    kill "${PFS[$idx]}" 2>/dev/null || true
    wait "${PFS[$idx]}" 2>/dev/null || true
  fi
  kubectl -n "$NS" port-forward "pod/$id" "${port}:3181" >/tmp/ol-pf-netem-${id}.log 2>&1 &
  PFS[$idx]=$!
  wait_tcp "127.0.0.1:${port}" || {
    bad "port-forward $id"
    return 1
  }
}

need kubectl
need colima
need go
need python3
[[ -d "$GO_CLIENT" ]] || {
  log "GO_CLIENT not found: $GO_CLIENT"
  exit 2
}
if (( BOOKIE_COUNT < 3 )); then
  log "BOOKIE_COUNT must be >= 3"
  exit 2
fi

for ((i = 0; i < BOOKIE_COUNT; i++)); do
  IDS+=("bookie-$i")
done

log "=== baseline: wait bookies Ready ==="
wait_sts
ready_pod "$TARGET" && ok "baseline $TARGET ready" || {
  bad "baseline"
  exit 1
}

log "=== port-forward ${BOOKIE_COUNT} bookies ==="
ADDRS=()
PFS=()
PORTS=()
for ((i = 0; i < BOOKIE_COUNT; i++)); do
  port=$(free_port)
  PORTS+=("$port")
  start_pf "${IDS[$i]}" "$port" "$i" || exit 1
  ADDRS+=("127.0.0.1:${port}")
done
ADDR_CSV=$(IFS=,; echo "${ADDRS[*]}")
ID_CSV=$(IFS=,; echo "${IDS[*]}")

start_traffic() {
  local tag=$1
  TRAFFIC_LOG="${ROOT}/target/traffic-${tag}.log"
  mkdir -p "$(dirname "$TRAFFIC_LOG")"
  (
    cd "$GO_CLIENT"
    go run ./cmd/traffic \
      --addrs="$ADDR_CSV" \
      --ids="$ID_CSV" \
      --e="$BOOKIE_COUNT" --qw=3 --qa=2 \
      --duration="$TRAFFIC_DURATION" \
      --report=2s \
      --retry=300ms
  ) >"$TRAFFIC_LOG" 2>&1 &
  TRAFFIC_PID=$!
  sleep 4
  if ! kill -0 "$TRAFFIC_PID" 2>/dev/null; then
    bad "traffic exited early ($tag)"
    cat "$TRAFFIC_LOG" || true
    exit 1
  fi
  ok "traffic running ($tag pid=$TRAFFIC_PID)"
}

wait_traffic() {
  local tag=$1
  set +e
  wait "$TRAFFIC_PID"
  local rc=$?
  set -e
  TRAFFIC_PID=""
  if [[ "$rc" -eq 0 ]]; then
    ok "traffic completed ($tag)"
  else
    bad "traffic exit=$rc ($tag)"
  fi
  local final_ok
  final_ok=$(grep -E 'writes_ok=' "$TRAFFIC_LOG" | tail -1 | sed -n 's/.*writes_ok=\([0-9]*\).*/\1/p' || echo 0)
  if [[ "${final_ok:-0}" -gt 0 ]]; then
    ok "$tag writes_ok=${final_ok}"
  else
    bad "$tag no writes_ok"
  fi
}

# --- C07: delay injection ---
log "=== C07 netem delay=${DELAY} on ${TARGET} ==="
start_traffic c07
writes_before=$(grep -E 'writes_ok=' "$TRAFFIC_LOG" | tail -1 | sed -n 's/.*writes_ok=\([0-9]*\).*/\1/p' || echo 0)
netem_apply_delay "$NS" "$TARGET" "$DELAY"
sleep 8
writes_mid=$(grep -E 'writes_ok=' "$TRAFFIC_LOG" | tail -1 | sed -n 's/.*writes_ok=\([0-9]*\).*/\1/p' || echo 0)
if [[ "${writes_mid:-0}" -gt "${writes_before:-0}" ]]; then
  ok "C07 writes advanced under delay (${writes_before} -> ${writes_mid})"
else
  bad "C07 writes stalled under delay (${writes_before} -> ${writes_mid})"
fi
netem_clear "$NS" "$TARGET"
wait_traffic c07

# --- C12: hard partition ---
log "=== C12 iptables partition on ${TARGET} ==="
start_traffic c12
writes_before=$(grep -E 'writes_ok=' "$TRAFFIC_LOG" | tail -1 | sed -n 's/.*writes_ok=\([0-9]*\).*/\1/p' || echo 0)
netem_apply_partition "$NS" "$TARGET"
sleep 10
writes_mid=$(grep -E 'writes_ok=' "$TRAFFIC_LOG" | tail -1 | sed -n 's/.*writes_ok=\([0-9]*\).*/\1/p' || echo 0)
if [[ "${writes_mid:-0}" -gt "${writes_before:-0}" ]]; then
  ok "C12 writes advanced while ${TARGET} partitioned (${writes_before} -> ${writes_mid})"
else
  bad "C12 writes stalled while partitioned (${writes_before} -> ${writes_mid})"
fi
netem_clear "$NS" "$TARGET"
# give pod a moment to recover sessions
sleep 5
ready_pod "$TARGET" && ok "C12 ${TARGET} ready after heal" || bad "C12 ${TARGET} not ready after heal"
wait_traffic c12

log "=== summary pass=${pass} fail=${fail} ==="
[[ "$fail" -eq 0 ]]
