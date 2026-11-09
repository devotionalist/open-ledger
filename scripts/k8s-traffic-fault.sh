#!/usr/bin/env bash
# Continuous Go client R/W (E=N, Qw=3, Qa=2) while deleting bookie pods.
# Expectation: writes keep succeeding while a single bookie is down; pods return Ready.
set -euo pipefail

NS="${NS:-open-ledger}"
TIMEOUT="${TIMEOUT:-180s}"
TRAFFIC_DURATION="${TRAFFIC_DURATION:-90s}"
BOOKIE_COUNT="${BOOKIE_COUNT:-8}"
GO_CLIENT="${GO_CLIENT:-$HOME/workspace/open-ledger-client-go}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

pass=0
fail=0
PFS=()
PORTS=()
IDS=()

log() { printf '%s\n' "$*"; }
ok() { pass=$((pass + 1)); log "PASS  $*"; }
bad() { fail=$((fail + 1)); log "FAIL  $*"; }

cleanup() {
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

need kubectl
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

log "=== C06 baseline: wait ${BOOKIE_COUNT} bookies Ready ==="
wait_sts
for id in "${IDS[@]}"; do
  ready_pod "$id" || {
    bad "baseline $id"
    exit 1
  }
done
ok "${BOOKIE_COUNT} bookies ready"

start_pf() {
  local id=$1
  local port=$2
  local idx=$3
  if [[ -n "${PFS[$idx]:-}" ]] && kill -0 "${PFS[$idx]}" 2>/dev/null; then
    kill "${PFS[$idx]}" 2>/dev/null || true
    wait "${PFS[$idx]}" 2>/dev/null || true
  fi
  kubectl -n "$NS" port-forward "pod/$id" "${port}:3181" >/tmp/ol-pf-${id}.log 2>&1 &
  PFS[$idx]=$!
  wait_tcp "127.0.0.1:${port}" || {
    bad "port-forward $id"
    return 1
  }
  log "  $id -> 127.0.0.1:${port} (pf pid=${PFS[$idx]})"
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

TRAFFIC_LOG="${ROOT}/target/traffic-c06.log"
mkdir -p "$(dirname "$TRAFFIC_LOG")"
log "=== start Go traffic E=${BOOKIE_COUNT}/Qw=3/Qa=2 duration=${TRAFFIC_DURATION} ==="
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

sleep 5
if ! kill -0 "$TRAFFIC_PID" 2>/dev/null; then
  bad "traffic exited early"
  cat "$TRAFFIC_LOG" || true
  exit 1
fi
ok "traffic running (pid=$TRAFFIC_PID)"

writes_before=$(grep -E 'writes_ok=' "$TRAFFIC_LOG" | tail -1 | sed -n 's/.*writes_ok=\([0-9]*\).*/\1/p' || echo 0)
log "=== C06a delete bookie-0 ==="
kubectl -n "$NS" delete pod bookie-0 --wait=true
wait_sts
ready_pod bookie-0 && ok "bookie-0 Ready after delete" || bad "bookie-0 not Ready"
start_pf bookie-0 "${PORTS[0]}" 0 || bad "rebind port-forward bookie-0"
sleep 8
writes_mid=$(grep -E 'writes_ok=' "$TRAFFIC_LOG" | tail -1 | sed -n 's/.*writes_ok=\([0-9]*\).*/\1/p' || echo 0)
if [[ "${writes_mid:-0}" -gt "${writes_before:-0}" ]]; then
  ok "writes advanced during/after bookie-0 fault (${writes_before} -> ${writes_mid})"
else
  bad "writes stalled around bookie-0 fault (${writes_before} -> ${writes_mid})"
fi

log "=== C06b delete bookie-$((BOOKIE_COUNT - 1)) ==="
writes_before2=$writes_mid
last=$((BOOKIE_COUNT - 1))
kubectl -n "$NS" delete pod "bookie-${last}" --wait=true
wait_sts
ready_pod "bookie-${last}" && ok "bookie-${last} Ready after delete" || bad "bookie-${last} not Ready"
start_pf "bookie-${last}" "${PORTS[$last]}" "$last" || bad "rebind port-forward bookie-${last}"
sleep 8
writes_end=$(grep -E 'writes_ok=' "$TRAFFIC_LOG" | tail -1 | sed -n 's/.*writes_ok=\([0-9]*\).*/\1/p' || echo 0)
if [[ "${writes_end:-0}" -gt "${writes_before2:-0}" ]]; then
  ok "writes advanced during/after bookie-${last} fault (${writes_before2} -> ${writes_end})"
else
  bad "writes stalled around bookie-${last} fault (${writes_before2} -> ${writes_end})"
fi

log "=== wait for traffic exit ==="
set +e
wait "$TRAFFIC_PID"
traffic_rc=$?
set -e
TRAFFIC_PID=""
log "traffic exit=${traffic_rc}"
tail -n 20 "$TRAFFIC_LOG" || true

if [[ "$traffic_rc" -eq 0 ]]; then
  ok "traffic completed cleanly"
else
  bad "traffic exit code ${traffic_rc}"
fi

final_ok=$(grep -E '^.*done writes_ok=' "$TRAFFIC_LOG" | tail -1 | sed -n 's/.*writes_ok=\([0-9]*\).*/\1/p' || true)
final_read=$(grep -E '^.*done writes_ok=' "$TRAFFIC_LOG" | tail -1 | sed -n 's/.*reads_ok=\([0-9]*\).*/\1/p' || true)
if [[ "${final_ok:-0}" -gt 0 && "${final_read:-0}" -gt 0 ]]; then
  ok "final writes_ok=${final_ok} reads_ok=${final_read}"
else
  bad "final counters writes_ok=${final_ok:-?} reads_ok=${final_read:-?}"
fi

log "=== summary pass=${pass} fail=${fail} ==="
[[ "$fail" -eq 0 ]]
