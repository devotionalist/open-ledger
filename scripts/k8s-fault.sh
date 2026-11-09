#!/usr/bin/env bash
# Process-level fault tests against the local open-ledger StatefulSet.
set -euo pipefail

NS="${NS:-open-ledger}"
TIMEOUT="${TIMEOUT:-180s}"
BOOKIE_COUNT="${BOOKIE_COUNT:-8}"
pass=0
fail=0

log() { printf '%s\n' "$*"; }
ok() { pass=$((pass + 1)); log "PASS  $*"; }
bad() { fail=$((fail + 1)); log "FAIL  $*"; }

need() {
  command -v "$1" >/dev/null 2>&1 || {
    log "missing command: $1"
    exit 2
  }
}

ready_pod() {
  local pod=$1
  kubectl -n "$NS" exec "$pod" -c openledgerd -- /usr/local/bin/openledgerd --data-dir=/data --ready
}

wait_sts() {
  kubectl -n "$NS" rollout status statefulset/bookie --timeout="$TIMEOUT"
}

cookie_on_node() {
  local ordinal=$1
  colima ssh -- sudo sh -c \
    "cat /var/lib/rancher/k3s/storage/pvc-*_open-ledger_data-bookie-${ordinal}/bookie_id 2>/dev/null" \
    | tr -d '\r' | head -n1
}

journal_on_node() {
  local ordinal=$1
  colima ssh -- sudo sh -c \
    "test -f /var/lib/rancher/k3s/storage/pvc-*_open-ledger_data-bookie-${ordinal}/journal/journal.log"
}

need kubectl
need colima

IDS=()
for ((i = 0; i < BOOKIE_COUNT; i++)); do
  IDS+=("bookie-$i")
done

log "=== F01 baseline (${BOOKIE_COUNT} bookies) ==="
wait_sts
all_ok=1
for id in "${IDS[@]}"; do
  if ! ready_pod "$id"; then
    all_ok=0
    break
  fi
done
if [[ "$all_ok" -eq 1 ]]; then
  ok "F01 ${BOOKIE_COUNT} pods ready"
else
  bad "F01 ready probe"
fi

log "=== F02 delete bookie-0 ==="
kubectl -n "$NS" delete pod bookie-0 --wait=true
wait_sts
if ready_pod bookie-0; then
  ok "F02 bookie-0 ready after delete"
else
  bad "F02 bookie-0 not ready"
fi
cookie=$(cookie_on_node 0 || true)
if [[ "$cookie" == "bookie-0" ]] && journal_on_node 0; then
  ok "F02 cookie+journal on node for bookie-0"
else
  bad "F02 node data bookie-0 cookie='${cookie}'"
fi

last=$((BOOKIE_COUNT - 1))
log "=== F03 delete bookie-${last} ==="
kubectl -n "$NS" delete pod "bookie-${last}" --wait=true
wait_sts
if ready_pod "bookie-${last}"; then
  ok "F03 bookie-${last} ready after delete"
else
  bad "F03 bookie-${last} not ready"
fi
cookie=$(cookie_on_node "$last" || true)
if [[ "$cookie" == "bookie-${last}" ]] && journal_on_node "$last"; then
  ok "F03 cookie+journal on node for bookie-${last}"
else
  bad "F03 node data bookie-${last} cookie='${cookie}'"
fi

log "=== F04 delete bookie-0 and bookie-1 ==="
kubectl -n "$NS" delete pod bookie-0 bookie-1 --wait=true
wait_sts
all_ok=1
for id in "${IDS[@]}"; do
  if ! ready_pod "$id"; then
    all_ok=0
    break
  fi
done
if [[ "$all_ok" -eq 1 ]]; then
  ok "F04 all recovered"
else
  bad "F04 recovery"
fi

log "=== F05 identity on PVC (sample 0,1,last) ==="
c0=$(cookie_on_node 0 || true)
c1=$(cookie_on_node 1 || true)
cl=$(cookie_on_node "$last" || true)
if [[ "$c0" == "bookie-0" && "$c1" == "bookie-1" && "$cl" == "bookie-${last}" ]]; then
  ok "F05 cookies match ordinals"
else
  bad "F05 cookies c0='${c0}' c1='${c1}' cl='${cl}'"
fi

log "=== summary pass=${pass} fail=${fail} ==="
[[ "$fail" -eq 0 ]]
