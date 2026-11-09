#!/usr/bin/env bash
# Cluster gRPC AutoRecovery e2e (M3):
#   seed CLOSED ledger meta in Oxia + entries on bookies → isolate one ensemble
#   member so registry ephemeral dies → Auditor enqueue → Worker recoveryAdd →
#   ensemble replace verified.
set -euo pipefail

NS="${NS:-open-ledger}"
TIMEOUT="${TIMEOUT:-300}"
GO_CLIENT="${GO_CLIENT:-$HOME/workspace/open-ledger-client-go}"
TARGET="${TARGET:-bookie-0}"
ENSEMBLE_IDS="${ENSEMBLE_IDS:-bookie-0,bookie-1,bookie-2}"
ENTRIES="${ENTRIES:-5}"
LEDGER_ID="${LEDGER_ID:-}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=lib/k8s-netem.sh
source "$ROOT/scripts/lib/k8s-netem.sh"

pass=0
fail=0
PFS=()

log() { printf '%s\n' "$*"; }
ok() { pass=$((pass + 1)); log "PASS  $*"; }
bad() { fail=$((fail + 1)); log "FAIL  $*"; }

cleanup() {
  netem_clear "$NS" "$TARGET" 2>/dev/null || true
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
  kubectl -n "$NS" port-forward "pod/$id" "${port}:3181" >/tmp/ol-pf-ar-${id}.log 2>&1 &
  PFS+=($!)
  wait_tcp "127.0.0.1:${port}" || {
    bad "port-forward $id"
    return 1
  }
}

oxia() {
  kubectl -n "$NS" exec oxia-0 -- oxia client "$@"
}

oxia_get_meta() {
  local lid=$1
  oxia get "/open-ledger/ledgers/${lid}" 2>/dev/null | tail -n +1
}

oxia_has_underreplicated() {
  local lid=$1
  oxia list -s /open-ledger/underreplicated/ -e '/open-ledger/underreplicated/~' 2>/dev/null | grep -q "/open-ledger/underreplicated/${lid}"
}

oxia_delete_bookie_reg() {
  local id=$1
  oxia delete "/open-ledger/bookies/${id}" >/dev/null 2>&1 || true
}

need kubectl
need colima
need go
need python3
[[ -d "$GO_CLIENT" ]] || {
  log "GO_CLIENT not found: $GO_CLIENT"
  exit 2
}

IFS=',' read -r -a ENS_ARR <<<"$ENSEMBLE_IDS"
if ((${#ENS_ARR[@]} < 3)); then
  log "ENSEMBLE_IDS must list >= 3 bookies"
  exit 2
fi
# Dial all ensemble members plus a few extras so verify can reach replacements.
DIAL_IDS=("${ENS_ARR[@]}")
for extra in bookie-3 bookie-4 bookie-5 bookie-6 bookie-7; do
  skip=0
  for e in "${ENS_ARR[@]}"; do
    [[ "$e" == "$extra" ]] && skip=1 && break
  done
  ((skip)) && continue
  DIAL_IDS+=("$extra")
done

if [[ -z "$LEDGER_ID" ]]; then
  LEDGER_ID=$((900000 + RANDOM % 90000))
fi

log "=== M3 AR e2e ledger=${LEDGER_ID} missing=${TARGET} ensemble=${ENSEMBLE_IDS} ==="

log "=== port-forward dial set (${#DIAL_IDS[@]}) ==="
ADDRS=()
for id in "${DIAL_IDS[@]}"; do
  port=$(free_port)
  start_pf "$id" "$port" || exit 1
  ADDRS+=("127.0.0.1:${port}")
done
ADDR_CSV=$(IFS=,; echo "${ADDRS[*]}")
ID_CSV=$(IFS=,; echo "${DIAL_IDS[*]}")

META_FILE=$(mktemp)
log "=== seed entries on ensemble ==="
(
  cd "$GO_CLIENT"
  go run ./cmd/ar-e2e \
    --mode=seed \
    --addrs="$ADDR_CSV" \
    --ids="$ID_CSV" \
    --ensemble="$ENSEMBLE_IDS" \
    --ledger="$LEDGER_ID" \
    --entries="$ENTRIES" \
    --qw=3 --qa=2
) | tee "$META_FILE"
ok "seeded ${ENTRIES} entries"

log "=== put CLOSED LedgerMeta into Oxia ==="
META_JSON=$(tr -d '\n' <"$META_FILE")
printf '%s' "$META_JSON" | kubectl -n "$NS" exec -i oxia-0 -- oxia client put --std-in "/open-ledger/ledgers/${LEDGER_ID}" >/dev/null
oxia get "/open-ledger/ledgers/${LEDGER_ID}" >/dev/null
ok "oxia ledger meta present"

log "=== wait for Auditor to observe full registry ==="
sleep 8

log "=== isolate ${TARGET} (partition + delete registry key) ==="
netem_apply_partition "$NS" "$TARGET"
oxia_delete_bookie_reg "$TARGET"
ok "partitioned ${TARGET}"

log "=== wait Auditor enqueue underreplicated/${LEDGER_ID} ==="
deadline=$((SECONDS + TIMEOUT))
enqueued=0
while ((SECONDS < deadline)); do
  if oxia_has_underreplicated "$LEDGER_ID"; then
    enqueued=1
    break
  fi
  # keep the registry key gone while partitioned (heartbeat may race before DROP fully bites)
  oxia_delete_bookie_reg "$TARGET"
  sleep 2
done
if ((enqueued)); then
  ok "underreplicated task enqueued"
else
  bad "underreplicated task not seen within ${TIMEOUT}s"
fi

log "=== wait ensemble replace (lost_bookie_delay≈15s + worker) ==="
replaced=0
NEW_ENSEMBLE=""
while ((SECONDS < deadline)); do
  raw=$(oxia get "/open-ledger/ledgers/${LEDGER_ID}" 2>/dev/null || true)
  if [[ -n "$raw" ]]; then
    NEW_ENSEMBLE=$(python3 - "$raw" <<'PY'
import json, sys
raw = sys.argv[1]
# oxia client get may print version header lines; find JSON object
start = raw.find("{")
end = raw.rfind("}")
if start < 0 or end < 0:
    sys.exit(1)
meta = json.loads(raw[start : end + 1])
ens = meta["fragments"][0]["ensemble"]
print(",".join(ens))
PY
) || NEW_ENSEMBLE=""
    if [[ -n "$NEW_ENSEMBLE" ]] && [[ ",${NEW_ENSEMBLE}," != *",${TARGET},"* ]]; then
      replaced=1
      break
    fi
  fi
  sleep 3
done

if ((replaced)); then
  ok "ensemble replaced: ${NEW_ENSEMBLE}"
else
  bad "ensemble still contains ${TARGET} after timeout"
  log "meta=$(oxia get "/open-ledger/ledgers/${LEDGER_ID}" 2>/dev/null || true)"
  log "tasks=$(oxia list -s /open-ledger/underreplicated/ -e '/open-ledger/underreplicated/~' 2>/dev/null || true)"
  kubectl -n "$NS" logs "$TARGET" -c autorecovery --tail=40 || true
  for id in bookie-1 bookie-3 bookie-5; do
    kubectl -n "$NS" logs "$id" -c autorecovery --tail=20 || true
  done
fi

# Heal network so verify can dial former/new members via port-forward.
netem_clear "$NS" "$TARGET"
sleep 3
# Re-establish port-forwards that may have died while TARGET was down.
for i in "${!DIAL_IDS[@]}"; do
  id="${DIAL_IDS[$i]}"
  port="${ADDRS[$i]#*:}"
  if ! wait_tcp "127.0.0.1:${port}"; then
    start_pf "$id" "$port" || true
  fi
done

if ((replaced)); then
  log "=== verify recoveryAdd payloads on new ensemble ==="
  if (
    cd "$GO_CLIENT"
    go run ./cmd/ar-e2e \
      --mode=verify \
      --addrs="$ADDR_CSV" \
      --ids="$ID_CSV" \
      --ensemble="$NEW_ENSEMBLE" \
      --missing="$TARGET" \
      --ledger="$LEDGER_ID" \
      --entries="$ENTRIES"
  ); then
    ok "recoveryAdd payloads verified"
  else
    bad "recoveryAdd verify failed"
  fi

  # Task should be deleted once a worker finishes.
  sleep 3
  if oxia_has_underreplicated "$LEDGER_ID"; then
    log "NOTE  underreplicated key still present (may be racing); acceptable if ensemble fixed"
  else
    ok "underreplicated task deleted"
  fi
fi

log "=== wait ${TARGET} Ready after heal ==="
kubectl -n "$NS" wait --for=condition=ready "pod/${TARGET}" --timeout=120s && ok "${TARGET} ready" || bad "${TARGET} not ready"

rm -f "$META_FILE"
log "=== summary pass=${pass} fail=${fail} ==="
[[ "$fail" -eq 0 ]]
