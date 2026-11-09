#!/usr/bin/env bash
# Production-oriented E/Qw/Qa recover suite against local k3s (3 bookies).
# Runs Go integration tests including pod-kill + RecoverLedger scenarios.
set -euo pipefail

NS="${NS:-open-ledger}"
GO_CLIENT="${GO_CLIENT:-$HOME/workspace/open-ledger-client-go}"
TIMEOUT="${TIMEOUT:-300s}"
BOOKIE_COUNT="${BOOKIE_COUNT:-8}"

log() { printf '%s\n' "$*"; }

need() {
  command -v "$1" >/dev/null 2>&1 || {
    log "missing command: $1"
    exit 2
  }
}

need kubectl
need go
[[ -d "$GO_CLIENT" ]] || {
  log "GO_CLIENT not found: $GO_CLIENT"
  exit 2
}

log "=== wait ${BOOKIE_COUNT} bookies Ready ==="
kubectl -n "$NS" rollout status statefulset/bookie --timeout="$TIMEOUT"
kubectl -n "$NS" rollout status statefulset/oxia --timeout="$TIMEOUT" 2>/dev/null || true
for ((i = 0; i < BOOKIE_COUNT; i++)); do
  kubectl -n "$NS" wait --for=condition=Ready "pod/bookie-$i" --timeout="$TIMEOUT"
done

log "=== go test -tags=integration (recover + fault, E=${BOOKIE_COUNT}/Qw=3/Qa=2) ==="
cd "$GO_CLIENT"
export OPEN_LEDGER_NS="$NS"
export OPEN_LEDGER_BOOKIE_COUNT="$BOOKIE_COUNT"
go test -tags=integration ./integration/ -count=1 -timeout=20m -v \
  -run 'TestR0|TestConfirmed|TestConcurrent|TestOldWriter|TestDirty|TestStripe|TestFence|TestHole|TestRecoverEmpty|TestSingleWriter|TestTwoWriters'

log "=== recover suite OK ==="
