#!/usr/bin/env bash
#
# One real end-to-end run against the Anthropic API.
#
# This is M4's fourth acceptance criterion. It is a script rather than a test
# because it spends real money and needs a real credential, so it must be run
# deliberately and never by CI.
#
#   ./scripts/live-check.sh [model]
#
set -euo pipefail

MODEL=${1:-claude-opus-5-5}
DSN=${ANCHOR_DATABASE_URL:-postgres://anchor:anchor@localhost:5433/anchor?sslmode=disable}
export ANCHOR_DATABASE_URL="$DSN"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# The SDK resolves credentials in order: ANTHROPIC_API_KEY, then
# ANTHROPIC_AUTH_TOKEN, then a profile written by `ant auth login`. An unset
# env var does not mean there are none, so check properly rather than guessing.
if [ -z "${ANTHROPIC_API_KEY:-}" ] && [ -z "${ANTHROPIC_AUTH_TOKEN:-}" ]; then
  if ! command -v ant >/dev/null 2>&1 || ! ant auth status >/dev/null 2>&1; then
    echo "No Anthropic credentials found." >&2
    echo "Either export ANTHROPIC_API_KEY, or run 'ant auth login'." >&2
    exit 1
  fi
  echo "==> using credentials from the active 'ant' profile"
fi

echo "==> migrating"
go run ./cmd/anchorctl migrate >/dev/null

echo "==> registering a single-model-call agent on $MODEL"
cat > /tmp/anchor-live-graph.json <<JSON
{"agent":"live","v":1,"nodes":[
  {"id":"n1","type":"model_call","model":"$MODEL",
   "system":"Answer in one short sentence.",
   "prompt":"In one sentence: what problem does durable execution solve?"}
]}
JSON
go run ./cmd/anchorctl agent-register -name live -file /tmp/anchor-live-graph.json

echo "==> starting a worker against the real provider"
# Built rather than `go run`: go run spawns the binary as a CHILD process, so
# $! is the wrapper's pid and killing it would orphan the worker -- leaving a
# process holding a lease and talking to a paid API after the script exits.
WORKER_BIN="$(mktemp -d)/anchor-worker"
go build -o "$WORKER_BIN" ./cmd/anchor-worker
"$WORKER_BIN" -provider anthropic -pool 1 -id live-check &
WORKER_PID=$!
trap 'kill -TERM "$WORKER_PID" 2>/dev/null || true; wait "$WORKER_PID" 2>/dev/null || true' EXIT
sleep 2

echo "==> submitting"
OUT=$(go run ./cmd/anchorctl submit -agent live -input '{}' -budget-tokens 20000)
echo "$OUT"
RID=$(echo "$OUT" | awk '{print $3}')

echo "==> waiting for the run to finish"
for _ in $(seq 1 60); do
  if command -v psql >/dev/null 2>&1; then
    STATUS=$(psql -qtAX "$DSN" -c "SELECT status FROM runs WHERE id = '$RID'")
  else
    STATUS=$(docker exec -i anchor-postgres psql -qtAX -U anchor -d anchor \
      -c "SELECT status FROM runs WHERE id = '$RID'")
  fi
  case "$STATUS" in
    completed|failed|cancelled|needs_review) break ;;
  esac
  sleep 1
done

echo
go run ./cmd/anchorctl show -run "$RID"
echo

if [ "$STATUS" = "completed" ]; then
  echo "PASS: one real end-to-end run against the Anthropic API completed"
else
  echo "FAIL: run ended as '$STATUS'" >&2
  exit 1
fi
