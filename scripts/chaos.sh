#!/usr/bin/env bash
#
# Anchor chaos demo: kill workers mid-flight and show that no run is lost and
# no effect happens twice.
#
# This is the demo from the README. Run it beside the trace viewer and kill a
# worker on camera; the viewer stalls for the lease timeout, then another worker
# picks the run up at the exact next step.
#
#   ./scripts/chaos.sh [runs] [workers] [seconds]
#
set -euo pipefail

RUNS=${1:-40}
WORKERS=${2:-3}
DURATION=${3:-30}

DSN=${ANCHOR_DATABASE_URL:-postgres://anchor:anchor@localhost:5433/anchor?sslmode=disable}
export ANCHOR_DATABASE_URL="$DSN"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BIN="$(mktemp -d)/anchor-worker"
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    kill -9 "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

psql_do() { docker exec -i anchor-postgres psql -qtAX -U anchor -d anchor -c "$1"; }

echo "==> building worker"
go build -o "$BIN" ./cmd/anchor-worker

echo "==> applying migrations"
go run ./cmd/anchorctl migrate >/dev/null

echo "==> resetting demo state"
psql_do "DROP TABLE IF EXISTS chaos_effects" >/dev/null
psql_do "TRUNCATE queue, leases, steps, run_events, runs, agents RESTART IDENTITY CASCADE" >/dev/null

echo "==> registering the chaos agent"
cat > /tmp/anchor-chaos-graph.json <<'JSON'
{"agent":"chaos","v":1,"nodes":[
  {"id":"n1","type":"model_call","model":"mock","prompt":"think about {{input.marker}}"},
  {"id":"n2","type":"tool_call","tool":"chaos_count","input":{"marker":"{{input.marker}}"}}
]}
JSON
go run ./cmd/anchorctl agent-register -name chaos -file /tmp/anchor-chaos-graph.json

echo "==> submitting $RUNS runs"
for i in $(seq 1 "$RUNS"); do
  go run ./cmd/anchorctl submit -agent chaos -input "{\"marker\":\"run-$i\"}" >/dev/null
done

start_worker() {
  "$BIN" -id "chaos-$1" -pool 2 -lease 2s -heartbeat 700ms \
         -reap 500ms -mock-latency 400ms -chaos-tools 2>&1 | sed "s/^/[worker-$1] /" &
  PIDS[$1]=$!
}

echo "==> starting $WORKERS workers"
for w in $(seq 1 "$WORKERS"); do start_worker "$w"; done

echo
echo "==> chaos: SIGKILL a random worker every 3s for ${DURATION}s"
echo "    (watch the trace viewer: a killed worker's run stalls for the lease"
echo "     timeout, then another worker continues it from the next step)"
echo
KILLS=0
END=$((SECONDS + DURATION))
while [ $SECONDS -lt $END ]; do
  sleep 3
  victim=$(( (RANDOM % WORKERS) + 1 ))
  echo "    !! kill -9 worker-$victim"
  kill -9 "${PIDS[$victim]}" 2>/dev/null || true
  wait "${PIDS[$victim]}" 2>/dev/null || true
  start_worker "$victim"
  KILLS=$((KILLS + 1))
done

echo
echo "==> chaos over after $KILLS kills; waiting for the remaining runs"
for _ in $(seq 1 60); do
  done_count=$(psql_do "SELECT count(*) FROM runs WHERE status = 'completed'")
  [ "$done_count" -eq "$RUNS" ] && break
  sleep 1
done

COMPLETED=$(psql_do "SELECT count(*) FROM runs WHERE status = 'completed'")
EFFECTS=$(psql_do "SELECT count(*) FROM chaos_effects")
DISTINCT=$(psql_do "SELECT count(DISTINCT marker) FROM chaos_effects")
RESUMED=$(psql_do "SELECT count(*) FROM run_events WHERE type = 'RunStarted' AND payload->>'resumed' = 'true'")

echo
echo "────────────────────────────────────────────"
printf "  worker kills          %s\n" "$KILLS"
printf "  runs resumed          %s\n" "$RESUMED"
printf "  runs completed        %s / %s\n" "$COMPLETED" "$RUNS"
printf "  effects fired         %s\n" "$EFFECTS"
printf "  distinct effects      %s\n" "$DISTINCT"
echo "────────────────────────────────────────────"

STATUS=0
[ "$COMPLETED" -eq "$RUNS" ] || { echo "  FAIL: runs were lost"; STATUS=1; }
[ "$EFFECTS" -eq "$RUNS" ]   || { echo "  FAIL: $EFFECTS effects for $RUNS runs (duplicates)"; STATUS=1; }
[ "$RESUMED" -gt 0 ]         || { echo "  WARN: no run was resumed; chaos never interrupted one"; }

if [ "$STATUS" -eq 0 ]; then
  echo "  PASS: no runs lost, zero duplicate effects under $KILLS kills"
fi

echo
echo "==> verifying projections still match the event log"
go run ./cmd/anchorctl rebuild -verify

exit "$STATUS"
