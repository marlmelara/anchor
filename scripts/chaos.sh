#!/usr/bin/env bash
#
# Anchor chaos demo: kill workers mid-flight and show that no run is lost and
# no effect happens twice.
#
# This is the demo from the README. Run it beside the trace viewer and kill a
# worker on camera; the viewer stalls for the lease timeout, then another worker
# picks the run up at the exact next step.
#
#   ./scripts/chaos.sh [runs] [workers] [chaos-seconds]
#
# Everything happens in its own Postgres schema, so running this never destroys
# whatever is in the development database.
set -euo pipefail

RUNS=${1:-40}
WORKERS=${2:-3}
DURATION=${3:-20}
SCHEMA=${ANCHOR_CHAOS_SCHEMA:-chaos_demo}

# One run per goroutine at a time, so concurrency is WORKERS * POOL. These
# defaults are chosen so the work OUTLASTS the chaos loop: with 3 workers x 1
# goroutine and a 2s model call, 40 runs take roughly 40 * 2 / 3 = 27s against a
# 20s kill loop. Get this wrong and the kills land on idle workers, nothing is
# ever interrupted, and the demo shows a recovery that never happened.
POOL=${ANCHOR_CHAOS_POOL:-1}
LATENCY=${ANCHOR_CHAOS_LATENCY:-2s}

DSN=${ANCHOR_DATABASE_URL:-postgres://anchor:anchor@localhost:5433/anchor?sslmode=disable}
export ANCHOR_DATABASE_URL="$DSN"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BIN_DIR="$(mktemp -d)"
WORKER_BIN="$BIN_DIR/anchor-worker"
CTL_BIN="$BIN_DIR/anchorctl"
PIDS=()

cleanup() {
  # Kill by pid and do NOT call a bare `wait`. An unqualified wait blocks on
  # every background job, so a single survivor hangs the script forever -- which
  # is exactly what happened while the pid bug above was live.
  for pid in "${PIDS[@]:-}"; do
    [ -n "$pid" ] || continue
    kill -9 "$pid" 2>/dev/null || true
    # Reap this one pid. Without it bash prints "Killed: 9" for each survivor
    # as the script exits, which lands after the summary -- the last thing on
    # screen when this is being recorded. Targeted, so the warning above holds.
    wait "$pid" 2>/dev/null || true
  done
  # Belt and braces: anything still wearing this run's schema label.
  pkill -f "anchor-worker -id chaos-.* -schema $SCHEMA" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# psql pinned to the demo schema via PGOPTIONS, so these queries can never read
# or write the default schema by accident.
#
# Two ways to reach Postgres, because this runs in two places. Locally it is a
# docker compose container; in CI it is a GitHub Actions service with no
# container name to exec into, but psql is on PATH. Preferring a direct psql
# when one exists covers both without the caller choosing.
if command -v psql >/dev/null 2>&1; then
  psql_do() { PGOPTIONS="-c search_path=$SCHEMA" psql -qtAX "$DSN" -c "$1"; }
  psql_raw() { psql -qtAX "$DSN" -c "$1"; }
else
  psql_do() {
    docker exec -e PGOPTIONS="-c search_path=$SCHEMA" -i anchor-postgres \
      psql -qtAX -U anchor -d anchor -c "$1"
  }
  psql_raw() { docker exec -i anchor-postgres psql -qtAX -U anchor -d anchor -c "$1"; }
fi

echo "==> building binaries once"
# Built, not `go run`: go run launches the binary as a CHILD, so $! would be the
# wrapper's pid and kill -9 would leave the real worker running. It also saves a
# process launch per submitted run.
go build -o "$WORKER_BIN" ./cmd/anchor-worker
go build -o "$CTL_BIN" ./cmd/anchorctl

echo "==> preparing schema '$SCHEMA'"
psql_raw "CREATE SCHEMA IF NOT EXISTS $SCHEMA" >/dev/null
"$CTL_BIN" migrate -schema "$SCHEMA" >/dev/null
psql_do "DROP TABLE IF EXISTS chaos_effects" >/dev/null
psql_do "DROP TABLE IF EXISTS chaos_attempts" >/dev/null
psql_do "TRUNCATE queue, leases, steps, run_events, runs, agents RESTART IDENTITY CASCADE" >/dev/null

echo "==> registering the chaos agent"
cat > "$BIN_DIR/graph.json" <<'JSON'
{"agent":"chaos","v":1,"nodes":[
  {"id":"n1","type":"model_call","model":"mock","prompt":"think about {{input.marker}}"},
  {"id":"n2","type":"tool_call","tool":"chaos_count","input":{"marker":"{{input.marker}}"}}
]}
JSON
"$CTL_BIN" agent-register -schema "$SCHEMA" -name chaos -file "$BIN_DIR/graph.json"

echo "==> submitting $RUNS runs"
for i in $(seq 1 "$RUNS"); do
  "$CTL_BIN" submit -schema "$SCHEMA" -agent chaos -input "{\"marker\":\"run-$i\"}" >/dev/null
done

start_worker() {
  # Process substitution, NOT a pipeline. In `cmd | sed &` the shell sets $! to
  # the LAST command in the pipeline -- sed -- so kill -9 would take out the log
  # filter and leave the worker running. With `> >(sed ...)` the worker itself is
  # the backgrounded command, so $! is the pid we actually need to kill.
  "$WORKER_BIN" -id "chaos-$1" -pool "$POOL" -lease 2s -heartbeat 700ms \
    -reap 500ms -mock-latency "$LATENCY" -chaos-tools -schema "$SCHEMA" \
    > >(sed "s/^/[worker-$1] /") 2>&1 &
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
for _ in $(seq 1 90); do
  done_count=$(psql_do "SELECT count(*) FROM runs WHERE status = 'completed'")
  [ "$done_count" -eq "$RUNS" ] && break
  sleep 1
done

COMPLETED=$(psql_do "SELECT count(*) FROM runs WHERE status = 'completed'")
EFFECTS=$(psql_do "SELECT count(*) FROM chaos_effects")
ATTEMPTS=$(psql_do "SELECT count(*) FROM chaos_attempts")
RESUMED=$(psql_do "SELECT count(*) FROM run_events WHERE type = 'RunStarted' AND payload->>'resumed' = 'true'")

echo
echo "────────────────────────────────────────────"
printf "  worker kills          %s\n" "$KILLS"
printf "  runs resumed          %s\n" "$RESUMED"
printf "  runs completed        %s / %s\n" "$COMPLETED" "$RUNS"
printf "  effects (once each)   %s\n" "$EFFECTS"
printf "  tool executions       %s\n" "$ATTEMPTS"
printf "  retried after a kill  %s\n" "$((ATTEMPTS - EFFECTS))"
echo "────────────────────────────────────────────"

STATUS=0
[ "$COMPLETED" -eq "$RUNS" ] || { echo "  FAIL: runs were lost"; STATUS=1; }
# chaos_effects is keyed by marker, so its count is the number of
# EFFECTIVELY-ONCE effects -- what Anchor promises: at-least-once delivery plus
# an idempotent effect. Executions may exceed it under chaos, because a worker
# killed in the ambiguous window leaves a step whose outcome is unknown and an
# idempotent tool is retried by design. That excess is evidence the kills landed
# mid-effect, not a failure.
[ "$EFFECTS" -eq "$RUNS" ]     || { echo "  FAIL: $EFFECTS effectively-once effects for $RUNS runs"; STATUS=1; }
[ "$ATTEMPTS" -ge "$RUNS" ]    || { echo "  FAIL: only $ATTEMPTS tool executions for $RUNS runs"; STATUS=1; }
# A green run that never interrupted anything proves nothing. The whole claim is
# that a killed worker's run is picked up and finished, so no resumption means
# the demo failed to demonstrate its own point.
if [ "$KILLS" -gt 0 ] && [ "$RESUMED" -eq 0 ]; then
  echo "  FAIL: $KILLS kills but no run was resumed -- the chaos never interrupted one."
  echo "        The work finished before the kill loop could bite. Raise the run"
  echo "        count or ANCHOR_CHAOS_LATENCY so work outlasts the chaos."
  STATUS=1
fi

if [ "$STATUS" -eq 0 ]; then
  echo "  PASS: no runs lost, every effect applied exactly once under $KILLS kills"
fi

echo
echo "==> verifying projections still match the event log"
"$CTL_BIN" rebuild -schema "$SCHEMA" -verify

exit "$STATUS"
