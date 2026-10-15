#!/usr/bin/env bash
#
# Runs Anchor's benchmark sweep and prints figures for docs/NUMBERS.md.
#
# Everything here measures ANCHOR: the worker uses the mock provider at a fixed
# latency, so the numbers describe queueing, claiming, journaling and projection
# cost rather than a model's response time.
#
#   ./scripts/bench.sh [rate] [duration] [workers]
#
set -euo pipefail

RATE=${1:-50}
DURATION=${2:-30s}
POOL=${3:-8}
LATENCY=${ANCHOR_BENCH_LATENCY:-50ms}
SCHEMA=${ANCHOR_BENCH_SCHEMA:-bench}

DSN=${ANCHOR_DATABASE_URL:-postgres://anchor:anchor@localhost:5433/anchor?sslmode=disable}
export ANCHOR_DATABASE_URL="$DSN"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BIN="$(mktemp -d)"
PIDS=()
cleanup() {
  for pid in "${PIDS[@]:-}"; do [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT INT TERM

psql_do() {
  docker exec -e PGOPTIONS="-c search_path=$SCHEMA" -i anchor-postgres \
    psql -qtAX -U anchor -d anchor -c "$1"
}

echo "==> building"
go build -o "$BIN/anchor-server" ./cmd/anchor-server
go build -o "$BIN/anchor-worker" ./cmd/anchor-worker
go build -o "$BIN/anchorctl" ./cmd/anchorctl

echo "==> preparing schema '$SCHEMA'"
docker exec -i anchor-postgres psql -qtAX -U anchor -d anchor \
  -c "CREATE SCHEMA IF NOT EXISTS $SCHEMA" >/dev/null
"$BIN/anchorctl" migrate -schema "$SCHEMA" >/dev/null
psql_do "TRUNCATE queue, leases, steps, run_events, runs, agents RESTART IDENTITY CASCADE" >/dev/null

cat > "$BIN/graph.json" <<'JSON'
{"agent":"bench","v":1,"nodes":[
  {"id":"n1","type":"model_call","model":"mock","prompt":"bench {{input.n}}"},
  {"id":"n2","type":"tool_call","tool":"echo","input":{"said":"{{n1.output}}"}}
]}
JSON
"$BIN/anchorctl" agent-register -schema "$SCHEMA" -name bench -file "$BIN/graph.json" >/dev/null

echo "==> starting server and $POOL-goroutine worker (mock latency $LATENCY)"
PGOPTIONS="-c search_path=$SCHEMA" "$BIN/anchor-server" -addr :18080 -migrate=false \
  > "$BIN/server.log" 2>&1 &
PIDS+=($!)
"$BIN/anchor-worker" -schema "$SCHEMA" -pool "$POOL" -id bench-worker \
  -mock-latency "$LATENCY" -metrics-addr :19090 > "$BIN/worker.log" 2>&1 &
PIDS+=($!)
sleep 3

echo "==> k6: $RATE submissions/sec for $DURATION"
k6 run --quiet --summary-export "$BIN/k6.json" \
  -e ANCHOR_URL=http://localhost:18080 -e AGENT=bench \
  -e RATE="$RATE" -e DURATION="$DURATION" \
  loadtest/submit.js 2>&1 | tail -20

echo
echo "==> draining the queue"
START_DRAIN=$(date +%s)
for _ in $(seq 1 300); do
  PENDING=$(psql_do "SELECT (SELECT count(*) FROM queue) + (SELECT count(*) FROM leases)")
  [ "$PENDING" -eq 0 ] && break
  sleep 1
done
DRAIN=$(( $(date +%s) - START_DRAIN ))

# Quantiles come from the worker's Prometheus histograms, not from k6: k6 never
# sees scheduling latency or step duration, because both happen after the API
# has already returned 202.
echo "==> reading worker metrics"
METRICS=$(curl -s localhost:19090/metrics)

# Quantiles are read from the worker's Prometheus histograms with python3,
# not awk: macOS ships BSD awk, which has no match(str, regex, array) -- that
# is a GNU extension, and a script relying on it fails only on a Mac.
#
# Buckets are summed ACROSS LABELS. A labelled histogram emits one series per
# label set, so step_duration has a kind="model" series and a kind="tool" one;
# treating them as a single sorted list gives a nonsense answer (a 50ms model
# call reported as a 1ms p99). An optional third argument filters to one label
# instead, which is what makes "model" and "tool" separately reportable.
quantile() { # histogram, q, [label-filter]
  printf '%s' "$METRICS" | python3 -c '
import re, sys
from collections import defaultdict

hist, q = sys.argv[1], float(sys.argv[2])
want = sys.argv[3] if len(sys.argv) > 3 else ""

buckets, total = defaultdict(float), 0.0
for line in sys.stdin:
    line = line.strip()
    if want and want not in line:
        continue
    if line.startswith(hist + "_bucket"):
        m = re.search(r"le=\"([^\"]+)\"[^ ]*\s+([0-9.e+naN-]+)$", line)
        if m:
            le = float("inf") if m.group(1) == "+Inf" else float(m.group(1))
            try:
                buckets[le] += float(m.group(2))
            except ValueError:
                pass
    elif line.startswith(hist + "_count"):
        try:
            total += float(line.split()[-1])
        except ValueError:
            pass

if total == 0 or not buckets:
    print("n/a"); sys.exit()
target = total * q
for le in sorted(buckets):
    if buckets[le] >= target:
        print("inf" if le == float("inf") else "%.0f" % (le * 1000)); sys.exit()
print("inf")
' "$1" "$2" "${3:-}"
}

COMPLETED=$(psql_do "SELECT count(*) FROM runs WHERE status='completed'")
TOTAL=$(psql_do "SELECT count(*) FROM runs")
STEPS=$(psql_do "SELECT count(*) FROM steps WHERE status='completed'")
DUR_SECONDS=$(echo "$DURATION" | sed 's/s$//')
THROUGHPUT=$(awk -v s="$STEPS" -v d="$((DUR_SECONDS + DRAIN))" 'BEGIN{printf "%.1f", s/d}')

echo
echo "────────────────────────────────────────────────────────"
echo "  hardware            $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(sysctl -n hw.ncpu 2>/dev/null || nproc) cores"
echo "  config              rate=$RATE/s duration=$DURATION pool=$POOL mock-latency=$LATENCY"
echo "  runs submitted      $TOTAL"
echo "  runs completed      $COMPLETED"
echo "  steps completed     $STEPS"
echo "  drain after load    ${DRAIN}s"
printf "  throughput          %s steps/sec\n" "$THROUGHPUT"
echo "  scheduling latency  p50 $(quantile anchor_scheduling_latency_seconds 0.50)ms  p95 $(quantile anchor_scheduling_latency_seconds 0.95)ms  p99 $(quantile anchor_scheduling_latency_seconds 0.99)ms"
echo "  queue wait          p50 $(quantile anchor_queue_wait_seconds 0.50)ms  p99 $(quantile anchor_queue_wait_seconds 0.99)ms"
echo "  model step          p50 $(quantile anchor_step_duration_seconds 0.50 'kind="model"')ms  p99 $(quantile anchor_step_duration_seconds 0.99 'kind="model"')ms"
echo "  tool step           p50 $(quantile anchor_step_duration_seconds 0.50 'kind="tool"')ms  p99 $(quantile anchor_step_duration_seconds 0.99 'kind="tool"')ms"
echo "────────────────────────────────────────────────────────"
echo
echo "Copy these into docs/NUMBERS.md with today's date and the exact command."
