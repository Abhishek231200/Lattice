#!/usr/bin/env bash
# Autoscaler demo: ramp load with pgbench, observe scale-up, then idle-out to suspension.
#
# Prerequisites: pgbench on PATH, psql on PATH, Lattice stack running.
# Usage: ./bench/scenarios/autoscaler_demo.sh

set -euo pipefail

PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5433}"
PG_USER="${PG_USER:-lattice}"
PG_DB="${PG_DB:-bench}"
CP_URL="${CP_URL:-http://localhost:5002}"
DURATION_RAMP=60    # seconds of load ramp
DURATION_IDLE=90    # seconds of idle after ramp (trigger suspend)

echo "=== Lattice Autoscaler Demo ==="
echo "Postgres: $PG_HOST:$PG_PORT/$PG_DB"
echo "Control plane: $CP_URL"
echo ""

# 1. Initialize pgbench
echo "[1/5] Initializing pgbench schema..."
pgbench -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -i -s 10 2>&1 | tail -5

# 2. Baseline — record current decisions count
echo "[2/5] Baseline decisions count..."
DECISIONS_BEFORE=$(curl -sf "$CP_URL/autoscaler/decisions" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))")
echo "  decisions before: $DECISIONS_BEFORE"

# 3. Ramp load
echo "[3/5] Ramping load for ${DURATION_RAMP}s (watch Grafana → compute units rising)..."
pgbench -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" \
    -T "$DURATION_RAMP" -c 32 -j 4 --progress 10 2>&1 &
BENCH_PID=$!
wait "$BENCH_PID" || true

# 4. Idle
echo "[4/5] Load stopped. Idling for ${DURATION_IDLE}s (watch compute units fall, then suspend)..."
sleep "$DURATION_IDLE"

# 5. Report
echo "[5/5] Results:"
DECISIONS=$(curl -sf "$CP_URL/autoscaler/decisions")
DECISIONS_AFTER=$(echo "$DECISIONS" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))")
SCALE_UPS=$(echo "$DECISIONS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(sum(1 for x in d if x['action']=='ScaleUp'))")
SCALE_DOWNS=$(echo "$DECISIONS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(sum(1 for x in d if x['action']=='ScaleDown'))")
SUSPENSIONS=$(echo "$DECISIONS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(sum(1 for x in d if x['action']=='Suspend'))")

echo "  Scale-up decisions:   $SCALE_UPS"
echo "  Scale-down decisions: $SCALE_DOWNS"
echo "  Suspensions:          $SUSPENSIONS"
echo ""
echo "Open Grafana (http://localhost:3000) → Lattice Autoscaler dashboard"
echo "to see the compute units / p99 latency / CPU timeline."
