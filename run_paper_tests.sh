#!/usr/bin/env bash
# Run paper_test_orders_6 at market open.
# Scheduled via cron; logs to /tmp/paper_tests_<date>.log

set -euo pipefail

LOG="/tmp/paper_tests_$(date +%Y%m%d_%H%M%S).log"
IB_DIR="/home/ron/claude/ib"
IBCTL="python3 $IB_DIR/ibctl.py"

exec >> "$LOG" 2>&1
echo "=== paper_test_orders_6 run: $(date) ==="

cd "$IB_DIR"

# ── 1. Ensure engine is running ──────────────────────────────────────────────
if ! pgrep -f "ib.run_engine" > /dev/null 2>&1; then
    echo "Engine not running — starting..."
    python3 -m ib.run_engine --port 7497 --mode immediate --client-id 4 --verbose \
        >> /tmp/engine.log 2>&1 &
    echo "Engine PID: $!"
    sleep 12   # wait for connect + market-data probe
else
    echo "Engine already running (PID: $(pgrep -f 'ib.run_engine' | head -1))"
fi

# ── 2. Verify engine is responsive AND connected to IB ──────────────────────
STATUS=$($IBCTL status 2>&1) || true
if ! echo "$STATUS" | grep -q "Engine: running"; then
    echo "ERROR: engine not responding — aborting"
    exit 1
fi
if ! echo "$STATUS" | grep -q "Connected: True"; then
    echo "ERROR: engine running but not connected to IB — aborting"
    echo "       Check that TWS/Gateway is open and accepting connections."
    exit 1
fi
echo "Engine responsive and connected to IB."

# ── 3. Load & start plugin (idempotent — engine ignores duplicate loads) ─────
$IBCTL plugin load plugins.paper_tests.paper_test_orders_6 2>&1 || true
$IBCTL plugin start paper_test_orders_6 2>&1 || true

# ── 4. Run tests (long timeout — full suite takes ~15 min) ──────────────────
echo "Triggering run_tests..."
$IBCTL plugin request paper_test_orders_6 run_tests --timeout 1800 2>&1

echo "=== Done: $(date) ==="
