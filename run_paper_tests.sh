#!/usr/bin/env bash
# run_paper_tests.sh — Cron-safe wrapper for paper trading test plugins.
#
# Ensures the engine is running and connected, then delegates to
# run_paper_tests.py with the supplied flags.
#
# Usage:
#   ./run_paper_tests.sh                  # orders 1-5 (market hours)
#   ./run_paper_tests.sh --open           # MOO / LOO / Auction  (before 9:25 AM ET)
#   ./run_paper_tests.sh --close          # MOC / LOC            (before 3:50 PM ET)
#   ./run_paper_tests.sh --orders6        # round-trip lifecycle  (after open)
#   ./run_paper_tests.sh --feeds          # feed tests
#   ./run_paper_tests.sh --historical     # historical data tests
#   ./run_paper_tests.sh --all            # everything except --open / --close
#   ./run_paper_tests.sh --only 1 3       # specific order plugins
#   ./run_paper_tests.sh --dry-run        # show plan, don't execute
#   Any extra flags are forwarded to run_paper_tests.py unchanged.
#
# Suggested cron schedule (US Eastern, weekdays only):
#   00 08 * * 1-5  /home/ron/claude/ib/run_paper_tests.sh --open
#   35 09 * * 1-5  /home/ron/claude/ib/run_paper_tests.sh --orders6
#   00 10 * * 1-5  /home/ron/claude/ib/run_paper_tests.sh
#   30 15 * * 1-5  /home/ron/claude/ib/run_paper_tests.sh --close
#
# Logs are written to /tmp/paper_tests_<mode>_<timestamp>.log

set -euo pipefail

IB_DIR="/home/ron/claude/ib"
IBCTL="python3 $IB_DIR/ibctl.py"
ENGINE_PORT="${ENGINE_PORT:-7497}"
ENGINE_STARTUP_WAIT="${ENGINE_STARTUP_WAIT:-12}"

# ---------------------------------------------------------------------------
# Derive a short label from the first recognisable flag for the log filename.
# ---------------------------------------------------------------------------
MODE="orders"
for arg in "$@"; do
    case "$arg" in
        --open)     MODE="open";     break ;;
        --close)    MODE="close";    break ;;
        --orders6)  MODE="orders6";  break ;;
        --feeds)    MODE="feeds";    break ;;
        --historical) MODE="historical"; break ;;
        --all)      MODE="all";      break ;;
        --dry-run)  MODE="dry-run";  break ;;
    esac
done

LOG="/tmp/paper_tests_${MODE}_$(date +%Y%m%d_%H%M%S).log"
exec >> "$LOG" 2>&1

echo "=== paper tests [${MODE}] started: $(date) ==="
echo "    args: $*"
echo "    log:  $LOG"

cd "$IB_DIR"

# ---------------------------------------------------------------------------
# 1. Ensure engine is running
# ---------------------------------------------------------------------------
if ! pgrep -f "ib.run_engine" > /dev/null 2>&1; then
    echo "Engine not running — starting on port ${ENGINE_PORT}..."
    python3 -m ib.run_engine \
        --port "$ENGINE_PORT" \
        --mode immediate \
        --client-id 4 \
        --verbose \
        >> /tmp/engine.log 2>&1 &
    echo "Engine PID: $!"
    sleep "$ENGINE_STARTUP_WAIT"
else
    echo "Engine already running (PID: $(pgrep -f 'ib.run_engine' | head -1))"
fi

# ---------------------------------------------------------------------------
# 2. Verify engine is responsive AND connected to IB
# ---------------------------------------------------------------------------
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
echo "Engine responsive and connected."

# ---------------------------------------------------------------------------
# 3. Delegate to run_paper_tests.py (handles load, run, report)
# ---------------------------------------------------------------------------
python3 "$IB_DIR/run_paper_tests.py" "$@"

echo "=== paper tests [${MODE}] done: $(date) ==="
