#!/bin/bash
#
# start_trading.sh - Start the IB Trading Engine under a restart supervisor
#
# Usage:
#   ./start_trading.sh              # Default: paper trading on port 7497
#   ./start_trading.sh 4002         # Use IB Gateway paper port
#   ./start_trading.sh 7497 immediate  # Actually place paper orders
#   ./start_trading.sh 7497 dry_run /path/to/plugins  # Custom plugin directory
#
# Supervision:
#   The engine is restarted automatically with exponential backoff when it
#   exits abnormally (startup aborts like a missing managed account, crashes).
#   It is NOT restarted after:
#     - a clean exit (code 0 — graceful shutdown), or
#     - an operator interrupt/terminate (SIGINT/SIGTERM, codes 130/143), or
#     - Ctrl+C delivered to this wrapper while the engine runs.
#   Backoff: 5s doubling to 300s, reset after any run lasting >= 10 minutes.
#   Every restart is appended to logs/engine_restarts.log — point an external
#   liveness check at that file: lines appearing there mean the engine is
#   flapping; the wrapper itself dying means nothing is running at all.
#
#   NO_RESTART=1 ./start_trading.sh ...   # old behavior: single run, no loop
#
# STATE restore:
#   RESTORE_STATE=1 ./start_trading.sh ...            # restore ./STATE.json
#   RESTORE_STATE=/path/STATE.json ./start_trading.sh # restore a specific file
#   Applies to the FIRST run only. A supervised restart must not wind the engine
#   back to the snapshot: by then the restored state is already on disk and the
#   session has moved past it, so re-applying an old snapshot would discard live
#   progress. The engine writes a fresh ./STATE.json on every clean stop unless
#   NO_STATE_ON_STOP=1.
#
# Lid-switch inhibit:
#   While the engine process is alive, a systemd handle-lid-switch inhibitor
#   keeps closing the clamshell from suspending the machine (2026-07-29: a
#   closed lid put the box to sleep through five market hours).  logind
#   honors handle-lid-switch locks unconditionally — unlike plain "sleep"
#   inhibitors, which lid actions ignore by default.  The lock lives exactly
#   as long as the engine: during backoff waits and after exit the lid
#   behaves normally.  Idle/automatic suspend and a manual suspend are NOT
#   blocked, only the lid switch.
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export PORT="${1:-7497}"
export MODE="${2:-dry_run}"
export IB_PLUGIN_DIR="${3:-$SCRIPT_DIR/plugins}"

# Run from project root (where ib/ and plugins/ both live)
cd "$SCRIPT_DIR"

# See "Lid-switch inhibit" in the header.  Degrades to a bare run where
# systemd-inhibit is unavailable (containers, non-systemd boxes).
INHIBIT=()
if command -v systemd-inhibit >/dev/null 2>&1; then
    INHIBIT=(systemd-inhibit --what=handle-lid-switch
             --who="tws-engine"
             --why="IB trading engine running (port $PORT, mode $MODE)"
             --mode=block)
fi

# STATE flags. RESTORE_STATE=1 means "the default ./STATE.json"; any other value
# is taken as an explicit path.
STATE_ARGS=()
if [[ -n "${RESTORE_STATE:-}" ]]; then
    if [[ "$RESTORE_STATE" == "1" ]]; then
        STATE_ARGS+=(--restore-state)
    else
        STATE_ARGS+=(--restore-state "$RESTORE_STATE")
    fi
fi
if [[ "${NO_STATE_ON_STOP:-0}" == "1" ]]; then
    STATE_ARGS+=(--no-state-on-stop)
fi

if [[ "${NO_RESTART:-0}" == "1" ]]; then
    exec "${INHIBIT[@]}" python3 -m ib.run_engine --port "$PORT" --mode "$MODE" "${STATE_ARGS[@]}"
fi

BACKOFF=5
BACKOFF_MAX=300
RUN_RESET_SECS=600
RESTART_LOG="$SCRIPT_DIR/logs/engine_restarts.log"
mkdir -p "$SCRIPT_DIR/logs"

# Ctrl+C / kill reaches the engine too (same process group); remember that it
# happened so we stop instead of restarting once the engine finishes its
# graceful shutdown (which needs 3x Ctrl+C within 10s and may exit 0).
INTERRUPTED=0
trap 'INTERRUPTED=1' INT TERM

while true; do
    START_TS=$(date +%s)
    "${INHIBIT[@]}" python3 -m ib.run_engine --port "$PORT" --mode "$MODE" "${STATE_ARGS[@]}"
    CODE=$?
    ELAPSED=$(( $(date +%s) - START_TS ))

    # Drop --restore-state after the first run — see "STATE restore" in the
    # header. --no-state-on-stop, if set, must survive every restart.
    STATE_ARGS=()
    if [[ "${NO_STATE_ON_STOP:-0}" == "1" ]]; then
        STATE_ARGS+=(--no-state-on-stop)
    fi

    if [[ "$INTERRUPTED" == "1" || "$CODE" == "0" || "$CODE" == "130" || "$CODE" == "143" ]]; then
        echo "Engine exited (code=$CODE after ${ELAPSED}s) — not restarting."
        exit "$CODE"
    fi

    # Code 3: another engine already owns this environment (ib/environment.py
    # EXIT_ALREADY_RUNNING). Retrying cannot fix a misconfiguration, and
    # retrying quietly is how this went unnoticed for eight hours on
    # 2026-08-05 — a second supervisor relaunching every five minutes while
    # the first engine held the client id, so no orders were ever placed.
    if [[ "$CODE" == "3" ]]; then
        MSG="$(date -Is) engine refused to start: another engine already owns this environment — not restarting"
        echo "$MSG" >&2
        echo "$MSG" >> "$RESTART_LOG"
        exit "$CODE"
    fi

    # A healthy long run means the previous failure was resolved; start the
    # backoff ladder over instead of climbing from where it left off.
    if (( ELAPSED >= RUN_RESET_SECS )); then
        BACKOFF=5
    fi

    MSG="$(date -Is) engine exited code=$CODE after ${ELAPSED}s — restarting in ${BACKOFF}s"
    echo "$MSG" >&2
    echo "$MSG" >> "$RESTART_LOG"

    sleep "$BACKOFF" &
    wait $!   # backgrounded sleep + wait so INT/TERM interrupts the delay
    if [[ "$INTERRUPTED" == "1" ]]; then
        echo "Interrupted during backoff — not restarting."
        exit 130
    fi

    BACKOFF=$(( BACKOFF * 2 ))
    (( BACKOFF > BACKOFF_MAX )) && BACKOFF=$BACKOFF_MAX
done
