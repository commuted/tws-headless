#!/bin/bash
#
# gateway_watch.sh - Report when IB Gateway stops serving the API. Never acts.
#
# WHY THIS EXISTS
#   On 2026-08-11 at 23:45 IB Gateway went down at its daily reset on descartes
#   and never came back. The live engine did exactly what it should — retried
#   the connection 939 times over 15.6 hours — but there was nothing to connect
#   to, nothing restarted the gateway, and nothing told anybody. A live account
#   holding 50 GLD went a whole trading session unmanaged, noticed the next
#   afternoon. The engine cannot cover this: when the gateway is gone the engine
#   is blind by definition, so the watch has to live outside it.
#
# WHY IT ONLY REPORTS
#   This script used to start the gateway when it found it missing. That was
#   the same mistake as the TWS relaunch removed on 2026-08-14: IB Gateway asks
#   for credentials on startup, so launching it cannot restore an unattended
#   system. It replaces "nothing running" with "a login prompt nobody is at",
#   and on a one-minute timer it would leave a fresh prompt behind every minute.
#   Three such orphaned prompts from the TWS equivalent is what exposed this.
#
#   So: no launching, no killing, no credentials. The alert reaching a human IS
#   the recovery path. Gateway restarts are IB's own job (AutoRestart=1 in
#   jts.ini handles the daily reset and keeps its session for about a week);
#   this only notices when that has not worked.
#
# WHAT IT DOES  (run it every minute from a timer or cron)
#   port open                  -> healthy, silent, exit 0
#   port closed, process alive -> gateway up but not serving the API, in
#                                 practice a login or 2FA prompt: exit 2
#   port closed, no process    -> gateway is gone entirely: exit 2
#
#   Both failures exit non-zero so the systemd unit fails and shows up in
#   `systemctl --user status` and any unit-state monitoring, not only in a log
#   file nobody reads. Transitions are appended to logs/gateway_watch.log; a
#   healthy run stays silent so the log holds only events worth reading.
#
#   It does not touch the trading engine either. The engine's supervisor owns
#   that, and its connection manager reconnects on its own once the gateway is
#   back — it was already doing so correctly throughout the outage above.
#
# USAGE
#   ./gateway_watch.sh [--port PORT]
#
#     --port   API port to check (default 4001, the live gateway; 4002 paper)
#

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT=4001

while (($#)); do
    case "$1" in
        --port) PORT="$2"; shift 2 ;;
        *) echo "gateway_watch.sh: unknown argument '$1'" >&2; exit 64 ;;
    esac
done

LOG_DIR="$SCRIPT_DIR/logs"
LOG="$LOG_DIR/gateway_watch.log"
mkdir -p "$LOG_DIR"

note() { echo "$(date -Is) $*" | tee -a "$LOG" >&2; }

port_open() {
    # bash's /dev/tcp rather than ss/lsof: no extra dependency, and it proves
    # something will actually accept a connection, which is what the engine
    # needs. The subshell both opens and closes the descriptor; its exit status
    # is the answer, so nothing leaks into this shell.
    (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null
}

gateway_running() { pgrep -f "Jts/ibgateway/.*/ibgateway|install4j.ibgateway" >/dev/null 2>&1; }

if port_open; then
    # Only log recoveries, so a healthy minute-by-minute run stays silent.
    if [[ -f "$LOG_DIR/.gateway_down" ]]; then
        note "RECOVERED: IB Gateway is serving the API on port $PORT again"
        rm -f "$LOG_DIR/.gateway_down"
    fi
    exit 0
fi

touch "$LOG_DIR/.gateway_down"

if gateway_running; then
    note "NEEDS ATTENTION: IB Gateway is running but port $PORT is closed — " \
         "almost certainly waiting at a login or 2FA prompt. Log in on the " \
         "console; the engine cannot trade until you do."
else
    note "NEEDS ATTENTION: IB Gateway is not running and port $PORT is closed. " \
         "Start it and log in on the console; the engine cannot trade until " \
         "you do. Deliberately not started here — it would only leave a login " \
         "prompt nobody is at, one per run."
fi
exit 2
