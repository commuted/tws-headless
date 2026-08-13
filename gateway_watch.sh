#!/bin/bash
#
# gateway_watch.sh - Keep IB Gateway reachable, and make it loud when it is not.
#
# WHY THIS EXISTS
#   On 2026-08-11 at 23:45 IB Gateway went down at its daily reset on descartes
#   and never came back. The live engine did exactly what it should — retried
#   the connection 939 times over 15.6 hours — but there was nothing to connect
#   to, nothing restarted the gateway, and nothing told anybody. A live account
#   holding 50 GLD went an entire trading session unmanaged, and the outage was
#   only noticed the next afternoon.
#
#   The engine cannot cover this. When the gateway is gone the engine is blind
#   by definition, so the watch has to live outside it.
#
# WHAT IT DOES  (run it every minute from a timer or cron)
#   port open                  -> healthy, silent, exit 0
#   port closed, no process    -> start the gateway, log it, exit 1
#   port closed, process alive -> gateway is up but not serving the API, which
#                                 in practice means it is sitting at a login or
#                                 2FA prompt. It cannot be fixed from a script:
#                                 log NEEDS ATTENTION and exit 2.
#
#   Every transition is appended to logs/gateway_watch.log. A non-zero exit
#   makes the systemd unit fail, so `systemctl --user status` and any monitoring
#   that watches unit state both see it.
#
# WHAT IT DELIBERATELY DOES NOT DO
#   It does not touch the trading engine. The engine's own supervisor handles
#   that, and its connection manager reconnects on its own once the gateway is
#   back — it was already doing so correctly throughout the outage.
#
#   It does not store or type credentials. Auto-login means a live trading
#   password on disk; that is the operator's call, not this script's. With
#   AutoRestart=1 in jts.ini the gateway keeps its own session across daily
#   restarts for about a week, and the login prompt case above is the residue
#   this script reports rather than defeats.
#
# USAGE
#   ./gateway_watch.sh [--port PORT] [--gateway PATH] [--no-start]
#
#     --port      API port to check (default 4001, the live gateway; 4002 paper)
#     --gateway   ibgateway executable (default: newest under ~/Jts/ibgateway)
#     --no-start  check and report only, never launch anything
#

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT=4001
GATEWAY=""
NO_START=0

while (($#)); do
    case "$1" in
        --port)     PORT="$2";    shift 2 ;;
        --gateway)  GATEWAY="$2"; shift 2 ;;
        --no-start) NO_START=1;   shift ;;
        *) echo "gateway_watch.sh: unknown argument '$1'" >&2; exit 64 ;;
    esac
done

LOG_DIR="$SCRIPT_DIR/logs"
LOG="$LOG_DIR/gateway_watch.log"
mkdir -p "$LOG_DIR"

note() { echo "$(date -Is) $*" | tee -a "$LOG" >&2; }

# Default to the highest-numbered install, so a gateway auto-update does not
# silently leave this pointing at a version that is no longer there.
if [[ -z "$GATEWAY" ]]; then
    GATEWAY="$(find "$HOME/Jts/ibgateway" -maxdepth 2 -name ibgateway -type f 2>/dev/null \
               | sort -V | tail -1)"
fi

port_open() {
    # bash's /dev/tcp rather than ss/lsof: no extra dependency, and it proves
    # something will actually accept a connection, which is what the engine needs.
    # The subshell both opens and closes the descriptor; its exit status is the
    # answer, so nothing leaks into this shell.
    (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null
}

gateway_running() { pgrep -f "Jts/ibgateway/.*/ibgateway|install4j.ibgateway" >/dev/null 2>&1; }

if port_open; then
    # Only log recoveries, so a healthy minute-by-minute run stays silent and
    # the log holds nothing but events worth reading.
    if [[ -f "$LOG_DIR/.gateway_down" ]]; then
        note "RECOVERED: IB Gateway is serving the API on port $PORT again"
        rm -f "$LOG_DIR/.gateway_down"
    fi
    exit 0
fi

touch "$LOG_DIR/.gateway_down"

if gateway_running; then
    note "NEEDS ATTENTION: IB Gateway is running but port $PORT is closed — " \
         "almost certainly waiting at a login or 2FA prompt. No script can " \
         "clear this; log in on the console. The engine cannot trade until it does."
    exit 2
fi

if ((NO_START)); then
    note "DOWN: IB Gateway is not running and port $PORT is closed (--no-start, not launching)"
    exit 1
fi

if [[ -z "$GATEWAY" || ! -x "$GATEWAY" ]]; then
    note "DOWN: IB Gateway is not running and no executable was found " \
         "(looked under $HOME/Jts/ibgateway). Pass --gateway PATH."
    exit 1
fi

note "DOWN: IB Gateway not running and port $PORT closed — starting $GATEWAY"
nohup "$GATEWAY" -J-DjtsConfigDir="$HOME/Jts" >/dev/null 2>&1 &
disown

# The gateway needs a moment to bind. Report whether the start actually worked
# rather than assuming it did — a launch that lands on a login prompt looks
# identical to a successful one until the port opens.
for _ in $(seq 1 20); do
    sleep 3
    if port_open; then
        note "RECOVERED: IB Gateway started and is serving the API on port $PORT"
        rm -f "$LOG_DIR/.gateway_down"
        exit 0
    fi
done

note "NEEDS ATTENTION: started IB Gateway but port $PORT did not open within 60s — " \
     "it is most likely waiting for login. The engine cannot trade until it does."
exit 2
