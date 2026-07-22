#!/bin/bash
#
# relaunch_tws.sh - Bounce the running TWS process, not the trading engine.
#
# Automates exactly the recovery already proven live on 2026-07-20: a market
# data farm can go bad for one specific contract's route while everything
# else keeps working, with no error TWS surfaces via the API — the running
# GLD live-bar feed silently stopped delivering bars while every account
# health check (Connected: True, positions loading, other symbols' feeds)
# stayed green, and only a full TWS close+reopen cleared it.
#
# This is deliberately NOT GUI automation (no xdotool, no IBC). It kills the
# TWS process and relaunches its own installed launcher — nothing here reads
# window contents or clicks a menu, so it cannot be broken by a future
# self-update moving the File menu around. The ~/Jts/jts.ini `Restart=OK`
# flag is what lets the relaunch skip re-authentication; that is an
# account/session-level flag, unrelated to UI layout.
#
# Usage:
#   ./relaunch_tws.sh            # find, gracefully stop, relaunch TWS
#   ./relaunch_tws.sh --dry-run  # print what would happen, do nothing
#
# Exit codes: 0 success, 1 could not find a running TWS process,
#             2 TWS did not exit after SIGTERM within the grace period.

set -euo pipefail

# Matches this specific installed product, not any install4j-based app
# (pgrep -f alone over-matches: it also hits shells whose argv happens to
# quote these flags as text, e.g. a shell history/snapshot line). Overridable
# so tests can point this at a harmless decoy process instead of real TWS,
# and so a future TWS version that changes these flags doesn't require an
# edit here.
MATCH_PATTERN="${TWS_MATCH_PATTERN:-DprivateLabel=ib.*DproductName=Trader Workstation}"
REQUIRED_COMM="${TWS_REQUIRED_COMM:-java}"
TERM_GRACE_SECS="${TWS_TERM_GRACE_SECS:-30}"
LAUNCHER="${TWS_LAUNCHER:-$HOME/Jts/tws}"
DRY_RUN=0

[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=1

find_tws_pid() {
    local pid
    for pid in $(pgrep -f "$MATCH_PATTERN" 2>/dev/null || true); do
        if [[ "$(cat /proc/"$pid"/comm 2>/dev/null || true)" == "$REQUIRED_COMM" ]]; then
            echo "$pid"
            return 0
        fi
    done
    return 1
}

pid=$(find_tws_pid || true)

if [[ -z "${pid:-}" ]]; then
    echo "No running TWS process found (pattern: $MATCH_PATTERN)." >&2
    exit 1
fi

echo "Found TWS at pid $pid."

if [[ "$DRY_RUN" == "1" ]]; then
    echo "[dry-run] would: kill -TERM $pid, wait up to ${TERM_GRACE_SECS}s, then exec $LAUNCHER"
    exit 0
fi

echo "Sending SIGTERM (graceful — avoids killing TWS mid-write to its session/config files)..."
kill -TERM "$pid"

waited=0
while kill -0 "$pid" 2>/dev/null; do
    if (( waited >= TERM_GRACE_SECS )); then
        echo "TWS did not exit within ${TERM_GRACE_SECS}s of SIGTERM." >&2
        echo "Not escalating to SIGKILL automatically — an abrupt kill risks a" >&2
        echo "corrupt session file needing manual repair on next launch, which" >&2
        echo "would defeat the point of an unattended recovery. Check manually." >&2
        exit 2
    fi
    sleep 1
    (( waited += 1 ))
done
echo "TWS exited after ${waited}s."

if [[ ! -x "$LAUNCHER" ]]; then
    echo "Launcher not found or not executable: $LAUNCHER" >&2
    exit 1
fi

echo "Relaunching via $LAUNCHER..."
nohup "$LAUNCHER" >/dev/null 2>&1 &
disown
echo "Relaunch issued (pid $!). TWS is starting; our engine's own connection_manager will reconnect once it's ready — no separate readiness check is done here."
