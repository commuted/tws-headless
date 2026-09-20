#!/bin/bash
#
# migrate-collect.sh — build a migration bundle for relocating this platform.
#
# Part one of two. Produces a tarball that, together with a git clone, is
# enough to stand the engine up on another machine. Part two is
# migrate-deploy.sh, which unpacks it with safety checks.
#
# WHAT THIS ADDS OVER `ibctl state collect`
#
# STATE.json already carries the engine's own state — the four stores in
# ib/state_file.py, three of which live in $HOME rather than the repo. What it
# deliberately does not carry is HOST CONFIGURATION: the launch script, the
# systemd units, the VNC config. That is the half that is least portable and
# most easily lost, because it exists on exactly one machine and in no
# snapshot. This bundle carries both halves plus a manifest.
#
# WHAT IS STILL NOT CARRIED, AND WHY
#
#   historical/bars.db   A rebuildable market-data cache, but not a free one:
#                        refetching two years of 5-min bars is ~25 paced IB
#                        requests per series. Copy it separately if you care
#                        about the warm cache; it is often larger than
#                        everything else here combined.
#   ~/.ib_executions_*   An audit log of past fills. Not needed to resume
#                        trading. Copy it separately if you want cost
#                        reporting continuity across the move.
#   IB Gateway settings  ~/Jts holds its own encrypted settings store and a
#                        login you will redo on the new host anyway.
#
# Both omissions are recorded in the manifest so a restore can tell you what
# was left behind rather than leaving you to discover it.
#
# Usage:
#   tools/migrate-collect.sh --account U1234567 [--out DIR] [--include-bars]
#
# Run against a STOPPED engine. Collection reads files and SQLite directly
# with no engine connection, but a running engine may flush state mid-read.

set -euo pipefail

ACCOUNT=""
OUTDIR="."
INCLUDE_BARS=0

while [ $# -gt 0 ]; do
    case "$1" in
        --account) ACCOUNT="${2:-}"; shift 2 ;;
        --out)     OUTDIR="${2:-}";  shift 2 ;;
        --include-bars) INCLUDE_BARS=1; shift ;;
        -h|--help) sed -n '2,40p' "$0"; exit 0 ;;
        *) echo "unknown option: $1" >&2; exit 2 ;;
    esac
done

[ -n "$ACCOUNT" ] || { echo "ERROR: --account is required" >&2; exit 2; }

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

# Refuse to collect from a live engine: state.json is rewritten on a 5-minute
# auto-save, and a snapshot taken mid-write is a snapshot of nothing coherent.
# Matched on the actual python invocation rather than a loose pattern: a bare
# `pgrep -f ib.run_engine` also matches any shell whose own command line
# happens to contain the string, including this script's caller.
if ps -eo pid,cmd --no-headers | grep -qE "python3? -m ib\.run_engine"; then
    echo "ERROR: an engine process is running. Stop it first:" >&2
    echo "         ./ibctl.py --env <env> stop" >&2
    exit 1
fi

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
mkdir -p "$STAGE/host" "$STAGE/systemd"

echo "==> collecting engine state for $ACCOUNT"
./ibctl.py state collect --account "$ACCOUNT" >/dev/null
[ -f STATE.json ] || { echo "ERROR: ibctl state collect produced no STATE.json" >&2; exit 1; }
cp STATE.json "$STAGE/STATE.json"

echo "==> collecting host configuration"
# Launch scripts hold the flags that are operationally mandatory (--account,
# --operator-id) and exist nowhere else.
for f in start-live.sh start-real.sh start-paper.sh; do
    [ -f "$REPO/$f" ] && cp "$REPO/$f" "$STAGE/host/$f" && echo "    $f"
done
for f in "$HOME/.vnc/config" "$HOME/.ib_forex_cost_basis.json"; do
    [ -f "$f" ] && cp "$f" "$STAGE/host/$(basename "$f")" && echo "    $(basename "$f")"
done
# Units we may have installed; ignore the ones we did not.
for u in /etc/systemd/system/xvfb@.service; do
    [ -f "$u" ] && cp "$u" "$STAGE/systemd/$(basename "$u")" && echo "    $(basename "$u")"
done

if [ "$INCLUDE_BARS" -eq 1 ] && [ -f historical/bars.db ]; then
    echo "==> including historical/bars.db ($(du -h historical/bars.db | cut -f1))"
    mkdir -p "$STAGE/historical"
    # sqlite3 .backup is safe against a live reader; plain cp is not.
    sqlite3 historical/bars.db ".backup '$STAGE/historical/bars.db'" 2>/dev/null \
        || cp historical/bars.db "$STAGE/historical/bars.db"
fi

echo "==> writing manifest"
REPO_COMMIT="$(git -C "$REPO" rev-parse HEAD 2>/dev/null || echo unknown)"
REPO_DIRTY="$(git -C "$REPO" status --porcelain 2>/dev/null | grep -cv '^??' || true)"
python3 - "$STAGE" "$ACCOUNT" "$STAMP" "$REPO_COMMIT" "$REPO_DIRTY" "$INCLUDE_BARS" <<'PY'
import hashlib, json, socket, sys
from datetime import datetime, timezone
from pathlib import Path

stage, account, stamp, commit, dirty, bars = sys.argv[1:7]
stage = Path(stage)

def digest(p):
    h = hashlib.sha256()
    h.update(p.read_bytes())
    return h.hexdigest()

files = {}
for p in sorted(stage.rglob("*")):
    if p.is_file() and p.name != "manifest.json":
        files[str(p.relative_to(stage))] = {"sha256": digest(p), "bytes": p.stat().st_size}

# collected_at is the value migrate-deploy.sh compares against the target's
# existing state. Deploying a bundle OLDER than what the target already has
# would roll the strategy's memory backwards — it would forget trades it has
# made and could repeat them.
snap = json.loads((stage / "STATE.json").read_text())
manifest = {
    "bundle_version": 1,
    "collected_at": datetime.now(timezone.utc).isoformat(),
    "source_host": socket.gethostname(),
    "account_id": account,
    "env": snap.get("source", {}).get("env"),
    "repo_commit": commit,
    "repo_dirty_tracked_files": int(dirty or 0),
    "state_created_at": snap.get("created_at"),
    "files": files,
    "not_carried": {
        "historical/bars.db": "included" if bars == "1" else
            "rebuildable cache; refetching costs paced IB requests",
        "~/.ib_executions_*.db": "audit log of past fills; not needed to resume",
        "~/Jts (IB Gateway settings)": "host-local; you will re-login anyway",
    },
}
(stage / "manifest.json").write_text(json.dumps(manifest, indent=2))
print(f"    account {account}  env {manifest['env']}  commit {commit[:8]}")
if manifest["repo_dirty_tracked_files"]:
    print(f"    WARNING: {manifest['repo_dirty_tracked_files']} uncommitted tracked "
          f"file(s) — the target clone will not have them")
PY

OUT="$OUTDIR/tws-migrate-${ACCOUNT}-${STAMP}.tar.gz"
tar -czf "$OUT" -C "$STAGE" .
echo "==> $OUT  ($(du -h "$OUT" | cut -f1))"
echo
echo "Deploy with:  tools/migrate-deploy.sh $(basename "$OUT")"
