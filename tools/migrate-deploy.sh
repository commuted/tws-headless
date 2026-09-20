#!/bin/bash
#
# migrate-deploy.sh — unpack a migration bundle onto this machine.
#
# Part two of two. Reads a tarball from migrate-collect.sh, checks that
# applying it here is sensible, and only then writes anything.
#
# THE THREE CHECKS, AND WHY EACH EXISTS
#
#  1. EXISTING INSTALL. A repo, plugin state, or a running engine already
#     here. Deploying over a live system is how you end up with two engines
#     believing they own the same position. Refused without --force.
#
#  2. STALE BUNDLE (the important one). If this machine already has plugin
#     state NEWER than the bundle, applying it rolls the strategy's memory
#     backwards: holding_gld, trade_count, pending orders and the regime all
#     revert. A plugin that forgets it already sold will sell again. Refused
#     without --force regardless of anything else.
#
#  3. ACCOUNT / ENV MATCH. A bundle collected from a live account applied to
#     a paper host — or the reverse — produces state scoped to the wrong
#     account directory, which reconciliation will then try to settle against
#     positions that were never there. Warned, and refused for a live/paper
#     mismatch without --force.
#
# Dry run by default: it reports what it would do and exits. Nothing is
# written without --confirm.
#
# Usage:
#   tools/migrate-deploy.sh BUNDLE.tar.gz            # dry run
#   tools/migrate-deploy.sh BUNDLE.tar.gz --confirm
#   tools/migrate-deploy.sh BUNDLE.tar.gz --confirm --force

set -euo pipefail

BUNDLE=""
CONFIRM=0
FORCE=0

while [ $# -gt 0 ]; do
    case "$1" in
        --confirm) CONFIRM=1; shift ;;
        --force)   FORCE=1;   shift ;;
        -h|--help) sed -n '2,32p' "$0"; exit 0 ;;
        *) BUNDLE="$1"; shift ;;
    esac
done

[ -n "$BUNDLE" ] && [ -f "$BUNDLE" ] || { echo "ERROR: give a bundle path" >&2; exit 2; }

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

tar -xzf "$BUNDLE" -C "$STAGE"
[ -f "$STAGE/manifest.json" ] || { echo "ERROR: no manifest.json — not a migration bundle" >&2; exit 1; }

echo "=== bundle ==="
python3 - "$STAGE" <<'PY'
import json, sys
from pathlib import Path
m = json.loads((Path(sys.argv[1]) / "manifest.json").read_text())
print(f"  collected   {m['collected_at']}")
print(f"  from host   {m['source_host']}")
print(f"  account     {m['account_id']}  ({m.get('env')})")
print(f"  repo commit {(m.get('repo_commit') or '?')[:12]}")
print(f"  files       {len(m.get('files', {}))}")
for k, v in (m.get("not_carried") or {}).items():
    print(f"  not carried {k}: {v}")
PY

echo
echo "=== checks ==="
# Exits non-zero (1) if any check is blocking. --force downgrades blocks to
# warnings; it never silences them.
set +e
python3 - "$STAGE" "$REPO" "$FORCE" <<'PY'
import json, os, re, subprocess, sys
from datetime import datetime, timezone
from pathlib import Path

stage, repo, force = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3] == "1"
m = json.loads((stage / "manifest.json").read_text())
account = m["account_id"]
blocking = []

def iso(s):
    if not s:
        return None
    try:
        d = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except ValueError:
        return None

# --- 1. existing install -------------------------------------------------
found = []
# A command-line match alone is not enough: the string also appears in the
# command line of anything that merely mentions the engine, including this
# script's own caller. Require a real python interpreter, and skip ourselves
# and our ancestors. Same filter as migrate-collect.sh.
def ancestry(pid):
    seen = []
    while pid and pid != 1 and pid not in seen:
        seen.append(pid)
        try:
            stat = Path(f"/proc/{pid}/stat").read_text()
            pid = int(stat[stat.rindex(")") + 1:].split()[1])
        except (OSError, ValueError):
            break
    return set(seen)

mine = ancestry(os.getpid())
running = ""
try:
    ps = subprocess.run(["ps", "-eo", "pid,cmd", "--no-headers"],
                        capture_output=True, text=True).stdout
    for line in ps.splitlines():
        if not re.search(r"python3? -m ib\.run_engine", line):
            continue
        try:
            pid = int(line.split()[0])
        except ValueError:
            continue
        if pid in mine:
            continue
        try:
            exe = os.path.basename(os.readlink(f"/proc/{pid}/exe"))
        except OSError:
            exe = ""
        if exe and not exe.startswith("python"):
            continue
        running = str(pid)
        break
except FileNotFoundError:
    pass
if running:
    found.append(f"an engine is RUNNING (pid {running})")

plugin_root = repo / "plugins"
existing_state = []
if plugin_root.is_dir():
    for p in plugin_root.glob(f"*/{account}/state.json"):
        existing_state.append(p)
    if existing_state:
        found.append(f"{len(existing_state)} existing plugin state file(s) for {account}")
for db in Path.home().glob(f".ib_plugin_store_{account}.db"):
    found.append(f"existing plugin store {db.name}")

if found:
    print("  [!] EXISTING INSTALL:")
    for f in found:
        print(f"        - {f}")
    blocking.append("existing install")
else:
    print("  [ok] no existing install for this account")

# --- 2. stale bundle: would this roll state backwards? -------------------
bundle_at = iso(m.get("collected_at"))
newest, newest_path = None, None
for p in existing_state:
    try:
        d = json.loads(p.read_text())
        t = iso(d.get("saved_at"))
    except Exception:
        t = None
    if t is None:
        t = datetime.fromtimestamp(p.stat().st_mtime, timezone.utc)
    if newest is None or t > newest:
        newest, newest_path = t, p

if bundle_at and newest:
    delta = (newest - bundle_at).total_seconds()
    if delta > 0:
        print(f"  [!] STALE BUNDLE: this machine's state is NEWER than the bundle")
        print(f"        target state {newest.isoformat()}  ({newest_path.name})")
        print(f"        bundle       {bundle_at.isoformat()}")
        print(f"        applying it rewinds the strategy by {delta/3600:.1f}h — it would")
        print(f"        forget trades already made and may repeat them")
        blocking.append("stale bundle")
    else:
        print(f"  [ok] bundle is {abs(delta)/3600:.1f}h newer than existing state")
elif bundle_at:
    print(f"  [ok] no existing state to compare against")

# --- 3. account / env match ----------------------------------------------
env = (m.get("env") or "").lower()
here_live = list(Path.home().glob(".ib_executions_U*.db"))
here_paper = list(Path.home().glob(".ib_executions_D*.db"))
if env == "live" and here_paper and not here_live:
    print(f"  [!] ENV MISMATCH: bundle is LIVE, this host looks like paper only")
    blocking.append("env mismatch")
elif env == "paper" and here_live and not here_paper:
    print(f"  [!] ENV MISMATCH: bundle is PAPER, this host looks like live only")
    blocking.append("env mismatch")
elif not here_live and not here_paper:
    # A fresh host has no execution logs either way, so there is nothing to
    # disagree with. Say that, rather than reporting a match we never made:
    # this is the ordinary new-machine case, and it is the one where a false
    # [ok] would be read as confirmation that the env was checked.
    print(f"  [--] env {env or 'unknown'}: nothing on this host to compare "
          f"against (fresh machine)")
else:
    print(f"  [ok] env {env or 'unknown'} is consistent with this host")

# --- checksums ------------------------------------------------------------
import hashlib
bad = []
for rel, meta in (m.get("files") or {}).items():
    p = stage / rel
    if not p.is_file():
        bad.append(f"{rel} missing"); continue
    if hashlib.sha256(p.read_bytes()).hexdigest() != meta["sha256"]:
        bad.append(f"{rel} checksum mismatch")
if bad:
    print("  [!] BUNDLE INTEGRITY:")
    for b in bad:
        print(f"        - {b}")
    blocking.append("integrity")
else:
    print(f"  [ok] all {len(m.get('files') or {})} files match their checksums")

if blocking:
    print()
    if force:
        print(f"  --force given: proceeding despite {', '.join(blocking)}")
    else:
        print(f"  BLOCKED by: {', '.join(blocking)}")
        print(f"  Re-run with --force only if you understand each one.")
        sys.exit(1)
sys.exit(0)
PY
CHECKS=$?
set -e
[ "$CHECKS" -eq 0 ] || exit 1

if [ "$CONFIRM" -eq 0 ]; then
    echo
    echo "=== dry run — nothing written. Re-run with --confirm to apply. ==="
    echo "Would place:"
    echo "  STATE.json          -> $REPO/STATE.json   (then: start_trading.sh --restore-state)"
    [ -d "$STAGE/host" ]     && ls -1 "$STAGE/host"     2>/dev/null | sed "s|^|  host config      -> $HOME or $REPO: |"
    [ -d "$STAGE/systemd" ]  && ls -1 "$STAGE/systemd"  2>/dev/null | sed "s|^|  systemd unit     -> /etc/systemd/system/ (needs sudo): |"
    [ -f "$STAGE/historical/bars.db" ] && echo "  historical/bars.db -> $REPO/historical/"
    exit 0
fi

echo
echo "=== applying ==="
cp "$STAGE/STATE.json" "$REPO/STATE.json"; echo "  STATE.json"
for f in "$STAGE"/host/start-*.sh; do
    [ -f "$f" ] || continue
    cp "$f" "$REPO/$(basename "$f")"; chmod +x "$REPO/$(basename "$f")"
    echo "  $(basename "$f")  (review the flags before using)"
done
[ -f "$STAGE/host/config" ] && { mkdir -p "$HOME/.vnc"; cp "$STAGE/host/config" "$HOME/.vnc/config"; echo "  ~/.vnc/config"; }
[ -f "$STAGE/host/.ib_forex_cost_basis.json" ] && { cp "$STAGE/host/.ib_forex_cost_basis.json" "$HOME/"; echo "  ~/.ib_forex_cost_basis.json"; }
if [ -f "$STAGE/historical/bars.db" ]; then
    mkdir -p "$REPO/historical"; cp "$STAGE/historical/bars.db" "$REPO/historical/bars.db"; echo "  historical/bars.db"
fi
for u in "$STAGE"/systemd/*; do
    [ -f "$u" ] || continue
    echo "  systemd unit NOT installed automatically: $(basename "$u")"
    echo "      sudo cp '$u' /etc/systemd/system/ && sudo systemctl daemon-reload"
done

echo
echo "=== next ==="
echo "  1. Check out the matching commit:"
python3 -c "import json,sys;print('       git checkout', json.load(open(sys.argv[1]))['repo_commit'][:12])" "$STAGE/manifest.json" 2>/dev/null || true
echo "  2. Fill in / verify the launch script flags (--account, --operator-id)."
echo "  3. Restore and start:  ./start_trading.sh --restore-state ...  "
echo "     Restore writes back what the snapshot believes; the engine's startup"
echo "     reconciliation then settles it against live IB positions."
