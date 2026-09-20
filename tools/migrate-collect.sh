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
# WHAT IS CHECKED BEFORE ANYTHING IS READ
#
# Two conditions make a collected bundle actively dangerous rather than merely
# useless, so both are checked first and both refuse without --force.
#
#   RUNNING. state.json is rewritten on a 5-minute auto-save; a snapshot taken
#   mid-write is a snapshot of nothing coherent. Checked three ways, because
#   any one of them can miss: the process table, the working directory of each
#   match (an engine running from a DIFFERENT checkout of the same account is
#   just as fatal, and a bare process match cannot tell the two apart), and a
#   live connect() to the command socket, which catches an engine started in a
#   way the process match did not anticipate.
#
#   STALE. The mirror of the stale-bundle check in migrate-deploy.sh, applied
#   at the source end instead of the target. If this directory's state is
#   already behind reality, collecting it just packages that staleness up and
#   carries it to the new machine, where the deploy-side check cannot catch it
#   — from there the bundle looks perfectly fresh. Two signals: fills recorded
#   AFTER the newest state save (hard evidence the snapshot predates known
#   trades), and a state save old enough in trading days to suggest this is no
#   longer the authoritative machine.
#
# Usage:
#   tools/migrate-collect.sh --account U1234567 [--out DIR] [--include-bars]
#                            [--force]

set -euo pipefail

ACCOUNT=""
OUTDIR="."
INCLUDE_BARS=0
FORCE=0

while [ $# -gt 0 ]; do
    case "$1" in
        --account) ACCOUNT="${2:-}"; shift 2 ;;
        --out)     OUTDIR="${2:-}";  shift 2 ;;
        --include-bars) INCLUDE_BARS=1; shift ;;
        --force)   FORCE=1; shift ;;
        -h|--help) sed -n '2,60p' "$0"; exit 0 ;;
        *) echo "unknown option: $1" >&2; exit 2 ;;
    esac
done

[ -n "$ACCOUNT" ] || { echo "ERROR: --account is required" >&2; exit 2; }

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

echo "==> checking this directory is safe to collect from"
set +e
python3 - "$REPO" "$ACCOUNT" "$FORCE" <<'PY'
import json, os, re, socket, sqlite3, subprocess, sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

repo, account, force = Path(sys.argv[1]), sys.argv[2], sys.argv[3] == "1"
blocking = []

def iso(s):
    if not s:
        return None
    try:
        d = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)

# --- 1. is an engine running? --------------------------------------------
# A command-line match alone is not enough. The string also appears in the
# command line of anything that merely MENTIONS the engine — this script's own
# caller, an editor open on the launch script, a grep — and such a process
# usually has the repo as its cwd, so it lands in the worst bucket. Two
# filters remove that whole class: the process must really be a python
# interpreter, and it must not be us or one of our ancestors.
def ancestry(pid):
    """This process and every parent up to init."""
    seen = []
    while pid and pid != 1 and pid not in seen:
        seen.append(pid)
        try:
            # field 4 of /proc/pid/stat is PPid; comm (field 2) may itself
            # contain spaces or brackets, so split after the closing paren.
            stat = Path(f"/proc/{pid}/stat").read_text()
            pid = int(stat[stat.rindex(")") + 1:].split()[1])
        except (OSError, ValueError):
            break
    return set(seen)

mine = ancestry(os.getpid())

here, elsewhere = [], []
try:
    ps = subprocess.run(["ps", "-eo", "pid,cmd", "--no-headers"],
                        capture_output=True, text=True).stdout
except FileNotFoundError:
    ps = ""
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
        exe = ""            # unreadable: keep it, better a false alarm here
    if exe and not exe.startswith("python"):
        continue
    try:
        cwd = Path(os.readlink(f"/proc/{pid}/cwd")).resolve()
    except OSError:
        cwd = None          # exited between ps and readlink, or not ours to read
    (here if cwd == repo.resolve() else elsewhere).append((pid, cwd))

# A socket file can outlive the process that made it, so connect rather than
# trusting its presence; only a completed connect proves someone is listening.
live_sockets = []
for env in ("paper", "live"):
    p = Path.home() / f".tws_headless_{env}.sock"
    if not p.exists():
        continue
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(1.0)
    try:
        s.connect(str(p))
        live_sockets.append(env)
    except OSError:
        pass                # stale socket file, nothing listening
    finally:
        s.close()

if here:
    print("  [!] ENGINE RUNNING IN THIS DIRECTORY:")
    for pid, cwd in here:
        print(f"        pid {pid}  cwd {cwd}")
    blocking.append("engine running here")
if elsewhere:
    print("  [!] ENGINE RUNNING FROM ANOTHER CHECKOUT:")
    for pid, cwd in elsewhere:
        print(f"        pid {pid}  cwd {cwd or 'unreadable'}")
    print("        If it drives the same account, THAT directory holds the")
    print("        authoritative state and this one is a stale copy.")
    blocking.append("engine running elsewhere")
if live_sockets:
    print(f"  [!] COMMAND SOCKET IS LIVE: {', '.join(live_sockets)}")
    print("        Something is listening and accepting commands.")
    if not here and not elsewhere:
        print("        No matching process was found, so stop it via ibctl.")
    blocking.append("command socket live")
if not here and not elsewhere and not live_sockets:
    print("  [ok] no engine process and no live command socket")

# --- 2. is this directory's state already stale? -------------------------
states = sorted(repo.glob(f"plugins/*/{account}/state.json"))
newest, newest_path = None, None
for p in states:
    try:
        t = iso(json.loads(p.read_text()).get("saved_at"))
    except Exception:
        t = None
    if t is None:
        t = datetime.fromtimestamp(p.stat().st_mtime, timezone.utc)
    if newest is None or t > newest:
        newest, newest_path = t, p

if not states:
    print(f"  [--] no plugin state for {account} in this directory — nothing "
          f"to be stale, but check you named the right account")
else:
    # Hard signal: a fill recorded after the last state save means the
    # snapshot demonstrably predates trades the account has already made.
    db = Path.home() / f".ib_executions_{account}.db"
    last_fill = None
    if db.is_file():
        try:
            con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
            row = con.execute("SELECT MAX(timestamp) FROM executions").fetchone()
            last_fill = iso(row[0]) if row else None
            con.close()
        except sqlite3.Error:
            pass

    if last_fill and newest and last_fill > newest:
        gap = (last_fill - newest).total_seconds() / 3600
        print("  [!] STALE SOURCE: fills recorded AFTER the last state save")
        print(f"        last state save {newest.isoformat()}  ({newest_path.name})")
        print(f"        last fill       {last_fill.isoformat()}")
        print(f"        the snapshot is {gap:.1f}h behind known trades — it would")
        print(f"        carry a strategy that has forgotten those fills")
        blocking.append("stale source")
    elif newest:
        # Soft signal: age in trading days, so a normal weekend does not read
        # as neglect. Holidays are not accounted for, so a long weekend will
        # show one day more than it should.
        now = datetime.now(timezone.utc)
        days = sum(1 for i in range(1, (now.date() - newest.date()).days + 1)
                   if (newest.date() + timedelta(days=i)).weekday() < 5)
        age_h = (now - newest).total_seconds() / 3600
        detail = f"{age_h:.1f}h old ({days} trading day(s))"
        if days > 2:
            print(f"  [!] STALE SOURCE: last state save is {detail}")
            print(f"        {newest.isoformat()}  ({newest_path.name})")
            print(f"        The engine has not saved in a while. Confirm this is")
            print(f"        still the machine that trades {account}.")
            blocking.append("stale source")
        else:
            print(f"  [ok] state is current: last save {detail}")
        if last_fill:
            print(f"       last fill {last_fill.isoformat()} (before that save)")

if blocking:
    print()
    if force:
        print(f"  --force given: collecting despite {', '.join(blocking)}")
    else:
        print(f"  REFUSING to collect: {', '.join(blocking)}")
        print(f"  Stop the engine with  ./ibctl.py --env <env> stop , or re-run")
        print(f"  with --force if you know this directory is the right source.")
        sys.exit(1)
sys.exit(0)
PY
CHECKS=$?
set -e
[ "$CHECKS" -eq 0 ] || exit 1

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
    # The sqlite backup API is safe against a concurrent writer; plain cp is
    # not. Driven from python rather than the sqlite3 CLI: the engine already
    # requires the stdlib sqlite3 module (plugin_store, bar_store,
    # execution_db), so any machine that can run the engine can run this,
    # while the CLI is a separate binary the project needs nowhere else and
    # which is absent on at least one of our own hosts. The old shell form
    # was `sqlite3 .backup ... 2>/dev/null || cp`, so a missing CLI fell
    # straight through to the unsafe copy the backup existed to avoid, and
    # the redirect hid it.
    python3 - historical/bars.db "$STAGE/historical/bars.db" <<'PY'
import sqlite3, sys
src, dst = sys.argv[1], sys.argv[2]
s = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
d = sqlite3.connect(dst)
with d:
    s.backup(d)
s.close(); d.close()
PY
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
