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
# COMPARING AGAINST ANOTHER MACHINE
#
# Both checks above are answerable only from what this host can see, and the
# question they are really trying to answer is not local: it is whether THIS
# directory is the authoritative one for the account. A machine that has been
# switched off for a week looks calm and self-consistent from the inside. The
# only way to know is to ask the other machine, so --compare-with does.
#
# The comparison runs tools/state-probe.py on the far side. That program is
# stdlib-only and imports nothing from this project precisely so it can be
# piped to a host with no virtualenv and an older checkout that does not
# contain it. The transport is deliberately not welded in:
#
#   --compare-with ssh:descartes               ~/tws-headless on that host
#   --compare-with ssh:ron@descartes:/srv/tws  explicit remote path
#   --compare-with path:/mnt/olddisk/tws       a mounted or local checkout
#   --compare-with file:peer-probe.json        a probe someone ran by hand
#   --compare-with -                           the same, on stdin
#
# Set $MIGRATE_SSH to control how the ssh form connects — an identity file, a
# port, a jump host — or put a Host entry in ~/.ssh/config and set nothing.
#
# The file and stdin forms exist for the case where the two machines cannot
# reach each other directly: run state-probe.py over there by whatever means
# you have, carry the JSON across, and the comparison is identical.
#
# A comparison that was ASKED FOR and could not be completed refuses like any
# other failed check. Not knowing is not the same as being fine.
#
# Usage:
#   tools/migrate-collect.sh --account U1234567 [--out DIR] [--include-bars]
#                            [--compare-with SPEC] [--force]

set -euo pipefail

ACCOUNT=""
OUTDIR="."
INCLUDE_BARS=0
FORCE=0
COMPARE_WITH=""

while [ $# -gt 0 ]; do
    case "$1" in
        --account) ACCOUNT="${2:-}"; shift 2 ;;
        --out)     OUTDIR="${2:-}";  shift 2 ;;
        --include-bars) INCLUDE_BARS=1; shift ;;
        --compare-with) COMPARE_WITH="${2:-}"; shift 2 ;;
        --force)   FORCE=1; shift ;;
        -h|--help) sed -n '2,85p' "$0"; exit 0 ;;
        *) echo "unknown option: $1" >&2; exit 2 ;;
    esac
done

[ -n "$ACCOUNT" ] || { echo "ERROR: --account is required" >&2; exit 2; }

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

PROBE_PY="$REPO/tools/state-probe.py"
[ -f "$PROBE_PY" ] || { echo "ERROR: missing $PROBE_PY" >&2; exit 1; }

# Scratch for the probe JSON. STAGE is created later, once the checks pass;
# the trap has to cover whichever of them exists when we exit.
TMPD="$(mktemp -d)"
STAGE=""
trap 'rm -rf "$TMPD" ${STAGE:+"$STAGE"}' EXIT

# Fetch a peer probe over whichever transport the spec names. Writes JSON to
# stdout; a non-zero return means we could not find out.
fetch_probe() {
    local spec="$1" scheme rest target rpath
    scheme="${spec%%:*}"
    rest="${spec#*:}"
    case "$scheme" in
        ssh)
            # user@host, optionally followed by :path. A bare host leaves
            # rest == target, which is how we detect the path was omitted.
            target="${rest%%:*}"
            if [ "$rest" = "$target" ]; then rpath="tws-headless"; else rpath="${rest#*:}"; fi
            # $MIGRATE_SSH lets the caller supply an identity file, a port, a
            # jump host or a different client entirely, without this script
            # growing a passthrough flag for each one:
            #
            #   MIGRATE_SSH="ssh -i ~/.ssh/ed25519_descartes" \
            #       tools/migrate-collect.sh ... --compare-with ssh:host
            #
            # A Host entry in ~/.ssh/config is the tidier answer and needs
            # nothing here. BatchMode so a host wanting a password fails fast
            # and visibly instead of hanging on a prompt nobody is watching.
            # Unquoted on purpose: MIGRATE_SSH is a command line, not a path.
            ${MIGRATE_SSH:-ssh} -o BatchMode=yes -o ConnectTimeout=10 "$target" \
                "python3 - --account '$ACCOUNT' --repo '$rpath'" < "$PROBE_PY"
            ;;
        path) python3 "$PROBE_PY" --account "$ACCOUNT" --repo "$rest" ;;
        file) cat -- "$rest" ;;
        -)    cat ;;
        *)    echo "unknown --compare-with transport: '$scheme'" >&2; return 2 ;;
    esac
}

echo "==> checking this directory is safe to collect from"

python3 "$PROBE_PY" --account "$ACCOUNT" --repo "$REPO" > "$TMPD/local.json"

PEER_JSON=""
PEER_ERROR=""
if [ -n "$COMPARE_WITH" ]; then
    echo "==> asking $COMPARE_WITH about $ACCOUNT"
    if fetch_probe "$COMPARE_WITH" > "$TMPD/peer.json" 2>"$TMPD/peer.err"; then
        PEER_JSON="$TMPD/peer.json"
    else
        PEER_ERROR="$(head -3 "$TMPD/peer.err" 2>/dev/null | tr '\n' ' ')"
        [ -n "$PEER_ERROR" ] || PEER_ERROR="transport failed with no message"
    fi
fi

set +e
python3 - "$TMPD/local.json" "$FORCE" "$PEER_JSON" "$PEER_ERROR" <<'PY'
import json, sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

local = json.loads(Path(sys.argv[1]).read_text())
force = sys.argv[2] == "1"
peer = json.loads(Path(sys.argv[3]).read_text()) if sys.argv[3] else None
peer_error = sys.argv[4] if len(sys.argv) > 4 else ""
account = local["account"]
blocking = []


def iso(value):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def trading_days(since, until):
    """Weekdays strictly after `since` up to and including `until`.

    Holidays are not modelled, so a long weekend reads as one day more than
    it is. That errs toward warning, which is the right direction here.
    """
    return sum(1 for i in range(1, (until.date() - since.date()).days + 1)
               if (since.date() + timedelta(days=i)).weekday() < 5)


# --- 1. is an engine running here? ---------------------------------------
engines = local.get("engines")
sockets = local.get("live_sockets") or []
if engines is None:
    print("  [--] could not read the process table; engine state unknown")
else:
    here = [e for e in engines if e.get("here")]
    elsewhere = [e for e in engines if not e.get("here")]
    if here:
        print("  [!] ENGINE RUNNING IN THIS DIRECTORY:")
        for e in here:
            print("        pid %s  cwd %s" % (e["pid"], e["cwd"]))
        blocking.append("engine running here")
    if elsewhere:
        print("  [!] ENGINE RUNNING FROM ANOTHER CHECKOUT:")
        for e in elsewhere:
            print("        pid %s  cwd %s" % (e["pid"], e["cwd"] or "unreadable"))
        print("        If it drives the same account, THAT directory holds the")
        print("        authoritative state and this one is a stale copy.")
        blocking.append("engine running elsewhere")
    if sockets:
        print("  [!] COMMAND SOCKET IS LIVE: %s" % ", ".join(sockets))
        print("        Something is listening and accepting commands.")
        if not here and not elsewhere:
            print("        No matching process was found, so stop it via ibctl.")
        blocking.append("command socket live")
    if not here and not elsewhere and not sockets:
        print("  [ok] no engine process and no live command socket")

# --- 2. is this directory's state already stale? -------------------------
newest = iso(local.get("newest_state_save"))
newest_file = (local.get("newest_state_file") or "").split("/")[-1]
last_fill = iso(local.get("last_fill"))
now = iso(local.get("probed_at")) or datetime.now(timezone.utc)

if not local.get("state_file_count"):
    print("  [--] no plugin state for %s in this directory — nothing to be "
          "stale, but check you named the right account" % account)
elif last_fill and newest and last_fill > newest:
    gap = (last_fill - newest).total_seconds() / 3600
    print("  [!] STALE SOURCE: fills recorded AFTER the last state save")
    print("        last state save %s  (%s)" % (newest.isoformat(), newest_file))
    print("        last fill       %s" % last_fill.isoformat())
    print("        the snapshot is %.1fh behind known trades — it would" % gap)
    print("        carry a strategy that has forgotten those fills")
    blocking.append("stale source")
elif newest:
    days = trading_days(newest, now)
    detail = "%.1fh old (%d trading day(s))" % (
        (now - newest).total_seconds() / 3600, days)
    if days > 2:
        print("  [!] STALE SOURCE: last state save is %s" % detail)
        print("        %s  (%s)" % (newest.isoformat(), newest_file))
        print("        The engine has not saved in a while. Confirm this is")
        print("        still the machine that trades %s." % account)
        blocking.append("stale source")
    else:
        print("  [ok] state is current: last save %s" % detail)
    if last_fill:
        print("       last fill %s (before that save)" % last_fill.isoformat())

# --- 3. is another machine more authoritative? ---------------------------
# Only reachable when --compare-with was given. Everything above answers
# "does this host look self-consistent"; only this answers "is this host the
# one that trades the account".
if peer_error:
    print("  [!] COMPARISON FAILED: %s" % peer_error.strip())
    print("        A comparison was asked for and could not be completed, so")
    print("        whether this is the live machine is simply unknown.")
    blocking.append("comparison failed")
elif peer is not None:
    label = "%s:%s" % (peer.get("host", "?"), peer.get("repo", "?"))
    if peer.get("account") != account:
        print("  [!] PEER ANSWERED ABOUT A DIFFERENT ACCOUNT: %s"
              % peer.get("account"))
        blocking.append("peer account mismatch")
    elif not peer.get("repo_exists"):
        print("  [!] PEER HAS NO CHECKOUT AT %s" % peer.get("repo"))
        print("        Nothing to compare. Point --compare-with at the right")
        print("        path if that host does hold the live state.")
        blocking.append("peer checkout missing")
    else:
        verdict = []
        aside = []
        peer_state = iso(peer.get("newest_state_save"))
        peer_fill = iso(peer.get("last_fill"))

        # An engine on the far side only speaks to authority over THIS
        # account. A host running live while holding stale paper state is
        # perfectly normal, and counting that as authoritative would fire on
        # every cross-env collect until the operator stopped reading it.
        # Unknown is treated as relevant: the command line not saying is a
        # reason to look, not a reason to dismiss.
        for e in (peer.get("engines") or []):
            if not e.get("here"):
                continue
            drives = e.get("drives_account")
            if drives is False:
                aside.append("an engine runs there on %s, but drives another "
                             "account" % (e.get("env") or "an unknown env"))
            elif drives is None:
                verdict.append("an engine is running there (pid %s) and its "
                               "command line does not say which account"
                               % e["pid"])
            else:
                verdict.append("an engine is running there (pid %s) on this "
                               "account" % e["pid"])

        # Sockets are env-keyed, so only the one matching this account's env
        # is evidence. The U/D prefix is the same rule migrate-deploy.sh uses.
        want_env = "live" if account.upper().startswith("U") else "paper"
        for env in (peer.get("live_sockets") or []):
            if env == want_env:
                verdict.append("its %s command socket is up" % env)
            else:
                aside.append("its %s command socket is up, a different env "
                             "from this account's" % env)
        if peer_state and (newest is None or peer_state > newest):
            ahead = ((peer_state - newest).total_seconds() / 3600
                     if newest else None)
            verdict.append("its state is newer (%s%s)"
                           % (peer_state.isoformat(),
                              ", +%.1fh" % ahead if ahead else ""))
        if peer_fill and (last_fill is None or peer_fill > last_fill):
            verdict.append("it has seen fills we have not (latest %s)"
                           % peer_fill.isoformat())

        if verdict:
            print("  [!] NOT THE AUTHORITATIVE MACHINE: %s" % label)
            for v in verdict:
                print("        - %s" % v)
            print("        Collect from THAT host instead. A bundle built here")
            print("        would restore a strategy that has forgotten")
            print("        everything the other machine has done since.")
            blocking.append("peer is authoritative")
        else:
            print("  [ok] this host is at least as current as %s" % label)
        # Printed either way: when nothing blocks, this is what was weighed
        # and set aside, which is the part worth being able to check.
        for a in aside:
            print("       (%s)" % a)

if blocking:
    print()
    if force:
        print("  --force given: collecting despite %s" % ", ".join(blocking))
    else:
        print("  REFUSING to collect: %s" % ", ".join(blocking))
        print("  Stop the engine with  ./ibctl.py --env <env> stop , or re-run")
        print("  with --force if you know this directory is the right source.")
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
