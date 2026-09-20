#!/usr/bin/env python3
"""state-probe.py — report how fresh one checkout's engine state is.

Prints a single JSON object describing what this machine knows about an
account: when its plugin state was last saved, when it last saw a fill, and
whether an engine is running against it. Nothing is written and nothing is
connected to; it is safe to run on a live trading host.

DELIBERATELY SELF-CONTAINED

Stdlib only, and it imports nothing from this project. It has to run on the
far side of an ssh connection, on a machine that may have no virtualenv, no
ibapi, and an older checkout of this repo that does not contain this file —
so it is designed to be piped in rather than called in place:

    ssh peer python3 - --account U1234567 --repo tws-headless < state-probe.py

That constraint is the reason for the duplication between this file and the
engine's own code: ib/execution_db.py could tell us the last fill, but
importing it would drag in ibapi and defeat the point.

EVERY FIELD IS OPTIONAL

The caller must treat a missing field as "unknown", never as "none". A peer
that cannot read its own execution database still returns a useful answer
about its plugin state, and an unreadable /proc entry must not turn into a
confident claim that no engine is running.

Usage:
    state-probe.py --account U1234567 [--repo DIR]
"""

import argparse
import json
import os
import re
import socket
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

PROBE_VERSION = 1


def _iso(value):
    """Normalise a timestamp to a tz-aware ISO string, or None."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.isoformat()


def _ancestry(pid):
    """This process and every parent up to init.

    Used to drop ourselves from a process scan. Without it the probe reports
    the ssh command that launched it, whose command line contains the very
    string being matched.
    """
    seen = []
    while pid and pid != 1 and pid not in seen:
        seen.append(pid)
        try:
            # Field 4 of /proc/pid/stat is PPid, but comm (field 2) can itself
            # contain spaces and parens, so split after the LAST closing paren.
            stat = Path("/proc/%d/stat" % pid).read_text()
            pid = int(stat[stat.rindex(")") + 1:].split()[1])
        except (OSError, ValueError):
            break
    return set(seen)


def _flag(cmdline, name):
    """Value of --name VALUE or --name=VALUE in a command line, or None."""
    match = re.search(r"--%s[ =]([^\s]+)" % re.escape(name), cmdline)
    return match.group(1) if match else None


def find_engines(repo, account):
    """Engine processes, each tagged with whether it drives `repo`.

    A command-line match alone is not enough: the string appears in anything
    that merely mentions the engine, including our own ssh invocation and any
    editor open on a launch script. Require a real python interpreter, and
    skip ourselves and our ancestors.

    Each engine also reports whether it drives the account we were asked
    about. A host can legitimately run a live engine while holding stale
    paper state, and treating any running engine as proof of authority makes
    the check fire on every cross-env collect — which teaches the operator to
    override it, which is worse than not checking. `drives_account` is None
    when the command line does not say, because unknown must not read as no.

    No account id is copied out of a command line — only the boolean. The
    engine found on a peer may be trading an account that has nothing to do
    with the one we asked about, and this JSON gets piped between machines
    and pasted into terminals. A boolean answers the question without
    dragging a third account's number along with it. (The probe does echo
    the account it was ASKED about, and state paths contain it, since the
    output is not interpretable otherwise.)
    """
    mine = _ancestry(os.getpid())
    found = []
    try:
        listing = subprocess.run(
            ["ps", "-eo", "pid,cmd", "--no-headers"],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        ).stdout.decode("utf-8", "replace")
    except (OSError, ValueError):
        return None                     # unknown, not empty
    for line in listing.splitlines():
        if not re.search(r"python3? -m ib\.run_engine", line):
            continue
        try:
            pid = int(line.split()[0])
        except (ValueError, IndexError):
            continue
        if pid in mine:
            continue
        try:
            exe = os.path.basename(os.readlink("/proc/%d/exe" % pid))
        except OSError:
            exe = ""                    # unreadable: keep it, a false alarm
        if exe and not exe.startswith("python"):
            continue
        try:
            cwd = str(Path(os.readlink("/proc/%d/cwd" % pid)).resolve())
        except OSError:
            cwd = None
        seen_account = _flag(line, "account")
        found.append({
            "pid": pid,
            "cwd": cwd,
            "here": cwd == str(repo),
            "env": _flag(line, "env"),
            "drives_account": None if seen_account is None
                              else seen_account == account,
        })
    return found


def live_sockets():
    """Command sockets that actually accept a connection.

    A socket file outlives the process that made it, so its presence proves
    nothing; only a completed connect() does.
    """
    live = []
    for env in ("paper", "live"):
        path = Path.home() / (".tws_headless_%s.sock" % env)
        if not path.exists():
            continue
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(1.0)
        try:
            sock.connect(str(path))
            live.append(env)
        except OSError:
            pass                        # stale file, nothing listening
        finally:
            sock.close()
    return live


def newest_state(repo, account):
    """Latest plugin state save for this account, and how many files there are."""
    files = sorted(repo.glob("plugins/*/%s/state.json" % account))
    newest, newest_name = None, None
    for path in files:
        saved = None
        try:
            saved = _iso(json.loads(path.read_text()).get("saved_at"))
        except (OSError, ValueError):
            saved = None
        if saved is None:
            # No usable saved_at: mtime is a weaker answer but still an answer.
            saved = datetime.fromtimestamp(
                path.stat().st_mtime, timezone.utc).isoformat()
        if newest is None or saved > newest:
            newest, newest_name = saved, str(path.relative_to(repo))
    return newest, newest_name, len(files)


def last_fill(account):
    """Most recent execution and the total count, or (None, None) if unknown."""
    db = Path.home() / (".ib_executions_%s.db" % account)
    if not db.is_file():
        return None, None
    try:
        con = sqlite3.connect("file:%s?mode=ro" % db, uri=True)
        newest = con.execute("SELECT MAX(timestamp) FROM executions").fetchone()[0]
        count = con.execute("SELECT COUNT(*) FROM executions").fetchone()[0]
        con.close()
    except sqlite3.Error:
        return None, None
    return _iso(newest), count


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--account", required=True)
    ap.add_argument("--repo", default=".",
                    help="checkout to probe (default: current directory)")
    args = ap.parse_args()

    repo = Path(os.path.expanduser(args.repo)).resolve()
    probe = {
        "probe_version": PROBE_VERSION,
        "host": socket.gethostname(),
        "repo": str(repo),
        "repo_exists": repo.is_dir(),
        "account": args.account,
        "probed_at": datetime.now(timezone.utc).isoformat(),
    }

    if repo.is_dir():
        saved, name, count = newest_state(repo, args.account)
        probe["newest_state_save"] = saved
        probe["newest_state_file"] = name
        probe["state_file_count"] = count
        probe["engines"] = find_engines(repo, args.account)
    probe["live_sockets"] = live_sockets()
    fill_at, fill_count = last_fill(args.account)
    probe["last_fill"] = fill_at
    probe["fill_count"] = fill_count

    json.dump(probe, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
