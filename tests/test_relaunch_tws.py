"""
Tests for relaunch_tws.sh — the process-level (not GUI) TWS bounce the
watchdog's auto-relaunch escalation shells out to.

These spawn a harmless decoy process instead of touching real TWS, using the
script's env-var overrides (TWS_MATCH_PATTERN / TWS_REQUIRED_COMM /
TWS_LAUNCHER / TWS_TERM_GRACE_SECS) added for exactly this purpose.
"""
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

SCRIPT = str(Path(__file__).resolve().parent.parent / "relaunch_tws.sh")
MARKER = "decoy-relaunch-test-marker-xyz"


@pytest.fixture
def decoy():
    """A process whose command line contains MARKER, so the script's pgrep -f
    can find it without matching anything real on the system.

    A background thread reaps it the instant it exits: relaunch_tws.sh's own
    `kill -0 $pid` liveness check would otherwise see a zombie (a terminated
    but unreaped child still occupies its PID) as "still alive" for as long
    as this fixture's own process — the decoy's real parent — hasn't called
    wait() on it, which without this thread would only happen at teardown,
    well after the script's own grace-period wait loop had already timed out.
    """
    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)", MARKER]
    )
    reaper = threading.Thread(target=proc.wait, daemon=True)
    reaper.start()
    yield proc
    if proc.poll() is None:
        proc.kill()
    reaper.join(timeout=5)


def _run(*args, env_overrides=None):
    base = {
        "TWS_MATCH_PATTERN": MARKER,
        "TWS_REQUIRED_COMM": "python3",
        "TWS_TERM_GRACE_SECS": "3",   # decoys die on SIGTERM near-instantly
    }
    base.update(env_overrides or {})
    env = dict(os.environ, **base)
    return subprocess.run([SCRIPT, *args], env=env,
                          capture_output=True, text=True, timeout=20)


class TestNoProcessFound:
    def test_dry_run_reports_not_found(self):
        result = _run("--dry-run", env_overrides={"TWS_MATCH_PATTERN": MARKER + "-nothing-matches-this"})
        assert result.returncode == 1
        assert "No running TWS process found" in result.stderr

    def test_real_run_reports_not_found(self):
        result = _run(env_overrides={"TWS_MATCH_PATTERN": MARKER + "-nothing-matches-this"})
        assert result.returncode == 1


class TestDryRun:
    def test_finds_decoy_without_killing_it(self, decoy):
        result = _run("--dry-run")
        assert result.returncode == 0
        assert f"Found TWS at pid {decoy.pid}" in result.stdout
        assert "[dry-run] would:" in result.stdout
        assert decoy.poll() is None   # still alive — dry-run touched nothing


class TestRealRun:
    def test_terminates_decoy_and_relaunches_via_configured_launcher(self, tmp_path, decoy):
        launcher_ran = tmp_path / "launcher_ran"
        fake_launcher = tmp_path / "fake_launcher.sh"
        fake_launcher.write_text(f"#!/bin/sh\ntouch {launcher_ran}\n")
        fake_launcher.chmod(0o755)

        result = _run(env_overrides={"TWS_LAUNCHER": str(fake_launcher)})

        assert result.returncode == 0
        assert decoy.poll() is not None   # process actually gone
        # The relaunch is backgrounded (nohup ... & disown); give it a moment.
        deadline = time.time() + 5
        while not launcher_ran.exists() and time.time() < deadline:
            time.sleep(0.1)
        assert launcher_ran.exists(), "configured launcher was never invoked"

    def test_missing_launcher_reported(self, decoy):
        result = _run(env_overrides={"TWS_LAUNCHER": "/no/such/launcher"})
        assert result.returncode == 1
        assert "Launcher not found" in result.stderr

    def test_process_ignoring_sigterm_reports_timeout_not_sigkill(self, tmp_path):
        """A process that won't exit on SIGTERM must be left alone (no
        SIGKILL escalation) — an abrupt kill risks a corrupt session file,
        which would defeat the point of unattended recovery."""
        script = tmp_path / "stubborn.py"
        script.write_text(
            "import signal, time\n"
            "signal.signal(signal.SIGTERM, lambda *a: None)\n"
            "time.sleep(30)\n"
        )
        # MARKER must be an actual argv entry — pgrep -f matches the command
        # line, not the script file's contents.
        proc = subprocess.Popen([sys.executable, str(script), MARKER])
        try:
            result = _run(env_overrides={"TWS_TERM_GRACE_SECS": "1"})
            assert result.returncode == 2
            assert "did not exit" in result.stderr
            assert proc.poll() is None   # never killed, just reported
        finally:
            proc.send_signal(signal.SIGKILL)
            proc.wait(timeout=5)
