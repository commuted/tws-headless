"""run_paper_tests: a plugin's failed checks must make the overall verdict fail.

It used to print "[WARN] ... errors=6" and then "Overall: PASS", because the
per-plugin summary was printed and never returned.
"""

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("run_paper_tests_under_test", ROOT / "run_paper_tests.py")
rpt = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rpt)


def test_summary_with_errors_fails(capsys):
    assert rpt._print_summary("BarStore", {"total": 9, "filled": 3, "errors": ["cold_fetch_gld"]}) is False
    assert "[WARN]" in capsys.readouterr().out


def test_clean_summary_passes():
    assert rpt._print_summary("BarStore", {"total": 9, "filled": 9, "errors": []}) is True


def test_round_trip_summary_needs_every_trip():
    assert rpt._print_summary("orders6", {"total": 2, "passed": 1, "round_trips": 1}) is False
    assert rpt._print_summary("orders6", {"total": 2, "passed": 2, "round_trips": 2}) is True


def test_run_plugin_returns_the_summary_verdict(monkeypatch):
    monkeypatch.setattr(rpt, "_is_loaded", lambda name, sock: True)
    monkeypatch.setattr(rpt, "_poll_status", lambda *a, **k: None)
    responses = {"summary": {"total": 9, "filled": 3, "errors": ["x"]}, "results": []}

    def fake_cmd(command, socket_path, timeout):
        return True, "ok", responses if "run_tests" in command else {}
    monkeypatch.setattr(rpt, "_cmd", fake_cmd)
    info = {"name": "paper_test_bar_store", "module": "m", "label": "BarStore", "timeout": 5}
    assert rpt.run_plugin(info, "/tmp/x.sock", 5, dry_run=False) is False
