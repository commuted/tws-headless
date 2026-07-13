"""
Tests for clean paper/live separation.

Covers:
  - ib.environment: env derivation, resource path helpers, and the two guardrails
    (check_env_consistency, require_live_confirmation)
  - ib.execution_db.configure_execution_db: per-account DB singleton rebinding
  - ibctl.resolve_socket_path: socket selection priority
"""

import importlib.util
from pathlib import Path

import pytest

from ib import environment as env
from ib.environment import TradingEnv


# ---------------------------------------------------------------------------
# env_from_port
# ---------------------------------------------------------------------------

class TestEnvFromPort:
    @pytest.mark.parametrize("port", [7497, 4002])
    def test_paper_ports(self, port):
        assert env.env_from_port(port) is TradingEnv.PAPER

    @pytest.mark.parametrize("port", [7496, 4001])
    def test_live_ports(self, port):
        assert env.env_from_port(port) is TradingEnv.LIVE

    @pytest.mark.parametrize("port", [9999, 0, None])
    def test_unknown_ports_return_none(self, port):
        assert env.env_from_port(port) is None


# ---------------------------------------------------------------------------
# env_from_account
# ---------------------------------------------------------------------------

class TestEnvFromAccount:
    @pytest.mark.parametrize("acct", ["DU1234567", "DF9999999", "du123", "  DU1 "])
    def test_paper_prefixes(self, acct):
        assert env.env_from_account(acct) is TradingEnv.PAPER

    @pytest.mark.parametrize("acct", ["U1234567", "X999", ""])
    def test_live_or_default(self, acct):
        assert env.env_from_account(acct) is TradingEnv.LIVE


# ---------------------------------------------------------------------------
# resolve_env
# ---------------------------------------------------------------------------

class TestResolveEnv:
    def test_explicit_wins_over_port(self):
        # paper port but explicit live -> live
        assert env.resolve_env(7497, "live") is TradingEnv.LIVE

    def test_falls_back_to_port(self):
        assert env.resolve_env(4001, None) is TradingEnv.LIVE

    def test_unknown_port_no_explicit_raises(self):
        with pytest.raises(ValueError):
            env.resolve_env(9999, None)

    def test_explicit_accepts_enum(self):
        assert env.resolve_env(None, TradingEnv.PAPER) is TradingEnv.PAPER


# ---------------------------------------------------------------------------
# path helpers
# ---------------------------------------------------------------------------

class TestPathHelpers:
    def test_socket_path_differs_by_env(self):
        assert env.socket_path_for("paper") != env.socket_path_for("live")
        assert env.socket_path_for("paper").endswith(".tws_headless_paper.sock")

    def test_execution_db_path_includes_account(self):
        p = env.execution_db_path_for("DU1234567")
        assert "DU1234567" in str(p)
        assert str(p).endswith(".db")

    def test_execution_db_paths_differ_by_account(self):
        assert env.execution_db_path_for("DU1") != env.execution_db_path_for("U1")

    def test_log_path_under_env_dir(self, tmp_path):
        p = env.log_path_for("live", base_dir=tmp_path)
        assert p == tmp_path / "logs" / "live" / "engine.log"

    def test_log_paths_differ_by_env(self, tmp_path):
        assert env.log_path_for("paper", tmp_path) != env.log_path_for("live", tmp_path)


# ---------------------------------------------------------------------------
# Enum inputs — run_engine passes TradingEnv enums (not strings) into these
# helpers, so they must accept both. str(TradingEnv.PAPER) is 'TradingEnv.PAPER',
# so a naive str().lower() would break; these lock that regression.
# ---------------------------------------------------------------------------

class TestAcceptsEnumInputs:
    def test_socket_path_for_enum(self):
        assert env.socket_path_for(TradingEnv.PAPER) == env.socket_path_for("paper")

    def test_log_path_for_enum(self, tmp_path):
        assert env.log_path_for(TradingEnv.LIVE, tmp_path) == env.log_path_for("live", tmp_path)

    def test_check_env_consistency_enum(self):
        assert env.check_env_consistency(TradingEnv.PAPER, "DU1", False) is None
        assert env.check_env_consistency(TradingEnv.PAPER, "U1", False) is not None

    def test_require_live_confirmation_enum(self):
        assert env.require_live_confirmation(TradingEnv.LIVE, "immediate", False) is not None
        assert env.require_live_confirmation(TradingEnv.PAPER, "immediate", False) is None


# ---------------------------------------------------------------------------
# check_env_consistency (mismatch guardrail)
# ---------------------------------------------------------------------------

class TestCheckEnvConsistency:
    def test_matching_paper_ok(self):
        assert env.check_env_consistency("paper", "DU123", False) is None

    def test_matching_live_ok(self):
        assert env.check_env_consistency("live", "U123", False) is None

    def test_mismatch_returns_error(self):
        err = env.check_env_consistency("paper", "U123", False)
        assert err is not None
        assert "mismatch" in err.lower()

    def test_mismatch_live_engine_paper_account(self):
        assert env.check_env_consistency("live", "DU123", False) is not None

    def test_override_allows_mismatch(self):
        assert env.check_env_consistency("paper", "U123", True) is None


# ---------------------------------------------------------------------------
# require_live_confirmation (live-order gate)
# ---------------------------------------------------------------------------

class TestRequireLiveConfirmation:
    @pytest.mark.parametrize("mode", ["immediate", "queued"])
    def test_live_real_orders_blocked_without_confirm(self, mode):
        assert env.require_live_confirmation("live", mode, False) is not None

    @pytest.mark.parametrize("mode", ["immediate", "queued"])
    def test_live_real_orders_allowed_with_confirm(self, mode):
        assert env.require_live_confirmation("live", mode, True) is None

    def test_live_dry_run_always_ok(self):
        assert env.require_live_confirmation("live", "dry_run", False) is None

    def test_paper_never_gated(self):
        assert env.require_live_confirmation("paper", "immediate", False) is None

    def test_accepts_enum_like_order_mode(self):
        class FakeMode:
            value = "immediate"
        assert env.require_live_confirmation("live", FakeMode(), False) is not None
        assert env.require_live_confirmation("live", FakeMode(), True) is None


# ---------------------------------------------------------------------------
# configure_execution_db — per-account singleton rebinding
# ---------------------------------------------------------------------------

class TestConfigureExecutionDb:
    def test_rebinds_singleton_to_account_path(self, monkeypatch, tmp_path):
        import ib.execution_db as edb

        # Redirect execution_db paths into tmp so we don't touch the real home dir.
        monkeypatch.setattr(
            env, "execution_db_path_for",
            lambda acct: tmp_path / f".ib_executions_{acct}.db",
        )
        monkeypatch.setattr(edb, "_execution_db", None)

        edb.configure_execution_db("DU1234567")
        db = edb.get_execution_db()
        assert "DU1234567" in str(db.db_path)
        assert db.db_path.exists()

    def test_reconfigure_switches_db(self, monkeypatch, tmp_path):
        import ib.execution_db as edb
        monkeypatch.setattr(
            env, "execution_db_path_for",
            lambda acct: tmp_path / f".ib_executions_{acct}.db",
        )
        monkeypatch.setattr(edb, "_execution_db", None)

        edb.configure_execution_db("DU1")
        first = edb.get_execution_db().db_path
        edb.configure_execution_db("U9")
        second = edb.get_execution_db().db_path
        assert first != second
        assert "U9" in str(second)


# ---------------------------------------------------------------------------
# ibctl.resolve_socket_path — selection priority
# ---------------------------------------------------------------------------

class TestIbctlSocketResolution:
    def _ibctl(self):
        import ibctl
        return ibctl

    def test_explicit_socket_wins(self):
        m = self._ibctl()
        assert m.resolve_socket_path("/tmp/custom.sock", "paper", 7497) == "/tmp/custom.sock"

    def test_env_selects_socket(self):
        m = self._ibctl()
        assert m.resolve_socket_path(None, "live", None).endswith("_live.sock")

    def test_port_derives_env(self):
        m = self._ibctl()
        assert m.resolve_socket_path(None, None, 7497).endswith("_paper.sock")
        assert m.resolve_socket_path(None, None, 4001).endswith("_live.sock")

    def test_unknown_port_falls_back_to_legacy(self):
        m = self._ibctl()
        assert m.resolve_socket_path(None, None, 9999) == m.DEFAULT_SOCKET_PATH

    def test_nothing_falls_back_to_legacy(self):
        m = self._ibctl()
        assert m.resolve_socket_path(None, None, None) == m.DEFAULT_SOCKET_PATH
