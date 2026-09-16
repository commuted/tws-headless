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
# resolve_account — never pick the traded account positionally
# ---------------------------------------------------------------------------

class TestResolveAccount:
    def test_sole_account_needs_no_flag(self):
        acct, err = env.resolve_account(None, ["U9876543"])
        assert (acct, err) == ("U9876543", None)

    def test_several_accounts_without_flag_is_refused(self):
        """The bug this guards: managed_accounts[0] is whichever account IB
        listed first, so a reordering silently routes live orders elsewhere."""
        acct, err = env.resolve_account(None, ["U9876543", "U8765432"])
        assert acct is None
        assert err is not None
        assert "U9876543" in err and "U8765432" in err
        assert "--account" in err

    def test_explicit_account_is_honoured(self):
        acct, err = env.resolve_account("U8765432", ["U9876543", "U8765432"])
        assert (acct, err) == ("U8765432", None)

    def test_explicit_account_not_on_the_login_is_refused(self):
        acct, err = env.resolve_account("U9999999", ["U9876543", "U8765432"])
        assert acct is None
        assert err is not None and "U9999999" in err

    def test_no_accounts_at_all_is_refused(self):
        acct, err = env.resolve_account(None, [])
        assert acct is None and err is not None
        acct, err = env.resolve_account("U9876543", [])
        assert acct is None and err is not None

    def test_whitespace_and_blanks_are_tolerated(self):
        acct, err = env.resolve_account(" U9876543 ", ["", "U9876543 "])
        assert (acct, err) == ("U9876543", None)


# ---------------------------------------------------------------------------
# run_engine --account
# ---------------------------------------------------------------------------

class TestRunEngineOperatorIdFlags:
    """Operator IDs come from startup configuration, never from constants in
    the tree — they identify real people and belong outside the repository,
    like the account id."""

    def _parse(self, argv, env=None):
        import os
        from unittest.mock import patch
        from ib.run_engine import parse_args
        with patch("sys.argv", ["run_engine"] + argv), \
             patch.dict(os.environ, env or {}, clear=False):
            return parse_args()

    def test_flags_are_parsed(self):
        args = self._parse(["--operator-id", "A1", "--manual-operator-id", "M1"])
        assert (args.operator_id, args.manual_operator_id) == ("A1", "M1")

    def test_absent_is_none_not_a_default(self):
        """A baked-in fallback would put a real identifier in the tree, and
        would also quietly mis-attribute orders to whoever it named."""
        import os
        from unittest.mock import patch
        from ib.run_engine import parse_args
        clean = {k: v for k, v in os.environ.items()
                 if k not in ("IB_OPERATOR_ID", "IB_MANUAL_OPERATOR_ID")}
        with patch("sys.argv", ["run_engine", "--port", "4001"]), \
             patch.dict(os.environ, clean, clear=True):
            args = parse_args()
        assert args.operator_id is None
        assert args.manual_operator_id is None


class TestRunEngineAccountFlag:
    """The flag that lets resolve_account be given an answer. The resolution
    logic itself is tested above; this covers the wiring that reaches it."""

    def _parse(self, argv):
        from unittest.mock import patch
        from ib.run_engine import parse_args
        with patch("sys.argv", ["run_engine"] + argv):
            return parse_args()

    def test_account_defaults_to_none(self):
        assert self._parse(["--port", "4001"]).account is None

    def test_account_is_parsed(self):
        args = self._parse(["--port", "4001", "--account", "U9876543"])
        assert args.account == "U9876543"

    def test_account_coexists_with_the_other_live_guardrails(self):
        args = self._parse([
            "--port", "4001", "--env", "live", "--account", "U9876543",
            "--mode", "immediate", "--live-confirmed",
        ])
        assert (args.account, args.env, args.mode, args.live_confirmed) == \
            ("U9876543", "live", "immediate", True)


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
    """resolve_socket_path returns (path, error). exists is injected so the
    tests are hermetic — the developer machine may have real engine sockets.
    Auto-discovery behavior is covered in test_ibctl.py."""

    _NONE_EXIST = staticmethod(lambda p: False)

    def _ibctl(self):
        import ibctl
        return ibctl

    def test_explicit_socket_wins(self):
        m = self._ibctl()
        path, err = m.resolve_socket_path("/tmp/custom.sock", "paper", 7497,
                                          exists=self._NONE_EXIST)
        assert path == "/tmp/custom.sock" and err is None

    def test_env_selects_socket(self):
        m = self._ibctl()
        path, err = m.resolve_socket_path(None, "live", None,
                                          exists=self._NONE_EXIST)
        assert path.endswith("_live.sock") and err is None

    def test_port_derives_env(self):
        m = self._ibctl()
        path, _ = m.resolve_socket_path(None, None, 7497, exists=self._NONE_EXIST)
        assert path.endswith("_paper.sock")
        path, _ = m.resolve_socket_path(None, None, 4001, exists=self._NONE_EXIST)
        assert path.endswith("_live.sock")

    def test_unknown_port_is_rejected(self):
        """An unrecognized --port is a hard error, not a fallback.

        This asserted the opposite until 2026-08-04, having gone stale when
        fdc5eeb deliberately changed the behavior.
        """
        m = self._ibctl()
        path, err = m.resolve_socket_path(None, None, 9999,
                                          exists=self._NONE_EXIST)
        assert path is None
        assert "9999" in err

    def test_unknown_port_is_rejected_even_when_a_socket_exists(self):
        """The case the guard exists for: a typo must not land on a live engine.

        The old fallback ran auto-discovery, so `--port 9999` quietly used
        whichever engine happened to be up — with no warning that the port
        had been ignored.
        """
        m = self._ibctl()
        path, err = m.resolve_socket_path(None, None, 9999,
                                          exists=lambda p: True)
        assert path is None
        assert "9999" in err

    def test_nothing_falls_back_to_legacy(self):
        m = self._ibctl()
        path, err = m.resolve_socket_path(None, None, None,
                                          exists=self._NONE_EXIST)
        assert path == m.DEFAULT_SOCKET_PATH and err is None


# ---------------------------------------------------------------------------
# environment.engine_already_running — duplicate-engine guard
# ---------------------------------------------------------------------------

class TestEngineAlreadyRunning:
    """Two engines on one environment both default to --client-id 1, and IB
    answers only the first. The second sees a connection timeout and aborts on
    a missing managed account, with nothing naming the cause — eight hours of
    that on 2026-08-05. This guard is what makes it immediate and specific."""

    def test_live_socket_is_detected(self, tmp_path):
        import socket

        path = str(tmp_path / "live.sock")
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(path)
        srv.listen(1)
        try:
            assert env.engine_already_running(path) is True
        finally:
            srv.close()

    def test_stale_socket_file_does_not_block_startup(self, tmp_path):
        """A crashed engine leaves its socket behind.

        Treating a leftover file as "already running" would be a worse failure
        than the one being prevented: the engine could never restart without
        someone manually deleting it.
        """
        import socket

        path = str(tmp_path / "stale.sock")
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.bind(path)
        s.close()  # file remains, nobody listening

        import os
        assert os.path.exists(path)
        assert env.engine_already_running(path) is False

    def test_missing_socket_is_not_running(self, tmp_path):
        assert env.engine_already_running(str(tmp_path / "absent.sock")) is False

    def test_exit_code_is_distinct_from_generic_failure(self):
        """The supervisor keys on this to stop retrying, so it must not be 1."""
        assert env.EXIT_ALREADY_RUNNING == 3
        assert env.EXIT_ALREADY_RUNNING not in (0, 1, 130, 143)

    def test_fatal_config_exit_code_is_its_own(self):
        """A guardrail refusal must also stop the supervisor, and be
        distinguishable from the duplicate-engine case and from the exit codes
        start_trading.sh already treats as 'do not restart'."""
        assert env.EXIT_FATAL_CONFIG == 4
        assert env.EXIT_FATAL_CONFIG not in (0, 1, 130, 143)
        assert env.EXIT_FATAL_CONFIG != env.EXIT_ALREADY_RUNNING
