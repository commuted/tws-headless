"""
test_state_file.py — STATE snapshot collection and restore.

Covers the relocation contract: a snapshot collected from one machine's on-disk
stores, applied to an empty machine, must reproduce the plugin registry and the
per-plugin runtime files. Everything here runs offline — no engine, no IB.
"""

import json

import pytest

from ib import state_file
from ib.plugin_store import PluginStore


ACCOUNT = "DU1234567"
OTHER_ACCOUNT = "U7654321"


@pytest.fixture
def source(tmp_path):
    """A populated 'source machine': plugin dir + registry DB."""
    plugin_dir = tmp_path / "src" / "plugins"
    (plugin_dir / "gld_usd_swap" / ACCOUNT).mkdir(parents=True)
    (plugin_dir / "gld_usd_swap" / ACCOUNT / "state.json").write_text(
        json.dumps({"plugin_version": "1.0", "state": {"holding_gld": True, "trade_count": 7}})
    )
    (plugin_dir / "gld_usd_swap" / ACCOUNT / "holdings.json").write_text(
        json.dumps({"cash": 10000.0, "positions": {"GLD": 50}})
    )
    # A legacy flat-layout slot that predates account separation.
    (plugin_dir / "momentum_5day").mkdir(parents=True)
    (plugin_dir / "momentum_5day" / "state.json").write_text(json.dumps({"state": {"n": 1}}))

    store_path = tmp_path / "src" / "store.db"
    store = PluginStore(store_path)
    store.upsert_registry("gld_usd_swap", "/src/plugins/gld_usd_swap/plugin.py",
                          "1.0", "started", {"allocation_dollars": 25000})
    store.upsert_registry("watchdog", "/src/plugins/watchdog/plugin.py",
                          "1.0", "frozen", None)

    return {"plugin_dir": plugin_dir, "store_path": store_path, "root": tmp_path / "src"}


@pytest.fixture
def snapshot(source, monkeypatch, tmp_path):
    """A snapshot collected from the source machine, forex cost basis isolated."""
    monkeypatch.setattr(state_file, "FOREX_COST_BASIS_PATH", tmp_path / "forex.json")
    return state_file.collect_state(
        ACCOUNT,
        plugin_dir=source["plugin_dir"],
        plugin_store=source["store_path"],
        root=source["root"],
    )


class TestCollect:
    def test_carries_registry_and_config(self, snapshot):
        slots = {e["slot"]: e for e in snapshot["plugin_registry"]}
        assert set(slots) == {"gld_usd_swap", "watchdog"}
        assert slots["gld_usd_swap"]["status"] == "started"
        assert slots["gld_usd_swap"]["config"] == {"allocation_dollars": 25000}
        assert slots["watchdog"]["status"] == "frozen"

    def test_carries_both_layouts(self, snapshot):
        paths = {e["path"] for e in snapshot["plugin_files"]}
        assert f"gld_usd_swap/{ACCOUNT}/state.json" in paths
        assert f"gld_usd_swap/{ACCOUNT}/holdings.json" in paths
        assert "momentum_5day/state.json" in paths

    def test_records_account_and_env(self, snapshot):
        assert snapshot["source"]["account_id"] == ACCOUNT
        assert snapshot["source"]["env"] == "paper"
        assert snapshot["state_version"] == state_file.STATE_VERSION

    def test_lists_what_it_leaves_behind(self, snapshot):
        reasons = " ".join(item["reason"] for item in snapshot["not_carried"])
        paths = " ".join(item["path"] for item in snapshot["not_carried"])
        assert "executions" in reasons
        assert "bars.db" in paths

    def test_other_accounts_state_is_not_collected(self, source, tmp_path, monkeypatch):
        """A second account's files under the same slot must not leak in."""
        monkeypatch.setattr(state_file, "FOREX_COST_BASIS_PATH", tmp_path / "forex.json")
        other = source["plugin_dir"] / "gld_usd_swap" / OTHER_ACCOUNT
        other.mkdir(parents=True)
        (other / "state.json").write_text(json.dumps({"state": {"secret": True}}))

        snap = state_file.collect_state(
            ACCOUNT, plugin_dir=source["plugin_dir"],
            plugin_store=source["store_path"], root=source["root"],
        )
        assert not any(OTHER_ACCOUNT in e["path"] for e in snap["plugin_files"])

    def test_malformed_state_file_is_carried_verbatim(self, source, tmp_path, monkeypatch):
        monkeypatch.setattr(state_file, "FOREX_COST_BASIS_PATH", tmp_path / "forex.json")
        bad = source["plugin_dir"] / "momentum_5day" / "state.json"
        bad.write_text("{not json")

        snap = state_file.collect_state(
            ACCOUNT, plugin_dir=source["plugin_dir"],
            plugin_store=source["store_path"], root=source["root"],
        )
        entry = next(e for e in snap["plugin_files"] if e["path"] == "momentum_5day/state.json")
        assert entry["text"] == "{not json"


class TestRoundTrip:
    def test_restore_reproduces_registry_and_files(self, snapshot, tmp_path, monkeypatch):
        monkeypatch.setattr(state_file, "FOREX_COST_BASIS_PATH", tmp_path / "forex_dst.json")
        dst_plugins = tmp_path / "dst" / "plugins"
        dst_store = tmp_path / "dst" / "store.db"

        report = state_file.restore_state(
            snapshot, plugin_dir=dst_plugins, plugin_store=dst_store,
            expected_account=ACCOUNT,
        )

        assert sorted(report["registry_restored"]) == ["gld_usd_swap", "watchdog"]
        assert not report["files_failed"]

        restored = {e["slot"]: e for e in PluginStore(dst_store).list_registry()}
        assert restored["gld_usd_swap"]["status"] == "started"
        assert restored["gld_usd_swap"]["config"] == {"allocation_dollars": 25000}

        state = json.loads((dst_plugins / "gld_usd_swap" / ACCOUNT / "state.json").read_text())
        assert state["state"]["trade_count"] == 7
        holdings = json.loads((dst_plugins / "gld_usd_swap" / ACCOUNT / "holdings.json").read_text())
        assert holdings["positions"]["GLD"] == 50

    def test_write_then_load_survives_a_file(self, snapshot, tmp_path):
        path = state_file.write_state(snapshot, tmp_path / "STATE.json")
        assert state_file.load_state(path)["source"]["account_id"] == ACCOUNT

    def test_write_is_atomic(self, snapshot, tmp_path):
        """No .tmp left behind — a partial file must never look like a snapshot."""
        state_file.write_state(snapshot, tmp_path / "STATE.json")
        assert not (tmp_path / "STATE.json.tmp").exists()


class TestSafety:
    def test_account_mismatch_refuses(self, snapshot, tmp_path):
        with pytest.raises(ValueError, match="Refusing to restore state across"):
            state_file.restore_state(
                snapshot, plugin_dir=tmp_path / "dst",
                plugin_store=tmp_path / "dst.db", expected_account=OTHER_ACCOUNT,
            )

    def test_account_mismatch_writes_nothing(self, snapshot, tmp_path):
        dst = tmp_path / "dst"
        with pytest.raises(ValueError):
            state_file.restore_state(
                snapshot, plugin_dir=dst, plugin_store=tmp_path / "dst.db",
                expected_account=OTHER_ACCOUNT,
            )
        assert not dst.exists()

    def test_account_mismatch_can_be_overridden(self, snapshot, tmp_path, monkeypatch):
        monkeypatch.setattr(state_file, "FOREX_COST_BASIS_PATH", tmp_path / "forex.json")
        report = state_file.restore_state(
            snapshot, plugin_dir=tmp_path / "dst", plugin_store=tmp_path / "dst.db",
            expected_account=OTHER_ACCOUNT, allow_account_mismatch=True,
        )
        assert report["registry_restored"]

    def test_unknown_version_refuses(self, snapshot, tmp_path):
        snapshot["state_version"] = 999
        path = tmp_path / "STATE.json"
        path.write_text(json.dumps(snapshot))
        with pytest.raises(ValueError, match="understands"):
            state_file.load_state(path)

    def test_dry_run_writes_nothing(self, snapshot, tmp_path):
        dst_plugins = tmp_path / "dst" / "plugins"
        dst_store = tmp_path / "dst" / "store.db"
        report = state_file.restore_state(
            snapshot, plugin_dir=dst_plugins, plugin_store=dst_store,
            expected_account=ACCOUNT, dry_run=True,
        )
        assert report["dry_run"]
        assert report["registry_restored"]
        assert not dst_plugins.exists()
        assert not dst_store.exists()

    def test_extra_local_slots_are_reported_not_deleted(self, snapshot, tmp_path, monkeypatch):
        """A slot the target has but the snapshot doesn't must survive, and be flagged."""
        monkeypatch.setattr(state_file, "FOREX_COST_BASIS_PATH", tmp_path / "forex.json")
        dst_store = tmp_path / "dst.db"
        PluginStore(dst_store).upsert_registry(
            "leftover", "/dst/plugins/leftover/plugin.py", "1.0", "started", None
        )

        report = state_file.restore_state(
            snapshot, plugin_dir=tmp_path / "dst", plugin_store=dst_store,
            expected_account=ACCOUNT,
        )

        assert report["registry_extra"] == ["leftover"]
        assert "leftover" in {e["slot"] for e in PluginStore(dst_store).list_registry()}
        assert "leftover" in state_file.format_restore_report(report)

    def test_snapshot_without_account_refuses(self, snapshot, tmp_path):
        snapshot["source"]["account_id"] = ""
        with pytest.raises(ValueError, match="no source.account_id"):
            state_file.restore_state(
                snapshot, plugin_dir=tmp_path / "dst", plugin_store=tmp_path / "dst.db",
            )
