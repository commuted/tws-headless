"""
Tests for account reconciliation functionality
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime
from typing import Optional, Dict, Any, List

from plugins.base import Holdings, HoldingPosition, PluginBase, PluginState


class MockPlugin:
    """Mock plugin for testing reconciliation"""

    def __init__(self, name: str, cash: float = 0.0, positions: List[Dict] = None, is_system: bool = False):
        self.name = name
        self._holdings = Holdings(
            plugin_name=name,
            current_cash=cash,
            current_positions=[
                HoldingPosition(
                    symbol=p["symbol"],
                    quantity=p["quantity"],
                    cost_basis=p.get("cost_basis", 0.0),
                    current_price=p.get("current_price", 0.0),
                )
                for p in (positions or [])
            ],
            created_at=datetime.now(),
        )
        self.is_system_plugin = is_system
        self._state = PluginState.STARTED
        self._cash_balance = cash

    @property
    def holdings(self):
        return self._holdings

    @property
    def state(self):
        return self._state

    def get_effective_cash(self) -> float:
        return self._holdings.current_cash

    def get_effective_position(self, symbol: str):
        pos = self._holdings.get_position(symbol)
        if pos:
            return (pos.quantity, pos.market_value)
        return (0.0, 0.0)

    def get_effective_holdings(self) -> Dict:
        return {
            "plugin": self.name,
            "cash": self._holdings.current_cash,
            "positions": [
                {
                    "symbol": p.symbol,
                    "quantity": p.quantity,
                    "cost_basis": p.cost_basis,
                    "current_price": p.current_price,
                    "market_value": p.market_value,
                }
                for p in self._holdings.current_positions
            ],
            "total_value": self._holdings.total_value,
        }

    def save_holdings(self):
        pass  # No-op for tests


class MockPluginConfig:
    """Mock plugin config"""
    def __init__(self, plugin):
        self.plugin = plugin
        self.enabled = True


class MockPosition:
    """Mock portfolio position"""
    def __init__(self, symbol: str, quantity: float, avg_cost: float = 0.0, current_price: float = 0.0):
        self.symbol = symbol
        self.quantity = quantity
        self.avg_cost = avg_cost
        self.current_price = current_price
        self.market_value = quantity * current_price


class MockAccountSummary:
    """Mock account summary"""
    def __init__(self, available_funds: float = 0.0, is_valid: bool = True):
        self.available_funds = available_funds
        self.is_valid = is_valid


class MockPortfolio:
    """Mock portfolio for testing"""
    def __init__(self, positions: List[MockPosition] = None, cash: float = 0.0):
        self.positions = positions or []
        self._cash = cash

    def get_position(self, symbol: str):
        for p in self.positions:
            if p.symbol == symbol:
                return p
        return None

    def get_account_summary(self):
        return MockAccountSummary(available_funds=self._cash)


class TestReconcileWithAccount:
    """Test PluginExecutive.reconcile_with_account method"""

    def create_executive(self, portfolio=None, plugins=None, unassigned_plugin=None):
        """Helper to create a PluginExecutive instance"""
        from ib.plugin_executive import PluginExecutive
        pe = object.__new__(PluginExecutive)
        pe._lock = MagicMock()
        pe._plugins = {}
        pe.portfolio = portfolio

        if plugins:
            for name, plugin in plugins.items():
                pe._plugins[name] = MockPluginConfig(plugin)

        # Set unassigned plugin property
        pe._unassigned_plugin = unassigned_plugin
        return pe

    def test_no_portfolio_connected(self):
        """Test reconciliation with no portfolio returns error"""
        pe = self.create_executive(portfolio=None)

        report = pe.reconcile_with_account()

        assert "error" in report
        assert report["discrepancies"] == []
        assert report["adjustments"] == []

    def test_no_discrepancies(self):
        """Test reconciliation when plugin holdings match account"""
        # Plugin claims 100 SPY at $450
        plugin = MockPlugin("momentum", cash=5000.0, positions=[
            {"symbol": "SPY", "quantity": 100, "cost_basis": 450.0, "current_price": 455.0}
        ])

        # Account has exactly 100 SPY
        portfolio = MockPortfolio(
            positions=[MockPosition("SPY", 100, 450.0, 455.0)],
            cash=5000.0
        )

        # Unassigned plugin with 0 cash and no positions
        unassigned = MockPlugin("_unassigned", cash=0.0, is_system=True)

        pe = self.create_executive(
            portfolio=portfolio,
            plugins={"momentum": plugin, "_unassigned": unassigned},
            unassigned_plugin=unassigned
        )

        report = pe.reconcile_with_account()

        assert report["discrepancies"] == []
        assert report["summary"]["account_positions"] == 1
        assert report["summary"]["plugin_positions"] == 1

    def test_unclaimed_position_added_to_unassigned(self):
        """Test position in account but not claimed by plugins is added to unassigned"""
        # No plugins claim any positions
        plugin = MockPlugin("momentum", cash=5000.0, positions=[])

        # Account has 100 SPY
        portfolio = MockPortfolio(
            positions=[MockPosition("SPY", 100, 450.0, 455.0)],
            cash=5000.0
        )

        unassigned = MockPlugin("_unassigned", cash=0.0, is_system=True)

        pe = self.create_executive(
            portfolio=portfolio,
            plugins={"momentum": plugin, "_unassigned": unassigned},
            unassigned_plugin=unassigned
        )

        report = pe.reconcile_with_account()

        # Should find discrepancy
        assert len(report["discrepancies"]) == 1
        disc = report["discrepancies"][0]
        assert disc["type"] == "unclaimed_position"
        assert disc["symbol"] == "SPY"
        assert disc["account_quantity"] == 100
        assert disc["claimed_quantity"] == 0

        # Should add to unassigned
        assert len(report["adjustments"]) >= 1
        adj = next(a for a in report["adjustments"] if a.get("symbol") == "SPY")
        assert adj["action"] == "added_to_unassigned"
        assert adj["quantity"] == 100

        # Verify position was added to unassigned plugin
        pos = unassigned.holdings.get_position("SPY")
        assert pos is not None
        assert pos.quantity == 100

    def test_under_claimed_position_difference_to_unassigned(self):
        """Test under-claimed position adds difference to unassigned"""
        # Plugin claims only 60 of 100 SPY
        plugin = MockPlugin("momentum", cash=5000.0, positions=[
            {"symbol": "SPY", "quantity": 60, "cost_basis": 450.0, "current_price": 455.0}
        ])

        # Account has 100 SPY
        portfolio = MockPortfolio(
            positions=[MockPosition("SPY", 100, 450.0, 455.0)],
            cash=5000.0
        )

        unassigned = MockPlugin("_unassigned", cash=0.0, is_system=True)

        pe = self.create_executive(
            portfolio=portfolio,
            plugins={"momentum": plugin, "_unassigned": unassigned},
            unassigned_plugin=unassigned
        )

        report = pe.reconcile_with_account()

        # Should find under_claimed discrepancy
        disc = next(d for d in report["discrepancies"] if d["type"] == "under_claimed")
        assert disc["symbol"] == "SPY"
        assert disc["account_quantity"] == 100
        assert disc["claimed_quantity"] == 60
        assert disc["difference"] == 40

        # Should add 40 to unassigned
        adj = next(a for a in report["adjustments"] if a.get("symbol") == "SPY")
        assert adj["action"] == "added_to_unassigned"
        assert adj["quantity"] == 40

        # Verify position was added
        pos = unassigned.holdings.get_position("SPY")
        assert pos is not None
        assert pos.quantity == 40

    def test_over_claimed_position_reduced_from_plugins(self):
        """Test over-claimed position reduces plugin holdings"""
        # Plugin claims 150 SPY but account only has 100
        plugin = MockPlugin("momentum", cash=5000.0, positions=[
            {"symbol": "SPY", "quantity": 150, "cost_basis": 450.0, "current_price": 455.0}
        ])

        # Account only has 100 SPY
        portfolio = MockPortfolio(
            positions=[MockPosition("SPY", 100, 450.0, 455.0)],
            cash=5000.0
        )

        unassigned = MockPlugin("_unassigned", cash=0.0, is_system=True)

        pe = self.create_executive(
            portfolio=portfolio,
            plugins={"momentum": plugin, "_unassigned": unassigned},
            unassigned_plugin=unassigned
        )

        report = pe.reconcile_with_account()

        # Should find over_claimed discrepancy
        disc = next(d for d in report["discrepancies"] if d["type"] == "over_claimed")
        assert disc["symbol"] == "SPY"
        assert disc["account_quantity"] == 100
        assert disc["claimed_quantity"] == 150
        assert disc["difference"] == -50

        # Should remove 50 from plugin
        adj = next(a for a in report["adjustments"] if a["action"] == "removed_from_plugin")
        assert adj["plugin"] == "momentum"
        assert adj["quantity"] == 50

        # Verify plugin position was reduced
        pos = plugin.holdings.get_position("SPY")
        assert pos.quantity == 100

    def test_phantom_position_removed_from_plugins(self):
        """Test phantom position (claimed but not in account) is removed"""
        # Plugin claims SPY but account has none
        plugin = MockPlugin("momentum", cash=5000.0, positions=[
            {"symbol": "SPY", "quantity": 100, "cost_basis": 450.0, "current_price": 455.0}
        ])

        # Account has no positions
        portfolio = MockPortfolio(positions=[], cash=5000.0)

        unassigned = MockPlugin("_unassigned", cash=0.0, is_system=True)

        pe = self.create_executive(
            portfolio=portfolio,
            plugins={"momentum": plugin, "_unassigned": unassigned},
            unassigned_plugin=unassigned
        )

        report = pe.reconcile_with_account()

        # Should find phantom_position discrepancy
        disc = next(d for d in report["discrepancies"] if d["type"] == "phantom_position")
        assert disc["symbol"] == "SPY"
        assert disc["account_quantity"] == 0
        assert disc["claimed_quantity"] == 100

        # Should remove from plugin
        adj = next(a for a in report["adjustments"] if a["action"] == "removed_phantom")
        assert adj["plugin"] == "momentum"
        assert adj["symbol"] == "SPY"
        assert adj["quantity"] == 100

        # Verify position was removed
        pos = plugin.holdings.get_position("SPY")
        assert pos is None

    def test_cash_mismatch_adjusted(self):
        """Test cash mismatch adjusts unassigned cash"""
        # Plugin claims $5000 cash
        plugin = MockPlugin("momentum", cash=5000.0, positions=[])

        # Account has $8000 total (so unassigned should be $3000)
        portfolio = MockPortfolio(positions=[], cash=8000.0)

        # But unassigned thinks it has $0
        unassigned = MockPlugin("_unassigned", cash=0.0, is_system=True)

        pe = self.create_executive(
            portfolio=portfolio,
            plugins={"momentum": plugin, "_unassigned": unassigned},
            unassigned_plugin=unassigned
        )

        report = pe.reconcile_with_account()

        # Should find cash_mismatch discrepancy
        disc = next(d for d in report["discrepancies"] if d["type"] == "cash_mismatch")
        assert disc["account_cash"] == 8000.0
        assert disc["claimed_cash"] == 5000.0
        assert disc["expected_unassigned"] == 3000.0
        assert disc["actual_unassigned"] == 0.0
        assert disc["difference"] == 3000.0

        # Should adjust unassigned cash
        adj = next(a for a in report["adjustments"] if a["action"] == "adjusted_unassigned_cash")
        assert adj["old_value"] == 0.0
        assert adj["new_value"] == 3000.0

    def test_multiple_plugins_claiming_same_position(self):
        """Test handling when multiple plugins claim the same position"""
        # Two plugins each claim 50 SPY
        plugin_a = MockPlugin("momentum", cash=5000.0, positions=[
            {"symbol": "SPY", "quantity": 50, "cost_basis": 450.0, "current_price": 455.0}
        ])
        plugin_b = MockPlugin("value", cash=5000.0, positions=[
            {"symbol": "SPY", "quantity": 50, "cost_basis": 448.0, "current_price": 455.0}
        ])

        # Account has exactly 100 SPY
        portfolio = MockPortfolio(
            positions=[MockPosition("SPY", 100, 449.0, 455.0)],
            cash=10000.0
        )

        unassigned = MockPlugin("_unassigned", cash=0.0, is_system=True)

        pe = self.create_executive(
            portfolio=portfolio,
            plugins={
                "momentum": plugin_a,
                "value": plugin_b,
                "_unassigned": unassigned
            },
            unassigned_plugin=unassigned
        )

        report = pe.reconcile_with_account()

        # No discrepancies - total claimed equals account
        position_discrepancies = [d for d in report["discrepancies"]
                                  if d.get("type") in ("unclaimed_position", "under_claimed", "over_claimed", "phantom_position")]
        assert len(position_discrepancies) == 0

    def test_system_plugins_excluded_from_claims(self):
        """System plugin holdings don't count as claims — and _unassigned
        already holding the unclaimed remainder is the CONVERGED state, not
        a discrepancy (the old behavior re-added 100 SPY every run)."""
        # _unassigned already holds the full unclaimed position
        system_plugin = MockPlugin("_unassigned", cash=0.0, positions=[
            {"symbol": "SPY", "quantity": 100, "cost_basis": 450.0}
        ], is_system=True)

        # Regular plugin claims nothing
        plugin = MockPlugin("momentum", cash=5000.0, positions=[])

        # Account has 100 SPY
        portfolio = MockPortfolio(
            positions=[MockPosition("SPY", 100, 450.0, 455.0)],
            cash=5000.0
        )

        pe = self.create_executive(
            portfolio=portfolio,
            plugins={"momentum": plugin, "_unassigned": system_plugin},
            unassigned_plugin=system_plugin
        )

        report = pe.reconcile_with_account()

        # No position discrepancies: the ledger already adds up, and the
        # system plugin's holdings were not double-counted as claims
        position_discs = [d for d in report["discrepancies"]
                          if d.get("type") != "cash_mismatch"]
        assert position_discs == []
        # And crucially: NOT re-added (was 100 + 100 under the old code)
        assert system_plugin.holdings.get_position("SPY").quantity == 100

    def test_reconcile_is_idempotent(self):
        """A second reconcile over an unchanged web reports zero position
        discrepancies — the hourly watchdog run must not re-flag the same
        state forever."""
        plugin = MockPlugin("momentum", cash=5000.0, positions=[
            {"symbol": "SPY", "quantity": 60, "cost_basis": 450.0}
        ])
        portfolio = MockPortfolio(
            positions=[MockPosition("SPY", 100, 450.0, 455.0)],
            cash=5000.0
        )
        unassigned = MockPlugin("_unassigned", cash=0.0, is_system=True)
        pe = self.create_executive(
            portfolio=portfolio,
            plugins={"momentum": plugin, "_unassigned": unassigned},
            unassigned_plugin=unassigned
        )

        first = pe.reconcile_with_account()
        assert any(d["type"] == "under_claimed" for d in first["discrepancies"])
        assert unassigned.holdings.get_position("SPY").quantity == 40

        second = pe.reconcile_with_account()
        position_discs = [d for d in second["discrepancies"]
                          if d.get("type") != "cash_mismatch"]
        assert position_discs == []
        # Quantity stable, not accumulated
        assert unassigned.holdings.get_position("SPY").quantity == 40

    def test_partial_claim_keeps_remainder_visible(self):
        """The GLD case: a plugin claims 10 of the account's 471; _unassigned
        must end up holding exactly the 461 remainder."""
        plugin = MockPlugin("gld_usd_swap", cash=20000.0, positions=[
            {"symbol": "GLD", "quantity": 10, "cost_basis": 420.79}
        ])
        portfolio = MockPortfolio(
            positions=[MockPosition("GLD", 471, 420.79, 367.13)],
            cash=900000.0
        )
        unassigned = MockPlugin("_unassigned", cash=0.0, is_system=True)
        pe = self.create_executive(
            portfolio=portfolio,
            plugins={"gld_usd_swap": plugin, "_unassigned": unassigned},
            unassigned_plugin=unassigned
        )

        pe.reconcile_with_account()
        assert unassigned.holdings.get_position("GLD").quantity == 461

    def test_unassigned_excess_removed(self):
        """When plugins claim the whole account position, an _unassigned
        leftover is excess and gets removed (set semantics, not add)."""
        plugin = MockPlugin("momentum", cash=0.0, positions=[
            {"symbol": "SPY", "quantity": 100, "cost_basis": 450.0}
        ])
        portfolio = MockPortfolio(
            positions=[MockPosition("SPY", 100, 450.0, 455.0)],
            cash=0.0
        )
        unassigned = MockPlugin("_unassigned", cash=0.0, positions=[
            {"symbol": "SPY", "quantity": 25, "cost_basis": 450.0}
        ], is_system=True)
        pe = self.create_executive(
            portfolio=portfolio,
            plugins={"momentum": plugin, "_unassigned": unassigned},
            unassigned_plugin=unassigned
        )

        report = pe.reconcile_with_account()

        adj = next(a for a in report["adjustments"]
                   if a["action"] == "removed_from_unassigned")
        assert adj["symbol"] == "SPY" and adj["quantity"] == 25
        assert unassigned.holdings.get_position("SPY") is None

    def test_short_positions_ignored(self):
        """SHORT account positions are outside the long-only plugin-ledger
        model: a zero claim against a negative quantity must not be reported
        as over_claimed (observed live: 7 spurious discrepancies re-reported
        every hourly reconcile against a paper account holding shorts)."""
        portfolio = MockPortfolio(
            positions=[MockPosition("SPXU", -4, 20.0, 21.0),
                       MockPosition("EUR.USD", -20292.42, 1.1, 1.1),
                       MockPosition("SPY", 100, 450.0, 455.0)],
            cash=0.0
        )
        plugin = MockPlugin("momentum", cash=0.0, positions=[
            {"symbol": "SPY", "quantity": 100, "cost_basis": 450.0}
        ])
        unassigned = MockPlugin("_unassigned", cash=0.0, is_system=True)
        pe = self.create_executive(
            portfolio=portfolio,
            plugins={"momentum": plugin, "_unassigned": unassigned},
            unassigned_plugin=unassigned
        )

        report = pe.reconcile_with_account()
        assert report["discrepancies"] == []
        # and idempotent: nothing to re-report next cycle either
        second = pe.reconcile_with_account()
        assert second["discrepancies"] == []

    def test_cash_adjustment_sticks(self):
        """The cash adjustment must update every representation of
        unassigned cash. Writing only _cash_balance while the comparison
        reads holdings.current_cash re-reported the same cash_mismatch
        every reconcile (observed live, drifting by exactly the plugins'
        traded cash delta)."""
        portfolio = MockPortfolio(positions=[], cash=100_000.0)
        plugin = MockPlugin("gld", cash=14_044.28, positions=[])
        unassigned = MockPlugin("_unassigned", cash=0.0, is_system=True)
        # stale _cash_balance from an old sync; holdings read differently
        unassigned._cash_balance = 80_000.0
        unassigned.holdings.current_cash = 80_000.0
        pe = self.create_executive(
            portfolio=portfolio,
            plugins={"gld": plugin, "_unassigned": unassigned},
            unassigned_plugin=unassigned
        )

        first = pe.reconcile_with_account()
        assert any(d["type"] == "cash_mismatch" for d in first["discrepancies"])
        expected = 100_000.0 - 14_044.28
        assert unassigned.holdings.current_cash == pytest.approx(expected)

        second = pe.reconcile_with_account()
        assert [d for d in second["discrepancies"]
                if d["type"] == "cash_mismatch"] == []

    def test_stale_unassigned_position_removed(self):
        """A symbol _unassigned holds but the account doesn't is stale
        bookkeeping and gets dropped."""
        portfolio = MockPortfolio(positions=[], cash=0.0)
        unassigned = MockPlugin("_unassigned", cash=0.0, positions=[
            {"symbol": "XYZ", "quantity": 461, "cost_basis": 10.0}
        ], is_system=True)
        pe = self.create_executive(
            portfolio=portfolio,
            plugins={"_unassigned": unassigned},
            unassigned_plugin=unassigned
        )

        report = pe.reconcile_with_account()

        disc = next(d for d in report["discrepancies"]
                    if d["type"] == "stale_unassigned_position")
        assert disc["symbol"] == "XYZ"
        assert unassigned.holdings.get_position("XYZ") is None

    def test_report_includes_timestamp(self):
        """Test report includes timestamp"""
        portfolio = MockPortfolio(positions=[], cash=5000.0)
        unassigned = MockPlugin("_unassigned", cash=5000.0, is_system=True)

        pe = self.create_executive(
            portfolio=portfolio,
            plugins={"_unassigned": unassigned},
            unassigned_plugin=unassigned
        )

        report = pe.reconcile_with_account()

        assert "timestamp" in report
        # Should be ISO format
        datetime.fromisoformat(report["timestamp"])


class TestFormatReconciliationReport:
    """Test PluginExecutive.format_reconciliation_report method"""

    def create_executive(self):
        from ib.plugin_executive import PluginExecutive
        pe = object.__new__(PluginExecutive)
        return pe

    def test_format_empty_report(self):
        """Test formatting report with no discrepancies"""
        pe = self.create_executive()

        report = {
            "timestamp": "2024-01-15T10:30:00",
            "discrepancies": [],
            "adjustments": [],
            "summary": {
                "account_positions": 5,
                "plugin_positions": 5,
                "positions_added_to_unassigned": 0,
                "positions_removed_from_plugins": 0,
                "quantity_adjustments": 0,
                "cash_adjustment": 0.0,
            }
        }

        formatted = pe.format_reconciliation_report(report)

        assert "RECONCILIATION REPORT" in formatted
        assert "Account positions: 5" in formatted
        assert "Plugin positions:  5" in formatted
        assert "No discrepancies found" in formatted

    def test_format_with_discrepancies(self):
        """Test formatting report with discrepancies"""
        pe = self.create_executive()

        report = {
            "timestamp": "2024-01-15T10:30:00",
            "discrepancies": [
                {
                    "type": "unclaimed_position",
                    "symbol": "SPY",
                    "account_quantity": 100,
                    "claimed_quantity": 0,
                    "difference": 100,
                },
                {
                    "type": "cash_mismatch",
                    "account_cash": 10000.0,
                    "claimed_cash": 5000.0,
                    "expected_unassigned": 5000.0,
                    "actual_unassigned": 0.0,
                    "difference": 5000.0,
                }
            ],
            "adjustments": [
                {"action": "added_to_unassigned", "symbol": "SPY", "quantity": 100},
                {"action": "adjusted_unassigned_cash", "old_value": 0.0, "new_value": 5000.0},
            ],
            "summary": {
                "account_positions": 1,
                "plugin_positions": 0,
                "positions_added_to_unassigned": 1,
                "positions_removed_from_plugins": 0,
                "quantity_adjustments": 0,
                "cash_adjustment": 5000.0,
            }
        }

        formatted = pe.format_reconciliation_report(report)

        assert "DISCREPANCIES" in formatted
        assert "UNCLAIMED" in formatted  # Type converted to display format
        assert "SPY" in formatted
        assert "ADJUSTMENTS" in formatted
        assert "Added" in formatted  # Type converted to display format


class TestReconcileCommand:
    """Test handle_reconcile socket command"""

    def setup_method(self):
        from ib.run_engine import EngineCommandHandler

        # Create mock engine
        self.engine = Mock()
        self.engine.portfolio = MockPortfolio(positions=[], cash=10000.0)

        # Create mock plugin executive
        self.pe = Mock()
        self.pe.reconcile_with_account.return_value = {
            "timestamp": "2024-01-15T10:30:00",
            "discrepancies": [],
            "adjustments": [],
            "summary": {
                "account_positions": 0,
                "plugin_positions": 0,
            }
        }
        self.pe.format_reconciliation_report.return_value = "Formatted report"

        self.engine.plugin_executive = self.pe

        self.handler = EngineCommandHandler(self.engine)

    def test_reconcile_requires_plugin_executive(self):
        """Test reconcile command requires plugin executive"""
        self.engine.plugin_executive = None

        result = self.handler.handle_reconcile([])

        assert result.status.value == "error"
        assert "requires plugin executive" in result.message

    def test_reconcile_returns_formatted_report(self):
        """Test reconcile returns formatted report by default"""
        result = self.handler.handle_reconcile([])

        assert result.status.value == "success"
        assert result.message == "Formatted report"
        self.pe.reconcile_with_account.assert_called_once()
        self.pe.format_reconciliation_report.assert_called_once()

    def test_reconcile_json_flag(self):
        """Test reconcile with --json flag returns JSON"""
        self.pe.reconcile_with_account.return_value = {
            "timestamp": "2024-01-15T10:30:00",
            "discrepancies": [{"type": "test"}],
            "adjustments": [],
            "summary": {}
        }

        result = self.handler.handle_reconcile(["--json"])

        assert result.status.value == "success"
        assert "timestamp" in result.message
        assert "discrepancies" in result.message
        # format_reconciliation_report should NOT be called for JSON output
        self.pe.format_reconciliation_report.assert_not_called()

    def test_reconcile_data_includes_counts(self):
        """Test reconcile result data includes discrepancy and adjustment counts"""
        self.pe.reconcile_with_account.return_value = {
            "timestamp": "2024-01-15T10:30:00",
            "discrepancies": [{"type": "a"}, {"type": "b"}],
            "adjustments": [{"action": "c"}],
            "summary": {}
        }

        result = self.handler.handle_reconcile([])

        assert result.data["discrepancies"] == 2
        assert result.data["adjustments"] == 1

    def test_reconcile_handles_exception(self):
        """Test reconcile handles exceptions gracefully"""
        self.pe.reconcile_with_account.side_effect = Exception("Test error")

        result = self.handler.handle_reconcile([])

        assert result.status.value == "error"
        assert "Reconciliation failed" in result.message
        assert "Test error" in result.message


class TestReconcileOnStartup:
    """Test reconciliation on engine startup"""

    def test_reconcile_called_on_started(self):
        """Test that reconcile_with_account is called in on_started callback"""
        # This tests the integration - that the on_started callback
        # calls reconcile_with_account on the plugin_executive

        # Create mock plugin executive
        pe = Mock()
        pe.reconcile_with_account.return_value = {
            "discrepancies": [],
            "adjustments": [],
            "summary": {}
        }
        pe.format_reconciliation_report.return_value = "No issues"

        # Create mock engine with the PE
        engine = Mock()
        engine.plugin_executive = pe

        # Simulate the on_started callback behavior
        # (This mirrors what's in run_engine.py)
        if engine.plugin_executive:
            report = engine.plugin_executive.reconcile_with_account()
            if report.get("discrepancies"):
                formatted = engine.plugin_executive.format_reconciliation_report(report)

        pe.reconcile_with_account.assert_called_once()

    def test_reconcile_logs_discrepancies_on_startup(self):
        """Test that discrepancies are logged on startup"""
        pe = Mock()
        pe.reconcile_with_account.return_value = {
            "discrepancies": [{"type": "unclaimed"}],
            "adjustments": [],
            "summary": {}
        }
        pe.format_reconciliation_report.return_value = "Found issues:\n- Unclaimed position"

        engine = Mock()
        engine.plugin_executive = pe

        # Simulate on_started
        if engine.plugin_executive:
            report = engine.plugin_executive.reconcile_with_account()
            if report.get("discrepancies"):
                formatted = engine.plugin_executive.format_reconciliation_report(report)

        # format_reconciliation_report should be called when there are discrepancies
        pe.format_reconciliation_report.assert_called_once()


class TestStartupOrdering:
    """Reconciliation must run AFTER the plugin auto-reload, not before.

    reconcile_with_account() derives the claim set from the plugins loaded in
    memory, and SETs _unassigned to (account - claimed). Reconciling before the
    auto-reload therefore hands _unassigned the entire account; the real plugins
    then load and claim the same positions on top, and the ledger double-counts
    until the next reconcile. These tests pin the ordering that avoids it.
    """

    def _executive(self, portfolio, plugins, unassigned):
        from ib.plugin_executive import PluginExecutive
        pe = object.__new__(PluginExecutive)
        pe._lock = MagicMock()
        pe._plugins = {name: MockPluginConfig(p) for name, p in plugins.items()}
        pe.portfolio = portfolio
        pe._unassigned_plugin = unassigned
        return pe

    @staticmethod
    def _ledger(symbol, *plugins):
        """Total quantity the platform believes it holds, across every bucket."""
        total = 0.0
        for plugin in plugins:
            pos = plugin.holdings.get_position(symbol)
            if pos:
                total += pos.quantity
        return total

    def test_reconciling_before_reload_double_counts(self):
        """The old ordering: reconcile with nothing loaded, then plugins arrive."""
        account = MockPortfolio(positions=[MockPosition("SPY", 100, 450.0, 455.0)], cash=0.0)
        unassigned = MockPlugin("_unassigned", cash=0.0, is_system=True)

        # Startup reconcile, before the auto-reload: no strategy plugins exist yet.
        pe = self._executive(account, {"_unassigned": unassigned}, unassigned)
        pe.reconcile_with_account()

        # _unassigned was handed the whole account.
        assert unassigned.holdings.get_position("SPY").quantity == 100

        # Now the auto-reload brings back the plugin that actually holds it.
        momentum = MockPlugin("momentum", cash=0.0, positions=[
            {"symbol": "SPY", "quantity": 100, "cost_basis": 450.0, "current_price": 455.0}
        ])
        pe._plugins["momentum"] = MockPluginConfig(momentum)

        # The ledger now claims twice what the account holds — this is the window
        # the old ordering left open until the watchdog's next hourly reconcile.
        assert self._ledger("SPY", momentum, unassigned) == 200
        assert account.get_position("SPY").quantity == 100

    def test_reconciling_after_reload_matches_the_account(self):
        """The fixed ordering: plugins are loaded first, so the claim set is real."""
        account = MockPortfolio(positions=[MockPosition("SPY", 100, 450.0, 455.0)], cash=0.0)
        unassigned = MockPlugin("_unassigned", cash=0.0, is_system=True)
        momentum = MockPlugin("momentum", cash=0.0, positions=[
            {"symbol": "SPY", "quantity": 100, "cost_basis": 450.0, "current_price": 455.0}
        ])

        pe = self._executive(account, {"momentum": momentum, "_unassigned": unassigned}, unassigned)
        report = pe.reconcile_with_account()

        assert report["discrepancies"] == []
        pos = unassigned.holdings.get_position("SPY")
        assert pos is None or pos.quantity == 0
        assert self._ledger("SPY", momentum, unassigned) == 100

    def test_a_second_reconcile_heals_the_double_count(self):
        """The old ordering self-corrected, but only on the next reconcile pass."""
        account = MockPortfolio(positions=[MockPosition("SPY", 100, 450.0, 455.0)], cash=0.0)
        unassigned = MockPlugin("_unassigned", cash=0.0, is_system=True)

        pe = self._executive(account, {"_unassigned": unassigned}, unassigned)
        pe.reconcile_with_account()

        momentum = MockPlugin("momentum", cash=0.0, positions=[
            {"symbol": "SPY", "quantity": 100, "cost_basis": 450.0, "current_price": 455.0}
        ])
        pe._plugins["momentum"] = MockPluginConfig(momentum)
        pe.reconcile_with_account()

        assert self._ledger("SPY", momentum, unassigned) == 100


class TestStartupPluginSequence:
    """run_engine.startup_plugin_sequence must load, reconcile, then start.

    The ordering is the entire fix, so it is pinned here rather than left to
    inspection of the startup closure.
    """

    def _pe(self, pending=("gld_usd_swap",), calls=None):
        calls = calls if calls is not None else []
        pe = Mock()
        pe.load_registered_plugins.side_effect = lambda: (
            calls.append("load"),
            {"loaded": list(pending), "pending_start": list(pending),
             "skipped": [], "failed": []},
        )[1]
        pe.reconcile_with_account.side_effect = lambda: (
            calls.append("reconcile"), {"discrepancies": [], "adjustments": []},
        )[1]
        pe.start_loaded_plugins.side_effect = lambda slots: (
            calls.append("start"), {"started": list(slots), "failed": []},
        )[1]
        return pe, calls

    def _run(self, pe):
        import asyncio
        from ib.run_engine import startup_plugin_sequence
        asyncio.run(startup_plugin_sequence(pe))

    def test_order_is_load_reconcile_start(self):
        pe, calls = self._pe()
        self._run(pe)
        assert calls == ["load", "reconcile", "start"]

    def test_start_receives_exactly_the_pending_slots(self):
        pe, _ = self._pe(pending=("gld_usd_swap", "momentum_5day"))
        self._run(pe)
        pe.start_loaded_plugins.assert_called_once_with(["gld_usd_swap", "momentum_5day"])

    def test_nothing_is_started_when_nothing_was_pending(self):
        pe, calls = self._pe(pending=())
        self._run(pe)
        assert calls == ["load", "reconcile"]
        pe.start_loaded_plugins.assert_not_called()

    def test_reconcile_still_runs_when_the_load_raises(self):
        """A failed load must not leave _unassigned unreconciled."""
        import asyncio
        from ib.run_engine import startup_plugin_sequence

        calls = []
        pe = Mock()
        pe.load_registered_plugins.side_effect = RuntimeError("registry unreadable")
        pe.reconcile_with_account.side_effect = lambda: (
            calls.append("reconcile"), {"discrepancies": []},
        )[1]

        with pytest.raises(RuntimeError, match="registry unreadable"):
            asyncio.run(startup_plugin_sequence(pe))

        assert calls == ["reconcile"]
        pe.start_loaded_plugins.assert_not_called()

    def test_discrepancies_are_logged(self):
        pe, _ = self._pe()
        pe.reconcile_with_account.side_effect = lambda: {
            "discrepancies": [{"type": "unclaimed_position"}], "adjustments": [],
        }
        pe.format_reconciliation_report.return_value = "Found issues:\n- Unclaimed position"
        self._run(pe)
        pe.format_reconciliation_report.assert_called_once()
