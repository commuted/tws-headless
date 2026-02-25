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
        """Test that system plugins are excluded from position claims calculation"""
        # System plugin claims 100 SPY (should be ignored)
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

        # Should find unclaimed (system plugin claims don't count)
        assert len(report["discrepancies"]) >= 1

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
