"""
unassigned/plugin.py - System plugin for unattributed positions and cash

This is a special system-managed plugin that:
- Holds positions not attributed to any other plugin
- Tracks account cash balance
- Cannot be unloaded or deleted by users
- Does not generate trade signals

The plugin is automatically created and managed by PluginExecutive.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Set

from ..base import (
    PluginBase,
    PluginState,
    TradeSignal,
    Holdings,
    HoldingPosition,
    PluginInstrument,
)

logger = logging.getLogger(__name__)


# Reserved name for the unassigned plugin
UNASSIGNED_PLUGIN_NAME = "_unassigned"


class UnassignedPlugin(PluginBase):
    """
    System plugin for tracking unattributed positions and cash.

    This plugin automatically tracks:
    - Account cash balance
    - Positions not claimed by any other plugin

    It is managed by the system and cannot be unloaded by users.
    It does not generate trade signals - it only tracks holdings.

    Usage:
        # Created automatically by PluginExecutive
        plugin = UnassignedPlugin(portfolio=portfolio)
        plugin.load()
        plugin.start()

        # Sync with current portfolio state
        plugin.sync_from_portfolio(claimed_symbols={'SPY', 'QQQ'})

        # Get unassigned holdings
        holdings = plugin.get_effective_holdings()
    """

    VERSION = "1.0.0"
    IS_SYSTEM_PLUGIN = True  # Cannot be unloaded by users

    def __init__(
        self,
        portfolio=None,
        base_path: Optional[Path] = None,
        message_bus=None,
    ):
        """
        Initialize the unassigned plugin.

        Args:
            portfolio: Portfolio instance for reading account data
            base_path: Base path for plugin files
            message_bus: Optional MessageBus for notifications
        """
        super().__init__(
            name=UNASSIGNED_PLUGIN_NAME,
            base_path=base_path,
            portfolio=portfolio,
            message_bus=message_bus,
        )

        # Track which symbols are claimed by other plugins
        self._claimed_symbols: Set[str] = set()

        # Cash balance
        self._cash_balance: float = 0.0

        # Last sync time
        self._last_sync: Optional[datetime] = None

    @property
    def description(self) -> str:
        return "System plugin for tracking unattributed positions and account cash"

    @property
    def cash_balance(self) -> float:
        """Current cash balance"""
        return self._cash_balance

    @property
    def claimed_symbols(self) -> Set[str]:
        """Symbols claimed by other plugins"""
        return self._claimed_symbols.copy()

    @property
    def last_sync(self) -> Optional[datetime]:
        """Last time holdings were synced from portfolio"""
        return self._last_sync

    # =========================================================================
    # Lifecycle Methods
    # =========================================================================

    def start(self) -> bool:
        """Start the plugin"""
        try:
            state = self.load_state()
            self._cash_balance = state.get("cash_balance", 0.0)
            self._claimed_symbols = set(state.get("claimed_symbols", []))

            self.state = PluginState.STARTED
            logger.info(f"Unassigned plugin started: cash=${self._cash_balance:,.2f}")
            return True
        except Exception as e:
            logger.error(f"Failed to start unassigned plugin: {e}")
            self.state = PluginState.ERROR
            return False

    def stop(self) -> bool:
        """Stop the plugin"""
        try:
            self.save_state({
                "cash_balance": self._cash_balance,
                "claimed_symbols": list(self._claimed_symbols),
            })
            self.save_holdings()
            self.state = PluginState.STOPPED
            logger.info("Unassigned plugin stopped")
            return True
        except Exception as e:
            logger.error(f"Failed to stop unassigned plugin: {e}")
            return False

    def freeze(self) -> bool:
        """Freeze the plugin"""
        try:
            self.save_state({
                "cash_balance": self._cash_balance,
                "claimed_symbols": list(self._claimed_symbols),
            })
            self.state = PluginState.FROZEN
            return True
        except Exception as e:
            logger.error(f"Failed to freeze unassigned plugin: {e}")
            return False

    def resume(self) -> bool:
        """Resume the plugin"""
        self.state = PluginState.STARTED
        return True

    def handle_request(self, request_type: str, payload: Dict) -> Dict:
        """Handle custom requests"""
        if request_type == "sync":
            # Trigger a sync from portfolio
            claimed = set(payload.get("claimed_symbols", []))
            self.sync_from_portfolio(claimed)
            return {"success": True, "message": "Synced from portfolio"}

        elif request_type == "get_cash":
            return {"success": True, "cash": self._cash_balance}

        elif request_type == "get_unassigned":
            return {
                "success": True,
                "cash": self._cash_balance,
                "positions": [p.to_dict() for p in self._holdings.current_positions] if self._holdings else [],
            }

        return {"success": False, "message": f"Unknown request: {request_type}"}

    # =========================================================================
    # Trading Interface (no-op for this plugin)
    # =========================================================================

    def calculate_signals(self) -> List[TradeSignal]:
        """
        This plugin does not generate trade signals.
        """
        return []

    # =========================================================================
    # Sync Methods
    # =========================================================================

    def sync_from_portfolio(
        self,
        claimed_symbols: Optional[Set[str]] = None,
        claimed_cash: float = 0.0,
    ) -> bool:
        """
        Sync unassigned positions and cash from portfolio.

        Args:
            claimed_symbols: Symbols already claimed by other plugins
            claimed_cash: Cash already allocated to other plugins

        Returns:
            True if sync successful
        """
        if self.portfolio is None:
            logger.warning("Cannot sync: no portfolio connected")
            return False

        try:
            if claimed_symbols is not None:
                self._claimed_symbols = claimed_symbols

            # Get account summary for cash
            account = self.portfolio.get_account_summary()
            if account and account.is_valid:
                total_cash = account.available_funds or 0.0
                self._cash_balance = max(0.0, total_cash - claimed_cash)
            else:
                # Try to get cash from portfolio
                self._cash_balance = max(0.0, getattr(self.portfolio, 'cash', 0.0) - claimed_cash)

            # Get unassigned positions
            unassigned_positions = []
            for pos in self.portfolio.positions:
                if pos.symbol not in self._claimed_symbols:
                    unassigned_positions.append(HoldingPosition(
                        symbol=pos.symbol,
                        quantity=pos.quantity,
                        cost_basis=pos.avg_cost,
                        current_price=pos.current_price,
                        market_value=pos.market_value,
                    ))

            # Update holdings
            if self._holdings is None:
                self._holdings = Holdings(
                    plugin_name=self.name,
                    created_at=datetime.now(),
                )

            self._holdings.current_cash = self._cash_balance
            self._holdings.current_positions = unassigned_positions
            self._holdings.last_updated = datetime.now()

            self._last_sync = datetime.now()

            logger.debug(
                f"Synced unassigned: cash=${self._cash_balance:,.2f}, "
                f"positions={len(unassigned_positions)}"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to sync from portfolio: {e}")
            return False

    def set_claimed_symbols(self, symbols: Set[str]):
        """
        Update the set of symbols claimed by other plugins.

        Args:
            symbols: Set of symbols claimed by other plugins
        """
        self._claimed_symbols = symbols

    def add_claimed_symbol(self, symbol: str):
        """Add a symbol to the claimed set"""
        self._claimed_symbols.add(symbol.upper())

    def remove_claimed_symbol(self, symbol: str):
        """Remove a symbol from the claimed set"""
        self._claimed_symbols.discard(symbol.upper())

    def set_cash_balance(self, cash: float):
        """
        Directly set the cash balance.

        Args:
            cash: Cash balance to set
        """
        self._cash_balance = cash
        if self._holdings:
            self._holdings.current_cash = cash

    # =========================================================================
    # Override get_effective_holdings to include cash
    # =========================================================================

    def get_effective_holdings(self) -> Dict:
        """
        Get unassigned holdings including cash.

        Returns:
            Dict with cash, positions, total_value
        """
        if self._holdings:
            positions = [
                {
                    "symbol": p.symbol,
                    "quantity": p.quantity,
                    "current_price": p.current_price,
                    "market_value": p.market_value,
                    "cost_basis": p.cost_basis,
                }
                for p in self._holdings.current_positions
            ]
            position_value = sum(p.market_value for p in self._holdings.current_positions)
        else:
            positions = []
            position_value = 0.0

        return {
            "plugin": self.name,
            "is_system_plugin": True,
            "cash": self._cash_balance,
            "positions": positions,
            "position_value": position_value,
            "total_value": self._cash_balance + position_value,
            "last_sync": self._last_sync.isoformat() if self._last_sync else None,
        }

    def get_status(self) -> Dict[str, Any]:
        """Get plugin status with cash info"""
        status = super().get_status()
        status.update({
            "is_system_plugin": True,
            "cash_balance": self._cash_balance,
            "claimed_symbols": list(self._claimed_symbols),
            "unassigned_positions": len(self._holdings.current_positions) if self._holdings else 0,
            "last_sync": self._last_sync.isoformat() if self._last_sync else None,
        })
        return status

    # =========================================================================
    # Load/Initialize
    # =========================================================================

    def load(self) -> bool:
        """
        Load the plugin.

        For the unassigned plugin, we don't require instruments file.
        """
        try:
            self._load_holdings()
            self._loaded = True
            self.state = PluginState.LOADED
            logger.info(f"Unassigned plugin loaded")
            return True
        except Exception as e:
            logger.error(f"Failed to load unassigned plugin: {e}")
            self.state = PluginState.ERROR
            return False
