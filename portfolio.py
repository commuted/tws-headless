"""
portfolio.py - Portfolio loading and management

Handles downloading positions, market data, and account information
from Interactive Brokers.
"""

import logging
from threading import Event, Lock
from typing import Dict, List, Optional

from ibapi.contract import Contract
from ibapi.ticktype import TickTypeEnum
from ibapi.account_summary_tags import AccountSummaryTags

from .client import IBClient
from .models import Position, AssetType, AccountSummary

logger = logging.getLogger(__name__)


class Portfolio(IBClient):
    """
    Portfolio manager for Interactive Brokers.

    Extends IBClient with position tracking, market data,
    and account summary functionality.

    Usage:
        portfolio = Portfolio()
        if portfolio.connect():
            portfolio.load()
            for pos in portfolio.positions:
                print(pos)
            portfolio.disconnect()
    """

    def __init__(self, **kwargs):
        """Initialize the portfolio manager"""
        super().__init__(**kwargs)

        # Position storage
        self._positions: Dict[str, Position] = {}
        self._positions_lock = Lock()
        self._positions_done = Event()

        # Market data tracking
        self._market_data_requests: Dict[int, str] = {}  # reqId -> symbol
        self._market_data_done = Event()
        self._market_data_pending = 0
        self._market_data_received = 0

        # Account data
        self._account_summary: Dict[str, AccountSummary] = {}
        self._account_summary_done = Event()

    @property
    def positions(self) -> List[Position]:
        """Get list of all positions"""
        with self._positions_lock:
            return list(self._positions.values())

    @property
    def total_value(self) -> float:
        """Get total portfolio market value"""
        return sum(p.market_value for p in self.positions)

    @property
    def total_pnl(self) -> float:
        """Get total unrealized P&L"""
        return sum(p.unrealized_pnl for p in self.positions)

    def get_position(self, symbol: str) -> Optional[Position]:
        """Get a specific position by symbol"""
        with self._positions_lock:
            return self._positions.get(symbol)

    def get_account_summary(self, account: Optional[str] = None) -> Optional[AccountSummary]:
        """
        Get account summary.

        Args:
            account: Account ID. If None, uses first managed account.
        """
        if account is None and self.managed_accounts:
            account = self.managed_accounts[0]
        return self._account_summary.get(account)

    def load(
        self,
        fetch_prices: bool = True,
        fetch_account: bool = True,
        timeout: float = 30.0,
    ) -> bool:
        """
        Load portfolio data from IB.

        Args:
            fetch_prices: Whether to fetch current market prices
            fetch_account: Whether to fetch account summary
            timeout: Timeout in seconds

        Returns:
            True if successful
        """
        if not self.connected:
            logger.error("Not connected to IB")
            return False

        # Clear previous data
        with self._positions_lock:
            self._positions.clear()
        self._positions_done.clear()

        # Request positions
        logger.info("Requesting positions...")
        self.reqPositions()

        # Wait for positions
        if not self._positions_done.wait(timeout=timeout):
            logger.warning("Timeout waiting for positions")

        logger.info(f"Loaded {len(self._positions)} positions")

        # Fetch market prices
        if fetch_prices and self._positions:
            logger.info("Fetching market prices...")
            self._fetch_market_data(timeout=timeout)

        # Calculate allocations
        self._calculate_allocations()

        # Fetch account summary
        if fetch_account:
            logger.info("Fetching account summary...")
            self._fetch_account_summary(timeout=timeout)

        return True

    def _fetch_market_data(self, timeout: float = 30.0):
        """Fetch market data for all positions"""
        self._market_data_requests.clear()
        self._market_data_done.clear()
        self._market_data_pending = len(self._positions)
        self._market_data_received = 0

        for symbol, pos in self._positions.items():
            if pos.contract:
                req_id = self.get_next_req_id()
                self._market_data_requests[req_id] = symbol
                # Use delayed data (free, no subscription required)
                self.reqMarketDataType(3)
                self.reqMktData(req_id, pos.contract, "", True, False, [])

        self._market_data_done.wait(timeout=timeout)

    def _fetch_account_summary(self, timeout: float = 10.0):
        """Fetch account summary"""
        self._account_summary_done.clear()

        req_id = self.get_next_req_id()
        self.reqAccountSummary(req_id, "All", AccountSummaryTags.AllTags)

        self._account_summary_done.wait(timeout=timeout)
        self.cancelAccountSummary(req_id)

    def _calculate_allocations(self):
        """Calculate allocation percentages for all positions"""
        total = self.total_value
        if total > 0:
            for pos in self._positions.values():
                pos.allocation_pct = (pos.market_value / total) * 100

    def to_dataframe(self):
        """
        Convert positions to pandas DataFrame.

        Returns:
            pandas.DataFrame with position data
        """
        try:
            import pandas as pd
            data = [pos.to_dict() for pos in self.positions]
            return pd.DataFrame(data)
        except ImportError:
            raise ImportError("pandas required: pip install pandas")

    # =========================================================================
    # EWrapper Callbacks for Positions
    # =========================================================================

    def position(self, account: str, contract: Contract, pos: float, avgCost: float):
        """Handle position data from IB"""
        if pos == 0:
            return  # Skip closed positions

        asset_type = AssetType.from_sec_type(contract.secType)
        position = Position(
            symbol=contract.symbol,
            asset_type=asset_type,
            quantity=pos,
            avg_cost=avgCost,
            contract=contract,
            account=account,
        )

        with self._positions_lock:
            self._positions[contract.symbol] = position

        logger.debug(f"Position: {contract.symbol} {pos} @ ${avgCost:.2f}")

        if "position" in self._callbacks:
            self._callbacks["position"](position)

    def positionEnd(self):
        """Called when all positions have been received"""
        logger.debug(f"Position download complete: {len(self._positions)} positions")
        self._positions_done.set()

        if "positionEnd" in self._callbacks:
            self._callbacks["positionEnd"]()

    # =========================================================================
    # EWrapper Callbacks for Market Data
    # =========================================================================

    def tickPrice(self, reqId, tickType, price, attrib):
        """Handle market data price updates"""
        if reqId not in self._market_data_requests:
            return

        # Accept last, close, or delayed prices
        valid_types = (
            TickTypeEnum.LAST,
            TickTypeEnum.CLOSE,
            TickTypeEnum.DELAYED_LAST,
            TickTypeEnum.DELAYED_CLOSE,
        )

        if tickType in valid_types and price > 0:
            symbol = self._market_data_requests[reqId]

            with self._positions_lock:
                if symbol in self._positions:
                    self._positions[symbol].update_market_data(price)
                    logger.debug(f"Price update: {symbol} = ${price:.2f}")

            # Cancel subscription after receiving price
            self.cancelMktData(reqId)
            del self._market_data_requests[reqId]
            self._market_data_received += 1

            if self._market_data_received >= self._market_data_pending:
                self._market_data_done.set()

    def tickSnapshotEnd(self, reqId: int):
        """Called when snapshot is complete"""
        if reqId in self._market_data_requests:
            self._market_data_received += 1
            if self._market_data_received >= self._market_data_pending:
                self._market_data_done.set()

    # =========================================================================
    # EWrapper Callbacks for Account Summary
    # =========================================================================

    def accountSummary(
        self, reqId: int, account: str, tag: str, value: str, currency: str
    ):
        """Handle account summary data"""
        if account not in self._account_summary:
            self._account_summary[account] = AccountSummary(account_id=account)

        summary = self._account_summary[account]
        summary.values[tag] = {"value": value, "currency": currency}

        # Parse key values
        try:
            if tag == "NetLiquidation":
                summary.net_liquidation = float(value)
            elif tag == "TotalCashValue":
                summary.total_cash = float(value)
            elif tag == "BuyingPower":
                summary.buying_power = float(value)
            elif tag == "AvailableFunds":
                summary.available_funds = float(value)
        except ValueError:
            pass

    def accountSummaryEnd(self, reqId: int):
        """Called when account summary is complete"""
        logger.debug("Account summary complete")
        self._account_summary_done.set()

        if "accountSummaryEnd" in self._callbacks:
            self._callbacks["accountSummaryEnd"]()


def quick_load(
    host: str = "127.0.0.1",
    port: int = 7497,
    client_id: int = 1,
    fetch_prices: bool = True,
) -> List[Position]:
    """
    Convenience function to quickly load portfolio positions.

    Args:
        host: IB Gateway/TWS host
        port: IB Gateway/TWS port
        client_id: Client ID for connection
        fetch_prices: Whether to fetch current prices

    Returns:
        List of Position objects
    """
    portfolio = Portfolio(host=host, port=port, client_id=client_id)

    if not portfolio.connect():
        return []

    try:
        portfolio.load(fetch_prices=fetch_prices)
        return portfolio.positions
    finally:
        portfolio.disconnect()
