"""
portfolio.py - Portfolio loading and management

Handles downloading positions, market data, and account information
from Interactive Brokers. Supports both snapshot and streaming market data.
"""

import logging
from threading import Event, Lock
from typing import Dict, List, Optional, Callable
from datetime import datetime

from ibapi.contract import Contract
from ibapi.ticktype import TickTypeEnum
from ibapi.account_summary_tags import AccountSummaryTags

from .client import IBClient
from .models import Position, AssetType, AccountSummary

logger = logging.getLogger(__name__)


# Tick types we care about for price updates
PRICE_TICK_TYPES = (
    TickTypeEnum.LAST,
    TickTypeEnum.CLOSE,
    TickTypeEnum.DELAYED_LAST,
    TickTypeEnum.DELAYED_CLOSE,
    TickTypeEnum.BID,
    TickTypeEnum.ASK,
)

# Mapping tick type to name for display
TICK_TYPE_NAMES = {
    TickTypeEnum.LAST: "LAST",
    TickTypeEnum.CLOSE: "CLOSE",
    TickTypeEnum.DELAYED_LAST: "DELAYED_LAST",
    TickTypeEnum.DELAYED_CLOSE: "DELAYED_CLOSE",
    TickTypeEnum.BID: "BID",
    TickTypeEnum.ASK: "ASK",
    TickTypeEnum.HIGH: "HIGH",
    TickTypeEnum.LOW: "LOW",
    TickTypeEnum.OPEN: "OPEN",
    TickTypeEnum.VOLUME: "VOLUME",
}


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

        # Market data tracking (for snapshots)
        self._market_data_requests: Dict[int, str] = {}  # reqId -> symbol
        self._market_data_done = Event()
        self._market_data_pending = 0
        self._market_data_received = 0

        # Streaming market data
        self._streaming: bool = False
        self._stream_subscriptions: Dict[int, str] = {}  # reqId -> symbol
        self._stream_req_ids: Dict[str, int] = {}  # symbol -> reqId
        self._on_tick: Optional[Callable[[str, float, str], None]] = None
        self._last_prices: Dict[str, Dict[str, float]] = {}  # symbol -> {type: price}

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

    @property
    def is_streaming(self) -> bool:
        """Check if streaming is active"""
        return self._streaming and len(self._stream_subscriptions) > 0

    @property
    def streaming_symbols(self) -> List[str]:
        """Get list of symbols currently being streamed"""
        return list(self._stream_subscriptions.values())

    def get_position(self, symbol: str) -> Optional[Position]:
        """Get a specific position by symbol"""
        with self._positions_lock:
            return self._positions.get(symbol)

    def get_last_price(self, symbol: str, tick_type: str = "LAST") -> Optional[float]:
        """
        Get the last received price for a symbol.

        Args:
            symbol: The symbol to get price for
            tick_type: Type of price (LAST, BID, ASK, etc.)

        Returns:
            Last price or None if not available
        """
        if symbol in self._last_prices:
            return self._last_prices[symbol].get(tick_type)
        return None

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

    # =========================================================================
    # Streaming Market Data
    # =========================================================================

    def start_streaming(
        self,
        on_tick: Optional[Callable[[str, float, str], None]] = None,
        use_delayed: bool = True,
    ) -> bool:
        """
        Start streaming market data for all portfolio positions.

        Args:
            on_tick: Callback function(symbol, price, tick_type) called on each tick
            use_delayed: Use delayed data (free) vs live data (requires subscription)

        Returns:
            True if streaming started successfully
        """
        if not self.connected:
            logger.error("Not connected to IB")
            return False

        if not self._positions:
            logger.warning("No positions to stream - load portfolio first")
            return False

        if self._streaming:
            logger.warning("Already streaming - stop first")
            return False

        self._on_tick = on_tick
        self._streaming = True

        # Set market data type
        data_type = 3 if use_delayed else 1  # 3=delayed, 1=live
        self.reqMarketDataType(data_type)

        logger.info(f"Starting streams for {len(self._positions)} positions...")

        for symbol, pos in self._positions.items():
            if pos.contract:
                self._start_stream(symbol, pos.contract)

        logger.info(f"Streaming {len(self._stream_subscriptions)} symbols")
        return True

    def _start_stream(self, symbol: str, contract: Contract) -> int:
        """Start streaming for a single symbol"""
        req_id = self.get_next_req_id()

        self._stream_subscriptions[req_id] = symbol
        self._stream_req_ids[symbol] = req_id
        self._last_prices[symbol] = {}

        # Request streaming data (snapshot=False for continuous updates)
        # Generic tick types: 233=RTVolume, 236=Shortable, etc.
        self.reqMktData(req_id, contract, "", False, False, [])

        logger.debug(f"Started stream for {symbol} (reqId={req_id})")
        return req_id

    def stop_streaming(self):
        """Stop all streaming market data subscriptions"""
        if not self._streaming:
            return

        logger.info(f"Stopping {len(self._stream_subscriptions)} streams...")

        for req_id, symbol in list(self._stream_subscriptions.items()):
            self.cancelMktData(req_id)
            logger.debug(f"Stopped stream for {symbol}")

        self._stream_subscriptions.clear()
        self._stream_req_ids.clear()
        self._streaming = False
        self._on_tick = None

        logger.info("All streams stopped")

    def stream_symbol(
        self,
        symbol: str,
        contract: Optional[Contract] = None,
    ) -> bool:
        """
        Add a single symbol to the stream.

        Args:
            symbol: Symbol to stream
            contract: IB Contract (uses position's contract if None)

        Returns:
            True if added successfully
        """
        if not self.connected:
            return False

        # Get contract from position if not provided
        if contract is None:
            pos = self.get_position(symbol)
            if pos and pos.contract:
                contract = pos.contract
            else:
                logger.error(f"No contract for {symbol}")
                return False

        if symbol in self._stream_req_ids:
            logger.debug(f"{symbol} already streaming")
            return True

        self._streaming = True
        self._start_stream(symbol, contract)
        return True

    def unstream_symbol(self, symbol: str):
        """Remove a single symbol from the stream"""
        if symbol not in self._stream_req_ids:
            return

        req_id = self._stream_req_ids[symbol]
        self.cancelMktData(req_id)

        del self._stream_subscriptions[req_id]
        del self._stream_req_ids[symbol]

        logger.debug(f"Stopped stream for {symbol}")

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
        """Handle market data price updates (both snapshot and streaming)"""
        # Check if this is a streaming subscription
        if reqId in self._stream_subscriptions:
            self._handle_stream_tick(reqId, tickType, price)
            return

        # Handle snapshot request
        if reqId not in self._market_data_requests:
            return

        # Accept last, close, or delayed prices for snapshots
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

    def _handle_stream_tick(self, reqId: int, tickType: int, price: float):
        """Handle a streaming tick update"""
        if price <= 0:
            return

        symbol = self._stream_subscriptions.get(reqId)
        if not symbol:
            return

        # Get tick type name
        tick_name = TICK_TYPE_NAMES.get(tickType, f"TICK_{tickType}")

        # Only process price-related ticks
        if tickType not in PRICE_TICK_TYPES:
            return

        # Store the price
        if symbol not in self._last_prices:
            self._last_prices[symbol] = {}
        self._last_prices[symbol][tick_name] = price

        # Update position with LAST price
        if tickType in (TickTypeEnum.LAST, TickTypeEnum.DELAYED_LAST):
            with self._positions_lock:
                if symbol in self._positions:
                    self._positions[symbol].update_market_data(price)
                    self._calculate_allocations()

        # Call the callback if registered
        if self._on_tick:
            try:
                self._on_tick(symbol, price, tick_name)
            except Exception as e:
                logger.error(f"Error in tick callback: {e}")

        # Also invoke registered callback
        if "tick" in self._callbacks:
            self._callbacks["tick"](symbol, price, tick_name)

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
