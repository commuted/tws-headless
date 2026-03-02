"""
portfolio.py - Portfolio loading and management

Handles downloading positions, market data, and account information
from Interactive Brokers. Supports both snapshot and streaming market data.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Callable
from datetime import datetime

from ibapi.contract import Contract
from ibapi.ticktype import TickTypeEnum
from ibapi.account_summary_tags import AccountSummaryTags

from ibapi.order import Order

from .client import IBClient
from .models import (
    Position,
    AssetType,
    AccountSummary,
    Bar,
    BarSize,
    OrderRecord,
    OrderStatus,
    CommissionAndFeesReport,
    PnLData,
)
from .execution_db import (
    ExecutionDatabase,
    ExecutionRecord,
    CommissionRecord,
    get_execution_db,
)

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

# Size tick types (from tickSize callback)
SIZE_TICK_TYPES = (
    TickTypeEnum.BID_SIZE,
    TickTypeEnum.ASK_SIZE,
    TickTypeEnum.LAST_SIZE,
    TickTypeEnum.VOLUME,
    TickTypeEnum.DELAYED_BID_SIZE,
    TickTypeEnum.DELAYED_ASK_SIZE,
    TickTypeEnum.DELAYED_LAST_SIZE,
    TickTypeEnum.DELAYED_VOLUME,
)

# Mapping size tick type to name
SIZE_TICK_TYPE_NAMES = {
    TickTypeEnum.BID_SIZE: "BID_SIZE",
    TickTypeEnum.ASK_SIZE: "ASK_SIZE",
    TickTypeEnum.LAST_SIZE: "LAST_SIZE",
    TickTypeEnum.VOLUME: "VOLUME",
    TickTypeEnum.DELAYED_BID_SIZE: "DELAYED_BID_SIZE",
    TickTypeEnum.DELAYED_ASK_SIZE: "DELAYED_ASK_SIZE",
    TickTypeEnum.DELAYED_LAST_SIZE: "DELAYED_LAST_SIZE",
    TickTypeEnum.DELAYED_VOLUME: "DELAYED_VOLUME",
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
        self._positions_done = asyncio.Event()

        # Market data tracking (for snapshots)
        self._market_data_requests: Dict[int, str] = {}  # reqId -> symbol
        self._market_data_done = asyncio.Event()
        self._market_data_pending = 0
        self._market_data_received = 0

        # Streaming market data (ticks)
        self._streaming: bool = False
        self._stream_subscriptions: Dict[int, str] = {}  # reqId -> symbol
        self._stream_req_ids: Dict[str, int] = {}  # symbol -> reqId
        self._on_tick: Optional[Callable[[str, float, str], None]] = None
        self._on_tick_size: Optional[Callable[[str, int, str], None]] = None
        self._last_prices: Dict[str, Dict[str, float]] = {}  # symbol -> {type: price}
        self._last_sizes: Dict[str, Dict[str, int]] = {}  # symbol -> {type: size}

        # Streaming bar data
        self._bar_streaming: bool = False
        self._bar_subscriptions: Dict[int, str] = {}  # reqId -> symbol
        self._bar_req_ids: Dict[str, int] = {}  # symbol -> reqId
        self._on_bar: Optional[Callable[[Bar], None]] = None
        self._last_bars: Dict[str, Bar] = {}  # symbol -> last bar

        # Historical data requests (one-shot, per-requester callbacks)
        # reqId -> (on_bar_cb, on_end_cb, accumulated_bars)
        self._historical_requests: Dict[int, tuple] = {}

        # Account data
        self._account_summary: Dict[str, AccountSummary] = {}
        self._account_summary_done = asyncio.Event()

        # Forex cash balances (for synthetic forex positions from short sales)
        # When you sell EUR.USD, you get negative EUR cash balance instead of a position
        self._forex_cash: Dict[str, float] = {}  # currency -> cash balance (e.g., {"EUR": -20000})
        self._forex_rates: Dict[str, float] = {}  # currency -> exchange rate to USD
        self._forex_positions: Dict[str, Position] = {}  # symbol -> synthetic forex position
        self._forex_cost_basis: Dict[str, float] = {}  # currency -> original cost basis (persisted)
        self._account_updates_done = asyncio.Event()

        # Execution tracking
        self._executions_done = asyncio.Event()
        self._execution_db: Optional[ExecutionDatabase] = None

        # Load persisted forex cost basis
        self._load_forex_cost_basis()

        # Order tracking
        self._orders: Dict[int, OrderRecord] = {}  # orderId -> OrderRecord
        self._pending_orders: Dict[int, asyncio.Event] = {}  # orderId -> completion event
        self._on_order_status: Optional[Callable[[OrderRecord], None]] = None

        # Commission tracking - maps exec_id to commission report
        self._commission_reports: Dict[str, "CommissionAndFeesReport"] = {}
        self._on_commission: Optional[Callable[[str, float, float], None]] = None

        # Tick-by-tick subscriptions
        self._tbt_subscriptions: Dict[int, str] = {}   # reqId -> symbol
        self._tbt_req_ids: Dict[str, Dict[str, int]] = {}  # symbol -> {tick_type -> reqId}
        self._on_tick_by_tick: Optional[Callable] = None

        # Market depth subscriptions and books
        self._depth_subscriptions: Dict[int, str] = {}  # reqId -> symbol
        self._depth_req_ids: Dict[str, int] = {}         # symbol -> reqId
        self._depth_books: Dict[str, dict] = {}          # symbol -> {"bids": {pos: level}, "asks": {pos: level}}
        self._on_depth: Optional[Callable] = None

        # P&L subscriptions
        self._pnl_req_id: Optional[int] = None                  # account-level reqId
        self._pnl_single_req_ids: Dict[int, str] = {}           # reqId -> symbol
        self._pnl_single_symbols: Dict[str, int] = {}           # symbol -> reqId
        self._on_pnl: Optional[Callable] = None                 # fires for both account + single

    @property
    def positions(self) -> List[Position]:
        """Get list of all positions including synthetic forex positions"""
        # Combine regular positions with synthetic forex positions
        all_positions = list(self._positions.values())
        all_positions.extend(self._forex_positions.values())
        return all_positions

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

    @property
    def is_bar_streaming(self) -> bool:
        """Check if bar streaming is active"""
        return self._bar_streaming and len(self._bar_subscriptions) > 0

    @property
    def bar_streaming_symbols(self) -> List[str]:
        """Get list of symbols currently streaming bars"""
        return list(self._bar_subscriptions.values())

    def get_position(self, symbol: str) -> Optional[Position]:
        """Get a specific position by symbol (including synthetic forex positions)"""
        # Check regular positions first
        pos = self._positions.get(symbol)
        if pos:
            return pos
        # Then check synthetic forex positions
        return self._forex_positions.get(symbol)

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

    def get_last_bar(self, symbol: str) -> Optional[Bar]:
        """
        Get the last received bar for a symbol.

        Args:
            symbol: The symbol to get bar for

        Returns:
            Last Bar or None if not available
        """
        return self._last_bars.get(symbol)

    @property
    def is_shutting_down(self) -> bool:
        """Check if shutdown is in progress"""
        return getattr(self, '_shutting_down', False)

    def get_account_summary(self, account: Optional[str] = None) -> Optional[AccountSummary]:
        """
        Get account summary.

        Args:
            account: Account ID. If None, uses first managed account.
        """
        if account is None and self.managed_accounts:
            account = self.managed_accounts[0]
        return self._account_summary.get(account)

    # =========================================================================
    # P&L Subscriptions
    # =========================================================================

    def request_pnl(self, account: str, model_code: str = "") -> int:
        """
        Subscribe to account-level P&L updates via IB's reqPnL.

        Args:
            account: IB account ID
            model_code: Model code for FA accounts (empty string for single account)

        Returns:
            Request ID used for this subscription
        """
        req_id = self.get_next_req_id()
        self._pnl_req_id = req_id
        self.reqPnL(req_id, account, model_code)
        logger.debug(f"Requested P&L for account {account} (reqId={req_id})")
        return req_id

    def cancel_pnl(self) -> None:
        """Cancel the account-level P&L subscription."""
        if self._pnl_req_id is not None:
            self.cancelPnL(self._pnl_req_id)
            logger.debug(f"Cancelled P&L subscription (reqId={self._pnl_req_id})")
            self._pnl_req_id = None

    def request_pnl_single(
        self, account: str, symbol: str, model_code: str = ""
    ) -> int:
        """
        Subscribe to per-position P&L updates via IB's reqPnLSingle.

        Looks up the contract's conId from current positions.

        Args:
            account: IB account ID
            symbol: Symbol to subscribe to
            model_code: Model code for FA accounts

        Returns:
            Request ID used for this subscription
        """
        con_id = 0
        pos = self._positions.get(symbol)
        if pos and pos.contract:
            con_id = pos.contract.conId

        req_id = self.get_next_req_id()
        self._pnl_single_req_ids[req_id] = symbol
        self._pnl_single_symbols[symbol] = req_id
        self.reqPnLSingle(req_id, account, model_code, con_id)
        logger.debug(
            f"Requested single P&L for {symbol} conId={con_id} (reqId={req_id})"
        )
        return req_id

    def cancel_pnl_single(self, symbol: str) -> None:
        """
        Cancel a per-position P&L subscription.

        Args:
            symbol: Symbol to cancel subscription for
        """
        req_id = self._pnl_single_symbols.get(symbol)
        if req_id is not None:
            self.cancelPnLSingle(req_id)
            logger.debug(f"Cancelled single P&L for {symbol} (reqId={req_id})")
            del self._pnl_single_req_ids[req_id]
            del self._pnl_single_symbols[symbol]

    # =========================================================================
    # P&L EWrapper Callbacks
    # =========================================================================

    def pnl(self, reqId: int, dailyPnL: float, unrealizedPnL: float, realizedPnL: float):
        """Called by IB for account-level P&L updates (reqPnL)."""
        account = self.managed_accounts[0] if self.managed_accounts else ""
        pnl_data = PnLData(
            account=account,
            daily_pnl=dailyPnL,
            unrealized_pnl=unrealizedPnL,
            realized_pnl=realizedPnL,
        )
        logger.debug(
            f"Account P&L: daily={dailyPnL:.2f} unrealized={unrealizedPnL:.2f} "
            f"realized={realizedPnL:.2f}"
        )
        if self._on_pnl:
            self._on_pnl(pnl_data)

    def pnlSingle(
        self,
        reqId: int,
        pos: int,
        dailyPnL: float,
        unrealizedPnL: float,
        realizedPnL: float,
        value: float,
    ):
        """Called by IB for per-position P&L updates (reqPnLSingle)."""
        symbol = self._pnl_single_req_ids.get(reqId, "")
        account = self.managed_accounts[0] if self.managed_accounts else ""
        pnl_data = PnLData(
            account=account,
            daily_pnl=dailyPnL,
            unrealized_pnl=unrealizedPnL,
            realized_pnl=realizedPnL,
            symbol=symbol,
            position=pos,
            value=value,
        )
        logger.debug(
            f"Single P&L {symbol}: pos={pos} daily={dailyPnL:.2f} "
            f"unrealized={unrealizedPnL:.2f} value={value:.2f}"
        )
        if self._on_pnl:
            self._on_pnl(pnl_data)

    def shutdown(self):
        """
        Gracefully shutdown all portfolio operations.

        Stops all streams, cancels pending orders, and disconnects.
        Call this on SIGINT for orderly shutdown.
        """
        if getattr(self, '_shutting_down', False):
            return  # Already shutting down
        self._shutting_down = True

        logger.info("Portfolio shutdown initiated...")

        # Stop tick streaming
        if self._streaming:
            try:
                self.stop_streaming()
            except Exception as e:
                logger.error(f"Error stopping tick streams: {e}")

        # Stop bar streaming
        if self._bar_streaming:
            try:
                self.stop_bar_streaming()
            except Exception as e:
                logger.error(f"Error stopping bar streams: {e}")

        # Cancel tick-by-tick subscriptions
        for req_id in list(self._tbt_subscriptions.keys()):
            try:
                self.cancelTickByTick(req_id)
            except Exception:
                pass
        self._tbt_subscriptions.clear()
        self._tbt_req_ids.clear()

        # Cancel market depth subscriptions
        for req_id, symbol in list(self._depth_subscriptions.items()):
            try:
                self.cancelMktDepth(req_id, False)
            except Exception:
                pass
        self._depth_subscriptions.clear()
        self._depth_req_ids.clear()
        self._depth_books.clear()

        # Cancel pending market data requests
        for req_id in list(self._market_data_requests.keys()):
            try:
                self.cancelMktData(req_id)
            except Exception:
                pass
        self._market_data_requests.clear()
        self._market_data_done.set()

        # Cancel P&L subscriptions
        if self._pnl_req_id is not None:
            try:
                self.cancelPnL(self._pnl_req_id)
            except Exception:
                pass
            self._pnl_req_id = None
        for req_id in list(self._pnl_single_req_ids):
            try:
                self.cancelPnLSingle(req_id)
            except Exception:
                pass
        self._pnl_single_req_ids.clear()
        self._pnl_single_symbols.clear()

        # Signal any waiting operations to complete
        self._positions_done.set()
        self._account_summary_done.set()

        # Signal pending order events
        for event in self._pending_orders.values():
            event.set()

        logger.info("Portfolio shutdown complete")

    async def load(
        self,
        fetch_prices: bool = True,
        fetch_account: bool = True,
        fetch_executions: bool = True,
        timeout: float = 30.0,
    ) -> bool:
        """
        Load portfolio data from IB.

        Args:
            fetch_prices: Whether to fetch current market prices
            fetch_account: Whether to fetch account summary
            fetch_executions: Whether to fetch today's executions for database
            timeout: Timeout in seconds

        Returns:
            True if successful
        """
        if not self.connected:
            logger.error("Not connected to IB")
            return False

        # Clear previous data
        self._positions.clear()
        self._forex_positions.clear()
        self._forex_cash.clear()
        self._forex_rates.clear()
        self._positions_done.clear()
        self._account_updates_done.clear()

        # Request positions
        logger.info("Requesting positions...")
        self.reqPositions()

        # Wait for positions
        try:
            await asyncio.wait_for(self._positions_done.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("Timeout waiting for positions")

        logger.info(f"Loaded {len(self._positions)} positions from reqPositions")

        # Request account updates for forex cash balances
        # This captures short forex positions that appear as negative cash balances
        logger.info("Requesting account updates for forex positions...")
        self.reqAccountUpdates(True, "")  # Empty string for all accounts

        # Wait for account updates
        try:
            await asyncio.wait_for(self._account_updates_done.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("Timeout waiting for account updates")

        # Cancel subscription after initial load
        self.reqAccountUpdates(False, "")

        if self._forex_positions:
            logger.info(f"Loaded {len(self._forex_positions)} synthetic forex positions")

        # Apply exchange rates to any forex positions from reqPositions
        self._apply_forex_rates()

        # Fetch market prices
        if fetch_prices and self._positions:
            logger.info("Fetching market prices...")
            await self._fetch_market_data(timeout=timeout)

        # Calculate allocations
        self._calculate_allocations()

        # Fetch account summary
        if fetch_account:
            logger.info("Fetching account summary...")
            await self._fetch_account_summary(timeout=timeout)

        # Fetch today's executions to populate database
        if fetch_executions:
            logger.info("Fetching today's executions...")
            await self.request_executions(timeout=timeout)

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

    # =========================================================================
    # Streaming Bar Data (5-second real-time bars)
    # =========================================================================

    def start_bar_streaming(
        self,
        on_bar: Optional[Callable[[Bar], None]] = None,
        what_to_show: str = "TRADES",
        use_rth: bool = True,
    ) -> bool:
        """
        Start streaming 5-second bars for all portfolio positions.

        Uses IB's reqRealTimeBars which provides 5-second OHLCV bars.
        This is the only bar size available for real-time streaming.
        For other bar sizes, use historical data with keepUpToDate=True.

        Args:
            on_bar: Callback function(bar: Bar) called on each new bar
            what_to_show: Type of data - TRADES, MIDPOINT, BID, ASK
            use_rth: Use regular trading hours only

        Returns:
            True if streaming started successfully
        """
        if not self.connected:
            logger.error("Not connected to IB")
            return False

        if not self._positions:
            logger.warning("No positions to stream - load portfolio first")
            return False

        if self._bar_streaming:
            logger.warning("Already streaming bars - stop first")
            return False

        self._on_bar = on_bar
        self._bar_streaming = True

        logger.info(f"Starting bar streams for {len(self._positions)} positions...")

        for symbol, pos in self._positions.items():
            if pos.contract:
                self._start_bar_stream(symbol, pos.contract, what_to_show, use_rth)

        logger.info(f"Streaming bars for {len(self._bar_subscriptions)} symbols")
        return True

    def _start_bar_stream(
        self,
        symbol: str,
        contract: Contract,
        what_to_show: str = "TRADES",
        use_rth: bool = True,
    ) -> int:
        """Start bar streaming for a single symbol"""
        req_id = self.get_next_req_id()

        self._bar_subscriptions[req_id] = symbol
        self._bar_req_ids[symbol] = req_id

        # Request real-time 5-second bars
        # barSize is always 5 seconds for reqRealTimeBars
        self.reqRealTimeBars(
            req_id,
            contract,
            5,  # bar size in seconds (must be 5)
            what_to_show,
            use_rth,
            [],  # realTimeBarsOptions
        )

        logger.debug(f"Started bar stream for {symbol} (reqId={req_id})")
        return req_id

    def stop_bar_streaming(self):
        """Stop all bar streaming subscriptions"""
        if not self._bar_streaming:
            return

        logger.info(f"Stopping {len(self._bar_subscriptions)} bar streams...")

        for req_id, symbol in list(self._bar_subscriptions.items()):
            self.cancelRealTimeBars(req_id)
            logger.debug(f"Stopped bar stream for {symbol}")

        self._bar_subscriptions.clear()
        self._bar_req_ids.clear()
        self._bar_streaming = False
        self._on_bar = None

        logger.info("All bar streams stopped")

    def bar_stream_symbol(
        self,
        symbol: str,
        contract: Optional[Contract] = None,
        what_to_show: str = "TRADES",
        use_rth: bool = True,
    ) -> bool:
        """
        Add a single symbol to bar streaming.

        Args:
            symbol: Symbol to stream
            contract: IB Contract (uses position's contract if None)
            what_to_show: Type of data - TRADES, MIDPOINT, BID, ASK
            use_rth: Use regular trading hours only

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

        if symbol in self._bar_req_ids:
            logger.debug(f"{symbol} already streaming bars")
            return True

        self._bar_streaming = True
        self._start_bar_stream(symbol, contract, what_to_show, use_rth)
        return True

    def unstream_bar_symbol(self, symbol: str):
        """Remove a single symbol from bar streaming"""
        if symbol not in self._bar_req_ids:
            return

        req_id = self._bar_req_ids[symbol]
        self.cancelRealTimeBars(req_id)

        del self._bar_subscriptions[req_id]
        del self._bar_req_ids[symbol]

        logger.debug(f"Stopped bar stream for {symbol}")

    # =========================================================================
    # Tick-by-Tick Data (reqTickByTick)
    # =========================================================================

    def request_tick_by_tick(
        self,
        symbol: str,
        contract,
        tick_type: str = "Last",
        numberOfTicks: int = 0,
        ignoreSize: bool = False,
    ) -> int:
        """
        Subscribe to tick-by-tick data for a symbol.

        Args:
            symbol: Symbol identifier
            contract: IB Contract
            tick_type: "Last", "AllLast", "BidAsk", or "MidPoint"
            numberOfTicks: Historical ticks to deliver first (0 = streaming only)
            ignoreSize: Ignore size in BidAsk filter

        Returns:
            Request ID
        """
        req_id = self.get_next_req_id()

        self._tbt_subscriptions[req_id] = symbol
        if symbol not in self._tbt_req_ids:
            self._tbt_req_ids[symbol] = {}
        self._tbt_req_ids[symbol][tick_type] = req_id

        self.reqTickByTick(req_id, contract, tick_type, numberOfTicks, ignoreSize)
        logger.debug(f"Started tick-by-tick stream for {symbol} type={tick_type} (reqId={req_id})")
        return req_id

    def cancel_tick_by_tick(self, symbol: str, tick_type: Optional[str] = None):
        """
        Cancel tick-by-tick subscriptions for a symbol.

        Args:
            symbol: Symbol to cancel
            tick_type: Specific type to cancel, or None to cancel all types
        """
        if symbol not in self._tbt_req_ids:
            return

        if tick_type is not None:
            req_id = self._tbt_req_ids[symbol].pop(tick_type, None)
            if req_id is not None:
                self.cancelTickByTick(req_id)
                self._tbt_subscriptions.pop(req_id, None)
                logger.debug(f"Stopped tick-by-tick {tick_type} for {symbol}")
        else:
            for tt, req_id in list(self._tbt_req_ids[symbol].items()):
                self.cancelTickByTick(req_id)
                self._tbt_subscriptions.pop(req_id, None)
                logger.debug(f"Stopped tick-by-tick {tt} for {symbol}")
            del self._tbt_req_ids[symbol]

    # =========================================================================
    # Market Depth Data (reqMktDepth)
    # =========================================================================

    def request_market_depth(
        self,
        symbol: str,
        contract,
        numRows: int = 10,
        isSmartDepth: bool = False,
    ) -> int:
        """
        Subscribe to L2 market depth for a symbol.

        Args:
            symbol: Symbol identifier
            contract: IB Contract
            numRows: Number of depth levels to receive
            isSmartDepth: Use SMART depth aggregation

        Returns:
            Request ID
        """
        req_id = self.get_next_req_id()

        self._depth_subscriptions[req_id] = symbol
        self._depth_req_ids[symbol] = req_id
        self._depth_books[symbol] = {"bids": {}, "asks": {}}

        self.reqMktDepth(req_id, contract, numRows, isSmartDepth, [])
        logger.debug(f"Started market depth for {symbol} (reqId={req_id})")
        return req_id

    def cancel_market_depth(self, symbol: str):
        """
        Cancel market depth subscription for a symbol.

        Args:
            symbol: Symbol to cancel
        """
        req_id = self._depth_req_ids.pop(symbol, None)
        if req_id is None:
            return

        is_smart = False  # we never set smart depth in request_market_depth default
        self.cancelMktDepth(req_id, is_smart)
        self._depth_subscriptions.pop(req_id, None)
        self._depth_books.pop(symbol, None)
        logger.debug(f"Stopped market depth for {symbol}")

    # =========================================================================
    # Historical Data
    # =========================================================================

    def request_historical_data(
        self,
        contract,
        end_date_time: str = "",
        duration_str: str = "1 W",
        bar_size_setting: str = "1 day",
        what_to_show: str = "TRADES",
        use_rth: bool = True,
        on_bar: Optional[Callable] = None,
        on_end: Optional[Callable] = None,
    ) -> int:
        """
        Request historical bar data from IB.

        Each call allocates its own request ID so multiple concurrent
        requests from different plugins never interfere.

        Args:
            contract:         IB Contract to fetch data for
            end_date_time:    End of the requested period ("" = now)
            duration_str:     How far back to go, e.g. "1 W", "3 M", "1 Y"
            bar_size_setting: Bar width, e.g. "1 day", "1 hour", "5 mins"
            what_to_show:     TRADES, MIDPOINT, BID, ASK, etc.
            use_rth:          Regular trading hours only
            on_bar:           Optional callback(bar) called for each bar as it arrives
            on_end:           Callback(bars, start, end) called when request completes

        Returns:
            Request ID (pass to cancel_historical_data() to abort early)
        """
        req_id = self.get_next_req_id()
        self._historical_requests[req_id] = (on_bar, on_end, [])
        logger.debug(
            f"reqHistoricalData req_id={req_id} "
            f"duration={duration_str} bar_size={bar_size_setting} "
            f"what_to_show={what_to_show}"
        )
        self.reqHistoricalData(
            req_id,
            contract,
            end_date_time,
            duration_str,
            bar_size_setting,
            what_to_show,
            1 if use_rth else 0,
            1,      # formatDate=1 → human-readable date strings
            False,  # keepUpToDate=False → one-shot historical fetch
            [],
        )
        return req_id

    def cancel_historical_data(self, req_id: int) -> None:
        """Cancel an in-progress historical data request."""
        if req_id in self._historical_requests:
            self.cancelHistoricalData(req_id)
            del self._historical_requests[req_id]
            logger.debug(f"Cancelled historical data request req_id={req_id}")

    # =========================================================================
    # Order Placement and Management
    # =========================================================================

    @property
    def orders(self) -> List[OrderRecord]:
        """Get list of all tracked orders"""
        return list(self._orders.values())

    @property
    def pending_orders(self) -> List[OrderRecord]:
        """Get list of pending (non-complete) orders"""
        return [o for o in self._orders.values() if not o.is_complete]

    def get_order(self, order_id: int) -> Optional[OrderRecord]:
        """Get an order by ID"""
        return self._orders.get(order_id)

    def place_order(
        self,
        contract: Contract,
        action: str,
        quantity: float,
        order_type: str = "MKT",
        limit_price: float = 0.0,
        stop_price: float = 0.0,
        tif: str = "DAY",
    ) -> Optional[int]:
        """
        Place an order through IB.

        Args:
            contract: IB Contract to trade
            action: "BUY" or "SELL"
            quantity: Number of shares
            order_type: "MKT", "LMT", "STP", "STP LMT"
            limit_price: Limit price (for LMT orders)
            stop_price: Stop price (for STP orders)
            tif: Time in force - "DAY", "GTC", "IOC", "FOK"

        Returns:
            Order ID if submitted, None if failed
        """
        if not self.connected:
            logger.error("Not connected to IB")
            return None

        if self._next_order_id is None:
            logger.error("No valid order ID available")
            return None

        # Get next order ID
        order_id = self._next_order_id
        self._next_order_id += 1

        # Create order object
        order = Order()
        order.action = action
        order.totalQuantity = quantity
        order.orderType = order_type
        order.tif = tif

        if order_type in ("LMT", "STP LMT"):
            order.lmtPrice = limit_price
        if order_type in ("STP", "STP LMT"):
            order.auxPrice = stop_price

        # Create order record for tracking
        order_record = OrderRecord(
            order_id=order_id,
            symbol=contract.symbol,
            action=action,
            quantity=quantity,
            order_type=order_type,
            submitted_time=datetime.now().isoformat(),
        )

        # Set up completion event
        completion_event = asyncio.Event()

        self._orders[order_id] = order_record
        self._pending_orders[order_id] = completion_event

        # Submit order
        try:
            self.placeOrder(order_id, contract, order)
            logger.info(f"Placed order {order_id}: {action} {quantity} {contract.symbol} @ {order_type}")
            return order_id
        except Exception as e:
            logger.error(f"Error placing order: {e}")
            order_record.status = OrderStatus.ERROR
            order_record.error_message = str(e)
            completion_event.set()
            return None

    def place_market_order(
        self,
        contract: Contract,
        action: str,
        quantity: float,
    ) -> Optional[int]:
        """Place a market order (convenience method)"""
        return self.place_order(contract, action, quantity, order_type="MKT")

    def place_limit_order(
        self,
        contract: Contract,
        action: str,
        quantity: float,
        limit_price: float,
    ) -> Optional[int]:
        """Place a limit order (convenience method)"""
        return self.place_order(
            contract, action, quantity,
            order_type="LMT", limit_price=limit_price
        )

    def cancel_order(self, order_id: int) -> bool:
        """
        Cancel an order.

        Args:
            order_id: Order ID to cancel

        Returns:
            True if cancel request sent
        """
        if not self.connected:
            return False

        try:
            self.cancelOrder(order_id, "")
            logger.info(f"Sent cancel request for order {order_id}")
            return True
        except Exception as e:
            logger.error(f"Error cancelling order {order_id}: {e}")
            return False

    def allocate_order_ids(self, count: int = 1) -> List[int]:
        """
        Allocate a block of consecutive order IDs.

        Use with place_order_raw() for multi-leg orders (bracket, OCA).

        Args:
            count: Number of consecutive IDs to allocate

        Returns:
            List of allocated order IDs, empty list if not connected
        """
        if self._next_order_id is None:
            return []
        start_id = self._next_order_id
        self._next_order_id += count
        return list(range(start_id, start_id + count))

    def place_order_raw(self, order_id: int, contract: Contract, order: Order) -> bool:
        """
        Place a pre-allocated order, registering it for status tracking.

        Use together with allocate_order_ids() for multi-leg orders
        (bracket, OCA, conditions) where consecutive IDs must be pre-reserved.

        Args:
            order_id: Pre-allocated order ID
            contract: IB Contract to trade
            order: Fully-configured Order object

        Returns:
            True if submitted successfully
        """
        if not self.connected:
            logger.error("Not connected to IB")
            return False

        record = OrderRecord(
            order_id=order_id,
            symbol=contract.symbol,
            action=order.action,
            quantity=float(order.totalQuantity),
            order_type=order.orderType,
            submitted_time=datetime.now().isoformat(),
        )
        completion_event = asyncio.Event()
        self._orders[order_id] = record
        self._pending_orders[order_id] = completion_event

        try:
            self.placeOrder(order_id, contract, order)
            logger.info(
                f"Placed order {order_id}: {order.action} {order.totalQuantity} "
                f"{contract.symbol} @ {order.orderType}"
            )
            return True
        except Exception as e:
            logger.error(f"Error placing order {order_id}: {e}")
            record.status = OrderStatus.ERROR
            record.error_message = str(e)
            completion_event.set()
            return False

    def place_order_custom(self, contract: Contract, order: Order) -> Optional[int]:
        """
        Place an arbitrary Order object, allocating the next order ID.

        For plugins/tests that need order types beyond the simple place_order()
        interface (midprice, trailing, pegged, conditions, etc.).

        Args:
            contract: IB Contract to trade
            order: Fully-configured Order object (action, orderType, qty, etc.)

        Returns:
            Order ID if submitted, None if failed
        """
        ids = self.allocate_order_ids(1)
        if not ids:
            logger.error("No valid order ID available")
            return None
        order_id = ids[0]
        order.orderId = order_id
        return order_id if self.place_order_raw(order_id, contract, order) else None

    async def wait_for_order(self, order_id: int, timeout: float = 30.0) -> Optional[OrderRecord]:
        """
        Wait for an order to complete (fill, cancel, or error).

        Args:
            order_id: Order ID to wait for
            timeout: Maximum time to wait in seconds

        Returns:
            OrderRecord when complete, or None if timeout
        """
        event = self._pending_orders.get(order_id)
        if not event:
            return self.get_order(order_id)

        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(f"Timeout waiting for order {order_id}")
        return self.get_order(order_id)

    async def wait_for_all_orders(self, timeout: float = 60.0) -> bool:
        """
        Wait for all pending orders to complete.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            True if all orders completed, False if timeout
        """
        start = datetime.now()
        while True:
            pending = self.pending_orders
            if not pending:
                return True

            elapsed = (datetime.now() - start).total_seconds()
            if elapsed >= timeout:
                logger.warning(f"Timeout waiting for {len(pending)} orders")
                return False

            await asyncio.sleep(0.1)

    async def request_executions(self, timeout: float = 10.0) -> bool:
        """
        Request today's executions from IB.

        This downloads execution reports for today's trades. The executions
        are automatically stored in the execution database via the
        execDetails callback.

        Args:
            timeout: Timeout in seconds

        Returns:
            True if request completed successfully
        """
        if not self.connected:
            logger.error("Not connected to IB")
            return False

        from ibapi.execution import ExecutionFilter

        self._executions_done.clear()

        req_id = self.get_next_req_id()
        exec_filter = ExecutionFilter()
        # Empty filter gets all executions for today

        logger.info("Requesting today's executions...")
        self.reqExecutions(req_id, exec_filter)

        try:
            await asyncio.wait_for(self._executions_done.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("Timeout waiting for executions")
            return False

        db = get_execution_db()
        logger.info(f"Executions stored: {db.get_execution_count()} total in database")
        return True

    async def _fetch_market_data(self, timeout: float = 30.0):
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

        try:
            await asyncio.wait_for(self._market_data_done.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("Timeout waiting for market data")

    async def _fetch_account_summary(self, timeout: float = 10.0):
        """Fetch account summary"""
        self._account_summary_done.clear()

        req_id = self.get_next_req_id()
        self.reqAccountSummary(req_id, "All", AccountSummaryTags.AllTags)

        try:
            await asyncio.wait_for(self._account_summary_done.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("Timeout waiting for account summary")
        self.cancelAccountSummary(req_id)

    def _calculate_allocations(self):
        """
        Calculate allocation percentages for all positions including forex.

        Uses gross portfolio value (sum of absolute market values) as denominator.
        This ensures:
        - Long positions show positive allocation
        - Short positions show negative allocation (since they're obligations)
        """
        # Calculate gross portfolio value (sum of absolute values)
        # This gives a better picture of total exposure
        all_positions = list(self._positions.values()) + list(self._forex_positions.values())
        gross_value = sum(abs(p.market_value) for p in all_positions)

        if gross_value > 0:
            # Calculate for regular positions
            for pos in self._positions.values():
                pos.allocation_pct = (pos.market_value / gross_value) * 100
            # Calculate for synthetic forex positions
            for pos in self._forex_positions.values():
                pos.allocation_pct = (pos.market_value / gross_value) * 100

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

        # Use localSymbol for forex (CASH) to get full pair (e.g., "EUR.USD" instead of "EUR")
        # For other asset types, use symbol
        if contract.secType == "CASH" and contract.localSymbol:
            symbol = contract.localSymbol
        else:
            symbol = contract.symbol

        asset_type = AssetType.from_sec_type(contract.secType)
        position = Position(
            symbol=symbol,
            asset_type=asset_type,
            quantity=float(pos),  # Ensure float conversion from Decimal
            avg_cost=float(avgCost),
            contract=contract,
            account=account,
        )

        self._positions[symbol] = position

        logger.debug(f"Position: {symbol} {pos} @ ${avgCost:.2f}")

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

    def tickSize(self, reqId: int, tickType: int, size: int):
        """Handle market data size updates (bid size, ask size, last size, volume)"""
        if reqId in self._stream_subscriptions:
            self._handle_stream_tick_size(reqId, tickType, size)

    def _handle_stream_tick_size(self, reqId: int, tickType: int, size: int):
        """Handle a streaming size tick update"""
        if size < 0:
            return

        symbol = self._stream_subscriptions.get(reqId)
        if not symbol:
            return

        # Only process size-related ticks
        if tickType not in SIZE_TICK_TYPES:
            return

        # Get tick type name
        tick_name = SIZE_TICK_TYPE_NAMES.get(tickType, f"SIZE_{tickType}")

        # Store the size
        if symbol not in self._last_sizes:
            self._last_sizes[symbol] = {}
        self._last_sizes[symbol][tick_name] = size

        # Call the size callback if registered
        if self._on_tick_size:
            try:
                self._on_tick_size(symbol, size, tick_name)
            except Exception as e:
                logger.error(f"Error in tick size callback: {e}")

    def tickSnapshotEnd(self, reqId: int):
        """Called when snapshot is complete"""
        if reqId in self._market_data_requests:
            self._market_data_received += 1
            if self._market_data_received >= self._market_data_pending:
                self._market_data_done.set()

    # =========================================================================
    # EWrapper Callbacks for Real-Time Bars
    # =========================================================================

    def realtimeBar(
        self,
        reqId: int,
        time: int,
        open_: float,
        high: float,
        low: float,
        close: float,
        volume: int,
        wap: float,
        count: int,
    ):
        """Handle real-time bar data (5-second bars)"""
        if reqId not in self._bar_subscriptions:
            return

        symbol = self._bar_subscriptions[reqId]

        # Convert Unix timestamp to ISO format
        timestamp = datetime.fromtimestamp(time).isoformat()

        bar = Bar(
            symbol=symbol,
            timestamp=timestamp,
            open=open_,
            high=high,
            low=low,
            close=close,
            volume=volume,
            wap=wap,
            bar_count=count,
        )

        # Store last bar
        self._last_bars[symbol] = bar

        # Update position with close price
        if symbol in self._positions:
            self._positions[symbol].update_market_data(close)
            self._calculate_allocations()

        # Call the callback if registered
        if self._on_bar:
            try:
                self._on_bar(bar)
            except Exception as e:
                logger.error(f"Error in bar callback: {e}")

        # Also invoke registered callback
        if "bar" in self._callbacks:
            self._callbacks["bar"](bar)

    # =========================================================================
    # EWrapper Callbacks for Tick-by-Tick Data
    # =========================================================================

    def tickByTickAllLast(
        self, reqId, tickType, time, price, size,
        tickAttribLast, exchange, specialConditions,
    ):
        """IB callback: tick-by-tick Last or AllLast trade event"""
        from .data_feed import TickByTickData
        symbol = self._tbt_subscriptions.get(reqId)
        if not symbol:
            return

        tbt = TickByTickData(
            symbol=symbol,
            tick_type="Last" if tickType == 1 else "AllLast",
            timestamp=datetime.fromtimestamp(time),
            price=price,
            size=int(size),
            exchange=exchange,
            special_conditions=specialConditions,
            past_limit=bool(getattr(tickAttribLast, 'pastLimit', False)),
            unreported=bool(getattr(tickAttribLast, 'unreported', False)),
        )

        if self._on_tick_by_tick:
            try:
                self._on_tick_by_tick(symbol, tbt)
            except Exception as e:
                logger.error(f"Error in tick_by_tick callback for {symbol}: {e}")

    def tickByTickBidAsk(
        self, reqId, tickType, time, bidPrice, askPrice,
        bidSize, askSize, tickAttribBidAsk,
    ):
        """IB callback: tick-by-tick BidAsk quote event"""
        from .data_feed import TickByTickData
        symbol = self._tbt_subscriptions.get(reqId)
        if not symbol:
            return

        tbt = TickByTickData(
            symbol=symbol,
            tick_type="BidAsk",
            timestamp=datetime.fromtimestamp(time),
            bid_price=bidPrice,
            ask_price=askPrice,
            bid_size=int(bidSize),
            ask_size=int(askSize),
            bid_past_low=bool(getattr(tickAttribBidAsk, 'bidPastLow', False)),
            ask_past_high=bool(getattr(tickAttribBidAsk, 'askPastHigh', False)),
        )

        if self._on_tick_by_tick:
            try:
                self._on_tick_by_tick(symbol, tbt)
            except Exception as e:
                logger.error(f"Error in tick_by_tick callback for {symbol}: {e}")

    def tickByTickMidPoint(self, reqId, tickType, time, midPoint):
        """IB callback: tick-by-tick MidPoint event"""
        from .data_feed import TickByTickData
        symbol = self._tbt_subscriptions.get(reqId)
        if not symbol:
            return

        tbt = TickByTickData(
            symbol=symbol,
            tick_type="MidPoint",
            timestamp=datetime.fromtimestamp(time),
            mid_point=midPoint,
        )

        if self._on_tick_by_tick:
            try:
                self._on_tick_by_tick(symbol, tbt)
            except Exception as e:
                logger.error(f"Error in tick_by_tick callback for {symbol}: {e}")

    # =========================================================================
    # EWrapper Callbacks for Market Depth
    # =========================================================================

    def _apply_depth_update(self, symbol, position, operation, side, price, size, market_maker=""):
        """Apply a depth book update and fire the depth callback."""
        from .data_feed import DepthLevel, MarketDepth
        book = self._depth_books.get(symbol)
        if book is None:
            return

        side_key = "bids" if side == 1 else "asks"
        if operation in (0, 1):  # insert or update
            book[side_key][position] = DepthLevel(price=price, size=int(size), market_maker=market_maker)
        elif operation == 2:  # delete
            book[side_key].pop(position, None)

        bids = sorted(book["bids"].values(), key=lambda l: -l.price)
        asks = sorted(book["asks"].values(), key=lambda l: l.price)
        depth = MarketDepth(symbol=symbol, bids=bids, asks=asks)

        if self._on_depth:
            try:
                self._on_depth(symbol, depth)
            except Exception as e:
                logger.error(f"Error in depth callback for {symbol}: {e}")

    def updateMktDepth(self, reqId, position, operation, side, price, size):
        """IB callback: L1 market depth update"""
        symbol = self._depth_subscriptions.get(reqId)
        if not symbol:
            return
        self._apply_depth_update(symbol, position, operation, side, price, size)

    def updateMktDepthL2(self, reqId, position, marketMaker, operation, side, price, size, isSmartDepth):
        """IB callback: L2 market depth update (includes market maker)"""
        symbol = self._depth_subscriptions.get(reqId)
        if not symbol:
            return
        self._apply_depth_update(symbol, position, operation, side, price, size, market_maker=marketMaker)

    # =========================================================================
    # EWrapper Callbacks for Historical Data
    # =========================================================================

    def historicalData(self, reqId: int, bar) -> None:
        """IB callback: one bar of historical data has arrived."""
        entry = self._historical_requests.get(reqId)
        if entry is None:
            return
        on_bar, _on_end, bars = entry
        bars.append(bar)
        if on_bar:
            try:
                on_bar(bar)
            except Exception as e:
                logger.error(f"Error in historicalData on_bar callback: {e}")

    def historicalDataEnd(self, reqId: int, start: str, end: str) -> None:
        """IB callback: historical data request is complete."""
        entry = self._historical_requests.pop(reqId, None)
        if entry is None:
            return
        _on_bar, on_end, bars = entry
        logger.debug(
            f"historicalDataEnd req_id={reqId} bars={len(bars)} "
            f"start={start} end={end}"
        )
        if on_end:
            try:
                on_end(bars, start, end)
            except Exception as e:
                logger.error(f"Error in historicalDataEnd on_end callback: {e}")

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

    # =========================================================================
    # EWrapper Callbacks for Account Updates (Forex Cash Balances)
    # =========================================================================

    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str):
        """
        Handle account value updates from reqAccountUpdates.

        Used to track forex cash balances for synthetic position construction.
        When you sell EUR.USD, you get a negative EUR CashBalance instead of a position.
        Also updates exchange rates for forex positions from reqPositions.
        """
        try:
            value = float(val)
        except (ValueError, TypeError):
            return

        # Track cash balances by currency for forex position construction
        if key == "CashBalance" and currency != "BASE":
            self._forex_cash[currency] = value
            self._update_forex_positions()

        # Track exchange rates to USD and update forex position prices
        elif key == "ExchangeRate" and currency != "BASE":
            self._forex_rates[currency] = value
            self._update_forex_positions()
            self._update_forex_prices(currency, value)

    def accountDownloadEnd(self, accountName: str):
        """Called when account update download is complete"""
        logger.debug(f"Account download complete for {accountName}")
        self._account_updates_done.set()

        if "accountDownloadEnd" in self._callbacks:
            self._callbacks["accountDownloadEnd"]()

    def _update_forex_prices(self, currency: str, rate: float):
        """
        Update forex position prices using exchange rates from updateAccountValue.

        Forex positions from reqPositions() have price=0. We use the ExchangeRate
        from updateAccountValue to fill in the current price.
        """
        # Build the symbol pattern to look for (e.g., "GBP.USD" for currency "GBP")
        symbol = f"{currency}.USD"

        # Check if we have a position with this symbol
        if symbol in self._positions:
            pos = self._positions[symbol]
            if pos.asset_type == AssetType.FOREX:
                pos.current_price = rate
                pos.market_value = pos.quantity * rate
                pos.unrealized_pnl = pos.market_value - (pos.quantity * pos.avg_cost)
                logger.debug(f"Updated forex price: {symbol} rate={rate} value={pos.market_value}")

    def _apply_forex_rates(self):
        """
        Apply stored exchange rates to all forex positions.

        Called after loading to ensure forex positions have correct prices.
        """
        for symbol, pos in self._positions.items():
            if pos.asset_type == AssetType.FOREX:
                # Extract currency from symbol (e.g., "GBP" from "GBP.USD")
                currency = symbol.split(".")[0] if "." in symbol else symbol
                rate = self._forex_rates.get(currency)
                if rate:
                    pos.current_price = rate
                    pos.market_value = pos.quantity * rate
                    pos.unrealized_pnl = pos.market_value - (pos.quantity * pos.avg_cost)
                    logger.info(f"Applied forex rate to {symbol}: rate={rate} value={pos.market_value}")

    def _load_forex_cost_basis(self):
        """Load persisted forex cost basis from file."""
        import json
        from pathlib import Path

        cost_file = Path.home() / ".ib_forex_cost_basis.json"
        if cost_file.exists():
            try:
                with open(cost_file) as f:
                    self._forex_cost_basis = json.load(f)
                logger.debug(f"Loaded forex cost basis: {self._forex_cost_basis}")
            except Exception as e:
                logger.warning(f"Failed to load forex cost basis: {e}")
                self._forex_cost_basis = {}

    def _save_forex_cost_basis(self):
        """Save forex cost basis to file for persistence."""
        import json
        from pathlib import Path

        cost_file = Path.home() / ".ib_forex_cost_basis.json"
        try:
            with open(cost_file, "w") as f:
                json.dump(self._forex_cost_basis, f)
            logger.debug(f"Saved forex cost basis: {self._forex_cost_basis}")
        except Exception as e:
            logger.warning(f"Failed to save forex cost basis: {e}")

    def _update_forex_positions(self):
        """
        Construct synthetic forex positions from non-USD cash balances.

        When you sell EUR.USD, IB reports a negative EUR cash balance.
        We convert this to a synthetic position for display.

        Note: We skip currencies that already have positions from reqPositions()
        to avoid duplicates (e.g., buying GBP.USD creates both a position AND a cash balance).
        """
        for currency, cash_balance in self._forex_cash.items():
            # Skip USD - that's not a forex position
            if currency == "USD":
                continue

            # Skip zero balances - remove any existing synthetic position
            if cash_balance == 0:
                symbol = f"{currency}.USD"
                self._forex_positions.pop(symbol, None)
                continue

            # Construct symbol as CURRENCY.USD (e.g., EUR.USD)
            symbol = f"{currency}.USD"

            # Skip if we already have this position from reqPositions()
            # This avoids duplicates when buying forex (creates both position and cash balance)
            if symbol in self._positions:
                continue

            # Get exchange rate (default to 1 if not available)
            rate = self._forex_rates.get(currency, 1.0)

            # Get cost basis - use persisted value if available, else current rate
            cost_basis = self._forex_cost_basis.get(currency, rate)

            # Check if we already have this synthetic position
            existing = self._forex_positions.get(symbol)
            if existing:
                # Update existing position with new rate, preserve cost basis
                existing.quantity = cash_balance
                existing.current_price = rate
                existing.market_value = cash_balance * rate
                # P&L: for short (negative qty), profit when rate drops
                existing.unrealized_pnl = (rate - existing.avg_cost) * cash_balance
                logger.debug(f"Updated synthetic forex: {symbol} qty={cash_balance} rate={rate} pnl={existing.unrealized_pnl:.2f}")
            else:
                # Create new synthetic position
                # For a short EUR position (negative cash), quantity is negative
                position = Position(
                    symbol=symbol,
                    asset_type=AssetType.FOREX,
                    quantity=cash_balance,
                    avg_cost=cost_basis,  # Use persisted cost basis or current rate
                    current_price=rate,
                    market_value=cash_balance * rate,
                    unrealized_pnl=(rate - cost_basis) * cash_balance,
                )
                self._forex_positions[symbol] = position

                # Persist cost basis if this is a new position
                if currency not in self._forex_cost_basis:
                    self._forex_cost_basis[currency] = rate
                    self._save_forex_cost_basis()

                logger.debug(f"New synthetic forex: {symbol} qty={cash_balance} cost={cost_basis} rate={rate}")

    # =========================================================================
    # EWrapper Callbacks for Orders
    # =========================================================================

    def orderStatus(
        self,
        orderId: int,
        status: str,
        filled: float,
        remaining: float,
        avgFillPrice: float,
        permId: int,
        parentId: int,
        lastFillPrice: float,
        clientId: int,
        whyHeld: str,
        mktCapPrice: float = 0.0,
    ):
        """Handle order status updates"""
        order_record = self._orders.get(orderId)
        if not order_record:
            return

        # Update order record
        order_record.status = OrderStatus.from_ib_status(status)
        order_record.filled_quantity = filled
        order_record.remaining = remaining
        order_record.avg_fill_price = avgFillPrice
        order_record.last_fill_price = lastFillPrice
        if whyHeld:
            order_record.why_held = whyHeld

        # Mark completion time if terminal state
        if order_record.is_complete:
            order_record.filled_time = datetime.now().isoformat()

            # Signal completion event
            event = self._pending_orders.get(orderId)
            if event:
                event.set()

        logger.info(
            f"Order {orderId} status: {status}, "
            f"filled={filled}/{order_record.quantity} @ ${avgFillPrice:.2f}"
        )

        # Call callback if registered
        if self._on_order_status:
            try:
                self._on_order_status(order_record)
            except Exception as e:
                logger.error(f"Error in order status callback: {e}")

        if "orderStatus" in self._callbacks:
            self._callbacks["orderStatus"](order_record)

    def openOrder(
        self,
        orderId: int,
        contract: Contract,
        order: Order,
        orderState,
    ):
        """Handle open order information"""
        logger.debug(f"Open order {orderId}: {order.action} {order.totalQuantity} {contract.symbol}")

        if "openOrder" in self._callbacks:
            self._callbacks["openOrder"](orderId, contract, order, orderState)

    def execDetails(self, reqId: int, contract: Contract, execution):
        """Handle execution details and store to database"""
        logger.info(
            f"Execution: {execution.execId} {execution.side} "
            f"{float(execution.shares):.0f} {contract.symbol} @ ${execution.avgPrice:.4f}"
        )

        # Store to database
        try:
            # Use localSymbol for forex (e.g., "EUR.USD" instead of "EUR")
            symbol = contract.localSymbol if contract.secType == "CASH" and contract.localSymbol else contract.symbol

            exec_record = ExecutionRecord(
                exec_id=execution.execId,
                order_id=execution.orderId,
                symbol=symbol,
                sec_type=contract.secType,
                exchange=execution.exchange or contract.exchange or "",
                currency=contract.currency or "",
                local_symbol=contract.localSymbol or "",
                shares=float(execution.shares),
                cum_qty=float(execution.cumQty) if execution.cumQty else 0,
                avg_price=execution.avgPrice,
                side=execution.side,  # BOT or SLD
                account=execution.acctNumber or "",
                timestamp=datetime.now(),  # IB's execution.time is a string, use now()
            )

            db = get_execution_db()
            db.insert_execution(exec_record)

            # Update forex cost basis if this is a forex trade
            if contract.secType == "CASH":
                currency = contract.symbol  # Base currency (e.g., "EUR")
                if execution.side == "SLD":
                    # Selling currency - this creates/adds to short position
                    # Store the rate as cost basis
                    self._forex_cost_basis[currency] = execution.avgPrice
                    self._save_forex_cost_basis()
                    logger.info(f"Updated forex cost basis for {currency}: {execution.avgPrice}")

        except Exception as e:
            logger.error(f"Failed to store execution: {e}")

        if "execDetails" in self._callbacks:
            self._callbacks["execDetails"](reqId, contract, execution)

    def execDetailsEnd(self, reqId: int):
        """Called when execution details download is complete"""
        logger.debug(f"Execution details download complete (reqId={reqId})")
        self._executions_done.set()

        if "execDetailsEnd" in self._callbacks:
            self._callbacks["execDetailsEnd"](reqId)

    def commissionReport(self, commissionReport):
        """Legacy callback name - forwards to commissionAndFeesReport"""
        self.commissionAndFeesReport(commissionReport)

    def commissionAndFeesReport(self, commissionAndFeesReport):
        """
        Handle commission and fees report from IB.

        This callback is called after each execution fill with commission
        and fee details. The exec_id links this to the corresponding execution.

        Args:
            commissionAndFeesReport: IB CommissionAndFeesReport object containing:
                - execId: Execution ID linking to execDetails
                - commissionAndFees: Commission amount
                - currency: Commission currency
                - realizedPNL: Realized P&L for closing trades
                - yield_: Yield for bonds
                - yieldRedemptionDate: Redemption date for bonds
        """
        exec_id = commissionAndFeesReport.execId
        commission = commissionAndFeesReport.commissionAndFees
        realized_pnl = commissionAndFeesReport.realizedPNL
        currency = getattr(commissionAndFeesReport, "currency", "USD")

        # Check for max float value (IB uses this to indicate no realized PNL)
        # 1.7976931348623157e+308 is approximately sys.float_info.max
        if realized_pnl > 1e307:
            realized_pnl_display = "N/A"
            realized_pnl_db = None  # Store as NULL in database
        else:
            realized_pnl_display = f"${realized_pnl:.2f}"
            realized_pnl_db = realized_pnl

        logger.info(
            f"Commission: exec_id={exec_id}, "
            f"commission=${commission:.2f} {currency}, realized_pnl={realized_pnl_display}"
        )

        # Store to database
        try:
            comm_record = CommissionRecord(
                exec_id=exec_id,
                commission=commission,
                currency=currency,
                realized_pnl=realized_pnl_db,
                timestamp=datetime.now(),
            )

            db = get_execution_db()
            db.insert_commission(comm_record)

        except Exception as e:
            logger.error(f"Failed to store commission: {e}")

        # Store the commission report keyed by execution ID (legacy support)
        report = CommissionAndFeesReport(
            execId=exec_id,
            commissionAndFees=commission,
            currency=currency,
            realizedPNL=realized_pnl,
            yield_=getattr(commissionAndFeesReport, "yield_", 0.0),
            yieldRedemptionDate=getattr(commissionAndFeesReport, "yieldRedemptionDate", 0),
        )
        self._commission_reports[exec_id] = report

        # Notify callback if registered
        if self._on_commission:
            self._on_commission(exec_id, commission, realized_pnl)

        if "commissionReport" in self._callbacks:
            self._callbacks["commissionReport"](commissionAndFeesReport)
        if "commissionAndFeesReport" in self._callbacks:
            self._callbacks["commissionAndFeesReport"](commissionAndFeesReport)

    def get_commission_report(self, exec_id: str) -> Optional[CommissionAndFeesReport]:
        """
        Get commission report for a specific execution.

        Args:
            exec_id: The execution ID

        Returns:
            CommissionAndFeesReport if found, None otherwise
        """
        return self._commission_reports.get(exec_id)

    def get_all_commission_reports(self) -> Dict[str, CommissionAndFeesReport]:
        """
        Get all stored commission reports.

        Returns:
            Dictionary mapping exec_id to CommissionAndFeesReport
        """
        return self._commission_reports.copy()


async def quick_load(
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

    if not await portfolio.connect():
        return []

    try:
        await portfolio.load(fetch_prices=fetch_prices)
        return portfolio.positions
    finally:
        await portfolio.disconnect()
