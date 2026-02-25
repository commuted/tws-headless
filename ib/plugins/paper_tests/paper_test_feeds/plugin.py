"""
paper_test_feeds/plugin.py - Paper trading feed test plugin

One-shot test plugin that serially tests feed subscriptions in pairs,
verifying both ticks and bars arrive with valid data. Includes a safety
gate that refuses to run unless connected to a paper trading socket.
"""

import logging
import time
import threading
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Any

from ib.plugins.base import PluginBase, TradeSignal

from .feed_test_specs import DEFAULT_TEST_PAIRS, FeedTestSpec, FeedTestPair

logger = logging.getLogger(__name__)


@dataclass
class FeedTestResult:
    """Result of a single feed test (tick or bar for one symbol)"""
    test_name: str
    feed_type: str
    data_type: str  # "tick" or "bar"
    symbol: str
    passed: bool
    details: Dict[str, Any] = field(default_factory=dict)
    error_message: str = ""
    duration_seconds: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "test_name": self.test_name,
            "feed_type": self.feed_type,
            "data_type": self.data_type,
            "symbol": self.symbol,
            "passed": self.passed,
            "details": self.details,
            "error_message": self.error_message,
            "duration_seconds": self.duration_seconds,
        }


class PaperTestFeedsPlugin(PluginBase):
    """
    One-shot feed test plugin for paper trading verification.

    Tests feed subscriptions in pairs, verifying both ticks and bars
    arrive with valid data. Refuses to run on live accounts.

    Usage:
        result = plugin.handle_request("run_tests", {})
    """

    VERSION = "1.0.0"
    IS_SYSTEM_PLUGIN = False

    def __init__(
        self,
        base_path: Optional[Path] = None,
        portfolio=None,
        shared_holdings=None,
        message_bus=None,
    ):
        super().__init__(
            "paper_test_feeds",
            base_path,
            portfolio,
            shared_holdings,
            message_bus,
        )

        self._results: List[FeedTestResult] = []
        self._running = False

        # Capture state for callback interception
        self._captured_ticks: Dict[str, List[Dict]] = {}
        self._captured_bars: Dict[str, List[Dict]] = {}
        self._capture_lock = threading.Lock()
        self._tick_events: Dict[str, threading.Event] = {}
        self._bar_events: Dict[str, threading.Event] = {}

    @property
    def description(self) -> str:
        return (
            "Paper Test Feeds: Verifies tick and bar data feeds are delivering "
            "valid data for forex and stock subscriptions on paper trading accounts."
        )

    # =========================================================================
    # LIFECYCLE METHODS
    # =========================================================================

    def start(self) -> bool:
        logger.info(f"Starting plugin '{self.name}'")
        saved_state = self.load_state()
        if saved_state:
            self._results = [
                FeedTestResult(**r) for r in saved_state.get("results", [])
            ]
        return True

    def stop(self) -> bool:
        logger.info(f"Stopping plugin '{self.name}'")
        self.save_state({
            "results": [r.to_dict() for r in self._results],
        })
        self.unsubscribe_all()
        return True

    def freeze(self) -> bool:
        logger.info(f"Freezing plugin '{self.name}'")
        self.save_state({
            "results": [r.to_dict() for r in self._results],
        })
        return True

    def resume(self) -> bool:
        logger.info(f"Resuming plugin '{self.name}'")
        return True

    def calculate_signals(
        self,
        market_data: Dict[str, List[Dict]],
    ) -> List[TradeSignal]:
        return []

    # =========================================================================
    # REQUEST HANDLING
    # =========================================================================

    def handle_request(self, request_type: str, payload: Dict) -> Dict:
        if request_type == "run_tests":
            return self._run_all_tests()

        elif request_type == "get_results":
            return {
                "success": True,
                "data": {
                    "results": [r.to_dict() for r in self._results],
                    "summary": self._build_summary(),
                },
            }

        elif request_type == "get_status":
            return {
                "success": True,
                "data": {
                    "running": self._running,
                    "test_pairs": [p.name for p in DEFAULT_TEST_PAIRS],
                    "result_count": len(self._results),
                },
            }

        return {"success": False, "message": f"Unknown request type: {request_type}"}

    # =========================================================================
    # PAPER VERIFICATION
    # =========================================================================

    def _verify_paper_connection(self) -> Optional[str]:
        """
        Verify we are connected to a paper trading account.

        Returns:
            None if paper connection verified, error message string otherwise.
        """
        if not self.portfolio:
            return "No portfolio instance available"

        if not self.portfolio.connected:
            return "Not connected to IB"

        # Port check: 7497 (TWS paper) or 4002 (Gateway paper)
        port = self.portfolio.port
        if port not in (7497, 4002):
            return (
                f"SAFETY: Port {port} is not a paper trading port. "
                f"Paper ports are 7497 (TWS) and 4002 (Gateway). "
                f"Refusing to run feed tests on a live connection."
            )

        # Account check: paper accounts start with "D"
        accounts = self.portfolio.managed_accounts
        if not accounts:
            return "No managed accounts found"

        account = accounts[0]
        if not account.startswith("D"):
            return (
                f"SAFETY: Account '{account}' does not start with 'D'. "
                f"Paper trading accounts start with 'D'. "
                f"Refusing to run feed tests on a live account."
            )

        return None

    # =========================================================================
    # TEST ORCHESTRATION
    # =========================================================================

    def _run_all_tests(self) -> Dict:
        """Execute all test pairs serially, return full results + summary."""
        if self._running:
            return {"success": False, "message": "Tests already running"}

        self._running = True
        self._results = []

        try:
            # Step 1: Verify paper connection
            error = self._verify_paper_connection()
            if error:
                logger.error(f"Paper verification failed: {error}")
                return {"success": False, "message": error}

            # Step 2: Set delayed data mode
            logger.info("Setting market data type to delayed (3)")
            self.portfolio.reqMarketDataType(3)

            # Step 3: Run each test pair serially
            for pair in DEFAULT_TEST_PAIRS:
                logger.info(f"--- Testing pair: {pair.name} ---")
                self._run_test_pair(pair)

            # Build summary
            summary = self._build_summary()
            logger.info(
                f"Feed tests complete: {summary['passed']}/{summary['total']} passed"
            )

            return {
                "success": True,
                "data": {
                    "results": [r.to_dict() for r in self._results],
                    "summary": summary,
                },
            }

        except Exception as e:
            logger.error(f"Feed test error: {e}", exc_info=True)
            return {"success": False, "message": f"Test error: {e}"}

        finally:
            self._running = False

    def _run_test_pair(self, pair: FeedTestPair):
        """Run tick and bar tests for a pair of specs."""
        specs = [pair.spec_a, pair.spec_b]

        # Phase 1: Tick test
        logger.info(f"Phase 1: Tick test for {pair.name}")
        self._run_tick_test(specs)

        # Brief pause between phases
        time.sleep(1)

        # Phase 2: Bar test
        logger.info(f"Phase 2: Bar test for {pair.name}")
        self._run_bar_test(specs)

    # =========================================================================
    # TICK TEST
    # =========================================================================

    def _run_tick_test(self, specs: List[FeedTestSpec]):
        """Test tick data for a list of specs simultaneously."""
        # Save original callback
        original_on_tick = self.portfolio._on_tick

        # Reset capture state
        with self._capture_lock:
            self._captured_ticks = {}
            self._tick_events = {}
            for spec in specs:
                self._captured_ticks[spec.symbol] = []
                self._tick_events[spec.symbol] = threading.Event()

        # Install capture callback
        self.portfolio._on_tick = self._tick_capture_callback(original_on_tick)

        # Track which symbols we start vs pre-existing
        started_symbols = []

        try:
            # Subscribe
            for spec in specs:
                if spec.symbol in self.portfolio._stream_req_ids:
                    logger.info(f"  {spec.symbol} already streaming ticks (pre-existing)")
                else:
                    logger.info(f"  Subscribing tick stream: {spec.symbol}")
                    self.portfolio.stream_symbol(spec.symbol, spec.contract)
                    started_symbols.append(spec.symbol)

            # Wait for data using the max timeout across specs
            max_timeout = max(s.tick_timeout for s in specs)
            deadline = time.time() + max_timeout

            # Poll events until all fire or timeout
            while time.time() < deadline:
                all_done = all(
                    self._tick_events[s.symbol].is_set() for s in specs
                )
                if all_done:
                    break
                time.sleep(0.1)

            # Validate results
            for spec in specs:
                start_time = time.time()
                result = self._validate_tick_result(spec)
                result.duration_seconds = time.time() - start_time
                self._results.append(result)
                self._log_result(result)

        finally:
            # Unsubscribe only symbols we started
            for symbol in started_symbols:
                logger.info(f"  Unsubscribing tick stream: {symbol}")
                self.portfolio.unstream_symbol(symbol)

            # Restore original callback
            self.portfolio._on_tick = original_on_tick

    def _tick_capture_callback(self, original_callback):
        """Create a tick capture callback that chains to the original."""
        def callback(symbol, price, tick_type):
            with self._capture_lock:
                if symbol in self._captured_ticks:
                    self._captured_ticks[symbol].append({
                        "price": price,
                        "tick_type": tick_type,
                        "timestamp": datetime.now().isoformat(),
                    })
                    # Signal that we got data for this symbol
                    if symbol in self._tick_events:
                        self._tick_events[symbol].set()

            # Chain to original callback
            if original_callback:
                try:
                    original_callback(symbol, price, tick_type)
                except Exception as e:
                    logger.error(f"Error in chained tick callback: {e}")

        return callback

    def _validate_tick_result(self, spec: FeedTestSpec) -> FeedTestResult:
        """Validate tick test results for a spec."""
        test_name = f"tick_{spec.feed_type.value}_{spec.symbol}"

        with self._capture_lock:
            ticks = list(self._captured_ticks.get(spec.symbol, []))

        # Count valid ticks (price > 0)
        valid_ticks = [t for t in ticks if t["price"] > 0]
        tick_types_seen = list(set(t["tick_type"] for t in ticks))

        passed = len(valid_ticks) >= spec.min_tick_count

        details = {
            "ticks_received": len(ticks),
            "valid_ticks": len(valid_ticks),
            "min_required": spec.min_tick_count,
            "tick_types_seen": tick_types_seen,
            "timeout_seconds": spec.tick_timeout,
        }

        if valid_ticks:
            details["first_price"] = valid_ticks[0]["price"]
            details["last_price"] = valid_ticks[-1]["price"]

        error_message = ""
        if not passed:
            # Add diagnostic state
            last_prices = self.portfolio._last_prices.get(spec.symbol, {})
            details["portfolio_last_prices"] = dict(last_prices)
            error_message = (
                f"Timeout: received {len(valid_ticks)} valid ticks in "
                f"{spec.tick_timeout}s (need {spec.min_tick_count})"
            )

        return FeedTestResult(
            test_name=test_name,
            feed_type=spec.feed_type.value,
            data_type="tick",
            symbol=spec.symbol,
            passed=passed,
            details=details,
            error_message=error_message,
        )

    # =========================================================================
    # BAR TEST
    # =========================================================================

    def _run_bar_test(self, specs: List[FeedTestSpec]):
        """Test bar data for a list of specs simultaneously."""
        # Save original callback
        original_on_bar = self.portfolio._on_bar

        # Reset capture state
        with self._capture_lock:
            self._captured_bars = {}
            self._bar_events = {}
            for spec in specs:
                self._captured_bars[spec.symbol] = []
                self._bar_events[spec.symbol] = threading.Event()

        # Install capture callback
        self.portfolio._on_bar = self._bar_capture_callback(original_on_bar)

        # Track which symbols we start vs pre-existing
        started_symbols = []

        try:
            # Subscribe
            for spec in specs:
                if spec.symbol in self.portfolio._bar_req_ids:
                    logger.info(
                        f"  {spec.symbol} already streaming bars (pre-existing)"
                    )
                else:
                    logger.info(
                        f"  Subscribing bar stream: {spec.symbol} "
                        f"(what_to_show={spec.what_to_show}, use_rth={spec.use_rth})"
                    )
                    self.portfolio.bar_stream_symbol(
                        spec.symbol,
                        spec.contract,
                        spec.what_to_show,
                        spec.use_rth,
                    )
                    started_symbols.append(spec.symbol)

            # Wait for data using the max timeout across specs
            max_timeout = max(s.bar_timeout for s in specs)
            deadline = time.time() + max_timeout

            while time.time() < deadline:
                all_done = all(
                    self._bar_events[s.symbol].is_set() for s in specs
                )
                if all_done:
                    break
                time.sleep(0.1)

            # Validate results
            for spec in specs:
                start_time = time.time()
                result = self._validate_bar_result(spec)
                result.duration_seconds = time.time() - start_time
                self._results.append(result)
                self._log_result(result)

        finally:
            # Unsubscribe only symbols we started
            for symbol in started_symbols:
                logger.info(f"  Unsubscribing bar stream: {symbol}")
                self.portfolio.unstream_bar_symbol(symbol)

            # Restore original callback
            self.portfolio._on_bar = original_on_bar

    def _bar_capture_callback(self, original_callback):
        """Create a bar capture callback that chains to the original."""
        def callback(bar):
            symbol = bar.symbol
            with self._capture_lock:
                if symbol in self._captured_bars:
                    self._captured_bars[symbol].append({
                        "open": bar.open,
                        "high": bar.high,
                        "low": bar.low,
                        "close": bar.close,
                        "volume": bar.volume,
                        "timestamp": bar.timestamp,
                    })
                    if symbol in self._bar_events:
                        self._bar_events[symbol].set()

            # Chain to original callback
            if original_callback:
                try:
                    original_callback(bar)
                except Exception as e:
                    logger.error(f"Error in chained bar callback: {e}")

        return callback

    def _validate_bar_result(self, spec: FeedTestSpec) -> FeedTestResult:
        """Validate bar test results for a spec."""
        test_name = f"bar_{spec.feed_type.value}_{spec.symbol}"

        with self._capture_lock:
            bars = list(self._captured_bars.get(spec.symbol, []))

        # Check for valid bars: OHLC all > 0 and high >= low
        valid_bars = [
            b for b in bars
            if b["open"] > 0
            and b["high"] > 0
            and b["low"] > 0
            and b["close"] > 0
            and b["high"] >= b["low"]
        ]

        passed = len(valid_bars) >= 1

        details = {
            "bars_received": len(bars),
            "valid_bars": len(valid_bars),
            "what_to_show": spec.what_to_show,
            "use_rth": spec.use_rth,
            "timeout_seconds": spec.bar_timeout,
        }

        if valid_bars:
            first = valid_bars[0]
            details["first_bar_ohlc"] = {
                "open": first["open"],
                "high": first["high"],
                "low": first["low"],
                "close": first["close"],
            }
            details["ohlc_valid"] = True

        error_message = ""
        if not passed:
            # Add diagnostic state
            last_bar = self.portfolio._last_bars.get(spec.symbol)
            if last_bar:
                details["portfolio_last_bar"] = {
                    "open": last_bar.open,
                    "high": last_bar.high,
                    "low": last_bar.low,
                    "close": last_bar.close,
                    "timestamp": last_bar.timestamp,
                }
            error_message = (
                f"Timeout: received {len(valid_bars)} valid bars in "
                f"{spec.bar_timeout}s (need 1). "
                f"what_to_show={spec.what_to_show}"
            )

        return FeedTestResult(
            test_name=test_name,
            feed_type=spec.feed_type.value,
            data_type="bar",
            symbol=spec.symbol,
            passed=passed,
            details=details,
            error_message=error_message,
        )

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _log_result(self, result: FeedTestResult):
        """Log a test result with appropriate level."""
        if result.passed:
            tick_or_bar = result.details.get("valid_ticks") or result.details.get("valid_bars", 0)
            unit = "ticks" if result.data_type == "tick" else "bars"
            logger.info(f"  [PASS] {result.test_name}: {tick_or_bar} {unit} received")
        else:
            logger.warning(f"  [FAIL] {result.test_name}: {result.error_message}")
            logger.warning(f"  FAILURE DETAIL: {result.details}")

    def _build_summary(self) -> Dict[str, Any]:
        """Build a summary of all test results."""
        total = len(self._results)
        passed = sum(1 for r in self._results if r.passed)
        failed = total - passed

        return {
            "total": total,
            "passed": passed,
            "failed": failed,
            "all_passed": failed == 0,
            "results": [
                {
                    "test": r.test_name,
                    "passed": r.passed,
                    "error": r.error_message,
                }
                for r in self._results
            ],
        }
