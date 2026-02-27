"""
order_test_base.py - Shared infrastructure for paper order type tests

Provides ETF pair definitions, test case descriptors, result dataclasses,
and a base plugin class used by the five order-type test plugins.

Each test issues a LONG order on pair[0] and a SHORT order on pair[1].
When one fills the other is cancelled.  For immediate order types (market,
MOC) both are expected to fill.
"""

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from ibapi.contract import Contract
from ibapi.order import Order

from ib.data_feed import DataType
from plugins.base import PluginBase, TradeSignal

logger = logging.getLogger(__name__)

# ===========================================================================
# ETF pairs: (symbol_a, symbol_b)
#
# All pairs use leveraged ETFs for amplified price movement, which makes
# limit and stop conditions more likely to trigger in the test window.
#
# Three underlying pairs, assigned by test characteristics:
#
#   TQQQ/SQQQ  – QQQ +3×/−3× (inverse to each other).  Both legs fire in
#                the same market direction; ideal for immediate-fill tests
#                (market, MOC, MOO, MTL) where direction does not matter.
#
#   SPXU/SDS   – SPY −3×/−2× (same direction, opposite to SPY).  BUY side
#                fires when SPY rises; SELL side fires when SPY falls.
#                One leg fills regardless of market direction.
#
#   SDOW/DXD   – DOW −3×/−2× (same direction, opposite to DIA).  Same
#                property as SPXU/SDS on an independent underlying.
#
# Index → test use:
#   0 TQQQ/SQQQ  market / immediate
#   1 SPXU/SDS   limit
#   2 SDOW/DXD   stop
#   3 SPXU/SDS   stop-limit
#   4 TQQQ/SQQQ  MOC / immediate
#   5 SDOW/DXD   MOO / immediate
#   6 TQQQ/SQQQ  MTL / immediate
# ===========================================================================
ETF_PAIRS: List[Tuple[str, str]] = [
    ("TQQQ", "SQQQ"),   # 0 – QQQ +3× / −3× inverse
    ("SPXU", "SDS"),    # 1 – SPY −3× / −2× (same dir; one fires either way)
    ("SDOW", "DXD"),    # 2 – DOW −3× / −2× (same dir; one fires either way)
    ("SPXU", "SDS"),    # 3 – SPY −3× / −2× (same dir; one fires either way)
    ("TQQQ", "SQQQ"),   # 4 – QQQ +3× / −3× inverse
    ("SDOW", "DXD"),    # 5 – DOW −3× / −2× (same dir; one fires either way)
    ("TQQQ", "SQQQ"),   # 6 – QQQ +3× / −3× inverse
]

# Quantity for all test orders (1 share – small footprint on paper account)
TEST_QTY = Decimal("1")

# Fraction below/above current price for aggressive limit-type orders
OFFSET_BELOW = 0.995   # BUY limit/stop-limit below market
OFFSET_ABOVE = 1.005   # SELL limit/stop-limit above market

# Paper trading ports
PAPER_PORTS = (7497, 4002)


def make_stk_contract(symbol: str, exchange: str = "SMART",
                      currency: str = "USD") -> Contract:
    """Create a basic STK (equity) contract."""
    c = Contract()
    c.symbol = symbol
    c.secType = "STK"
    c.exchange = exchange
    c.currency = currency
    return c


# ===========================================================================
# Data classes
# ===========================================================================

@dataclass
class OrderTestCase:
    """Definition of one order-pair test (long + short)."""
    name: str                        # Human-readable name
    order_type_label: str            # Canonical order type identifier
    pair_index: int                  # Index into ETF_PAIRS (0-6)
    build_long: Callable             # fn(price: float) -> Order   (BUY pair[0])
    build_short: Callable            # fn(price: float) -> Order   (SELL pair[1])
    fill_timeout: float = 300.0      # Seconds to wait for a fill
    immediate: bool = False          # True if both orders fill immediately
    is_stub: bool = False            # True if order type cannot be tested
    stub_reason: str = ""
    notes: str = ""


@dataclass
class OrderPairResult:
    """Result of a single order-pair test."""
    test_name: str
    order_type: str
    symbol_long: str
    symbol_short: str
    submitted: bool = False
    long_order_id: Optional[int] = None
    short_order_id: Optional[int] = None
    fill_side: Optional[str] = None      # "long", "short", "both", or None
    fill_price: float = 0.0
    cancel_ok: bool = False
    error_message: str = ""
    duration_seconds: float = 0.0
    notes: str = ""
    is_stub: bool = False
    stub_reason: str = ""

    @property
    def passed(self) -> bool:
        """Stubs always pass. Real tests pass if submitted without error."""
        if self.is_stub:
            return True
        return self.submitted and not self.error_message

    def to_dict(self) -> Dict[str, Any]:
        return {
            "test_name": self.test_name,
            "order_type": self.order_type,
            "symbol_long": self.symbol_long,
            "symbol_short": self.symbol_short,
            "submitted": self.submitted,
            "fill_side": self.fill_side,
            "fill_price": self.fill_price,
            "cancel_ok": self.cancel_ok,
            "error_message": self.error_message,
            "duration_seconds": round(self.duration_seconds, 2),
            "notes": self.notes,
            "is_stub": self.is_stub,
            "stub_reason": self.stub_reason,
            "passed": self.passed,
        }


# ===========================================================================
# Base plugin
# ===========================================================================

class OrderTestPluginBase(PluginBase):
    """
    Base class for paper order-type test plugins.

    Subclasses set TEST_CASES (list of OrderTestCase) and a NAME / DESCRIPTION.
    Calling handle_request("run_tests") runs all cases serially.
    """

    VERSION = "1.0.0"
    IS_SYSTEM_PLUGIN = False

    # Subclasses override these
    TEST_CASES: List[OrderTestCase] = []

    def __init__(
        self,
        name: str,
        base_path: Optional[Path] = None,
        portfolio=None,
        shared_holdings=None,
        message_bus=None,
    ):
        super().__init__(name, base_path, portfolio, shared_holdings, message_bus)
        self._results: List[OrderPairResult] = []
        self._running = False

    # -----------------------------------------------------------------------
    # Lifecycle (minimal – one-shot plugins)
    # -----------------------------------------------------------------------

    def start(self) -> bool:
        logger.info(f"Starting plugin '{self.name}'")
        saved = self.load_state()
        if saved:
            self._results = [
                OrderPairResult(**{k: v for k, v in r.items() if k != "passed"})
                for r in saved.get("results", [])
            ]
        return True

    def stop(self) -> bool:
        logger.info(f"Stopping plugin '{self.name}'")
        self.save_state({"results": [r.to_dict() for r in self._results]})
        self.unsubscribe_all()
        return True

    def freeze(self) -> bool:
        self.save_state({"results": [r.to_dict() for r in self._results]})
        return True

    def resume(self) -> bool:
        return True

    def calculate_signals(self, market_data: Dict) -> List[TradeSignal]:
        return []

    # -----------------------------------------------------------------------
    # Request handling
    # -----------------------------------------------------------------------

    def handle_request(self, request_type: str, payload: Dict) -> Dict:
        if request_type == "run_tests":
            return self._run_all_tests()
        if request_type == "get_results":
            return {
                "success": True,
                "data": {
                    "results": [r.to_dict() for r in self._results],
                    "summary": self._build_summary(),
                },
            }
        if request_type == "get_status":
            return {
                "success": True,
                "data": {
                    "running": self._running,
                    "test_count": len(self.TEST_CASES),
                    "result_count": len(self._results),
                },
            }
        return {"success": False, "message": f"Unknown request: {request_type}"}

    # -----------------------------------------------------------------------
    # Paper verification
    # -----------------------------------------------------------------------

    def _verify_paper_connection(self) -> Optional[str]:
        """Return an error string if not on a paper account, else None."""
        if not self.portfolio:
            return "No portfolio instance"
        if not self.portfolio.connected:
            return "Not connected to IB"
        port = self.portfolio.port
        if port not in PAPER_PORTS:
            return (
                f"SAFETY: Port {port} is not a paper port "
                f"({PAPER_PORTS}). Refusing to run order tests on live."
            )
        accounts = self.portfolio.managed_accounts
        if not accounts:
            return "No managed accounts found"
        account = accounts[0]
        if not account.startswith("D"):
            return (
                f"SAFETY: Account '{account}' does not look like a paper account "
                f"(paper accounts start with 'D')."
            )
        return None

    # -----------------------------------------------------------------------
    # Price fetching
    # -----------------------------------------------------------------------

    def _fetch_price(self, symbol: str, contract: Contract,
                     timeout: float = 15.0) -> Optional[float]:
        """Subscribe to a tick stream, wait for one valid price, cancel stream.

        Uses delayed market data (type 3) so US equity ticks arrive on paper
        accounts; restores live mode (type 1) before returning so that any
        subsequent reqRealTimeBars calls are not silently suppressed.
        """
        price_event = threading.Event()
        captured: Dict[str, float] = {}

        def on_tick(sym: str, price: float, tick_type: str):
            if sym == symbol and price > 0 and not price_event.is_set():
                captured["price"] = price
                price_event.set()

        if self.portfolio:
            self.portfolio.reqMarketDataType(3)

        self.request_stream(
            symbol=symbol,
            contract=contract,
            data_types={DataType.TICK},
            on_tick=on_tick,
        )

        price_event.wait(timeout=timeout)
        self.cancel_stream(symbol)

        if self.portfolio:
            self.portfolio.reqMarketDataType(1)

        return captured.get("price")

    # -----------------------------------------------------------------------
    # Order placement helpers
    # -----------------------------------------------------------------------

    def _place(self, contract: Contract, order: Order) -> Optional[int]:
        """Place an order via portfolio.place_order_custom()."""
        if not self.portfolio:
            return None
        return self.portfolio.place_order_custom(contract, order)

    def _place_raw(self, order_id: int, contract: Contract,
                   order: Order) -> bool:
        """Place a pre-allocated order via portfolio.place_order_raw()."""
        if not self.portfolio:
            return False
        return self.portfolio.place_order_raw(order_id, contract, order)

    def _alloc_ids(self, count: int) -> List[int]:
        """Allocate consecutive order IDs from the portfolio."""
        if not self.portfolio:
            return []
        return self.portfolio.allocate_order_ids(count)

    def _cancel(self, order_id: Optional[int]) -> bool:
        if order_id is None or not self.portfolio:
            return False
        return self.portfolio.cancel_order(order_id)

    # -----------------------------------------------------------------------
    # Fill monitoring
    # -----------------------------------------------------------------------

    def _wait_fill_cancel_other(
        self,
        oid_long: Optional[int],
        oid_short: Optional[int],
        timeout: float,
        immediate: bool = False,
    ) -> Tuple[Optional[str], float]:
        """
        Poll for fills on both orders until one fills or timeout.

        For immediate orders (market/MOC) both are expected to fill;
        returns "both" in that case.

        Returns:
            (fill_side, fill_price) – fill_side is "long", "short", "both",
            or None on timeout.
        """
        deadline = time.time() + timeout
        port = self.portfolio

        while time.time() < deadline:
            rec_l = port.get_order(oid_long) if oid_long else None
            rec_s = port.get_order(oid_short) if oid_short else None
            filled_l = rec_l and rec_l.is_filled
            filled_s = rec_s and rec_s.is_filled

            if immediate:
                if filled_l and filled_s:
                    return "both", (rec_l.avg_fill_price + rec_s.avg_fill_price) / 2

            if filled_l and not filled_s:
                self._cancel(oid_short)
                return "long", rec_l.avg_fill_price
            if filled_s and not filled_l:
                self._cancel(oid_long)
                return "short", rec_s.avg_fill_price
            if filled_l and filled_s:
                return "both", (rec_l.avg_fill_price + rec_s.avg_fill_price) / 2

            time.sleep(0.5)

        # Timeout – cancel both
        self._cancel(oid_long)
        self._cancel(oid_short)
        return None, 0.0

    # -----------------------------------------------------------------------
    # Test orchestration
    # -----------------------------------------------------------------------

    def _run_test_case(self, tc: OrderTestCase) -> OrderPairResult:
        """Execute one OrderTestCase; return an OrderPairResult."""
        sym_a, sym_b = ETF_PAIRS[tc.pair_index]
        start = time.time()

        if tc.is_stub:
            return OrderPairResult(
                test_name=tc.name,
                order_type=tc.order_type_label,
                symbol_long=sym_a,
                symbol_short=sym_b,
                is_stub=True,
                stub_reason=tc.stub_reason,
                notes=tc.notes,
                duration_seconds=0.0,
            )

        con_a = make_stk_contract(sym_a)
        con_b = make_stk_contract(sym_b)

        result = OrderPairResult(
            test_name=tc.name,
            order_type=tc.order_type_label,
            symbol_long=sym_a,
            symbol_short=sym_b,
        )

        try:
            logger.info(f"  [{tc.name}] Fetching prices: {sym_a}, {sym_b}")
            price_a = self._fetch_price(sym_a, con_a)
            price_b = self._fetch_price(sym_b, con_b)

            if not price_a or not price_b:
                result.error_message = (
                    f"Could not fetch price: {sym_a}={price_a}, {sym_b}={price_b}"
                )
                result.duration_seconds = time.time() - start
                return result

            logger.info(
                f"  [{tc.name}] {sym_a}=${price_a:.4f}, {sym_b}=${price_b:.4f}"
            )

            order_a = tc.build_long(price_a)
            order_b = tc.build_short(price_b)

            oid_a = self._place(con_a, order_a)
            oid_b = self._place(con_b, order_b)

            if oid_a is None and oid_b is None:
                result.error_message = "Failed to submit both orders"
                result.duration_seconds = time.time() - start
                return result

            result.long_order_id = oid_a
            result.short_order_id = oid_b
            result.submitted = True
            result.notes = tc.notes

            fill_side, fill_price = self._wait_fill_cancel_other(
                oid_a, oid_b, tc.fill_timeout, tc.immediate
            )
            result.fill_side = fill_side
            result.fill_price = fill_price
            result.cancel_ok = fill_side is not None

            if fill_side:
                logger.info(
                    f"  [{tc.name}] Fill: {fill_side} @ ${fill_price:.4f}, other cancelled"
                )
            else:
                result.notes = (result.notes + " | " if result.notes else "") + \
                    "Timeout: orders submitted but did not fill in window"
                logger.warning(f"  [{tc.name}] Timeout – both orders cancelled")

        except Exception as e:
            result.error_message = str(e)
            logger.error(f"  [{tc.name}] Error: {e}", exc_info=True)

        result.duration_seconds = time.time() - start
        return result

    def _run_all_tests(self) -> Dict:
        """Run all TEST_CASES serially and return results."""
        if self._running:
            return {"success": False, "message": "Tests already running"}
        if not self._executive:
            return {"success": False, "message": "No executive/stream manager"}

        self._running = True
        self._results = []

        try:
            error = self._verify_paper_connection()
            if error:
                logger.error(f"Paper verification failed: {error}")
                return {"success": False, "message": error}

            # Note: do NOT force reqMarketDataType(3) here.  reqRealTimeBars
            # (used for price fetching) only works in live mode; forcing
            # delayed (3) silently drops bar callbacks regardless of subscription.
            for tc in self.TEST_CASES:
                logger.info(f"--- [{self.name}] Testing: {tc.name} ---")
                result = self._run_test_case(tc)
                self._results.append(result)
                self._log_result(result)

            summary = self._build_summary()
            logger.info(
                f"[{self.name}] Tests complete: "
                f"{summary['submitted']}/{summary['total']} submitted, "
                f"{summary['filled']}/{summary['total']} filled"
            )

            self.request_unload()

            return {
                "success": True,
                "data": {
                    "results": [r.to_dict() for r in self._results],
                    "summary": summary,
                },
            }

        except Exception as e:
            logger.error(f"[{self.name}] Test run error: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

        finally:
            self._running = False

    def _log_result(self, r: OrderPairResult):
        status = "STUB" if r.is_stub else ("SUBMITTED" if r.submitted else "FAILED")
        fill = f"fill={r.fill_side}@${r.fill_price:.4f}" if r.fill_side else "no-fill"
        logger.info(
            f"  [{r.test_name}] {status} {fill}"
            + (f" ERR: {r.error_message}" if r.error_message else "")
        )

    def _build_summary(self) -> Dict[str, Any]:
        total = len(self._results)
        submitted = sum(1 for r in self._results if r.submitted or r.is_stub)
        filled = sum(1 for r in self._results if r.fill_side)
        stubs = sum(1 for r in self._results if r.is_stub)
        errors = [r.test_name for r in self._results if r.error_message]
        return {
            "total": total,
            "submitted": submitted,
            "filled": filled,
            "stubs": stubs,
            "errors": errors,
        }
