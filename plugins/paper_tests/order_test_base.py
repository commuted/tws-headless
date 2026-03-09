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

# Primary exchange for each ETF used across the order test plugins.
# Required so IB can resolve the contract without ambiguity on SMART routing.
# All SPDR sector ETFs and the Russell 2000 ETF list on NYSE Arca.
_PRIMARY_EXCHANGE: Dict[str, str] = {
    "SPY": "ARCA",
    "QQQ": "NASDAQ",
    "IWM": "ARCA",
    "XLF": "ARCA",
    "XLK": "ARCA",
    "XLE": "ARCA",
}

from ib.data_feed import DataType, TickData
from plugins.base import PluginBase, TradeSignal

logger = logging.getLogger(__name__)

# ===========================================================================
# ETF pairs: (symbol_a, symbol_b)
#
# All ordinary (non-leveraged, non-inverse) US equity ETFs.  Six distinct
# tickers arranged so that each symbol appears on *one side only* — no
# ticker ever crosses from BUY to SELL across the 7 test cases.  This
# matches the original leveraged-ETF design discipline and prevents
# position accumulation from competing fills.
#
# BUY-only symbols  (sym_a):  SPY, IWM, XLK
# SELL-only symbols (sym_b):  QQQ, XLF, XLE
#
#   SPY / QQQ  – S&P 500 / Nasdaq 100.  Both move with the broad market;
#                immediate-fill tests (market, MOC, MTL) always fill.
#                Typical 5-min range: SPY ~0.10–0.18%, QQQ ~0.12–0.22%.
#
#   IWM / XLF  – Russell 2000 / Financials.  Small-caps vs. rate-sensitive
#                sector; conditional tests (limit, stop-limit) benefit from
#                independent underlying dynamics.
#                Typical 5-min range: IWM ~0.15–0.25%, XLF ~0.10–0.20%.
#
#   XLK / XLE  – Technology / Energy.  Often move in opposite directions
#                on macro events; provides a second independent theme for
#                stop and MOO tests.
#                Typical 5-min range: XLK ~0.12–0.22%, XLE ~0.15–0.30%.
#
# Offset calibration: 0.30% sits at ~1–2× the typical 5-min range for all
# six ETFs, giving a reasonable chance to fill on active days while still
# passing on "submitted" when limits are not hit in the window.
#
# Index → test use:
#   0 SPY/QQQ   market / immediate
#   1 IWM/XLF   limit              (conditional A)
#   2 XLK/XLE   stop               (conditional B)
#   3 IWM/XLF   stop-limit         (conditional A)
#   4 SPY/QQQ   MOC / immediate
#   5 XLK/XLE   MOO / immediate    (conditional B)
#   6 SPY/QQQ   MTL / immediate
# ===========================================================================
ETF_PAIRS: List[Tuple[str, str]] = [
    ("SPY",  "QQQ"),    # 0 – S&P 500 / Nasdaq 100 (immediate)
    ("IWM",  "XLF"),    # 1 – Russell 2000 / Financials (limit)
    ("XLK",  "XLE"),    # 2 – Technology / Energy (stop)
    ("IWM",  "XLF"),    # 3 – Russell 2000 / Financials (stop-limit)
    ("SPY",  "QQQ"),    # 4 – S&P 500 / Nasdaq 100 (MOC immediate)
    ("XLK",  "XLE"),    # 5 – Technology / Energy (MOO)
    ("SPY",  "QQQ"),    # 6 – S&P 500 / Nasdaq 100 (MTL immediate)
]

# Quantity for all test orders (1 share – small footprint on paper account)
TEST_QTY = Decimal("1")

# Fraction below/above current price for conditional order offsets.
# Calibrated to the typical 5-minute move of the ETFs above:
#   SPY/QQQ: avg 5-min range ~0.10–0.22%; 0.30% offset is ~1–2× that range.
#   IWM/XLF: avg 5-min range ~0.10–0.25%; 0.30% offset is within range on most days.
#   XLK/XLE: avg 5-min range ~0.12–0.30%; 0.30% offset hits on active sessions.
# Tests still PASS on "submitted" even when limits do not fill in window.
OFFSET_BELOW = 0.997   # BUY limit/stop 0.30% below market
OFFSET_ABOVE = 1.003   # SELL limit/stop 0.30% above market

# Paper trading ports
PAPER_PORTS = (7497, 4002)


def make_stk_contract(symbol: str, exchange: str = "SMART",
                      currency: str = "USD") -> Contract:
    """Create a basic STK (equity) contract.

    Sets primaryExch from _PRIMARY_EXCHANGE when available so IB can resolve
    the contract without ambiguity on SMART routing.
    """
    c = Contract()
    c.symbol = symbol
    c.secType = "STK"
    c.exchange = exchange
    c.currency = currency
    c.primaryExch = _PRIMARY_EXCHANGE.get(symbol, "")
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
        self._fill_events: Dict[int, threading.Event] = {}  # order_id -> fill event

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
        self._cancel_all_test_orders()
        self.save_state({"results": [r.to_dict() for r in self._results]})
        self.unsubscribe_all()
        return True

    def _cancel_all_test_orders(self):
        """Cancel all live test orders recorded in _results.

        Checks the actual order status before cancelling:
          - FILLED / CANCELLED: skip (already terminal)
          - INACTIVE: cancel and warn (IB accepted the order but it is
            not actively working; e.g. condition unmet, outside hours,
            or unsupported on paper).  Left uncancelled these can
            execute unexpectedly when the condition is eventually met.
          - PENDING / SUBMITTED / PARTIALLY_FILLED / UNKNOWN: cancel.

        Called before unload and again in stop() as a safety net.
        """
        if not self.portfolio:
            return

        from ib.models import OrderStatus  # avoid circular at module level

        for r in self._results:
            for oid in (r.long_order_id, r.short_order_id):
                if oid is None:
                    continue
                rec = self.portfolio.get_order(oid)
                if rec is None:
                    # Not tracked — send cancel defensively
                    logger.warning(f"[{self.name}] Order {oid} not in portfolio records; cancelling defensively")
                    self._cancel(oid)
                    continue

                if rec.status in (OrderStatus.FILLED, OrderStatus.CANCELLED):
                    logger.debug(f"[{self.name}] Order {oid} already {rec.status.value}; skipping")
                    continue

                if rec.status == OrderStatus.INACTIVE:
                    logger.warning(
                        f"[{self.name}] Order {oid} ({rec.symbol} {rec.action} "
                        f"{rec.order_type}) is INACTIVE — cancelling. "
                        f"whyHeld may indicate unsupported condition or outside hours."
                    )
                else:
                    logger.info(f"[{self.name}] Cancelling order {oid} ({rec.symbol} {rec.action} {rec.status.value})")

                self._cancel(oid)

    def freeze(self) -> bool:
        self.save_state({"results": [r.to_dict() for r in self._results]})
        return True

    def resume(self) -> bool:
        return True

    def calculate_signals(self) -> List[TradeSignal]:
        return []

    def on_order_fill(self, order_record) -> None:
        """Wake _wait_fill_cancel_other when a tracked order fills."""
        ev = self._fill_events.get(order_record.order_id)
        if ev:
            ev.set()

    def on_order_status(self, order_record) -> None:
        """Wake _wait_fill_cancel_other on any terminal state (fill or rejection)."""
        if order_record.is_complete:
            ev = self._fill_events.get(order_record.order_id)
            if ev:
                ev.set()

    def on_ib_error(self, req_id: int, error_code: int, error_string: str) -> None:
        """Log IB errors attributed to this plugin's orders or streams."""
        logger.warning(
            f"[{self.name}] IB error reqId={req_id} [{error_code}]: {error_string}"
        )

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
    # Post-placement status check
    # -----------------------------------------------------------------------

    def _check_placement_status(
        self,
        test_name: str,
        oid_a: Optional[int], sym_a: str,
        oid_b: Optional[int], sym_b: str,
    ) -> List[str]:
        """Check order statuses 2 seconds after placement.

        Returns a list of error strings for any order that is INACTIVE
        or ERROR.  An empty list means both orders look healthy.

        INACTIVE immediately after placement means IB accepted the order
        but is not working it — typical causes: condition unsupported on
        this account type, order placed outside market hours, or a
        problem with the order parameters.  The whyHeld field (if set)
        gives IB's reason.
        """
        from ib.models import OrderStatus

        errors = []
        for label, oid, sym in (("long", oid_a, sym_a), ("short", oid_b, sym_b)):
            if oid is None:
                errors.append(f"{label} order not placed for {sym}")
                continue
            rec = self.portfolio.get_order(oid) if self.portfolio else None
            if rec is None:
                logger.warning(f"  [{test_name}] {label} order {oid} not found in portfolio records")
                continue
            if rec.status == OrderStatus.INACTIVE:
                why = f" (whyHeld={rec.why_held!r})" if rec.why_held else ""
                msg = f"{label} order {oid} ({sym} {rec.order_type}) is INACTIVE{why}"
                logger.warning(f"  [{test_name}] {msg} — cancelling")
                self._cancel(oid)
                errors.append(msg)
            elif rec.status == OrderStatus.ERROR:
                msg = f"{label} order {oid} ({sym}) ERROR: {rec.error_message}"
                logger.error(f"  [{test_name}] {msg}")
                errors.append(msg)
            else:
                logger.info(
                    f"  [{test_name}] {label} order {oid} ({sym}) status: {rec.status.value}"
                    + (f" whyHeld={rec.why_held!r}" if rec.why_held else "")
                )
        return errors

    # -----------------------------------------------------------------------
    # Price fetching
    # -----------------------------------------------------------------------

    def _fetch_price(self, symbol: str, contract: Contract,
                     timeout: float = 15.0) -> Optional[float]:
        """Subscribe to a tick stream, wait for one valid price, cancel stream."""
        price_event = threading.Event()
        captured: Dict[str, float] = {}

        def on_tick(tick: TickData):
            if tick.symbol == symbol and tick.price > 0 and not price_event.is_set():
                captured["price"] = tick.price
                price_event.set()

        self.request_stream(
            symbol=symbol,
            contract=contract,
            data_types={DataType.TICK},
            on_tick=on_tick,
        )

        price_event.wait(timeout=timeout)
        self.cancel_stream(symbol)

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
        Wait for fills on both orders until one fills or timeout.

        Wakes immediately via on_order_fill() callback; falls back to a
        0.5-second poll interval if callbacks are unavailable.

        For immediate orders (market/MOC) both are expected to fill;
        returns "both" in that case.

        Returns:
            (fill_side, fill_price) – fill_side is "long", "short", "both",
            or None on timeout.
        """
        deadline = time.time() + timeout
        port = self.portfolio

        ev = threading.Event()
        for oid in (oid_long, oid_short):
            if oid is not None:
                self._fill_events[oid] = ev

        try:
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

                # Exit early if either order reached a terminal non-fill state
                # (rejected, cancelled, error) so we don't wait out the full timeout.
                if rec_l and rec_l.is_complete and not filled_l:
                    logger.warning(
                        f"  Order {oid_long} ({rec_l.symbol}) terminal without fill:"
                        f" {rec_l.status.value}"
                    )
                    self._cancel(oid_short)
                    return None, 0.0
                if rec_s and rec_s.is_complete and not filled_s:
                    logger.warning(
                        f"  Order {oid_short} ({rec_s.symbol}) terminal without fill:"
                        f" {rec_s.status.value}"
                    )
                    self._cancel(oid_long)
                    return None, 0.0

                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                ev.wait(timeout=min(0.5, remaining))
                ev.clear()

        finally:
            for oid in (oid_long, oid_short):
                if oid is not None:
                    self._fill_events.pop(oid, None)

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

            if oid_a is not None:
                self.register_order(oid_a)
            if oid_b is not None:
                self.register_order(oid_b)

            if oid_a is None and oid_b is None:
                result.error_message = "Failed to place both orders (no order ID returned)"
                result.duration_seconds = time.time() - start
                return result

            result.long_order_id = oid_a
            result.short_order_id = oid_b

            # Wait briefly for IB to send the initial orderStatus callback,
            # then verify each order is actually in a working state.
            time.sleep(2.0)
            placement_errors = self._check_placement_status(
                tc.name, oid_a, sym_a, oid_b, sym_b
            )
            if placement_errors:
                result.error_message = "; ".join(placement_errors)
                result.duration_seconds = time.time() - start
                return result

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

            self._cancel_all_test_orders()
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
