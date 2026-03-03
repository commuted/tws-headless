"""
paper_test_orders_6/plugin.py — Round-trip order lifecycle tests

Every test in this plugin opens a position on a single symbol and then closes
it, leaving the paper account flat.  This contrasts with plugins 1–5 which
place competing long/short pairs and cancel the unfilled side, never explicitly
closing the filled leg.

Additional order features exercised
────────────────────────────────────
  TIF variants   IOC (immediate-or-cancel), GTC (good-till-cancelled), FOK
  IB algo        Adaptive (urgency=Urgent) — smart routing with controlled pace
  Modifiers      displaySize / iceberg (shows 1 share at a time on book)

Round-trip tests (position opened AND closed on same symbol)
────────────────────────────────────────────────────────────
  mkt_mkt           MKT buy TQQQ  → MKT sell            (baseline, always fills)
  mkt_lmt           MKT buy SQQQ  → LMT sell fill+0.5%  (profit-target exit; fallback MKT)
  lmt_mkt           LMT buy SPXU −0.3% → MKT sell       (patient entry, instant exit)
  mkt_trail         MKT buy SDS   → TRAIL 0.5%           (trailing exit; fallback MKT)
  mkt_stplmt        MKT buy SDOW  → STP LMT −1%/−1.5%  (protective exit; fallback MKT)
  adaptive          Adaptive BUY SPXU (Urgent) → MKT sell
  iceberg           LMT buy TQQQ 3 shares displaySize=1 → MKT sell 3

No-position tests (order placed, verified, then cancelled / killed)
────────────────────────────────────────────────────────────────────
  ioc_no_fill       IOC LMT at −2% DXD  → confirm cancel, no fill
  gtc_lifecycle     GTC LMT at −3% SQQQ → confirm submitted → explicit cancel
  fok_market        FOK MKT SPY         → fills (liquid) or kills; if filled: MKT exit

Run via: plugin request paper_test_orders_6 run_tests
"""

import datetime
import logging
import threading
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

from ibapi.order import Order
from ibapi.tag_value import TagValue

from plugins.base import PluginBase, TradeSignal
from plugins.paper_tests.order_test_base import PAPER_PORTS, make_stk_contract

logger = logging.getLogger(__name__)

TEST_QTY = Decimal("1")

# Price offsets
_BELOW_003 = 0.997   # 0.3% below — aggressive limit, should fill within minutes
_BELOW_200 = 0.980   # 2.0% below — IOC bait, should NOT fill
_BELOW_300 = 0.970   # 3.0% below — GTC bait, should NOT fill
_ABOVE_005 = 1.005   # 0.5% above — limit profit target


# =============================================================================
# Result type
# =============================================================================

@dataclass
class RoundTripResult:
    """Result of one round-trip (or no-position lifecycle) test."""
    test_name: str
    order_type: str          # e.g. "MKT→MKT", "IOC", "GTC+cancel"
    symbol: str
    quantity: float = 1.0

    # Entry leg
    entry_submitted: bool = False
    entry_order_id: Optional[int] = None
    entry_fill_price: float = 0.0

    # Exit leg (absent for no-position tests)
    exit_submitted: bool = False
    exit_order_id: Optional[int] = None
    exit_fill_price: float = 0.0

    # Outcome
    round_trip_complete: bool = False   # both entry AND exit filled
    net_pnl: float = 0.0               # exit_fill − entry_fill × quantity

    # No-position test outcomes
    expected_no_fill: bool = False
    no_fill_confirmed: bool = False

    duration_seconds: float = 0.0
    error_message: str = ""
    notes: str = ""

    @property
    def passed(self) -> bool:
        if self.error_message:
            return False
        if self.expected_no_fill:
            return self.no_fill_confirmed
        return self.entry_submitted

    def to_dict(self) -> Dict[str, Any]:
        return {
            "test_name": self.test_name,
            "order_type": self.order_type,
            "symbol": self.symbol,
            "quantity": self.quantity,
            "entry_submitted": self.entry_submitted,
            "entry_fill_price": round(self.entry_fill_price, 4),
            "exit_submitted": self.exit_submitted,
            "exit_fill_price": round(self.exit_fill_price, 4),
            "round_trip_complete": self.round_trip_complete,
            "net_pnl": round(self.net_pnl, 4),
            "expected_no_fill": self.expected_no_fill,
            "no_fill_confirmed": self.no_fill_confirmed,
            "duration_seconds": round(self.duration_seconds, 2),
            "error_message": self.error_message,
            "notes": self.notes,
            "passed": self.passed,
        }


# =============================================================================
# Plugin
# =============================================================================

class PaperTestOrders6Plugin(PluginBase):
    """
    Paper test: round-trip order lifecycle.

    Each test that opens a position also closes it, leaving the account flat.
    Also tests TIF variants (IOC, GTC, FOK), IB Adaptive algo, and iceberg
    (displaySize) orders.

    Refuses to run on live accounts.
    Run via: plugin request paper_test_orders_6 run_tests
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
            "paper_test_orders_6",
            base_path, portfolio, shared_holdings, message_bus,
        )
        self._results: List[RoundTripResult] = []
        self._running = False
        # order_id → Event: set by on_order_fill / on_order_status
        self._fill_events: Dict[int, threading.Event] = {}

    @property
    def description(self) -> str:
        return (
            "Paper Test Orders 6 — Round-Trip: every test that opens a "
            "position also closes it; covers IOC, GTC, FOK, Adaptive, Iceberg."
        )

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def start(self) -> bool:
        logger.info(f"Starting plugin '{self.name}'")
        saved = self.load_state()
        if saved:
            self._results = [
                RoundTripResult(**{k: v for k, v in r.items() if k != "passed"})
                for r in saved.get("results", [])
            ]
        return True

    def stop(self) -> bool:
        logger.info(f"Stopping plugin '{self.name}'")
        self._cancel_all_open_orders()
        self.save_state({"results": [r.to_dict() for r in self._results]})
        self.unsubscribe_all()
        return True

    def freeze(self) -> bool:
        self.save_state({"results": [r.to_dict() for r in self._results]})
        return True

    def resume(self) -> bool:
        return True

    def calculate_signals(self) -> List[TradeSignal]:
        return []

    def on_unload(self) -> str:
        total = len(self._results)
        passed = sum(1 for r in self._results if r.passed)
        complete = sum(1 for r in self._results if r.round_trip_complete)
        return (
            f"paper_test_orders_6: {passed}/{total} passed, "
            f"{complete} full round-trips completed"
        )

    # -------------------------------------------------------------------------
    # Fill / status callbacks
    # -------------------------------------------------------------------------

    def on_order_fill(self, order_record) -> None:
        ev = self._fill_events.get(order_record.order_id)
        if ev:
            ev.set()

    def on_order_status(self, order_record) -> None:
        if order_record.is_complete:
            ev = self._fill_events.get(order_record.order_id)
            if ev:
                ev.set()

    def on_ib_error(self, req_id: int, error_code: int, error_string: str) -> None:
        logger.warning(
            f"[{self.name}] IB error reqId={req_id} [{error_code}]: {error_string}"
        )
        # Wake any thread waiting on this order_id
        ev = self._fill_events.get(req_id)
        if ev:
            ev.set()

    # -------------------------------------------------------------------------
    # Request handling
    # -------------------------------------------------------------------------

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
                    "result_count": len(self._results),
                },
            }
        return {"success": False, "message": f"Unknown request: {request_type}"}

    # -------------------------------------------------------------------------
    # Safety gate
    # -------------------------------------------------------------------------

    def _verify_paper_connection(self) -> Optional[str]:
        if not self.portfolio:
            return "No portfolio instance"
        if not self.portfolio.connected:
            return "Not connected to IB"
        port = self.portfolio.port
        if port not in PAPER_PORTS:
            return (
                f"SAFETY: port {port} is not a paper port "
                f"({PAPER_PORTS}). Refusing to run on live."
            )
        accounts = self.portfolio.managed_accounts
        if not accounts:
            return "No managed accounts found"
        if not accounts[0].startswith("D"):
            return (
                f"SAFETY: account '{accounts[0]}' does not look like "
                f"a paper account (paper accounts start with 'D')."
            )
        return None

    # -------------------------------------------------------------------------
    # Test runner
    # -------------------------------------------------------------------------

    def _run_all_tests(self) -> Dict:
        if self._running:
            return {"success": False, "message": "Tests already running"}
        if not self._executive:
            return {"success": False, "message": "No executive/stream manager"}

        self._running = True
        self._results = []

        try:
            err = self._verify_paper_connection()
            if err:
                logger.error(f"Paper verification failed: {err}")
                return {"success": False, "message": err}

            if not self._is_market_open():
                return {
                    "success": False,
                    "message": (
                        "US equity market is currently closed. "
                        "Re-run during regular trading hours (9:30–16:00 ET, Mon–Fri)."
                    ),
                }

            test_fns = [
                self._test_mkt_mkt,
                self._test_mkt_lmt,
                self._test_lmt_mkt,
                self._test_mkt_trail,
                self._test_mkt_stplmt,
                self._test_ioc_no_fill,
                self._test_gtc_lifecycle,
                self._test_fok_market,
                self._test_adaptive_roundtrip,
                self._test_iceberg_roundtrip,
            ]

            for fn in test_fns:
                logger.info(f"--- [{self.name}] {fn.__name__} ---")
                r = fn()
                self._results.append(r)
                self._log_result(r)

            # Final safety sweep: cancel anything still open
            self._cancel_all_open_orders()

            summary = self._build_summary()
            logger.info(
                f"[{self.name}] Done: {summary['passed']}/{summary['total']} passed, "
                f"{summary['round_trips']} full round-trips, "
                f"total PnL=${summary['total_pnl']:.4f}"
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
            logger.error(f"[{self.name}] Unexpected error: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

        finally:
            self._running = False

    # -------------------------------------------------------------------------
    # Infrastructure helpers
    # -------------------------------------------------------------------------

    def _place(self, contract, order) -> Optional[int]:
        if not self.portfolio:
            return None
        oid = self.portfolio.place_order_custom(contract, order)
        if oid is not None:
            self.register_order(oid)
        return oid

    def _cancel(self, oid: Optional[int]) -> bool:
        if oid is None or not self.portfolio:
            return False
        return self.portfolio.cancel_order(oid)

    def _wait_fill(self, oid: int, timeout: float) -> Optional[float]:
        """
        Wait for order `oid` to fill or reach a terminal state.

        Returns the average fill price, or None on timeout or non-fill terminal
        (cancelled, rejected, error).  Uses on_order_fill / on_order_status
        callbacks for near-instant wake-up; falls back to 0.5s polling.
        """
        ev = threading.Event()
        self._fill_events[oid] = ev
        deadline = time.time() + timeout
        try:
            while time.time() < deadline:
                rec = self.portfolio.get_order(oid) if self.portfolio else None
                if rec and rec.is_filled:
                    return rec.avg_fill_price
                if rec and rec.is_complete and not rec.is_filled:
                    logger.info(
                        f"  [{self.name}] order {oid} terminal without fill: "
                        f"{rec.status.value}"
                    )
                    return None
                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                ev.wait(timeout=min(0.5, remaining))
                ev.clear()
        finally:
            self._fill_events.pop(oid, None)
        return None  # timeout

    # Primary exchange for each symbol used in this plugin.
    # Required so IB can route historical data requests without ambiguity.
    _PRIMARY_EXCHANGE: Dict[str, str] = {
        "TQQQ": "NASDAQ", "SQQQ": "NASDAQ",
        "SPXU": "ARCA",   "SDS":  "ARCA",
        "SDOW": "ARCA",   "DXD":  "ARCA",
        "SPY":  "ARCA",
    }

    def _make_contract(self, symbol: str):
        """Contract with primaryExch set — needed for historical data requests."""
        from ib.contract_builder import ContractBuilder
        return ContractBuilder.us_stock(symbol, self._PRIMARY_EXCHANGE.get(symbol, ""))

    def _fetch_price(self, symbol: str, timeout: float = 15.0) -> Optional[float]:
        """
        Fetch current market price.

        During market hours: tick stream gives a near-instant live price.
        After hours / no ticks within timeout: falls back to the close of the
        most recent daily bar (works any time IB has historical data).
        """
        from ib.data_feed import DataType, TickData
        contract = make_stk_contract(symbol)
        captured: Dict[str, float] = {}
        ev = threading.Event()

        def _on_tick(tick: TickData):
            if tick.symbol == symbol and tick.price > 0 and "price" not in captured:
                captured["price"] = tick.price
                ev.set()

        self.request_stream(
            symbol=symbol,
            contract=contract,
            data_types={DataType.TICK},
            on_tick=_on_tick,
        )
        # Use a shorter tick wait so after-hours fallback kicks in quickly.
        tick_timeout = min(timeout, 8.0)
        ev.wait(timeout=tick_timeout)
        self.cancel_stream(symbol)

        if captured.get("price"):
            return captured["price"]

        # Fallback: last close from 1-day historical bar (use primaryExch contract).
        # use_rth=False so IB returns data even when queried outside market hours.
        logger.info(f"  [{self.name}] no live tick for {symbol}; trying historical close")
        bars = self.get_historical_data(
            contract=self._make_contract(symbol),
            duration_str="2 D",
            bar_size_setting="1 day",
            what_to_show="TRADES",
            use_rth=False,
            timeout=30.0,
        )
        if bars:
            close = bars[-1].close
            if close and close > 0:
                logger.info(f"  [{self.name}] {symbol} historical close: {close}")
                return float(close)

        return None

    def _is_market_open(self, symbol: str = "TQQQ") -> bool:
        """
        Use reqContractDetails to check whether the symbol is currently in its
        liquid (regular) trading hours.

        IB returns liquidHours as a semicolon-separated list of segments:
            "20260303:0930-20260303:1600;20260304:0930-20260304:1600"
        or  "20260301:CLOSED;20260302:0930-20260302:1600"

        We check if NOW (in the contract's timezone) falls inside any open
        segment from today.  Falls back to True on any parse failure so that
        missing contract data never silently blocks a test run.
        """
        contract = self._make_contract(symbol)
        details_list = self.get_contract_details(contract, timeout=15.0)
        if not details_list:
            logger.warning(f"[{self.name}] _is_market_open: no contract details for {symbol}; assuming open")
            return True

        cd = details_list[0]
        tz_id = getattr(cd, "timeZoneId", "") or "America/New_York"
        liquid = getattr(cd, "liquidHours", "") or ""
        logger.info(f"[{self.name}] {symbol} liquidHours={liquid!r}  tz={tz_id!r}")

        try:
            import zoneinfo
            tz = zoneinfo.ZoneInfo(tz_id)
        except Exception:
            try:
                import pytz
                tz = pytz.timezone(tz_id)
            except Exception:
                logger.warning(f"[{self.name}] Cannot load timezone {tz_id!r}; assuming open")
                return True

        now = datetime.datetime.now(tz=tz)

        for segment in liquid.split(";"):
            segment = segment.strip()
            if not segment or "CLOSED" in segment:
                continue
            try:
                # "YYYYMMDD:HHMM-YYYYMMDD:HHMM"
                start_str, end_str = segment.split("-")
                def _parse(s: str) -> datetime.datetime:
                    date_part, time_part = s.split(":")
                    return datetime.datetime(
                        int(date_part[:4]), int(date_part[4:6]), int(date_part[6:8]),
                        int(time_part[:2]), int(time_part[2:4]),
                        tzinfo=tz,
                    )
                start_dt = _parse(start_str)
                end_dt = _parse(end_str)
                if start_dt <= now <= end_dt:
                    logger.info(f"[{self.name}] {symbol} market OPEN ({start_dt} – {end_dt})")
                    return True
            except Exception as e:
                logger.warning(f"[{self.name}] _is_market_open: parse error on {segment!r}: {e}")
                continue

        logger.info(f"[{self.name}] {symbol} market CLOSED at {now}")
        return False

    def _order_status_error(self, oid: int, symbol: str, label: str) -> Optional[str]:
        """
        Return an error string if the order is INACTIVE or ERROR; else None.
        Call 2 seconds after placement to catch immediately-rejected orders.
        """
        from ib.models import OrderStatus
        if not self.portfolio:
            return None
        rec = self.portfolio.get_order(oid)
        if rec is None:
            return None
        if rec.status == OrderStatus.INACTIVE:
            why = f" (whyHeld={rec.why_held!r})" if getattr(rec, "why_held", None) else ""
            return f"{label} order {oid} ({symbol}) INACTIVE{why}"
        if rec.status == OrderStatus.ERROR:
            return f"{label} order {oid} ({symbol}) ERROR: {getattr(rec, 'error_message', '')}"
        return None

    def _market_exit(self, symbol: str, action: str, qty: Decimal) -> Optional[int]:
        """Place a market order for emergency / fallback position close."""
        o = Order()
        o.action = action
        o.orderType = "MKT"
        o.totalQuantity = qty
        return self._place(make_stk_contract(symbol), o)

    def _cancel_all_open_orders(self):
        """Cancel any orders from this run that are not in a terminal state."""
        if not self.portfolio:
            return
        from ib.models import OrderStatus
        seen: set = set()
        for r in self._results:
            for oid in (r.entry_order_id, r.exit_order_id):
                if oid is None or oid in seen:
                    continue
                seen.add(oid)
                rec = self.portfolio.get_order(oid)
                if rec is None:
                    self._cancel(oid)
                    continue
                if rec.status not in (OrderStatus.FILLED, OrderStatus.CANCELLED):
                    logger.info(
                        f"[{self.name}] cleanup: cancelling order {oid} "
                        f"({rec.symbol} {rec.status.value})"
                    )
                    self._cancel(oid)

    # =========================================================================
    # Test methods — round-trip
    # =========================================================================

    def _test_mkt_mkt(self) -> RoundTripResult:
        """
        MKT buy TQQQ → MKT sell.
        Baseline round-trip: guaranteed to fill immediately on both legs.
        """
        sym = "TQQQ"
        r = RoundTripResult(test_name="mkt_mkt_roundtrip", order_type="MKT→MKT", symbol=sym)
        start = time.time()
        try:
            price = self._fetch_price(sym)
            if not price:
                r.error_message = f"Could not fetch price for {sym}"
                return r
            logger.info(f"  [{sym}] price=${price:.4f}")

            # Entry: market buy
            eo = Order(); eo.action = "BUY"; eo.orderType = "MKT"
            eo.totalQuantity = TEST_QTY
            oid_e = self._place(make_stk_contract(sym), eo)
            if oid_e is None:
                r.error_message = "Failed to place market entry order"
                return r
            r.entry_order_id = oid_e
            r.entry_submitted = True

            ef = self._wait_fill(oid_e, timeout=30.0)
            if ef is None:
                r.error_message = "Market entry did not fill in 30s"
                self._cancel(oid_e)
                return r
            r.entry_fill_price = ef
            logger.info(f"  [{sym}] Entry fill @ ${ef:.4f}")

            # Exit: market sell
            xo = Order(); xo.action = "SELL"; xo.orderType = "MKT"
            xo.totalQuantity = TEST_QTY
            oid_x = self._place(make_stk_contract(sym), xo)
            if oid_x is None:
                r.error_message = "Failed to place market exit order"
                return r
            r.exit_order_id = oid_x
            r.exit_submitted = True

            xf = self._wait_fill(oid_x, timeout=30.0)
            if xf is None:
                r.error_message = "Market exit did not fill in 30s"
                self._cancel(oid_x)
                return r
            r.exit_fill_price = xf
            r.round_trip_complete = True
            r.net_pnl = xf - ef
            r.notes = f"${ef:.4f} → ${xf:.4f}"
            logger.info(f"  [{sym}] Exit fill @ ${xf:.4f} | PnL=${r.net_pnl:.4f}")

        except Exception as e:
            r.error_message = str(e)
            logger.error(f"  [{r.test_name}] {e}", exc_info=True)
        r.duration_seconds = time.time() - start
        return r

    def _test_mkt_lmt(self) -> RoundTripResult:
        """
        MKT buy SQQQ → LMT sell at fill+0.5% (profit target).
        Falls back to MKT sell after 2 minutes if the limit does not fill.
        """
        sym = "SQQQ"
        r = RoundTripResult(
            test_name="mkt_lmt_roundtrip", order_type="MKT→LMT(fb:MKT)", symbol=sym
        )
        start = time.time()
        try:
            price = self._fetch_price(sym)
            if not price:
                r.error_message = f"Could not fetch price for {sym}"
                return r
            logger.info(f"  [{sym}] price=${price:.4f}")

            eo = Order(); eo.action = "BUY"; eo.orderType = "MKT"
            eo.totalQuantity = TEST_QTY
            oid_e = self._place(make_stk_contract(sym), eo)
            if oid_e is None:
                r.error_message = "Failed to place market entry order"
                return r
            r.entry_order_id = oid_e
            r.entry_submitted = True

            ef = self._wait_fill(oid_e, timeout=30.0)
            if ef is None:
                r.error_message = "Market entry did not fill in 30s"
                self._cancel(oid_e)
                return r
            r.entry_fill_price = ef
            logger.info(f"  [{sym}] Entry fill @ ${ef:.4f}")

            # Exit: limit sell at entry+0.5% — profit target
            lmt_px = round(ef * _ABOVE_005, 2)
            xo = Order(); xo.action = "SELL"; xo.orderType = "LMT"
            xo.totalQuantity = TEST_QTY
            xo.lmtPrice = lmt_px
            oid_x = self._place(make_stk_contract(sym), xo)
            if oid_x is None:
                r.error_message = "Failed to place limit exit order"
                return r
            r.exit_order_id = oid_x
            r.exit_submitted = True
            logger.info(f"  [{sym}] LMT exit @ ${lmt_px:.4f}, waiting 2min...")

            xf = self._wait_fill(oid_x, timeout=120.0)
            if xf is None:
                # Fallback: cancel limit, use market
                logger.info(f"  [{sym}] Limit timed out — fallback market sell")
                self._cancel(oid_x)
                time.sleep(0.5)
                fb_oid = self._market_exit(sym, "SELL", TEST_QTY)
                if fb_oid:
                    r.exit_order_id = fb_oid
                    xf = self._wait_fill(fb_oid, timeout=30.0)

            if xf is not None:
                r.exit_fill_price = xf
                r.round_trip_complete = True
                r.net_pnl = xf - ef
                r.notes = f"${ef:.4f} → lmt_target=${lmt_px:.4f} exit=${xf:.4f}"
                logger.info(f"  [{sym}] Exit fill @ ${xf:.4f} | PnL=${r.net_pnl:.4f}")
            else:
                r.error_message = "Exit did not fill (limit + fallback market both timed out)"

        except Exception as e:
            r.error_message = str(e)
            logger.error(f"  [{r.test_name}] {e}", exc_info=True)
        r.duration_seconds = time.time() - start
        return r

    def _test_lmt_mkt(self) -> RoundTripResult:
        """
        LMT buy SPXU 0.3% below market → MKT sell when filled.
        Tests a passive entry: waits up to 5 minutes for the limit to fill,
        then exits immediately at market.
        """
        sym = "SPXU"
        r = RoundTripResult(test_name="lmt_mkt_roundtrip", order_type="LMT→MKT", symbol=sym)
        start = time.time()
        try:
            price = self._fetch_price(sym)
            if not price:
                r.error_message = f"Could not fetch price for {sym}"
                return r

            lmt_px = round(price * _BELOW_003, 2)
            logger.info(f"  [{sym}] price=${price:.4f}, limit entry @ ${lmt_px:.4f}")

            eo = Order(); eo.action = "BUY"; eo.orderType = "LMT"
            eo.totalQuantity = TEST_QTY
            eo.lmtPrice = lmt_px
            oid_e = self._place(make_stk_contract(sym), eo)
            if oid_e is None:
                r.error_message = "Failed to place limit entry order"
                return r
            r.entry_order_id = oid_e
            r.entry_submitted = True

            time.sleep(2.0)
            status_err = self._order_status_error(oid_e, sym, "entry")
            if status_err:
                r.error_message = status_err
                self._cancel(oid_e)
                return r

            logger.info(f"  [{sym}] Limit entry placed, waiting up to 5min...")
            ef = self._wait_fill(oid_e, timeout=300.0)
            if ef is None:
                self._cancel(oid_e)
                r.notes = "Limit entry did not fill in 5min — no position opened"
                r.error_message = "Limit entry timeout"
                return r
            r.entry_fill_price = ef
            logger.info(f"  [{sym}] Entry fill @ ${ef:.4f}")

            # Immediate market exit
            oid_x = self._market_exit(sym, "SELL", TEST_QTY)
            if oid_x is None:
                r.error_message = "Failed to place market exit order"
                return r
            r.exit_order_id = oid_x
            r.exit_submitted = True

            xf = self._wait_fill(oid_x, timeout=30.0)
            if xf is None:
                r.error_message = "Market exit did not fill in 30s"
                return r
            r.exit_fill_price = xf
            r.round_trip_complete = True
            r.net_pnl = xf - ef
            r.notes = f"lmt_entry=${lmt_px:.4f} fill=${ef:.4f} → mkt_exit=${xf:.4f}"
            logger.info(f"  [{sym}] Exit fill @ ${xf:.4f} | PnL=${r.net_pnl:.4f}")

        except Exception as e:
            r.error_message = str(e)
            logger.error(f"  [{r.test_name}] {e}", exc_info=True)
        r.duration_seconds = time.time() - start
        return r

    def _test_mkt_trail(self) -> RoundTripResult:
        """
        MKT buy SDS → TRAIL STOP sell (trail = 0.5% of fill price).
        Tests using a trailing stop as the exit mechanism.
        Falls back to MKT sell after 5 minutes.
        """
        sym = "SDS"
        r = RoundTripResult(
            test_name="mkt_trail_roundtrip", order_type="MKT→TRAIL(fb:MKT)", symbol=sym
        )
        start = time.time()
        try:
            price = self._fetch_price(sym)
            if not price:
                r.error_message = f"Could not fetch price for {sym}"
                return r
            logger.info(f"  [{sym}] price=${price:.4f}")

            eo = Order(); eo.action = "BUY"; eo.orderType = "MKT"
            eo.totalQuantity = TEST_QTY
            oid_e = self._place(make_stk_contract(sym), eo)
            if oid_e is None:
                r.error_message = "Failed to place market entry order"
                return r
            r.entry_order_id = oid_e
            r.entry_submitted = True

            ef = self._wait_fill(oid_e, timeout=30.0)
            if ef is None:
                r.error_message = "Market entry did not fill in 30s"
                self._cancel(oid_e)
                return r
            r.entry_fill_price = ef
            logger.info(f"  [{sym}] Entry fill @ ${ef:.4f}")

            # Exit: trailing stop — trail amount is 0.5% of fill price
            trail_amt = round(ef * 0.005, 2)
            xo = Order(); xo.action = "SELL"; xo.orderType = "TRAIL"
            xo.totalQuantity = TEST_QTY
            xo.auxPrice = trail_amt   # dollar trail amount
            oid_x = self._place(make_stk_contract(sym), xo)
            if oid_x is None:
                r.error_message = "Failed to place trailing stop exit"
                return r
            r.exit_order_id = oid_x
            r.exit_submitted = True
            logger.info(f"  [{sym}] TRAIL STOP ${trail_amt:.4f} placed, waiting 5min...")

            xf = self._wait_fill(oid_x, timeout=300.0)
            if xf is None:
                logger.info(f"  [{sym}] Trail timed out — fallback market sell")
                self._cancel(oid_x)
                time.sleep(0.5)
                fb_oid = self._market_exit(sym, "SELL", TEST_QTY)
                if fb_oid:
                    r.exit_order_id = fb_oid
                    xf = self._wait_fill(fb_oid, timeout=30.0)

            if xf is not None:
                r.exit_fill_price = xf
                r.round_trip_complete = True
                r.net_pnl = xf - ef
                r.notes = f"trail=${trail_amt:.4f} entry=${ef:.4f} → exit=${xf:.4f}"
                logger.info(f"  [{sym}] Exit fill @ ${xf:.4f} | PnL=${r.net_pnl:.4f}")
            else:
                r.error_message = "Exit did not fill (trail + fallback both timed out)"

        except Exception as e:
            r.error_message = str(e)
            logger.error(f"  [{r.test_name}] {e}", exc_info=True)
        r.duration_seconds = time.time() - start
        return r

    def _test_mkt_stplmt(self) -> RoundTripResult:
        """
        MKT buy SDOW → STP LMT sell (stop=1% below fill, limit=1.5% below fill).
        Tests a protective stop-limit as the exit mechanism.
        Falls back to MKT sell after 3 minutes.
        """
        sym = "SDOW"
        r = RoundTripResult(
            test_name="mkt_stplmt_roundtrip", order_type="MKT→STP LMT(fb:MKT)", symbol=sym
        )
        start = time.time()
        try:
            price = self._fetch_price(sym)
            if not price:
                r.error_message = f"Could not fetch price for {sym}"
                return r
            logger.info(f"  [{sym}] price=${price:.4f}")

            eo = Order(); eo.action = "BUY"; eo.orderType = "MKT"
            eo.totalQuantity = TEST_QTY
            oid_e = self._place(make_stk_contract(sym), eo)
            if oid_e is None:
                r.error_message = "Failed to place market entry order"
                return r
            r.entry_order_id = oid_e
            r.entry_submitted = True

            ef = self._wait_fill(oid_e, timeout=30.0)
            if ef is None:
                r.error_message = "Market entry did not fill in 30s"
                self._cancel(oid_e)
                return r
            r.entry_fill_price = ef
            logger.info(f"  [{sym}] Entry fill @ ${ef:.4f}")

            # Exit: stop-limit — stop 1% below fill, limit 0.5% below stop
            stop_px = round(ef * 0.990, 2)
            lmt_px  = round(ef * 0.985, 2)
            xo = Order(); xo.action = "SELL"; xo.orderType = "STP LMT"
            xo.totalQuantity = TEST_QTY
            xo.auxPrice = stop_px
            xo.lmtPrice = lmt_px
            oid_x = self._place(make_stk_contract(sym), xo)
            if oid_x is None:
                r.error_message = "Failed to place stop-limit exit"
                return r
            r.exit_order_id = oid_x
            r.exit_submitted = True
            logger.info(
                f"  [{sym}] STP LMT stop=${stop_px:.4f} lmt=${lmt_px:.4f}, waiting 3min..."
            )

            xf = self._wait_fill(oid_x, timeout=180.0)
            if xf is None:
                logger.info(f"  [{sym}] STP LMT timed out — fallback market sell")
                self._cancel(oid_x)
                time.sleep(0.5)
                fb_oid = self._market_exit(sym, "SELL", TEST_QTY)
                if fb_oid:
                    r.exit_order_id = fb_oid
                    xf = self._wait_fill(fb_oid, timeout=30.0)

            if xf is not None:
                r.exit_fill_price = xf
                r.round_trip_complete = True
                r.net_pnl = xf - ef
                r.notes = f"stop=${stop_px:.4f} lmt=${lmt_px:.4f} entry=${ef:.4f} → exit=${xf:.4f}"
                logger.info(f"  [{sym}] Exit fill @ ${xf:.4f} | PnL=${r.net_pnl:.4f}")
            else:
                r.error_message = "Exit did not fill (STP LMT + fallback both timed out)"

        except Exception as e:
            r.error_message = str(e)
            logger.error(f"  [{r.test_name}] {e}", exc_info=True)
        r.duration_seconds = time.time() - start
        return r

    # =========================================================================
    # Test methods — no-position lifecycle
    # =========================================================================

    def _test_ioc_no_fill(self) -> RoundTripResult:
        """
        IOC LMT buy DXD 2% below market → confirm cancel, no position opened.
        A limit 2% below market will never fill within the IOC window; IB
        cancels it instantly.  The test verifies the cancel and that no shares
        were acquired.  If it unexpectedly fills, the position is closed.
        """
        sym = "DXD"
        r = RoundTripResult(
            test_name="ioc_no_fill", order_type="IOC", symbol=sym,
            expected_no_fill=True,
        )
        start = time.time()
        try:
            price = self._fetch_price(sym)
            if not price:
                r.error_message = f"Could not fetch price for {sym}"
                return r

            lmt_px = round(price * _BELOW_200, 2)
            logger.info(f"  [{sym}] price=${price:.4f}, IOC limit @ ${lmt_px:.4f} (−2%)")

            o = Order(); o.action = "BUY"; o.orderType = "LMT"
            o.totalQuantity = TEST_QTY
            o.lmtPrice = lmt_px
            o.tif = "IOC"
            oid = self._place(make_stk_contract(sym), o)
            if oid is None:
                r.error_message = "Failed to place IOC order"
                return r
            r.entry_order_id = oid
            r.entry_submitted = True

            # IOC cancels almost immediately; wait up to 10s
            ef = self._wait_fill(oid, timeout=10.0)
            if ef is None:
                r.no_fill_confirmed = True
                from ib.models import OrderStatus
                rec = self.portfolio.get_order(oid) if self.portfolio else None
                status_str = rec.status.value if rec else "unknown"
                r.notes = f"IOC lmt=${lmt_px:.4f} cancelled as expected (status={status_str})"
                logger.info(f"  [{sym}] IOC cancelled — no position ✓")
            else:
                # Unexpected fill — close the position immediately
                r.entry_fill_price = ef
                r.error_message = (
                    f"IOC filled at ${ef:.4f} (price moved to lmt ${lmt_px:.4f}); closing"
                )
                logger.warning(f"  [{sym}] IOC unexpectedly filled @ ${ef:.4f} — closing")
                fb_oid = self._market_exit(sym, "SELL", TEST_QTY)
                if fb_oid:
                    r.exit_order_id = fb_oid
                    self._wait_fill(fb_oid, timeout=30.0)

        except Exception as e:
            r.error_message = str(e)
            logger.error(f"  [{r.test_name}] {e}", exc_info=True)
        r.duration_seconds = time.time() - start
        return r

    def _test_gtc_lifecycle(self) -> RoundTripResult:
        """
        GTC LMT buy SQQQ 3% below market → verify it is live → explicit cancel.
        Confirms that (a) GTC orders are accepted, (b) they persist after
        placement, and (c) they can be explicitly cancelled before EOD.
        """
        sym = "SQQQ"
        r = RoundTripResult(
            test_name="gtc_lifecycle", order_type="GTC+cancel", symbol=sym,
            expected_no_fill=True,
        )
        start = time.time()
        try:
            price = self._fetch_price(sym)
            if not price:
                r.error_message = f"Could not fetch price for {sym}"
                return r

            lmt_px = round(price * _BELOW_300, 2)
            logger.info(f"  [{sym}] price=${price:.4f}, GTC limit @ ${lmt_px:.4f} (−3%)")

            o = Order(); o.action = "BUY"; o.orderType = "LMT"
            o.totalQuantity = TEST_QTY
            o.lmtPrice = lmt_px
            o.tif = "GTC"
            oid = self._place(make_stk_contract(sym), o)
            if oid is None:
                r.error_message = "Failed to place GTC order"
                return r
            r.entry_order_id = oid
            r.entry_submitted = True

            # Give IB 5s to acknowledge
            time.sleep(5.0)

            rec = self.portfolio.get_order(oid) if self.portfolio else None
            if rec and rec.is_filled:
                r.entry_fill_price = rec.avg_fill_price
                r.error_message = f"GTC filled unexpectedly @ ${rec.avg_fill_price:.4f}"
                logger.warning(f"  [{sym}] GTC filled unexpectedly — closing")
                fb_oid = self._market_exit(sym, "SELL", TEST_QTY)
                if fb_oid:
                    r.exit_order_id = fb_oid
                    self._wait_fill(fb_oid, timeout=30.0)
                return r

            # Cancel the GTC order before it can accidentally fill
            self._cancel(oid)
            time.sleep(2.0)

            rec = self.portfolio.get_order(oid) if self.portfolio else None
            if rec and rec.is_filled:
                r.entry_fill_price = rec.avg_fill_price
                r.error_message = f"GTC filled during cancel window @ ${rec.avg_fill_price:.4f}"
                fb_oid = self._market_exit(sym, "SELL", TEST_QTY)
                if fb_oid:
                    r.exit_order_id = fb_oid
                    self._wait_fill(fb_oid, timeout=30.0)
            else:
                r.no_fill_confirmed = True
                status_str = rec.status.value if rec else "unknown"
                r.notes = (
                    f"GTC lmt=${lmt_px:.4f} submitted then cancelled "
                    f"(final status={status_str})"
                )
                logger.info(f"  [{sym}] GTC lifecycle complete ✓ status={status_str}")

        except Exception as e:
            r.error_message = str(e)
            logger.error(f"  [{r.test_name}] {e}", exc_info=True)
        r.duration_seconds = time.time() - start
        return r

    def _test_fok_market(self) -> RoundTripResult:
        """
        FOK MKT buy SPY 1 share → fills immediately (SPY is highly liquid) or
        is killed; if filled: MKT sell to close.
        Tests Fill-or-Kill TIF: the order must fill completely or be cancelled
        with no partial fills.
        """
        sym = "SPY"
        r = RoundTripResult(test_name="fok_market", order_type="FOK→MKT", symbol=sym)
        start = time.time()
        try:
            price = self._fetch_price(sym)
            if not price:
                r.error_message = f"Could not fetch price for {sym}"
                return r
            logger.info(f"  [{sym}] price=${price:.4f}, FOK market buy")

            o = Order(); o.action = "BUY"; o.orderType = "MKT"
            o.totalQuantity = TEST_QTY
            o.tif = "FOK"
            oid = self._place(make_stk_contract(sym), o)
            if oid is None:
                r.error_message = "Failed to place FOK order"
                return r
            r.entry_order_id = oid
            r.entry_submitted = True

            ef = self._wait_fill(oid, timeout=15.0)
            if ef is None:
                # FOK killed — no position opened; this is a valid outcome
                from ib.models import OrderStatus
                rec = self.portfolio.get_order(oid) if self.portfolio else None
                status_str = rec.status.value if rec else "unknown"
                r.expected_no_fill = True
                r.no_fill_confirmed = True
                r.notes = f"FOK killed (status={status_str}); no position opened"
                logger.info(f"  [{sym}] FOK killed — no position")
            else:
                # Filled — close with market sell
                r.entry_fill_price = ef
                logger.info(f"  [{sym}] FOK filled @ ${ef:.4f}, placing market exit")
                oid_x = self._market_exit(sym, "SELL", TEST_QTY)
                if oid_x is None:
                    r.error_message = "FOK filled but could not place exit order"
                    return r
                r.exit_order_id = oid_x
                r.exit_submitted = True

                xf = self._wait_fill(oid_x, timeout=30.0)
                if xf is None:
                    r.error_message = "Exit MKT order did not fill in 30s"
                    return r
                r.exit_fill_price = xf
                r.round_trip_complete = True
                r.net_pnl = xf - ef
                r.notes = f"FOK filled=${ef:.4f} → exit=${xf:.4f}"
                logger.info(f"  [{sym}] Exit @ ${xf:.4f} | PnL=${r.net_pnl:.4f}")

        except Exception as e:
            r.error_message = str(e)
            logger.error(f"  [{r.test_name}] {e}", exc_info=True)
        r.duration_seconds = time.time() - start
        return r

    # =========================================================================
    # Test methods — order modifiers
    # =========================================================================

    def _test_adaptive_roundtrip(self) -> RoundTripResult:
        """
        Adaptive algo BUY SPXU (urgency=Urgent) → MKT sell when filled.
        IB's Adaptive algo routes through SMART with pacing; Urgent behaves
        close to a market order while still allowing price improvement.
        """
        sym = "SPXU"
        r = RoundTripResult(
            test_name="adaptive_roundtrip", order_type="ADAPT(Urgent)→MKT", symbol=sym
        )
        start = time.time()
        try:
            price = self._fetch_price(sym)
            if not price:
                r.error_message = f"Could not fetch price for {sym}"
                return r
            logger.info(f"  [{sym}] price=${price:.4f}, Adaptive BUY (Urgent)")

            eo = Order()
            eo.action = "BUY"
            eo.orderType = "MKT"
            eo.totalQuantity = TEST_QTY
            eo.algoStrategy = "Adaptive"
            eo.algoParams = [TagValue("adaptivePriority", "Urgent")]
            oid_e = self._place(make_stk_contract(sym), eo)
            if oid_e is None:
                r.error_message = "Failed to place Adaptive order"
                return r
            r.entry_order_id = oid_e
            r.entry_submitted = True

            time.sleep(2.0)
            status_err = self._order_status_error(oid_e, sym, "adaptive entry")
            if status_err:
                r.error_message = status_err
                self._cancel(oid_e)
                return r

            ef = self._wait_fill(oid_e, timeout=60.0)
            if ef is None:
                self._cancel(oid_e)
                r.error_message = "Adaptive order did not fill in 60s"
                return r
            r.entry_fill_price = ef
            logger.info(f"  [{sym}] Adaptive fill @ ${ef:.4f}")

            oid_x = self._market_exit(sym, "SELL", TEST_QTY)
            if oid_x is None:
                r.error_message = "Failed to place market exit order"
                return r
            r.exit_order_id = oid_x
            r.exit_submitted = True

            xf = self._wait_fill(oid_x, timeout=30.0)
            if xf is None:
                r.error_message = "Market exit did not fill in 30s"
                return r
            r.exit_fill_price = xf
            r.round_trip_complete = True
            r.net_pnl = xf - ef
            r.notes = f"Adaptive(Urgent) entry=${ef:.4f} → exit=${xf:.4f}"
            logger.info(f"  [{sym}] Exit @ ${xf:.4f} | PnL=${r.net_pnl:.4f}")

        except Exception as e:
            r.error_message = str(e)
            logger.error(f"  [{r.test_name}] {e}", exc_info=True)
        r.duration_seconds = time.time() - start
        return r

    def _test_iceberg_roundtrip(self) -> RoundTripResult:
        """
        Iceberg LMT buy TQQQ — 3 shares total, displaySize=1 (shows 1 at a time).
        → MKT sell 3 shares when the iceberg fully fills.
        Tests IB's reserve/iceberg order: only `displaySize` shares are shown
        on the order book at any time; refreshed automatically as each tranche fills.
        Uses 0.3% below market so the limit is aggressive enough to fill.
        """
        sym = "TQQQ"
        qty = Decimal("3")
        r = RoundTripResult(
            test_name="iceberg_roundtrip", order_type="ICE LMT→MKT",
            symbol=sym, quantity=3.0,
        )
        start = time.time()
        try:
            price = self._fetch_price(sym)
            if not price:
                r.error_message = f"Could not fetch price for {sym}"
                return r

            lmt_px = round(price * _BELOW_003, 2)
            logger.info(
                f"  [{sym}] price=${price:.4f}, iceberg lmt=${lmt_px:.4f} "
                f"qty=3 displaySize=1"
            )

            eo = Order()
            eo.action = "BUY"
            eo.orderType = "LMT"
            eo.totalQuantity = qty
            eo.lmtPrice = lmt_px
            eo.displaySize = 1   # show 1 share at a time on the book
            oid_e = self._place(make_stk_contract(sym), eo)
            if oid_e is None:
                r.error_message = "Failed to place iceberg order"
                return r
            r.entry_order_id = oid_e
            r.entry_submitted = True

            time.sleep(2.0)
            status_err = self._order_status_error(oid_e, sym, "iceberg entry")
            if status_err:
                r.error_message = status_err
                self._cancel(oid_e)
                return r

            logger.info(f"  [{sym}] Iceberg placed, waiting up to 5min for full 3-share fill...")
            ef = self._wait_fill(oid_e, timeout=300.0)
            if ef is None:
                self._cancel(oid_e)
                r.notes = "Iceberg did not fully fill in 5min — cancelled (no position)"
                r.error_message = "Iceberg entry timeout"
                return r
            r.entry_fill_price = ef
            logger.info(f"  [{sym}] Iceberg fully filled (avg) @ ${ef:.4f}")

            # Exit: sell all 3 shares at market
            oid_x = self._market_exit(sym, "SELL", qty)
            if oid_x is None:
                r.error_message = "Failed to place 3-share market exit"
                return r
            r.exit_order_id = oid_x
            r.exit_submitted = True

            xf = self._wait_fill(oid_x, timeout=30.0)
            if xf is None:
                r.error_message = "3-share market exit did not fill in 30s"
                return r
            r.exit_fill_price = xf
            r.round_trip_complete = True
            r.net_pnl = (xf - ef) * float(qty)
            r.notes = (
                f"3-share iceberg(display=1) lmt=${lmt_px:.4f} "
                f"entry=${ef:.4f} → exit=${xf:.4f}"
            )
            logger.info(f"  [{sym}] Exit @ ${xf:.4f} | PnL=${r.net_pnl:.4f}")

        except Exception as e:
            r.error_message = str(e)
            logger.error(f"  [{r.test_name}] {e}", exc_info=True)
        r.duration_seconds = time.time() - start
        return r

    # -------------------------------------------------------------------------
    # Logging / summary
    # -------------------------------------------------------------------------

    def _log_result(self, r: RoundTripResult):
        if r.error_message:
            logger.warning(
                f"  [{r.test_name}] FAIL  {r.error_message}"
            )
        elif r.expected_no_fill:
            status = "PASS" if r.no_fill_confirmed else "FAIL"
            logger.info(f"  [{r.test_name}] {status}  no-fill confirmed={r.no_fill_confirmed}")
        elif r.round_trip_complete:
            logger.info(
                f"  [{r.test_name}] COMPLETE  "
                f"entry=${r.entry_fill_price:.4f} exit=${r.exit_fill_price:.4f} "
                f"pnl=${r.net_pnl:.4f}"
            )
        else:
            logger.warning(
                f"  [{r.test_name}] PARTIAL  submitted={r.entry_submitted}"
            )

    def _build_summary(self) -> Dict[str, Any]:
        total = len(self._results)
        passed = sum(1 for r in self._results if r.passed)
        round_trips = sum(1 for r in self._results if r.round_trip_complete)
        no_fill_ok = sum(
            1 for r in self._results if r.expected_no_fill and r.no_fill_confirmed
        )
        total_pnl = sum(r.net_pnl for r in self._results)
        errors = [r.test_name for r in self._results if r.error_message]
        return {
            "total": total,
            "passed": passed,
            "round_trips": round_trips,
            "no_fill_confirmed": no_fill_ok,
            "total_pnl": round(total_pnl, 4),
            "errors": errors,
        }
