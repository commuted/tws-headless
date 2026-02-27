"""
paper_test_historical/plugin.py - Historical data API test plugin

Tests get_historical_data() across a range of instruments, bar sizes,
and durations.  Each test case calls the blocking API and validates that:
  - The request completes without timeout
  - At least the minimum number of bars is returned
  - Every bar has valid OHLC values (open > 0, high >= low, close > 0)
  - TRADES bars have positive volume

Test cases
──────────
  stock_daily_spy      SPY  2 W   1 day    TRADES  RTH
  stock_hourly_qqq     QQQ  3 D   1 hour   TRADES  RTH
  stock_5min_aapl      AAPL 1 D   5 mins   TRADES  RTH
  forex_daily_eurusd   EUR  1 W   1 day    MIDPOINT all-hours
  forex_hourly_eurusd  EUR  1 D   1 hour   MIDPOINT all-hours

Run via: plugin request paper_test_historical run_tests
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from ib.contract_builder import ContractBuilder
from plugins.base import PluginBase, TradeSignal

logger = logging.getLogger(__name__)

PAPER_PORTS = (7497, 4002)


# =============================================================================
# Test specification and result types
# =============================================================================

@dataclass
class HistoricalTestCase:
    """One historical data test."""
    name: str
    contract_fn: Callable        # no-arg callable that returns a Contract
    end_date_time: str           # "" = now
    duration_str: str            # e.g. "1 W", "3 D"
    bar_size_setting: str        # e.g. "1 day", "1 hour", "5 mins"
    what_to_show: str            # TRADES, MIDPOINT, etc.
    use_rth: bool
    min_bars: int                # minimum acceptable bar count
    timeout: float = 60.0        # seconds to wait for historicalDataEnd
    notes: str = ""


@dataclass
class HistoricalTestResult:
    """Result of one historical data test."""
    test_name: str
    duration_str: str
    bar_size: str
    what_to_show: str
    passed: bool
    bars_received: int = 0
    min_bars_required: int = 0
    ohlc_valid: bool = True
    volume_valid: bool = True
    first_date: str = ""
    last_date: str = ""
    error_message: str = ""
    notes: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "test_name": self.test_name,
            "duration_str": self.duration_str,
            "bar_size": self.bar_size,
            "what_to_show": self.what_to_show,
            "passed": self.passed,
            "bars_received": self.bars_received,
            "min_bars_required": self.min_bars_required,
            "ohlc_valid": self.ohlc_valid,
            "volume_valid": self.volume_valid,
            "first_date": self.first_date,
            "last_date": self.last_date,
            "error_message": self.error_message,
            "notes": self.notes,
        }


# =============================================================================
# Test case definitions
# =============================================================================

_CASES: List[HistoricalTestCase] = [
    HistoricalTestCase(
        name="stock_daily_spy",
        contract_fn=lambda: ContractBuilder.us_stock("SPY", primary_exchange="ARCA"),
        end_date_time="",
        duration_str="2 W",
        bar_size_setting="1 day",
        what_to_show="TRADES",
        use_rth=True,
        min_bars=5,
        timeout=30.0,
        notes="SPY daily bars, 2 weeks — expect ~10 trading days",
    ),
    HistoricalTestCase(
        name="stock_hourly_qqq",
        contract_fn=lambda: ContractBuilder.us_stock("QQQ", primary_exchange="NASDAQ"),
        end_date_time="",
        duration_str="3 D",
        bar_size_setting="1 hour",
        what_to_show="TRADES",
        use_rth=True,
        min_bars=10,
        timeout=30.0,
        notes="QQQ hourly bars, 3 days — expect ~18 RTH hours",
    ),
    HistoricalTestCase(
        name="stock_5min_aapl",
        contract_fn=lambda: ContractBuilder.us_stock("AAPL", primary_exchange="NASDAQ"),
        end_date_time="",
        duration_str="1 D",
        bar_size_setting="5 mins",
        what_to_show="TRADES",
        use_rth=True,
        min_bars=60,
        timeout=30.0,
        notes="AAPL 5-min bars, 1 day — expect ~78 RTH bars (6.5 h × 12)",
    ),
    HistoricalTestCase(
        name="forex_daily_eurusd",
        contract_fn=lambda: ContractBuilder.forex("EUR", "USD"),
        end_date_time="",
        duration_str="1 W",
        bar_size_setting="1 day",
        what_to_show="MIDPOINT",
        use_rth=False,
        min_bars=5,
        timeout=30.0,
        notes="EUR.USD daily bars, 1 week",
    ),
    HistoricalTestCase(
        name="forex_hourly_eurusd",
        contract_fn=lambda: ContractBuilder.forex("EUR", "USD"),
        end_date_time="",
        duration_str="2 D",
        bar_size_setting="1 hour",
        what_to_show="MIDPOINT",
        use_rth=False,
        min_bars=20,
        timeout=30.0,
        notes="EUR.USD hourly bars, 2 days — expect ~48 bars (24/5 forex market)",
    ),
]


# =============================================================================
# Plugin
# =============================================================================

class PaperTestHistoricalPlugin(PluginBase):
    """
    Paper test: historical data API.

    Calls get_historical_data() for each test case and validates the
    response.  Refuses to run on live accounts.

    Run via: plugin request paper_test_historical run_tests
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
            "paper_test_historical",
            base_path, portfolio, shared_holdings, message_bus,
        )
        self._results: List[HistoricalTestResult] = []
        self._running = False

    @property
    def description(self) -> str:
        return (
            "Paper Test Historical: validates get_historical_data() "
            "across stocks and forex with multiple bar sizes and durations."
        )

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def start(self) -> bool:
        logger.info(f"Starting plugin '{self.name}'")
        saved = self.load_state()
        if saved:
            self._results = [
                HistoricalTestResult(**r) for r in saved.get("results", [])
            ]
        return True

    def stop(self) -> bool:
        logger.info(f"Stopping plugin '{self.name}'")
        self.save_state({"results": [r.to_dict() for r in self._results]})
        return True

    def freeze(self) -> bool:
        self.save_state({"results": [r.to_dict() for r in self._results]})
        return True

    def resume(self) -> bool:
        return True

    def calculate_signals(self, market_data: Dict) -> List[TradeSignal]:
        return []

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
                    "test_count": len(_CASES),
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
                f"SAFETY: account '{accounts[0]}' does not look like a "
                f"paper account (paper accounts start with 'D')."
            )
        return None

    # -------------------------------------------------------------------------
    # Test runner
    # -------------------------------------------------------------------------

    def _run_all_tests(self) -> Dict:
        if self._running:
            return {"success": False, "message": "Tests already running"}

        self._running = True
        self._results = []

        try:
            err = self._verify_paper_connection()
            if err:
                logger.error(f"Paper verification failed: {err}")
                return {"success": False, "message": err}

            for tc in _CASES:
                logger.info(f"--- [{self.name}] {tc.name} ---")
                result = self._run_one(tc)
                self._results.append(result)
                self._log_result(result)

            summary = self._build_summary()
            logger.info(
                f"[{self.name}] Done: "
                f"{summary['passed']}/{summary['total']} passed"
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
            logger.error(f"[{self.name}] Error: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

        finally:
            self._running = False

    def _run_one(self, tc: HistoricalTestCase) -> HistoricalTestResult:
        result = HistoricalTestResult(
            test_name=tc.name,
            duration_str=tc.duration_str,
            bar_size=tc.bar_size_setting,
            what_to_show=tc.what_to_show,
            passed=False,
            min_bars_required=tc.min_bars,
            notes=tc.notes,
        )

        contract = tc.contract_fn()

        logger.info(
            f"  Requesting {tc.bar_size_setting} bars "
            f"({tc.duration_str}, {tc.what_to_show}, rth={tc.use_rth})"
        )

        bars = self.get_historical_data(
            contract=contract,
            end_date_time=tc.end_date_time,
            duration_str=tc.duration_str,
            bar_size_setting=tc.bar_size_setting,
            what_to_show=tc.what_to_show,
            use_rth=tc.use_rth,
            timeout=tc.timeout,
        )

        if bars is None:
            result.error_message = (
                f"Timeout: no response in {tc.timeout}s"
            )
            return result

        result.bars_received = len(bars)

        if not bars:
            result.error_message = "Received 0 bars"
            return result

        result.first_date = str(bars[0].date)
        result.last_date = str(bars[-1].date)

        # Validate OHLC on every bar
        bad_ohlc = []
        for b in bars:
            try:
                o, h, l, c = float(b.open), float(b.high), float(b.low), float(b.close)
                if o <= 0 or h <= 0 or l <= 0 or c <= 0 or h < l:
                    bad_ohlc.append(str(b.date))
            except (TypeError, ValueError) as e:
                bad_ohlc.append(f"{b.date}(err:{e})")

        if bad_ohlc:
            result.ohlc_valid = False
            result.error_message = (
                f"Invalid OHLC on bars: {', '.join(bad_ohlc[:5])}"
            )
            return result

        # Validate volume for TRADES bars
        if tc.what_to_show == "TRADES":
            bad_vol = []
            for b in bars:
                try:
                    if float(b.volume) <= 0:
                        bad_vol.append(str(b.date))
                except (TypeError, ValueError):
                    pass
            if bad_vol:
                result.volume_valid = False
                result.error_message = (
                    f"Zero/missing volume on {len(bad_vol)} bar(s): "
                    f"{', '.join(bad_vol[:5])}"
                )
                return result

        # Check minimum bar count
        if len(bars) < tc.min_bars:
            result.error_message = (
                f"Only {len(bars)} bars returned (need {tc.min_bars})"
            )
            return result

        result.passed = True
        return result

    # -------------------------------------------------------------------------
    # Logging / summary
    # -------------------------------------------------------------------------

    def _log_result(self, r: HistoricalTestResult):
        status = "PASS" if r.passed else "FAIL"
        detail = (
            f"bars={r.bars_received}/{r.min_bars_required} "
            f"dates=[{r.first_date}..{r.last_date}]"
        )
        logger.info(
            f"  [{r.test_name}] {status} {detail}"
            + (f" ERR: {r.error_message}" if r.error_message else "")
        )

    def _build_summary(self) -> Dict[str, Any]:
        total = len(self._results)
        passed = sum(1 for r in self._results if r.passed)
        errors = [r.test_name for r in self._results if not r.passed]
        return {
            "total": total,
            "passed": passed,
            "failed": total - passed,
            "errors": errors,
        }
