"""
paper_test_bar_store/plugin.py — End-to-end paper test for BarStore

Tests BarStore against a live TWS paper account:
  cold_fetch_gld     Cold cache → IB fetched → bars returned + coverage set
  cache_hit_gld      Same range second call → no second IB fetch (call_count stays 1)
  gap_fill_gld       Cache middle day; wider request fills both surrounding gaps without re-fetching cached portion
  gap_in_middle      Cache outer wings; full-range request fetches only the middle hole
  force_refetch_gld  force=True always calls IB even when cached
  coverage_summary   After fetch, coverage_summary() returns correct symbol/interval
  purge_and_refetch  purge() clears cache; next call re-fetches
  multi_symbol       GLD and UUP cached independently, no cross-contamination
  ohlc_valid         All returned BarRecords have valid OHLC + positive volume

Run via: plugin request paper_test_bar_store run_tests
"""

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from ib.bar_store import BarStore, duration_str
from ib.contract_builder import ContractBuilder
from plugins.base import PluginBase, TradeSignal

logger = logging.getLogger(__name__)

PAPER_PORTS = (7497, 4002)
UTC = timezone.utc

BAR_SIZE  = "5 mins"
WHAT      = "TRADES"
USE_RTH   = True


# =============================================================================
# Result type
# =============================================================================

@dataclass
class BarStoreTestResult:
    test_name: str
    passed: bool = False
    error_message: str = ""
    notes: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "test_name":     self.test_name,
            "passed":        self.passed,
            "error_message": self.error_message,
            "notes":         self.notes,
        }


# =============================================================================
# Plugin
# =============================================================================

class PaperTestBarStorePlugin(PluginBase):
    """
    Paper test: BarStore end-to-end against a live TWS paper account.

    Run via: plugin request paper_test_bar_store run_tests
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
            "paper_test_bar_store",
            base_path, portfolio, shared_holdings, message_bus,
        )
        self._results: List[BarStoreTestResult] = []
        self._running = False

    @property
    def description(self) -> str:
        return (
            "Paper Test BarStore: validates SQLite bar cache "
            "including cold fetch, cache hits, gap fill, force refetch, "
            "coverage tracking, purge, multi-symbol isolation, and OHLC validity."
        )

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def start(self) -> bool:
        logger.info(f"Starting plugin '{self.name}'")
        saved = self.load_state()
        if saved:
            self._results = [
                BarStoreTestResult(**r) for r in saved.get("results", [])
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

    def calculate_signals(self) -> List[TradeSignal]:
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
                    "running":       self._running,
                    "result_count":  len(self._results),
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
    # fetch_fn factory
    # -------------------------------------------------------------------------

    def _make_fetch_fn(self, symbol: str, bar_size: str, call_counter: List[int]):
        """Return a fetch_fn that wraps get_historical_data and counts calls."""
        def fetch_fn(start_dt: datetime, end_dt: datetime):
            call_counter[0] += 1
            end_str = end_dt.strftime("%Y%m%d-%H:%M:%S")
            dur = duration_str(start_dt, end_dt)
            return self.get_historical_data(
                contract=ContractBuilder.etf(symbol),
                end_date_time=end_str,
                duration_str=dur,
                bar_size_setting=bar_size,
                what_to_show=WHAT,
                use_rth=USE_RTH,
            ) or []
        return fetch_fn

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

            # Fresh DB for each run
            db_path = self._base_path / "bar_store_test.db"
            if db_path.exists():
                os.remove(db_path)

            store = BarStore(db_path)

            # Anchor: use midnight UTC boundaries so date arithmetic is clean.
            # We go back 4 days to guarantee ≥3 completed trading days of data.
            #
            # Timeline used by gap tests:
            #   d0        d1        d2        d3        d4
            #   |← day A →|← day B →|← day C →|← day D →|
            #
            # gap_fill_gld   : cache day B, request [d0,d4] → gaps A+C+D fetched, B from cache
            # gap_in_middle   : cache days A+C, request [d0,d3] → only day B (hole) fetched
            today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
            d0 = today - timedelta(days=4)
            d1 = today - timedelta(days=3)
            d2 = today - timedelta(days=2)
            d3 = today - timedelta(days=1)
            d4 = today

            # Aliases used by the single-day tests
            day1_start = d2
            day1_end   = d3

            tests = [
                ("cold_fetch_gld",    self._test_cold_fetch,       store, day1_start, day1_end),
                ("cache_hit_gld",     self._test_cache_hit,        store, day1_start, day1_end),
                ("gap_fill_gld",      self._test_gap_fill,         store, d0, d1, d2, d3, d4),
                ("gap_in_middle",     self._test_gap_in_middle,    store, d0, d1, d2, d3),
                ("force_refetch_gld", self._test_force_refetch,    store, day1_start, day1_end),
                ("coverage_summary",  self._test_coverage_summary, store, day1_start, day1_end),
                ("purge_and_refetch", self._test_purge_and_refetch, store, day1_start, day1_end),
                ("multi_symbol",      self._test_multi_symbol,     store, day1_start, day1_end),
                ("ohlc_valid",        self._test_ohlc_valid,       store, day1_start, day1_end),
            ]

            for entry in tests:
                name    = entry[0]
                fn      = entry[1]
                args    = entry[2:]
                logger.info(f"--- [{self.name}] {name} ---")
                result = BarStoreTestResult(test_name=name)
                try:
                    fn(result, *args)
                except Exception as exc:
                    result.passed = False
                    result.error_message = f"Exception: {exc}"
                    logger.error(f"  [{name}] exception", exc_info=True)
                self._results.append(result)
                self._log_result(result)

            summary = self._build_summary()
            logger.info(
                f"[{self.name}] Done: "
                f"{summary['passed']}/{summary['total']} passed"
            )

            self.save_state({"results": [r.to_dict() for r in self._results]})
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

    # -------------------------------------------------------------------------
    # Individual test implementations
    # -------------------------------------------------------------------------

    def _test_cold_fetch(
        self,
        result: BarStoreTestResult,
        store: BarStore,
        start_dt: datetime,
        end_dt: datetime,
    ) -> None:
        counter = [0]
        bars = store.get_bars(
            symbol="GLD", bar_size=BAR_SIZE, what_to_show=WHAT, use_rth=USE_RTH,
            start_dt=start_dt, end_dt=end_dt,
            fetch_fn=self._make_fetch_fn("GLD", BAR_SIZE, counter),
        )
        if counter[0] == 0:
            result.error_message = "fetch_fn was never called (expected ≥1 IB call)"
            return
        if not bars:
            result.error_message = "No bars returned on cold fetch"
            return
        cov = store.coverage_summary(symbol="GLD")
        if not cov:
            result.error_message = "Coverage not recorded after cold fetch"
            return
        result.notes = f"bars={len(bars)}, ib_calls={counter[0]}"
        result.passed = True

    def _test_cache_hit(
        self,
        result: BarStoreTestResult,
        store: BarStore,
        start_dt: datetime,
        end_dt: datetime,
    ) -> None:
        # Ensure range is already cached (cold_fetch ran first with same range)
        # Warm the cache if needed
        warm_counter = [0]
        store.get_bars(
            symbol="GLD", bar_size=BAR_SIZE, what_to_show=WHAT, use_rth=USE_RTH,
            start_dt=start_dt, end_dt=end_dt,
            fetch_fn=self._make_fetch_fn("GLD", BAR_SIZE, warm_counter),
        )

        # Now hit the cache — no new IB call expected
        counter = [0]
        bars = store.get_bars(
            symbol="GLD", bar_size=BAR_SIZE, what_to_show=WHAT, use_rth=USE_RTH,
            start_dt=start_dt, end_dt=end_dt,
            fetch_fn=self._make_fetch_fn("GLD", BAR_SIZE, counter),
        )
        if counter[0] != 0:
            result.error_message = (
                f"Expected 0 IB calls on cache hit, got {counter[0]}"
            )
            return
        if not bars:
            result.error_message = "Cache hit returned 0 bars"
            return
        result.notes = f"bars={len(bars)}, ib_calls={counter[0]}"
        result.passed = True

    def _test_gap_fill(
        self,
        result: BarStoreTestResult,
        store: BarStore,
        d0: datetime,
        d1: datetime,
        d2: datetime,
        d3: datetime,
        d4: datetime,
    ) -> None:
        # Cache day B [d1, d2] only.
        # Then request the full [d0, d4] window.
        # Gaps A=[d0,d1], C=[d2,d3], D=[d3,d4] must be fetched; B must not.
        symbol = "GLD_GAP"

        c_warm = [0]
        warm_bars = store.get_bars(
            symbol=symbol, bar_size=BAR_SIZE, what_to_show=WHAT, use_rth=USE_RTH,
            start_dt=d1, end_dt=d2,
            fetch_fn=self._make_fetch_fn("GLD", BAR_SIZE, c_warm),
        )

        c2 = [0]
        bars = store.get_bars(
            symbol=symbol, bar_size=BAR_SIZE, what_to_show=WHAT, use_rth=USE_RTH,
            start_dt=d0, end_dt=d4,
            fetch_fn=self._make_fetch_fn("GLD", BAR_SIZE, c2),
        )

        # At least 1 IB call required (3 gaps exist)
        if c2[0] == 0:
            result.error_message = "Expected ≥1 IB call for surrounding gaps, got 0"
            return

        # The wider range must return at least as many bars as the warm fetch
        if len(bars) < len(warm_bars):
            result.error_message = (
                f"Wider range returned fewer bars than cached window: "
                f"{len(bars)} < {len(warm_bars)}"
            )
            return

        # The cache was leveraged: gap calls alone should not exceed the number
        # of calls needed to fetch the full 4-day range cold.  Each gap is at
        # most 1 day, so ≤3 gap calls + possible chunking ≤ c_warm*4 is a
        # conservative upper bound.
        upper = max(c_warm[0] * 4, 6)
        if c2[0] > upper:
            result.error_message = (
                f"Suspiciously many IB calls ({c2[0]}) — cache may not have "
                f"been used for the warm window (warm_calls={c_warm[0]})"
            )
            return

        result.notes = (
            f"warm_bars={len(warm_bars)}, total_bars={len(bars)}, "
            f"warm_calls={c_warm[0]}, gap_calls={c2[0]}"
        )
        result.passed = True

    def _test_gap_in_middle(
        self,
        result: BarStoreTestResult,
        store: BarStore,
        d0: datetime,
        d1: datetime,
        d2: datetime,
        d3: datetime,
    ) -> None:
        # Cache the outer wings: day A=[d0,d1] and day C=[d2,d3].
        # Then request the full [d0,d3] window.
        # Only the middle hole B=[d1,d2] should be fetched.
        symbol = "GLD_MID"

        c_left = [0]
        left_bars = store.get_bars(
            symbol=symbol, bar_size=BAR_SIZE, what_to_show=WHAT, use_rth=USE_RTH,
            start_dt=d0, end_dt=d1,
            fetch_fn=self._make_fetch_fn("GLD", BAR_SIZE, c_left),
        )
        c_right = [0]
        right_bars = store.get_bars(
            symbol=symbol, bar_size=BAR_SIZE, what_to_show=WHAT, use_rth=USE_RTH,
            start_dt=d2, end_dt=d3,
            fetch_fn=self._make_fetch_fn("GLD", BAR_SIZE, c_right),
        )

        # Full-range request — only the hole B should trigger an IB call
        c_full = [0]
        full_bars = store.get_bars(
            symbol=symbol, bar_size=BAR_SIZE, what_to_show=WHAT, use_rth=USE_RTH,
            start_dt=d0, end_dt=d3,
            fetch_fn=self._make_fetch_fn("GLD", BAR_SIZE, c_full),
        )

        if c_full[0] == 0:
            result.error_message = (
                "Expected ≥1 IB call for the middle hole, got 0"
            )
            return

        # The full range must include at least the bars from the two wings
        min_expected = len(left_bars) + len(right_bars)
        if len(full_bars) < min_expected:
            result.error_message = (
                f"Full range returned {len(full_bars)} bars, expected ≥{min_expected} "
                f"(left={len(left_bars)}, right={len(right_bars)})"
            )
            return

        # The hole is one day wide; gap call count should be small
        upper = max(c_left[0], c_right[0], 1) * 2
        if c_full[0] > upper:
            result.error_message = (
                f"Too many IB calls for middle hole ({c_full[0]}); "
                f"wings may have been re-fetched "
                f"(left_calls={c_left[0]}, right_calls={c_right[0]})"
            )
            return

        result.notes = (
            f"left={len(left_bars)}, right={len(right_bars)}, "
            f"full={len(full_bars)}, hole_calls={c_full[0]}"
        )
        result.passed = True

    def _test_force_refetch(
        self,
        result: BarStoreTestResult,
        store: BarStore,
        start_dt: datetime,
        end_dt: datetime,
    ) -> None:
        symbol = "GLD_FORCE"

        # Warm the cache
        c1 = [0]
        store.get_bars(
            symbol=symbol, bar_size=BAR_SIZE, what_to_show=WHAT, use_rth=USE_RTH,
            start_dt=start_dt, end_dt=end_dt,
            fetch_fn=self._make_fetch_fn("GLD", BAR_SIZE, c1),
        )

        # Force refetch — must call IB again even though cached
        c2 = [0]
        bars = store.get_bars(
            symbol=symbol, bar_size=BAR_SIZE, what_to_show=WHAT, use_rth=USE_RTH,
            start_dt=start_dt, end_dt=end_dt,
            fetch_fn=self._make_fetch_fn("GLD", BAR_SIZE, c2),
            force=True,
        )
        if c2[0] == 0:
            result.error_message = "force=True did not trigger any IB call"
            return
        if not bars:
            result.error_message = "force refetch returned 0 bars"
            return
        result.notes = f"bars={len(bars)}, force_calls={c2[0]}"
        result.passed = True

    def _test_coverage_summary(
        self,
        result: BarStoreTestResult,
        store: BarStore,
        start_dt: datetime,
        end_dt: datetime,
    ) -> None:
        symbol = "GLD_COV"
        c = [0]
        store.get_bars(
            symbol=symbol, bar_size=BAR_SIZE, what_to_show=WHAT, use_rth=USE_RTH,
            start_dt=start_dt, end_dt=end_dt,
            fetch_fn=self._make_fetch_fn("GLD", BAR_SIZE, c),
        )

        summary = store.coverage_summary(symbol=symbol)
        if not summary:
            result.error_message = "coverage_summary() returned empty after fetch"
            return
        entry = summary[0]
        if entry["symbol"] != symbol:
            result.error_message = (
                f"coverage_summary symbol mismatch: {entry['symbol']!r}"
            )
            return
        if entry["bar_size"] != BAR_SIZE:
            result.error_message = (
                f"coverage_summary bar_size mismatch: {entry['bar_size']!r}"
            )
            return
        if not entry["intervals"]:
            result.error_message = "coverage_summary has no intervals"
            return
        if entry["total_bars"] == 0:
            result.error_message = "coverage_summary reports 0 total_bars"
            return
        result.notes = (
            f"intervals={len(entry['intervals'])}, total_bars={entry['total_bars']}"
        )
        result.passed = True

    def _test_purge_and_refetch(
        self,
        result: BarStoreTestResult,
        store: BarStore,
        start_dt: datetime,
        end_dt: datetime,
    ) -> None:
        symbol = "GLD_PURGE"

        # Initial fetch
        c1 = [0]
        store.get_bars(
            symbol=symbol, bar_size=BAR_SIZE, what_to_show=WHAT, use_rth=USE_RTH,
            start_dt=start_dt, end_dt=end_dt,
            fetch_fn=self._make_fetch_fn("GLD", BAR_SIZE, c1),
        )

        # Purge
        deleted = store.purge(
            symbol=symbol, bar_size=BAR_SIZE,
            what_to_show=WHAT, use_rth=USE_RTH,
        )
        if deleted == 0:
            result.error_message = "purge() deleted 0 rows — was anything cached?"
            return

        # Coverage should now be empty
        cov = store.coverage_summary(symbol=symbol)
        if cov:
            result.error_message = (
                f"coverage_summary non-empty after purge: {cov}"
            )
            return

        # Re-fetch — should call IB again
        c2 = [0]
        bars = store.get_bars(
            symbol=symbol, bar_size=BAR_SIZE, what_to_show=WHAT, use_rth=USE_RTH,
            start_dt=start_dt, end_dt=end_dt,
            fetch_fn=self._make_fetch_fn("GLD", BAR_SIZE, c2),
        )
        if c2[0] == 0:
            result.error_message = "After purge, refetch did not call IB"
            return
        if not bars:
            result.error_message = "After purge+refetch, 0 bars returned"
            return
        result.notes = f"deleted={deleted}, re-fetched bars={len(bars)}"
        result.passed = True

    def _test_multi_symbol(
        self,
        result: BarStoreTestResult,
        store: BarStore,
        start_dt: datetime,
        end_dt: datetime,
    ) -> None:
        # Fetch GLD and UUP into the same store
        c_gld = [0]
        gld_bars = store.get_bars(
            symbol="GLD_MS", bar_size=BAR_SIZE, what_to_show=WHAT, use_rth=USE_RTH,
            start_dt=start_dt, end_dt=end_dt,
            fetch_fn=self._make_fetch_fn("GLD", BAR_SIZE, c_gld),
        )

        c_uup = [0]
        uup_bars = store.get_bars(
            symbol="UUP_MS", bar_size=BAR_SIZE, what_to_show=WHAT, use_rth=USE_RTH,
            start_dt=start_dt, end_dt=end_dt,
            fetch_fn=self._make_fetch_fn("UUP", BAR_SIZE, c_uup),
        )

        if not gld_bars:
            result.error_message = "GLD returned 0 bars in multi_symbol test"
            return
        if not uup_bars:
            result.error_message = "UUP returned 0 bars in multi_symbol test"
            return

        # Verify coverage stored separately
        gld_cov = store.coverage_summary(symbol="GLD_MS")
        uup_cov = store.coverage_summary(symbol="UUP_MS")
        if not gld_cov:
            result.error_message = "No coverage for GLD_MS"
            return
        if not uup_cov:
            result.error_message = "No coverage for UUP_MS"
            return

        # No cross-contamination: UUP cache hit doesn't require GLD calls
        c_uup2 = [0]
        uup_bars2 = store.get_bars(
            symbol="UUP_MS", bar_size=BAR_SIZE, what_to_show=WHAT, use_rth=USE_RTH,
            start_dt=start_dt, end_dt=end_dt,
            fetch_fn=self._make_fetch_fn("UUP", BAR_SIZE, c_uup2),
        )
        if c_uup2[0] != 0:
            result.error_message = (
                f"UUP second call made {c_uup2[0]} IB calls (expected 0 — should be cached)"
            )
            return
        if len(uup_bars2) != len(uup_bars):
            result.error_message = (
                f"UUP bar count changed between calls: {len(uup_bars)} → {len(uup_bars2)}"
            )
            return

        result.notes = (
            f"gld_bars={len(gld_bars)}, uup_bars={len(uup_bars)}"
        )
        result.passed = True

    def _test_ohlc_valid(
        self,
        result: BarStoreTestResult,
        store: BarStore,
        start_dt: datetime,
        end_dt: datetime,
    ) -> None:
        symbol = "GLD_OHLC"
        c = [0]
        bars = store.get_bars(
            symbol=symbol, bar_size=BAR_SIZE, what_to_show=WHAT, use_rth=USE_RTH,
            start_dt=start_dt, end_dt=end_dt,
            fetch_fn=self._make_fetch_fn("GLD", BAR_SIZE, c),
        )

        if not bars:
            result.error_message = "No bars returned for OHLC validation"
            return

        bad_ohlc = []
        bad_vol  = []
        for b in bars:
            try:
                o, h, l, cl = float(b.open), float(b.high), float(b.low), float(b.close)
                if o <= 0 or h <= 0 or l <= 0 or cl <= 0 or h < l:
                    bad_ohlc.append(str(b.date))
            except (TypeError, ValueError) as e:
                bad_ohlc.append(f"{b.date}(err:{e})")
            try:
                if float(b.volume) <= 0:
                    bad_vol.append(str(b.date))
            except (TypeError, ValueError):
                pass

        if bad_ohlc:
            result.error_message = (
                f"Invalid OHLC on {len(bad_ohlc)} bar(s): "
                f"{', '.join(bad_ohlc[:5])}"
            )
            return
        if bad_vol:
            result.error_message = (
                f"Zero/missing volume on {len(bad_vol)} bar(s): "
                f"{', '.join(bad_vol[:5])}"
            )
            return

        result.notes = f"bars={len(bars)}, all OHLC valid"
        result.passed = True

    # -------------------------------------------------------------------------
    # Logging / summary
    # -------------------------------------------------------------------------

    def _log_result(self, r: BarStoreTestResult) -> None:
        status = "PASS" if r.passed else "FAIL"
        msg = f"  [{r.test_name}] {status}"
        if r.notes:
            msg += f" {r.notes}"
        if r.error_message:
            msg += f" ERR: {r.error_message}"
        logger.info(msg)

    def _build_summary(self) -> Dict[str, Any]:
        total  = len(self._results)
        passed = sum(1 for r in self._results if r.passed)
        errors = [r.test_name for r in self._results if not r.passed]
        return {
            "total":  total,
            "passed": passed,
            "failed": total - passed,
            "errors": errors,
        }
