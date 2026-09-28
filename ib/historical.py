"""
historical.py — the one path between IB historical data and the BarStore.

Every caller that fetches, caches or records bars goes through here:

    PluginBase.get_historical_data   fetch_bars          (no cache)
    PluginBase.get_bars_cached       cached_bars         read, fill gaps, write
    PluginBase.subscribe_live_bars   record_bar          write one live bar
    engine `historical fetch`        cached_bars         what ibctl and the
                                                         recorder now call

Before this module there were two cache paths that shared BarStore.get_bars
but differed around it, and the differences lost data:

  * The plugin path turned a TIMEOUT into [] (`get_historical_data(...) or
    []`). BarStore reads [] as "IB was reached and has no bars here" and
    records coverage, so one slow reply left a permanent hole.
  * Both paths turned an IB REFUSAL into []: portfolio.error answered 162 /
    321 / 354 by ending the request with no bars, and 162 is also IB's code
    for a pacing violation. Same hole.
  * ibctl ran its own BarStore in its own process, keyed on the pair name
    and fed ISO dates the parser rejected; plugins keyed a forex series on
    the base currency alone ("USD" for USD.JPY, colliding with every USD
    pair) and wrote epoch dates. One series, two writers, two formats.

Now: fetch_bars RAISES HistoricalDataError on a timeout or a refusal and
returns [] only when IB said "no data" (portfolio.historical_error_is_no_data).
BarStore leaves a raised chunk uncovered and retries it next call. The series
key comes from series_symbol() for every writer, the engine is the only
process that writes fetched bars, and every writer uses the same shared
BarStore instance (bar_store.shared_store) for the same database
(bar_store.resolve_db_path).
"""

import logging
import threading
from datetime import datetime
from typing import List, Optional

from .bar_store import BarRecord, duration_str

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 60.0


class HistoricalDataError(RuntimeError):
    """A historical request that FAILED — as opposed to one IB answered
    with no bars. `code` is IB's error code, or None for a timeout."""

    def __init__(self, code: Optional[int], message: str):
        super().__init__(f"[{code}] {message}" if code is not None else message)
        self.code = code
        self.message = message


def series_symbol(contract) -> str:
    """
    The BarStore series name for a contract: the symbol, or BASE.QUOTE for a
    CASH pair. A pair's contract.symbol is only its base currency, so keying
    on it put USD.JPY, USD.CAD and USD.CHF into one series called "USD".
    """
    if getattr(contract, "secType", "") == "CASH":
        return f"{contract.symbol}.{contract.currency}"
    return contract.symbol


def fetch_bars(portfolio, contract, end_date_time: str, duration: str,
               bar_size: str, what_to_show: str, use_rth: bool,
               timeout: float = DEFAULT_TIMEOUT) -> list:
    """
    One IB historical request, blocking. Returns the bars (possibly []) when
    IB answered; raises HistoricalDataError when it timed out or IB refused.
    """
    done = threading.Event()
    result: dict = {}

    def on_end(bars, start, end):
        result["bars"] = bars
        done.set()

    def on_error(code, message):
        result["error"] = (code, message)
        done.set()

    req_id = portfolio.request_historical_data(
        contract=contract,
        end_date_time=end_date_time,
        duration_str=duration,
        bar_size_setting=bar_size,
        what_to_show=what_to_show,
        use_rth=use_rth,
        on_end=on_end,
        on_error=on_error,
    )
    if not done.wait(timeout):
        portfolio.cancel_historical_data(req_id)
        raise HistoricalDataError(None, f"no reply within {timeout:g}s (req_id={req_id})")
    if "error" in result:
        raise HistoricalDataError(*result["error"])
    return result.get("bars") or []


def cached_bars(portfolio, store, contract, start_dt: datetime, end_dt: datetime,
                bar_size: str, what_to_show: str, use_rth: bool,
                force: bool = False, timeout: Optional[float] = None,
                stats: Optional[dict] = None) -> List[BarRecord]:
    """
    Bars for [start_dt, end_dt] through the cache: read what is held, fetch
    only the gaps (in IB-sized chunks), write them, and record coverage only
    for chunks IB actually answered.
    """
    wait = timeout or DEFAULT_TIMEOUT

    def fetch_fn(s, e):
        if portfolio is None:
            raise HistoricalDataError(None, "no IB connection")
        return fetch_bars(portfolio, contract, e.strftime("%Y%m%d-%H:%M:%S"),
                          duration_str(s, e), bar_size, what_to_show, use_rth,
                          wait)

    return store.get_bars(
        symbol=series_symbol(contract),
        bar_size=bar_size,
        what_to_show=what_to_show,
        use_rth=use_rth,
        start_dt=start_dt,
        end_dt=end_dt,
        fetch_fn=fetch_fn,
        force=force,
        stats=stats,
    )


def record_bar(store, contract, bar_size: str, what_to_show: str,
               use_rth: bool, bar) -> None:
    """Write one live bar under the same series key cached_bars uses."""
    store.insert_bar(series_symbol(contract), bar_size, what_to_show, use_rth, bar)
