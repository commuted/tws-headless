"""
plugins/market_hours_probe/plugin.py — drive ib/market_calendar.py against
the RUNNING engine's IB connection.

This is a test harness, not a strategy. It exists because market_calendar is
deliberately not integrated: its fetch helpers need a live, connected
Portfolio, and the only sanctioned way to reach one without opening a second
IB connection (and competing for a clientId) is to ride the engine's.

It places no orders, subscribes to no data, publishes nothing, and returns
HOLD-free empty signal lists. Every request below is a read: reqContractDetails
or a whatToShow=SCHEDULE historical request. Loaded via `plugin load` it
registers in MANUAL execution mode, so the executive never invokes it on its
own — it only ever runs when someone sends it a request.

    ./ibctl.py plugin load plugins/market_hours_probe
    ./ibctl.py plugin request market_hours_probe hours '{"symbol": "SPY"}'
    ./ibctl.py plugin request market_hours_probe raw '{"symbol": "GLD"}'
    ./ibctl.py plugin request market_hours_probe gap \
        '{"symbol": "SPY", "start": "2026-09-04T15:55", "end": "2026-09-08T09:35"}'
    ./ibctl.py plugin request market_hours_probe schedule '{"symbol": "SPY", "days": 30}'
    ./ibctl.py plugin unload market_hours_probe
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from ib.contract_builder import ContractBuilder

# `plugin load` evicts only the plugin package from sys.modules, so a cached
# ib.market_calendar would keep serving the version the engine started with.
# Reload it here so each load of this probe tests the code currently on disk —
# the whole point of a probe against a long-running engine.
import importlib
import ib.market_calendar as _mc
_mc = importlib.reload(_mc)

MarketCalendar = _mc.MarketCalendar
fetch_historical_schedule = _mc.fetch_historical_schedule
fetch_market_calendar = _mc.fetch_market_calendar

from ..base import PluginBase, TradeSignal

logger = logging.getLogger(__name__)


class MarketHoursProbe(PluginBase):
    """Exercises market_calendar against real contracts. Read-only."""

    VERSION = "1.0.0"

    def __init__(
        self,
        base_path: Optional[Path] = None,
        portfolio=None,
        shared_holdings=None,
        message_bus=None,
    ):
        super().__init__(
            "market_hours_probe",
            base_path,
            portfolio,
            shared_holdings,
            message_bus,
        )
        self._last: Dict[str, Any] = {}

    @property
    def description(self) -> str:
        return (
            "Market-hours probe: fetches trading/liquid hours and session "
            "schedules from IB and evaluates them with ib/market_calendar.py. "
            "Read-only; places no orders."
        )

    # -- lifecycle (nothing to do; there is no state worth keeping) --------

    def start(self) -> bool:
        return True

    def stop(self) -> bool:
        return True

    def freeze(self) -> bool:
        return True

    def resume(self) -> bool:
        return True

    def calculate_signals(self) -> List[TradeSignal]:
        return []

    def cli_help(self) -> str:
        return (
            "market_hours_probe commands:\n"
            "  plugin request market_hours_probe hours    "
            "{\"symbol\": \"SPY\", \"type\": \"etf\", \"at\": \"ISO\", \"rth\": true}\n"
            "  plugin request market_hours_probe raw      {\"symbol\": \"SPY\"}\n"
            "  plugin request market_hours_probe gap      "
            "{\"symbol\": \"SPY\", \"start\": \"ISO\", \"end\": \"ISO\"}\n"
            "  plugin request market_hours_probe schedule "
            "{\"symbol\": \"SPY\", \"days\": 30}\n"
        )

    # -- helpers -----------------------------------------------------------

    def _contract(self, payload: Dict):
        symbol = (payload.get("symbol") or "SPY").upper()
        kind = (payload.get("type") or "etf").lower()
        if kind == "stock":
            return ContractBuilder.us_stock(symbol)
        if kind == "forex":
            # "EURUSD" or {"symbol": "EUR", "quote": "USD"}. (Note: the
            # engine's own `historical fetch --type forex` calls
            # ContractBuilder.forex(symbol) with one argument and would
            # TypeError here — unrelated to this module, but real.)
            quote = payload.get("quote")
            if not quote and len(symbol) == 6:
                symbol, quote = symbol[:3], symbol[3:]
            return ContractBuilder.forex(symbol, quote or "USD")
        return ContractBuilder.etf(symbol)

    def _calendar(self, payload: Dict) -> Optional[MarketCalendar]:
        """Contract details, optionally extended backwards with SCHEDULE.

        Pass {"history": 30} to fold a 30-day session schedule in — needed
        for any question about a window that starts before today, since
        contract details only run forward.
        """
        contract = self._contract(payload)
        timeout = float(payload.get("timeout", 20.0))
        cal = fetch_market_calendar(self.portfolio, contract, timeout=timeout)
        if cal is None:
            return None
        days = int(payload.get("history", 0))
        if days > 0:
            rth = bool(payload.get("rth", True))
            past = fetch_historical_schedule(
                self.portfolio, contract, num_days=days, use_rth=rth,
                timeout=timeout,
            )
            if past is not None:
                cal = cal.with_history(past, rth=rth)
            else:
                logger.warning("market_hours_probe: no schedule; forward-only")
        return cal

    @staticmethod
    def _when(payload: Dict, key: str) -> Optional[datetime]:
        raw = payload.get(key)
        return datetime.fromisoformat(raw) if raw else None

    # -- requests ----------------------------------------------------------

    def handle_request(self, request_type: str, payload: Dict) -> Dict:
        try:
            if request_type == "hours":
                return self._req_hours(payload)
            if request_type == "raw":
                return self._req_raw(payload)
            if request_type == "gap":
                return self._req_gap(payload)
            if request_type == "schedule":
                return self._req_schedule(payload)
            if request_type == "last":
                return {"success": True, "data": self._last}
        except Exception as exc:
            logger.exception("market_hours_probe: request failed")
            return {"success": False, "message": f"{type(exc).__name__}: {exc}"}
        return {"success": False, "message": f"Unknown request: {request_type}"}

    def _req_hours(self, payload: Dict) -> Dict:
        cal = self._calendar(payload)
        if cal is None:
            return {"success": False, "message": "No contract details returned"}
        rth = bool(payload.get("rth", True))
        report = cal.describe(self._when(payload, "at"), rth=rth)
        sessions = cal.sessions(rth)
        report["days"] = {
            d.isoformat(): [
                f"{s.start:%H:%M}-{s.end:%H:%M}" for s in sessions.sessions_on(d)
            ]
            for d in sorted(sessions.days)
        }
        report["closed_days"] = [d.isoformat() for d in sorted(sessions.closed_days)]
        report["half_days"] = [
            d.isoformat() for d in sorted(sessions.days) if sessions.is_half_day(d)
        ]
        self._last = report
        return {
            "success": True,
            "message": (
                f"{report['symbol']}@{report['exchange']} is {report['state'].upper()} "
                f"({'RTH' if rth else 'extended'}) as of {report['as_of']}"
            ),
            "data": report,
        }

    def _req_raw(self, payload: Dict) -> Dict:
        """The unparsed strings, to see what this TWS build actually sends."""
        import threading

        done = threading.Event()
        box: Dict[str, Any] = {}

        def on_end(details):
            box["details"] = details
            done.set()

        self.portfolio.request_contract_details(
            contract=self._contract(payload), on_end=on_end
        )
        if not done.wait(timeout=float(payload.get("timeout", 20.0))):
            return {"success": False, "message": "Timeout waiting for contract details"}

        details = box.get("details") or []
        if not details:
            return {"success": False, "message": "No contract details returned"}
        cd = details[0]
        data = {
            "matches": len(details),
            "symbol": getattr(cd.contract, "symbol", ""),
            "exchange": getattr(cd.contract, "exchange", ""),
            "con_id": getattr(cd.contract, "conId", 0),
            "timeZoneId": getattr(cd, "timeZoneId", ""),
            "tradingHours": getattr(cd, "tradingHours", ""),
            "liquidHours": getattr(cd, "liquidHours", ""),
        }
        self._last = data
        return {"success": True, "message": "Raw contract hours", "data": data}

    def _req_gap(self, payload: Dict) -> Dict:
        cal = self._calendar(payload)
        if cal is None:
            return {"success": False, "message": "No contract details returned"}
        start = self._when(payload, "start")
        end = self._when(payload, "end")
        if not start or not end:
            return {"success": False, "message": "gap needs 'start' and 'end' (ISO)"}
        report = cal.classify_gap(start, end, rth=bool(payload.get("rth", True)))
        self._last = report
        return {
            "success": True,
            "message": f"{report['verdict'].upper()}: {report['explanation']}",
            "data": report,
        }

    def _req_schedule(self, payload: Dict) -> Dict:
        sessions = fetch_historical_schedule(
            self.portfolio,
            self._contract(payload),
            num_days=int(payload.get("days", 30)),
            use_rth=bool(payload.get("rth", True)),
            timeout=float(payload.get("timeout", 30.0)),
        )
        if sessions is None:
            return {"success": False, "message": "No historicalSchedule response"}
        rng = sessions.covered_range()
        data = {
            "time_zone_id": sessions.time_zone_id,
            "covered_from": rng[0].isoformat() if rng else None,
            "covered_to": rng[1].isoformat() if rng else None,
            "trading_days": len(sessions.days),
            "closed_days": [d.isoformat() for d in sorted(sessions.closed_days)],
            "half_days": [
                d.isoformat() for d in sorted(sessions.days) if sessions.is_half_day(d)
            ],
            "days": {
                d.isoformat(): [
                    f"{s.start:%H:%M}-{s.end:%H:%M}" for s in sessions.sessions_on(d)
                ]
                for d in sorted(sessions.days)
            },
        }
        self._last = data
        return {
            "success": True,
            "message": (
                f"{data['trading_days']} trading days, "
                f"{len(data['closed_days'])} closed, {len(data['half_days'])} short"
            ),
            "data": data,
        }
