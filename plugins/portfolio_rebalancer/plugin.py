"""
plugins/portfolio_rebalancer/plugin.py

Portfolio rebalancer plugin.  Like any other plugin it operates only on the
capital explicitly transferred to it (via `transfer cash` / `transfer position`).
It reads positions and total value from self._holdings, not from the full
account portfolio.

Target allocations are taken from instruments.json weight fields.  Weights
must sum to ~100%.  An instrument with weight=0 that is currently held is
treated as "sell everything" when manage_untracked=True.

Modes
-----
  manual     – rebalance only when triggered via message bus or handle_request
  threshold  – autonomous: rebalance when any position drifts beyond
               drift_threshold_pct from its target weight
  calendar   – autonomous: rebalance on a fixed schedule (daily/weekly/monthly)
               regardless of drift; each run rebalances to exact target weights
  combined   – autonomous: fire on drift OR schedule, whichever comes first

Message bus
-----------
Subscribe:  portfolio_rebalancer_cmd
  {"command": "rebalance"}
  {"command": "rebalance", "dry_run": false}
  {"command": "preview"}
  {"command": "set_mode", "mode": "threshold"}
  {"command": "set_parameter", "key": "drift_threshold_pct", "value": 3.0}

Publish:  portfolio_rebalancer_result
  {"trades": [...], "total_value": 123456, "dry_run": true, ...}

CLI
---
  plugin request portfolio_rebalancer preview
  plugin request portfolio_rebalancer rebalance
  plugin request portfolio_rebalancer rebalance '{"dry_run": false}'
  plugin request portfolio_rebalancer get_status
  plugin request portfolio_rebalancer get_targets
  plugin request portfolio_rebalancer get_parameters
  plugin request portfolio_rebalancer set_parameter '{"key":"mode","value":"threshold"}'
"""

import logging
import threading
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any

from ..base import PluginBase, TradeSignal

logger = logging.getLogger(__name__)


class PortfolioRebalancerPlugin(PluginBase):
    """
    Portfolio rebalancer plugin.

    Operates only on capital funded to this plugin via `transfer cash` and
    `transfer position`.  All position reads and value calculations use
    self._holdings — the plugin's own tracked holdings — not the full account.

    Three rebalancing strategies:
      threshold – trade only positions drifting beyond drift_threshold_pct
      calendar  – trade all positions to exact targets on a fixed schedule
      combined  – either trigger fires

    Manual mode disables the autonomous background thread; rebalances are
    triggered exclusively via handle_request or message bus commands.
    """

    VERSION = "1.0.0"
    INSTRUMENT_COMPLIANCE = True    # only trades symbols registered in instruments.json

    MODES     = frozenset(("manual", "threshold", "calendar", "combined"))
    SCHEDULES = frozenset(("daily", "weekly", "monthly"))

    def __init__(
        self,
        base_path: Optional[Path] = None,
        portfolio=None,
        shared_holdings=None,
        message_bus=None,
        mode: str = "threshold",
        drift_threshold_pct: float = 5.0,
        min_trade_value: float = 100.0,
        min_trade_shares: int = 1,
        cash_buffer_pct: float = 2.0,
        max_trades_per_run: int = 20,
        dry_run: bool = True,
        check_interval_secs: int = 300,
        calendar_schedule: str = "daily",
        manage_untracked: bool = False,
    ):
        super().__init__(
            "portfolio_rebalancer",
            base_path,
            portfolio,
            shared_holdings,
            message_bus,
        )

        # --- Parameters (all settable at runtime) ---
        self.mode                 = mode
        self.drift_threshold_pct  = drift_threshold_pct
        self.min_trade_value      = min_trade_value
        self.min_trade_shares     = min_trade_shares
        self.cash_buffer_pct      = cash_buffer_pct
        self.max_trades_per_run   = max_trades_per_run
        self.dry_run              = dry_run
        self.check_interval_secs  = check_interval_secs
        self.calendar_schedule    = calendar_schedule
        # When True, positions not listed in instruments.json are sold down to zero.
        self.manage_untracked     = manage_untracked

        # --- Runtime state ---
        self._price_cache: Dict[str, float]     = {}   # symbol -> latest bar close
        self._live_bar_req_ids: Dict[str, int]  = {}   # symbol -> req_id
        self._last_check_time: Optional[datetime]     = None
        self._last_calendar_date: Optional[date]      = None
        self._last_rebalance_time: Optional[datetime] = None
        self._rebalance_count:  int = 0
        self._fill_count:       int = 0
        self._last_trades:      List[Dict] = []
        self._pending_order_actions: Dict[int, str] = {}   # order_id -> symbol

        # --- Autonomous background thread ---
        self._stop_event   = threading.Event()
        self._check_thread: Optional[threading.Thread] = None

    # =========================================================================
    # PluginBase interface
    # =========================================================================

    @property
    def description(self) -> str:
        return (
            f"Portfolio Rebalancer — mode={self.mode}, "
            f"threshold={self.drift_threshold_pct}%, "
            f"schedule={self.calendar_schedule}, dry_run={self.dry_run}"
        )

    def calculate_signals(self) -> List[TradeSignal]:
        # Orders are placed directly via portfolio.place_order(); signals unused.
        return []

    # =========================================================================
    # Lifecycle
    # =========================================================================

    def start(self) -> bool:
        logger.info(f"Starting '{self.name}' (mode={self.mode}, dry_run={self.dry_run})")

        saved = self.load_state()
        if saved:
            self.mode                = saved.get("mode", self.mode)
            self.drift_threshold_pct = saved.get("drift_threshold_pct", self.drift_threshold_pct)
            self.min_trade_value     = saved.get("min_trade_value", self.min_trade_value)
            self.min_trade_shares    = saved.get("min_trade_shares", self.min_trade_shares)
            self.cash_buffer_pct     = saved.get("cash_buffer_pct", self.cash_buffer_pct)
            self.max_trades_per_run  = saved.get("max_trades_per_run", self.max_trades_per_run)
            self.dry_run             = saved.get("dry_run", self.dry_run)
            self.check_interval_secs = saved.get("check_interval_secs", self.check_interval_secs)
            self.calendar_schedule   = saved.get("calendar_schedule", self.calendar_schedule)
            self.manage_untracked    = saved.get("manage_untracked", self.manage_untracked)
            self._rebalance_count    = saved.get("rebalance_count", 0)
            self._fill_count         = saved.get("fill_count", 0)
            raw_date = saved.get("last_calendar_date")
            if raw_date:
                self._last_calendar_date = date.fromisoformat(raw_date)

        self._start_subscriptions()
        self.subscribe("portfolio_rebalancer_cmd", self._on_bus_command)

        self._stop_event.clear()
        if self.mode != "manual":
            self._check_thread = threading.Thread(
                target=self._autonomous_loop,
                daemon=True,
                name="rebalancer-check",
            )
            self._check_thread.start()

        return True

    def stop(self) -> bool:
        logger.info(f"Stopping '{self.name}'")
        self._stop_event.set()
        if self._check_thread and self._check_thread.is_alive():
            self._check_thread.join(timeout=5.0)
        self._cancel_subscriptions()
        self._save_full_state()
        self.unsubscribe_all()
        return True

    def freeze(self) -> bool:
        logger.info(f"Freezing '{self.name}'")
        self._stop_event.set()
        if self._check_thread and self._check_thread.is_alive():
            self._check_thread.join(timeout=5.0)
        self._cancel_subscriptions()
        self._save_full_state()
        return True

    def resume(self) -> bool:
        logger.info(f"Resuming '{self.name}'")
        self._start_subscriptions()
        self.subscribe("portfolio_rebalancer_cmd", self._on_bus_command)
        self._stop_event.clear()
        if self.mode != "manual":
            self._check_thread = threading.Thread(
                target=self._autonomous_loop,
                daemon=True,
                name="rebalancer-check",
            )
            self._check_thread.start()
        return True

    def on_unload(self) -> str:
        return (
            f"portfolio_rebalancer: {self._rebalance_count} rebalances, "
            f"{self._fill_count} fills, mode={self.mode}"
        )

    # =========================================================================
    # Order fill / status callbacks
    # =========================================================================

    def on_order_fill(self, order_record) -> None:
        symbol = self._pending_order_actions.pop(order_record.order_id, None)
        if symbol is None:
            return
        self._fill_count += 1
        logger.info(
            f"Fill: {order_record.action} {order_record.filled_quantity:.0f} {symbol} "
            f"@ ${order_record.avg_fill_price:.2f}"
        )

    def on_order_status(self, order_record) -> None:
        from ib.models import OrderStatus
        terminal = (OrderStatus.CANCELLED, OrderStatus.INACTIVE, OrderStatus.ERROR)
        if order_record.status in terminal:
            symbol = self._pending_order_actions.pop(order_record.order_id, None)
            if symbol:
                logger.warning(
                    f"Order {order_record.order_id} ({symbol}) terminated: "
                    f"{order_record.status.value}"
                )

    # =========================================================================
    # Message bus command handler
    # =========================================================================

    def _on_bus_command(self, message) -> None:
        payload = getattr(message, "payload", {}) or {}
        command = payload.get("command", "")
        logger.info(f"Bus command received: '{command}'")

        if command == "rebalance":
            dry_run = payload.get("dry_run", self.dry_run)
            self._run_rebalance(dry_run=dry_run)
        elif command == "preview":
            self._run_rebalance(dry_run=True)
        elif command == "set_mode":
            self.set_parameter("mode", payload.get("mode", ""))
        elif command == "set_parameter":
            self.set_parameter(payload.get("key", ""), payload.get("value"))
        else:
            logger.warning(f"Unknown portfolio_rebalancer_cmd: '{command}'")

    # =========================================================================
    # handle_request (ibctl plugin request <name> <type> [json])
    # =========================================================================

    def handle_request(self, request_type: str, payload: Dict) -> Dict:
        if request_type == "rebalance":
            dry_run = payload.get("dry_run", self.dry_run)
            result = self._run_rebalance(dry_run=dry_run)
            return {"success": True, "data": result}

        elif request_type == "preview":
            result = self._run_rebalance(dry_run=True)
            return {"success": True, "data": result}

        elif request_type == "get_status":
            return {"success": True, "data": self._build_status()}

        elif request_type == "get_targets":
            return {"success": True, "data": self._build_targets_summary()}

        elif request_type == "get_last_trades":
            return {"success": True, "data": {"trades": self._last_trades}}

        elif request_type == "get_parameters":
            return {"success": True, "data": self.get_parameters()}

        elif request_type == "set_parameter":
            key   = payload.get("key")
            value = payload.get("value")
            if not key:
                return {"success": False, "message": "Missing 'key' in payload"}
            if self.set_parameter(key, value):
                return {"success": True, "message": f"{key} = {value}"}
            return {"success": False, "message": f"Unknown or invalid parameter: '{key}'"}

        else:
            return {"success": False, "message": f"Unknown request: '{request_type}'"}

    def cli_help(self) -> str:
        return (
            "portfolio_rebalancer commands:\n"
            "  plugin request portfolio_rebalancer preview\n"
            "  plugin request portfolio_rebalancer rebalance\n"
            "  plugin request portfolio_rebalancer rebalance '{\"dry_run\": false}'\n"
            "  plugin request portfolio_rebalancer get_status\n"
            "  plugin request portfolio_rebalancer get_targets\n"
            "  plugin request portfolio_rebalancer get_last_trades\n"
            "  plugin request portfolio_rebalancer get_parameters\n"
            "  plugin request portfolio_rebalancer set_parameter '{\"key\": \"mode\", \"value\": \"calendar\"}'\n"
            "  plugin request portfolio_rebalancer set_parameter '{\"key\": \"drift_threshold_pct\", \"value\": 3.0}'\n"
            "  plugin request portfolio_rebalancer set_parameter '{\"key\": \"dry_run\", \"value\": false}'\n"
            "  plugin request portfolio_rebalancer set_parameter '{\"key\": \"calendar_schedule\", \"value\": \"weekly\"}'\n"
            "\n"
            "Message bus (publish to 'portfolio_rebalancer_cmd'):\n"
            "  {\"command\": \"rebalance\"}\n"
            "  {\"command\": \"rebalance\", \"dry_run\": false}\n"
            "  {\"command\": \"preview\"}\n"
            "  {\"command\": \"set_mode\", \"mode\": \"threshold\"}\n"
            "  {\"command\": \"set_parameter\", \"key\": \"drift_threshold_pct\", \"value\": 3.0}\n"
        )

    # =========================================================================
    # Parameters
    # =========================================================================

    def get_parameters(self) -> Dict[str, Any]:
        return {
            "mode":                self.mode,
            "drift_threshold_pct": self.drift_threshold_pct,
            "min_trade_value":     self.min_trade_value,
            "min_trade_shares":    self.min_trade_shares,
            "cash_buffer_pct":     self.cash_buffer_pct,
            "max_trades_per_run":  self.max_trades_per_run,
            "dry_run":             self.dry_run,
            "check_interval_secs": self.check_interval_secs,
            "calendar_schedule":   self.calendar_schedule,
            "manage_untracked":    self.manage_untracked,
        }

    def set_parameter(self, key: str, value: Any) -> bool:
        if key == "mode":
            if str(value) not in self.MODES:
                logger.error(f"Invalid mode '{value}'; valid: {sorted(self.MODES)}")
                return False
            old = self.mode
            self.mode = str(value)
            # Start/stop the autonomous thread to match the new mode.
            if old == "manual" and self.mode != "manual":
                self._stop_event.clear()
                self._check_thread = threading.Thread(
                    target=self._autonomous_loop,
                    daemon=True,
                    name="rebalancer-check",
                )
                self._check_thread.start()
            elif old != "manual" and self.mode == "manual":
                self._stop_event.set()
            return True

        elif key == "drift_threshold_pct":
            self.drift_threshold_pct = float(value)
        elif key == "min_trade_value":
            self.min_trade_value = float(value)
        elif key == "min_trade_shares":
            self.min_trade_shares = int(value)
        elif key == "cash_buffer_pct":
            self.cash_buffer_pct = float(value)
        elif key == "max_trades_per_run":
            self.max_trades_per_run = int(value)
        elif key == "dry_run":
            self.dry_run = value if isinstance(value, bool) else str(value).lower() == "true"
        elif key == "check_interval_secs":
            self.check_interval_secs = int(value)
        elif key == "calendar_schedule":
            if str(value) not in self.SCHEDULES:
                logger.error(f"Invalid schedule '{value}'; valid: {sorted(self.SCHEDULES)}")
                return False
            self.calendar_schedule = str(value)
        elif key == "manage_untracked":
            self.manage_untracked = value if isinstance(value, bool) else str(value).lower() == "true"
        else:
            return False
        return True

    def get_parameter_schema(self) -> Dict[str, Dict[str, Any]]:
        return {
            "mode":                {"type": "str",   "choices": sorted(self.MODES),
                                    "description": "Rebalancing trigger mode"},
            "drift_threshold_pct": {"type": "float", "min": 0.1, "max": 50.0,
                                    "description": "Min drift % to trigger threshold rebalance"},
            "min_trade_value":     {"type": "float", "min": 0.0,
                                    "description": "Min trade value in USD (smaller trades skipped)"},
            "min_trade_shares":    {"type": "int",   "min": 1,
                                    "description": "Min shares per trade"},
            "cash_buffer_pct":     {"type": "float", "min": 0.0, "max": 20.0,
                                    "description": "Cash % to reserve uninvested"},
            "max_trades_per_run":  {"type": "int",   "min": 1,  "max": 100,
                                    "description": "Max trades per rebalance run (safety cap)"},
            "dry_run":             {"type": "bool",
                                    "description": "Compute but do not execute trades"},
            "check_interval_secs": {"type": "int",   "min": 60,
                                    "description": "Seconds between autonomous drift checks"},
            "calendar_schedule":   {"type": "str",   "choices": sorted(self.SCHEDULES),
                                    "description": "Calendar rebalance frequency"},
            "manage_untracked":    {"type": "bool",
                                    "description": "Sell positions absent from instruments.json"},
        }

    # =========================================================================
    # Autonomous loop
    # =========================================================================

    def _autonomous_loop(self) -> None:
        logger.info(
            f"Autonomous rebalance loop started "
            f"(mode={self.mode}, interval={self.check_interval_secs}s)"
        )
        while not self._stop_event.wait(timeout=self.check_interval_secs):
            try:
                self._autonomous_check()
            except Exception as exc:
                logger.error(f"Error in autonomous rebalance loop: {exc}", exc_info=True)
        logger.info("Autonomous rebalance loop stopped")

    def _autonomous_check(self) -> None:
        should = False
        if self.mode == "threshold":
            should = self._should_threshold_rebalance()
        elif self.mode == "calendar":
            should = self._should_calendar_rebalance()
        elif self.mode == "combined":
            should = self._should_threshold_rebalance() or self._should_calendar_rebalance()

        if should:
            logger.info(f"Autonomous rebalance triggered (mode={self.mode})")
            self._run_rebalance(dry_run=self.dry_run)

    # =========================================================================
    # Rebalance orchestration
    # =========================================================================

    def _run_rebalance(self, *, dry_run: bool) -> Dict:
        """Compute and optionally execute trades. Returns a result summary dict."""
        if not self.portfolio:
            logger.warning("No portfolio — skipping rebalance")
            return {"error": "no portfolio"}

        if not self._holdings:
            logger.warning("Plugin has no holdings — fund it first via 'transfer cash'")
            return {"error": "no holdings — fund the plugin first"}

        total_value = self._portfolio_value()
        if total_value <= 0:
            logger.warning("Plugin holdings value is zero — skipping rebalance")
            return {"error": "zero holdings value"}

        # Investable capital after the cash buffer reserve
        investable = total_value * (1.0 - self.cash_buffer_pct / 100.0)

        # Build positions map from plugin holdings only (not the full account)
        positions_map: Dict[str, Any] = {
            p.symbol: p for p in self._holdings.current_positions
        }

        # Build target-weight map from enabled instruments
        target_weights: Dict[str, float] = {
            sym: inst.weight
            for sym, inst in self._instruments.items()
            if inst.enabled
        }

        # Normalise if weights drift from 100% (e.g. after manual edits)
        total_weight = sum(target_weights.values())
        if total_weight > 0 and not (98.0 <= total_weight <= 102.0):
            logger.warning(
                f"Instrument weights sum to {total_weight:.1f}%, normalising to 100%"
            )
            target_weights = {s: w / total_weight * 100.0 for s, w in target_weights.items()}

        # Optionally sell holdings positions absent from instruments.json
        if self.manage_untracked:
            for pos in self._holdings.current_positions:
                if pos.symbol not in target_weights:
                    target_weights[pos.symbol] = 0.0

        # Choose strategy based on current mode (calendar always uses exact targets)
        if self.mode == "calendar":
            trades = self._compute_exact_trades(target_weights, investable, positions_map, total_value)
        else:
            trades = self._compute_threshold_trades(target_weights, investable, positions_map, total_value)

        # Apply minimum size filters
        trades = [
            t for t in trades
            if t["value"] >= self.min_trade_value and t["shares"] >= self.min_trade_shares
        ]

        # Sells first (free up cash before buying)
        trades.sort(key=lambda t: (t["action"] != "SELL", -t["value"]))

        if len(trades) > self.max_trades_per_run:
            logger.warning(
                f"Capping to {self.max_trades_per_run} trades (computed {len(trades)})"
            )
            trades = trades[: self.max_trades_per_run]

        result: Dict = {
            "mode":         self.mode,
            "total_value":  total_value,
            "investable":   investable,
            "dry_run":      dry_run,
            "trades":       trades,
            "trade_count":  len(trades),
            "timestamp":    datetime.now(timezone.utc).isoformat(),
        }

        self._last_check_time = datetime.now(timezone.utc)

        if not trades:
            logger.info("Rebalance: portfolio is within tolerance — no trades needed")
            self._publish_result(result)
            return result

        logger.info(
            f"Rebalance ({self.mode}): {len(trades)} trades, "
            f"total=${total_value:,.0f}, dry_run={dry_run}"
        )
        for t in trades:
            logger.info(
                f"  {t['action']:4} {t['shares']:>6.0f} {t['symbol']:<8} "
                f"@ ${t['price']:>9.2f}  value=${t['value']:>10,.0f}  "
                f"drift={t['drift']:>+5.1f}%"
            )

        if not dry_run:
            self._execute_trades(trades, positions_map)
            self._rebalance_count += 1
            self._last_rebalance_time = datetime.now(timezone.utc)
            if self.mode in ("calendar", "combined"):
                self._last_calendar_date = date.today()
            self._last_trades = trades
            self._save_full_state()
        else:
            self._last_trades = trades

        self._publish_result(result)
        return result

    # =========================================================================
    # Strategy implementations
    # =========================================================================

    def _compute_threshold_trades(
        self,
        target_weights: Dict[str, float],
        investable: float,
        positions_map: Dict,
        total_value: float,
    ) -> List[Dict]:
        """
        Threshold strategy: generate a trade only when a position's current
        allocation deviates from its target by more than drift_threshold_pct.
        """
        trades = []
        for symbol, target_pct in target_weights.items():
            pos   = positions_map.get(symbol)
            price = self._price(symbol, pos)
            if not price:
                logger.warning(f"No price for {symbol} — skipping")
                continue

            current_value = pos.market_value if pos else 0.0
            current_pct   = (current_value / total_value * 100.0) if total_value > 0 else 0.0
            drift         = current_pct - target_pct

            if abs(drift) < self.drift_threshold_pct:
                continue

            current_qty = getattr(pos, "quantity", 0) if pos else 0
            target_qty  = int(investable * (target_pct / 100.0) / price)
            share_diff  = abs(target_qty - current_qty)

            if share_diff < self.min_trade_shares:
                continue

            trades.append({
                "symbol":      symbol,
                "action":      "SELL" if drift > 0 else "BUY",
                "shares":      share_diff,
                "price":       price,
                "value":       share_diff * price,
                "current_pct": round(current_pct, 2),
                "target_pct":  round(target_pct, 2),
                "drift":       round(drift, 2),
                "reason":      (
                    f"Drift {drift:+.1f}% exceeds threshold "
                    f"{self.drift_threshold_pct:.1f}%"
                ),
            })

        return trades

    def _compute_exact_trades(
        self,
        target_weights: Dict[str, float],
        investable: float,
        positions_map: Dict,
        total_value: float,
    ) -> List[Dict]:
        """
        Calendar strategy: trade every position to its exact target weight,
        ignoring the drift threshold.
        """
        trades = []
        for symbol, target_pct in target_weights.items():
            pos   = positions_map.get(symbol)
            price = self._price(symbol, pos)
            if not price:
                logger.warning(f"No price for {symbol} — skipping")
                continue

            current_qty   = getattr(pos, "quantity", 0) if pos else 0
            target_qty    = int(investable * (target_pct / 100.0) / price)
            share_diff    = target_qty - current_qty

            if share_diff == 0:
                continue

            current_value = pos.market_value if pos else 0.0
            current_pct   = (current_value / total_value * 100.0) if total_value > 0 else 0.0

            trades.append({
                "symbol":      symbol,
                "action":      "BUY" if share_diff > 0 else "SELL",
                "shares":      abs(share_diff),
                "price":       price,
                "value":       abs(share_diff) * price,
                "current_pct": round(current_pct, 2),
                "target_pct":  round(target_pct, 2),
                "drift":       round(current_pct - target_pct, 2),
                "reason":      "Calendar rebalance — exact target",
            })

        return trades

    def _execute_trades(self, trades: List[Dict], positions_map: Dict) -> None:
        """Place an IB market order for each trade and register for fill callbacks."""
        for trade in trades:
            symbol   = trade["symbol"]
            pos      = positions_map.get(symbol)
            contract = None

            if pos and getattr(pos, "contract", None):
                contract = pos.contract
            elif symbol in self._instruments:
                contract = self._instruments[symbol].to_contract()

            if not contract:
                logger.error(f"No contract available for {symbol} — skipping")
                continue

            oid = self.portfolio.place_order(
                contract=contract,
                action=trade["action"],
                quantity=trade["shares"],
                order_type="MKT",
            )

            if oid:
                self._pending_order_actions[oid] = symbol
                self.register_order(oid)
            else:
                logger.error(f"Failed to place {trade['action']} order for {symbol}")

    # =========================================================================
    # Trigger checks
    # =========================================================================

    def _should_threshold_rebalance(self) -> bool:
        """Return True if any instrument drifts beyond drift_threshold_pct."""
        if not self._holdings or not self._instruments:
            return False
        total = self._holdings.total_value
        if total <= 0:
            return False
        for sym, inst in self._instruments.items():
            if not inst.enabled:
                continue
            pos = self._holdings.get_position(sym)
            current_pct = ((pos.market_value / total * 100.0) if pos else 0.0)
            if abs(current_pct - inst.weight) >= self.drift_threshold_pct:
                return True
        return False

    def _should_calendar_rebalance(self) -> bool:
        """Return True if the configured schedule has elapsed since last rebalance."""
        today    = date.today()
        last     = self._last_calendar_date
        schedule = self.calendar_schedule

        if last is None:
            return True  # never rebalanced → trigger immediately

        if schedule == "daily":
            return last < today

        if schedule == "weekly":
            # Fire on the Monday of a new week.
            this_monday = today.toordinal() - today.weekday()
            last_monday = last.toordinal() - last.weekday()
            return last_monday < this_monday

        if schedule == "monthly":
            return (last.year, last.month) < (today.year, today.month)

        return False

    # =========================================================================
    # Price and portfolio value helpers
    # =========================================================================

    def _price(self, symbol: str, position=None) -> Optional[float]:
        """Best available price: live position current_price → 5-min bar cache."""
        if position is not None:
            p = getattr(position, "current_price", 0.0)
            if p and p > 0:
                return float(p)
        cached = self._price_cache.get(symbol, 0.0)
        return float(cached) if cached > 0 else None

    def _portfolio_value(self) -> float:
        """Total value of this plugin's holdings: cash + position market values."""
        if not self._holdings:
            return 0.0
        return float(self._holdings.total_value)

    # =========================================================================
    # Live bar subscriptions (price cache)
    # =========================================================================

    def _start_subscriptions(self) -> None:
        for inst in self.enabled_instruments:
            if inst.symbol in self._live_bar_req_ids:
                continue
            contract = inst.to_contract()
            symbol   = inst.symbol

            def _make_cb(sym: str):
                def _on_bar(bar) -> None:
                    close = getattr(bar, "close", 0.0)
                    if close and float(close) > 0:
                        self._price_cache[sym] = float(close)
                return _on_bar

            req_id = self.subscribe_live_bars(
                contract=contract,
                on_bar=_make_cb(symbol),
                bar_size_setting="5 mins",
                use_rth=True,
            )
            if req_id is not None:
                self._live_bar_req_ids[symbol] = req_id

    def _cancel_subscriptions(self) -> None:
        for req_id in self._live_bar_req_ids.values():
            self.cancel_live_bars(req_id)
        self._live_bar_req_ids.clear()

    # =========================================================================
    # State persistence
    # =========================================================================

    def _save_full_state(self) -> None:
        self.save_state({
            "mode":                self.mode,
            "drift_threshold_pct": self.drift_threshold_pct,
            "min_trade_value":     self.min_trade_value,
            "min_trade_shares":    self.min_trade_shares,
            "cash_buffer_pct":     self.cash_buffer_pct,
            "max_trades_per_run":  self.max_trades_per_run,
            "dry_run":             self.dry_run,
            "check_interval_secs": self.check_interval_secs,
            "calendar_schedule":   self.calendar_schedule,
            "manage_untracked":    self.manage_untracked,
            "rebalance_count":     self._rebalance_count,
            "fill_count":          self._fill_count,
            "last_calendar_date":  (
                self._last_calendar_date.isoformat() if self._last_calendar_date else None
            ),
            "last_rebalance_time": (
                self._last_rebalance_time.isoformat() if self._last_rebalance_time else None
            ),
        })

    # =========================================================================
    # Response helpers
    # =========================================================================

    def _publish_result(self, result: Dict) -> None:
        self.publish("portfolio_rebalancer_result", result, message_type="state")

    def _build_status(self) -> Dict:
        return {
            "mode":               self.mode,
            "dry_run":            self.dry_run,
            "rebalance_count":    self._rebalance_count,
            "fill_count":         self._fill_count,
            "drift_threshold_pct":self.drift_threshold_pct,
            "check_interval_secs":self.check_interval_secs,
            "calendar_schedule":  self.calendar_schedule,
            "manage_untracked":   self.manage_untracked,
            "instruments":        len(self._instruments),
            "holdings_value":     self._portfolio_value(),
            "holdings_cash":      (self._holdings.current_cash if self._holdings else 0.0),
            "cached_prices":      len(self._price_cache),
            "live_subscriptions": len(self._live_bar_req_ids),
            "last_check_time":    (
                self._last_check_time.isoformat() if self._last_check_time else None
            ),
            "last_rebalance_time":(
                self._last_rebalance_time.isoformat() if self._last_rebalance_time else None
            ),
            "last_calendar_date": (
                self._last_calendar_date.isoformat() if self._last_calendar_date else None
            ),
        }

    def _build_targets_summary(self) -> Dict:
        total_weight = sum(
            i.weight for i in self._instruments.values() if i.enabled
        )
        total_value = self._portfolio_value()
        rows = []
        for sym, inst in self._instruments.items():
            pos = self._holdings.get_position(sym) if self._holdings else None
            current_value = pos.market_value if pos else 0.0
            current_pct   = (current_value / total_value * 100.0) if total_value > 0 else 0.0
            rows.append({
                "symbol":        sym,
                "name":          inst.name,
                "target_pct":    inst.weight,
                "current_pct":   round(current_pct, 2),
                "drift":         round(current_pct - inst.weight, 2),
                "current_value": round(current_value, 2),
                "enabled":       inst.enabled,
                "cached_price":  self._price_cache.get(sym),
            })
        return {
            "total_weight":  round(total_weight, 2),
            "holdings_value": round(total_value, 2),
            "targets": rows,
        }
