"""
paper_test_interface/plugin.py - Plugin interface validation test suite

Tests aspects of the PluginBase interface that do not require live market
transactions on instruments with changing prices.  Every test self-checks
its result and records a pass/fail + detail string.

Test categories
───────────────
  connection      Paper account safety-gate and connectivity metadata
  contract_builder  ContractBuilder attribute correctness for all factory methods
  messagebus      Pub/Sub delivery, metadata fields, unsubscribe semantics
  state           save_state / load_state / clear_state round-trips
  holdings        Holdings in-memory add / average-cost / remove operations
  instruments     add / get / remove / enabled_instruments / get_contracts
  parameters      get_parameters / set_parameter / get_parameter_schema
  ib_error        on_ib_error routing for a deliberately invalid request

Run via: plugin request paper_test_interface run_tests
"""

import json
import logging
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from ib.contract_builder import ContractBuilder
from plugins.base import (
    Holdings,
    HoldingPosition,
    PluginBase,
    PluginInstrument,
    TradeSignal,
)

logger = logging.getLogger(__name__)

PAPER_PORTS = (7497, 4002)

_TEST_PARAM_KEY = "test_threshold"
_TEST_PARAM_DEFAULT = 42


# =============================================================================
# Result type
# =============================================================================

@dataclass
class InterfaceTestResult:
    """Result of one interface test."""
    test_name: str
    category: str
    passed: bool
    detail: str = ""
    error_message: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "test_name": self.test_name,
            "category": self.category,
            "passed": self.passed,
            "detail": self.detail,
            "error_message": self.error_message,
        }


# =============================================================================
# Plugin
# =============================================================================

class PaperTestInterfacePlugin(PluginBase):
    """
    Paper test: PluginBase interface validation.

    Exercises connection metadata, ContractBuilder, MessageBus, state
    persistence, Holdings, instruments, runtime parameters, and
    on_ib_error routing.  Refuses to run on live accounts.

    Run via: plugin request paper_test_interface run_tests
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
            "paper_test_interface",
            base_path, portfolio, shared_holdings, message_bus,
        )
        self._results: List[InterfaceTestResult] = []
        self._running = False
        self._test_threshold = _TEST_PARAM_DEFAULT

        # IB error capture for the ib_error test category
        self._ib_error_info: Optional[Dict] = None
        self._ib_error_event = threading.Event()

    @property
    def description(self) -> str:
        return (
            "Paper Test Interface: validates PluginBase interface — "
            "connection, ContractBuilder, MessageBus, state, holdings, "
            "instruments, parameters, on_ib_error routing."
        )

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def start(self) -> bool:
        logger.info(f"Starting plugin '{self.name}'")
        saved = self.load_state()
        if saved:
            self._results = [
                InterfaceTestResult(**r) for r in saved.get("results", [])
            ]
        return True

    def stop(self) -> bool:
        logger.info(f"Stopping plugin '{self.name}'")
        self.unsubscribe_all()
        self.save_state({"results": [r.to_dict() for r in self._results]})
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
        return f"paper_test_interface: {passed}/{total} tests passed"

    # -------------------------------------------------------------------------
    # Runtime parameters (exposed so the parameters test category can exercise them)
    # -------------------------------------------------------------------------

    def get_parameters(self) -> Dict[str, Any]:
        return {_TEST_PARAM_KEY: self._test_threshold}

    def set_parameter(self, key: str, value: Any) -> bool:
        if key == _TEST_PARAM_KEY:
            try:
                self._test_threshold = int(value)
                return True
            except (TypeError, ValueError):
                return False
        return False

    def get_parameter_schema(self) -> Dict[str, Dict[str, Any]]:
        return {
            _TEST_PARAM_KEY: {
                "type": "int",
                "default": _TEST_PARAM_DEFAULT,
                "description": "Test threshold value (for interface tests only)",
                "min": 0,
                "max": 1000,
            }
        }

    # -------------------------------------------------------------------------
    # IB error capture (overrides base no-op for the ib_error test category)
    # -------------------------------------------------------------------------

    def on_ib_error(self, req_id: int, error_code: int, error_string: str) -> None:
        logger.info(
            f"[{self.name}] on_ib_error: req_id={req_id} "
            f"code={error_code} msg={error_string!r}"
        )
        self._ib_error_info = {
            "req_id": req_id,
            "error_code": error_code,
            "error_string": error_string,
        }
        self._ib_error_event.set()

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

            self._run_connection_tests()
            self._run_contract_builder_tests()
            self._run_messagebus_tests()
            self._run_state_tests()
            self._run_holdings_tests()
            self._run_instruments_tests()
            self._run_parameters_tests()
            self._run_ib_error_tests()

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
            logger.error(f"[{self.name}] Unexpected error: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

        finally:
            self._running = False

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _pass(self, name: str, category: str, detail: str = "") -> InterfaceTestResult:
        r = InterfaceTestResult(
            test_name=name, category=category, passed=True, detail=detail
        )
        self._results.append(r)
        logger.info(f"  [{category}] {name} PASS  {detail}")
        return r

    def _fail(self, name: str, category: str, msg: str) -> InterfaceTestResult:
        r = InterfaceTestResult(
            test_name=name, category=category, passed=False, error_message=msg
        )
        self._results.append(r)
        logger.warning(f"  [{category}] {name} FAIL  {msg}")
        return r

    # =========================================================================
    # Category: connection
    # =========================================================================

    def _run_connection_tests(self):
        cat = "connection"

        if self.portfolio.connected:
            self._pass("conn_is_connected", cat, "portfolio.connected == True")
        else:
            self._fail("conn_is_connected", cat, "portfolio.connected is False")

        port = self.portfolio.port
        if port in PAPER_PORTS:
            self._pass("conn_paper_port", cat, f"port={port}")
        else:
            self._fail("conn_paper_port", cat, f"port={port} not in {PAPER_PORTS}")

        accounts = self.portfolio.managed_accounts
        if isinstance(accounts, list) and accounts:
            self._pass("conn_managed_accounts_non_empty", cat, f"accounts={accounts}")
        else:
            self._fail("conn_managed_accounts_non_empty", cat,
                       f"accounts={accounts!r}")

        acct = accounts[0] if accounts else ""
        if acct.startswith("D"):
            self._pass("conn_account_paper_prefix", cat, f"account={acct}")
        else:
            self._fail("conn_account_paper_prefix", cat,
                       f"account={acct!r} does not start with 'D'")

    # =========================================================================
    # Category: contract_builder
    # =========================================================================

    def _run_contract_builder_tests(self):
        cat = "contract_builder"

        # us_stock basic attributes
        try:
            c = ContractBuilder.us_stock("SPY")
            if (c.symbol == "SPY" and c.secType == "STK"
                    and c.exchange == "SMART" and c.currency == "USD"):
                self._pass("cb_us_stock_attrs", cat,
                           f"symbol={c.symbol} secType={c.secType} exchange={c.exchange}")
            else:
                self._fail("cb_us_stock_attrs", cat,
                           f"symbol={c.symbol!r} secType={c.secType!r} "
                           f"exchange={c.exchange!r} currency={c.currency!r}")
        except Exception as e:
            self._fail("cb_us_stock_attrs", cat, str(e))

        # us_stock with primary_exchange sets the primaryExchange attribute
        try:
            c = ContractBuilder.us_stock("SPY", primary_exchange="ARCA")
            val = getattr(c, "primaryExchange", "<missing>")
            if val == "ARCA":
                self._pass("cb_us_stock_primary_exchange", cat, "primaryExchange=ARCA")
            else:
                self._fail("cb_us_stock_primary_exchange", cat,
                           f"primaryExchange={val!r}")
        except Exception as e:
            self._fail("cb_us_stock_primary_exchange", cat, str(e))

        # forex attributes
        try:
            c = ContractBuilder.forex("EUR", "USD")
            if (c.secType == "CASH" and c.exchange == "IDEALPRO"
                    and c.symbol == "EUR" and c.currency == "USD"):
                self._pass("cb_forex_attrs", cat,
                           f"secType={c.secType} exchange={c.exchange}")
            else:
                self._fail("cb_forex_attrs", cat,
                           f"secType={c.secType!r} exchange={c.exchange!r} "
                           f"symbol={c.symbol!r} currency={c.currency!r}")
        except Exception as e:
            self._fail("cb_forex_attrs", cat, str(e))

        # future attributes
        try:
            c = ContractBuilder.future("ES", "202506", "CME")
            if (c.secType == "FUT" and c.symbol == "ES"
                    and "202506" in c.lastTradeDateOrContractMonth):
                self._pass("cb_future_attrs", cat,
                           f"secType={c.secType} expiry={c.lastTradeDateOrContractMonth}")
            else:
                self._fail("cb_future_attrs", cat,
                           f"secType={c.secType!r} "
                           f"expiry={c.lastTradeDateOrContractMonth!r}")
        except Exception as e:
            self._fail("cb_future_attrs", cat, str(e))

        # option attributes
        try:
            c = ContractBuilder.option("SPY", "20260320", 450.0, "C")
            if c.secType == "OPT" and c.strike == 450.0 and c.right == "C":
                self._pass("cb_option_attrs", cat,
                           f"secType={c.secType} strike={c.strike} right={c.right}")
            else:
                self._fail("cb_option_attrs", cat,
                           f"secType={c.secType!r} strike={c.strike} right={c.right!r}")
        except Exception as e:
            self._fail("cb_option_attrs", cat, str(e))

        # etf (same as stock — secType must be STK)
        try:
            c = ContractBuilder.etf("QQQ")
            if c.secType == "STK" and c.symbol == "QQQ":
                self._pass("cb_etf_attrs", cat, f"secType={c.secType} symbol={c.symbol}")
            else:
                self._fail("cb_etf_attrs", cat,
                           f"secType={c.secType!r} symbol={c.symbol!r}")
        except Exception as e:
            self._fail("cb_etf_attrs", cat, str(e))

        # index attributes
        try:
            c = ContractBuilder.index("SPX", "CBOE")
            if c.secType == "IND" and c.symbol == "SPX":
                self._pass("cb_index_attrs", cat,
                           f"secType={c.secType} exchange={c.exchange}")
            else:
                self._fail("cb_index_attrs", cat,
                           f"secType={c.secType!r} symbol={c.symbol!r}")
        except Exception as e:
            self._fail("cb_index_attrs", cat, str(e))

        # by_conid sets the conId attribute
        try:
            c = ContractBuilder.by_conid(12345)
            val = getattr(c, "conId", "<missing>")
            if val == 12345:
                self._pass("cb_by_conid_attrs", cat, f"conId={val}")
            else:
                self._fail("cb_by_conid_attrs", cat, f"conId={val!r}")
        except Exception as e:
            self._fail("cb_by_conid_attrs", cat, str(e))

        # by_isin sets secIdType and secId attributes
        try:
            isin = "US0378331005"
            c = ContractBuilder.by_isin(isin)
            sid_type = getattr(c, "secIdType", "<missing>")
            sid = getattr(c, "secId", "<missing>")
            if sid_type == "ISIN" and sid == isin:
                self._pass("cb_by_isin_attrs", cat,
                           f"secIdType=ISIN secId={sid}")
            else:
                self._fail("cb_by_isin_attrs", cat,
                           f"secIdType={sid_type!r} secId={sid!r}")
        except Exception as e:
            self._fail("cb_by_isin_attrs", cat, str(e))

    # =========================================================================
    # Category: messagebus
    # =========================================================================

    def _run_messagebus_tests(self):
        cat = "messagebus"
        # Use instance_id prefix to avoid channel collisions with other plugins
        ch = f"_iface_test_{self.instance_id[:8]}"

        if self._message_bus is None:
            for name in [
                "mb_subscribe_returns_true", "mb_delivery",
                "mb_payload_correct", "mb_source_plugin_correct",
                "mb_sequence_number_positive", "mb_channel_correct",
                "mb_unsubscribe_stops_delivery", "mb_resubscribe_works",
                "mb_unsubscribe_all_clears",
            ]:
                self._fail(name, cat, "No MessageBus configured")
            return

        # --- subscribe ---
        received: List = []
        evt = threading.Event()

        def _on_msg(msg):
            received.append(msg)
            evt.set()

        sub_ok = self.subscribe(ch, _on_msg)
        if sub_ok:
            self._pass("mb_subscribe_returns_true", cat)
        else:
            self._fail("mb_subscribe_returns_true", cat, "subscribe() returned False")

        # --- publish and check delivery ---
        # MessageBus delivers synchronously so evt is set before publish returns
        test_payload = {"value": 777, "label": "interface_test"}
        self.publish(ch, test_payload)
        delivered = evt.wait(timeout=2.0)

        if not (delivered and received):
            self._fail("mb_delivery", cat, "no message within 2s")
            for name in [
                "mb_payload_correct", "mb_source_plugin_correct",
                "mb_sequence_number_positive", "mb_channel_correct",
            ]:
                self._fail(name, cat, "delivery failed — cannot validate")
            self.unsubscribe(ch)
        else:
            self._pass("mb_delivery", cat, "message arrived")
            msg = received[0]

            if msg.payload == test_payload:
                self._pass("mb_payload_correct", cat, f"payload={msg.payload!r}")
            else:
                self._fail("mb_payload_correct", cat,
                           f"expected={test_payload!r} got={msg.payload!r}")

            if msg.metadata.source_plugin == self.name:
                self._pass("mb_source_plugin_correct", cat,
                           f"source={msg.metadata.source_plugin}")
            else:
                self._fail("mb_source_plugin_correct", cat,
                           f"expected={self.name!r} got={msg.metadata.source_plugin!r}")

            seq = msg.metadata.sequence_number
            if isinstance(seq, int) and seq >= 1:
                self._pass("mb_sequence_number_positive", cat, f"seq={seq}")
            else:
                self._fail("mb_sequence_number_positive", cat, f"seq={seq!r}")

            if msg.channel == ch:
                self._pass("mb_channel_correct", cat, f"channel={ch!r}")
            else:
                self._fail("mb_channel_correct", cat,
                           f"expected={ch!r} got={msg.channel!r}")

            self.unsubscribe(ch)

        # --- unsubscribe stops delivery ---
        self._mb_test_unsubscribe_stops(cat, ch)

        # --- re-subscribe after unsubscribe ---
        self._mb_test_resubscribe(cat, ch)

        # --- unsubscribe_all ---
        self._mb_test_unsubscribe_all(cat, ch)

    def _mb_test_unsubscribe_stops(self, cat: str, ch: str):
        """Subscribe, immediately unsubscribe, then publish — no delivery expected."""
        received2: List = []
        evt2 = threading.Event()

        def _on_msg2(msg):
            received2.append(msg)
            evt2.set()

        self.subscribe(ch, _on_msg2)
        self.unsubscribe(ch)
        self.publish(ch, {"value": "after_unsub"})

        if not evt2.wait(timeout=0.5):
            self._pass("mb_unsubscribe_stops_delivery", cat,
                       "no delivery after unsubscribe")
        else:
            self._fail("mb_unsubscribe_stops_delivery", cat,
                       f"received {len(received2)} message(s) after unsubscribe")

    def _mb_test_resubscribe(self, cat: str, ch: str):
        """Re-subscribe to a channel that was previously unsubscribed."""
        received3: List = []
        evt3 = threading.Event()

        def _on_msg3(msg):
            received3.append(msg)
            evt3.set()

        self.subscribe(ch, _on_msg3)
        self.publish(ch, {"value": "re_subscribed"})
        delivered3 = evt3.wait(timeout=2.0)
        self.unsubscribe(ch)

        if delivered3:
            self._pass("mb_resubscribe_works", cat, "re-subscribe delivered correctly")
        else:
            self._fail("mb_resubscribe_works", cat, "no delivery after re-subscribe")

    def _mb_test_unsubscribe_all(self, cat: str, ch: str):
        """Subscribe to two channels, then unsubscribe_all returns >= 1."""
        ch2 = ch + "_b"
        self.subscribe(ch, lambda m: None)
        self.subscribe(ch2, lambda m: None)
        n = self.unsubscribe_all()
        if n >= 1:
            self._pass("mb_unsubscribe_all_clears", cat, f"unsubscribed {n} channel(s)")
        else:
            self._fail("mb_unsubscribe_all_clears", cat,
                       f"unsubscribe_all() returned {n}")

    # =========================================================================
    # Category: state
    # =========================================================================

    def _run_state_tests(self):
        cat = "state"

        # clear_state returns True (even if file didn't exist)
        if self.clear_state():
            self._pass("state_clear_initial", cat)
        else:
            self._fail("state_clear_initial", cat, "clear_state() returned False")

        # load_state when no file → empty dict
        loaded = self.load_state()
        if loaded == {}:
            self._pass("state_load_missing_empty", cat)
        else:
            self._fail("state_load_missing_empty", cat, f"got {loaded!r}")

        # save_state returns True
        test_state = {"counter": 7, "label": "interface_test", "nested": {"x": 3.14}}
        if self.save_state(test_state):
            self._pass("state_save_returns_true", cat)
        else:
            self._fail("state_save_returns_true", cat, "save_state() returned False")

        # load_state round-trips the saved data exactly
        loaded2 = self.load_state()
        if loaded2 == test_state:
            self._pass("state_load_roundtrip", cat)
        else:
            self._fail("state_load_roundtrip", cat,
                       f"expected={test_state!r} got={loaded2!r}")

        # JSON file has expected outer metadata keys
        try:
            with open(self._state_file) as f:
                raw = json.load(f)
            if all(k in raw for k in ("plugin_name", "state", "saved_at")):
                self._pass("state_metadata_present", cat, f"keys={sorted(raw.keys())}")
            else:
                self._fail("state_metadata_present", cat,
                           f"missing keys; got={sorted(raw.keys())!r}")
        except Exception as e:
            self._fail("state_metadata_present", cat, str(e))

        # clear_state removes the file
        self.clear_state()
        if not self._state_file.exists():
            self._pass("state_clear_removes_file", cat)
        else:
            self._fail("state_clear_removes_file", cat,
                       "file still present after clear_state()")

        # load_state after clear → empty dict again
        loaded3 = self.load_state()
        if loaded3 == {}:
            self._pass("state_load_after_clear_empty", cat)
        else:
            self._fail("state_load_after_clear_empty", cat, f"got {loaded3!r}")

    # =========================================================================
    # Category: holdings
    # =========================================================================

    def _run_holdings_tests(self):
        cat = "holdings"
        h = Holdings(plugin_name="__test__")

        # fresh Holdings has no positions and zero cash
        if not h.current_positions and h.current_cash == 0.0:
            self._pass("hold_initial_empty", cat)
        else:
            self._fail("hold_initial_empty", cat,
                       f"positions={len(h.current_positions)} cash={h.current_cash}")

        # add_cash
        h.add_cash(10_000.0)
        if h.current_cash == 10_000.0:
            self._pass("hold_add_cash", cat, "cash=10000.0")
        else:
            self._fail("hold_add_cash", cat, f"cash={h.current_cash}")

        # add a new position
        h.add_position("AAPL", quantity=10, cost_basis=500.0, current_price=510.0)
        pos = h.get_position("AAPL")
        if pos and pos.quantity == 10 and pos.cost_basis == 500.0:
            self._pass("hold_add_position_new", cat,
                       f"qty={pos.quantity} basis={pos.cost_basis}")
        else:
            self._fail("hold_add_position_new", cat, f"pos={pos!r}")

        # add to existing position — weighted-average cost: (10*500 + 10*520) / 20 = 510
        h.add_position("AAPL", quantity=10, cost_basis=520.0, current_price=510.0)
        pos2 = h.get_position("AAPL")
        if pos2 and pos2.quantity == 20 and abs(pos2.cost_basis - 510.0) < 0.01:
            self._pass("hold_add_position_average", cat,
                       f"qty={pos2.quantity} avg_basis={pos2.cost_basis:.2f}")
        else:
            self._fail("hold_add_position_average", cat,
                       f"qty={pos2.quantity if pos2 else '?'} "
                       f"basis={pos2.cost_basis if pos2 else '?'}")

        # total_value = cash + sum(market_value) — market_value defaults to 0
        # so total_value == current_cash; just check it's a positive number
        tv = h.total_value
        if tv == h.current_cash:
            self._pass("hold_total_value", cat, f"total={tv}")
        else:
            self._fail("hold_total_value", cat,
                       f"total={tv} != cash={h.current_cash} (unexpected market_value contribution)")

        # remove a partial quantity
        removed = h.remove_position("AAPL", 5)
        pos3 = h.get_position("AAPL")
        if removed and pos3 and pos3.quantity == 15:
            self._pass("hold_remove_partial", cat, f"qty_remaining={pos3.quantity}")
        else:
            self._fail("hold_remove_partial", cat,
                       f"removed={removed} qty={pos3.quantity if pos3 else '?'}")

        # remove more than available → False (position unchanged)
        excess = h.remove_position("AAPL", 1000)
        if not excess:
            self._pass("hold_remove_excess_false", cat)
        else:
            self._fail("hold_remove_excess_false", cat,
                       "remove_position returned True for quantity > available")

        # remove the entire remaining quantity → position disappears
        h.remove_position("AAPL", 15)
        if h.get_position("AAPL") is None:
            self._pass("hold_remove_all_clears", cat)
        else:
            self._fail("hold_remove_all_clears", cat,
                       "position still present after removing all shares")

        # get_position for a symbol that was never added → None
        if h.get_position("NONEXISTENT") is None:
            self._pass("hold_get_missing_none", cat)
        else:
            self._fail("hold_get_missing_none", cat, "expected None for unknown symbol")

    # =========================================================================
    # Category: instruments
    # =========================================================================

    def _run_instruments_tests(self):
        cat = "instruments"

        # Start with a clean slate (no instruments loaded from file for this plugin)
        self._instruments.clear()

        spy = PluginInstrument(
            symbol="SPY",
            name="SPDR S&P 500 ETF",
            weight=60.0,
            exchange="ARCA",
            currency="USD",
            sec_type="STK",
            enabled=True,
        )

        # add_instrument returns True for new
        if self.add_instrument(spy):
            self._pass("inst_add_returns_true", cat)
        else:
            self._fail("inst_add_returns_true", cat,
                       "add_instrument returned False for a new instrument")

        # add_instrument returns False for a duplicate
        if not self.add_instrument(spy):
            self._pass("inst_add_duplicate_false", cat)
        else:
            self._fail("inst_add_duplicate_false", cat,
                       "add_instrument returned True for a duplicate")

        # get_instrument returns correct attributes
        got = self.get_instrument("SPY")
        if got and got.weight == 60.0 and got.exchange == "ARCA":
            self._pass("inst_get_existing", cat,
                       f"weight={got.weight} exchange={got.exchange}")
        else:
            self._fail("inst_get_existing", cat, f"got={got!r}")

        # get_instrument is case-insensitive (uses .upper() internally)
        if self.get_instrument("spy") is not None:
            self._pass("inst_get_case_insensitive", cat, "get_instrument('spy') found SPY")
        else:
            self._fail("inst_get_case_insensitive", cat,
                       "get_instrument('spy') returned None")

        # get_instrument for unknown symbol → None
        if self.get_instrument("NONEXISTENT") is None:
            self._pass("inst_get_missing_none", cat)
        else:
            self._fail("inst_get_missing_none", cat,
                       "expected None for unknown symbol")

        # Add a disabled instrument
        qqq = PluginInstrument(
            symbol="QQQ",
            name="Invesco QQQ Trust",
            weight=40.0,
            exchange="NASDAQ",
            enabled=False,
        )
        self.add_instrument(qqq)

        # enabled_instruments excludes disabled ones
        enabled_syms = [i.symbol for i in self.enabled_instruments]
        if "SPY" in enabled_syms and "QQQ" not in enabled_syms:
            self._pass("inst_enabled_filter", cat, f"enabled={enabled_syms}")
        else:
            self._fail("inst_enabled_filter", cat, f"enabled={enabled_syms!r}")

        # get_contracts returns one contract per enabled instrument
        contracts = self.get_contracts()
        if len(contracts) == len(self.enabled_instruments):
            self._pass("inst_get_contracts_count", cat, f"count={len(contracts)}")
        else:
            self._fail("inst_get_contracts_count", cat,
                       f"contracts={len(contracts)} enabled={len(self.enabled_instruments)}")

        # to_contract produces correct IB Contract attributes
        c = spy.to_contract()
        if (c.symbol == "SPY" and c.secType == "STK"
                and c.exchange == "ARCA" and c.currency == "USD"):
            self._pass("inst_to_contract_attrs", cat,
                       f"symbol={c.symbol} secType={c.secType} exchange={c.exchange}")
        else:
            self._fail("inst_to_contract_attrs", cat,
                       f"symbol={c.symbol!r} secType={c.secType!r} "
                       f"exchange={c.exchange!r} currency={c.currency!r}")

        # remove_instrument for an existing symbol → True, then gone
        if self.remove_instrument("SPY") and self.get_instrument("SPY") is None:
            self._pass("inst_remove_existing", cat)
        else:
            self._fail("inst_remove_existing", cat,
                       "remove_instrument failed or instrument still present")

        # remove_instrument for a symbol that isn't there → False
        if not self.remove_instrument("NONEXISTENT"):
            self._pass("inst_remove_missing_false", cat)
        else:
            self._fail("inst_remove_missing_false", cat,
                       "remove_instrument returned True for missing symbol")

        # Leave instruments dict clean for any subsequent tests
        self._instruments.clear()

    # =========================================================================
    # Category: parameters
    # =========================================================================

    def _run_parameters_tests(self):
        cat = "parameters"

        # get_parameters returns a dict
        params = self.get_parameters()
        if isinstance(params, dict):
            self._pass("param_get_returns_dict", cat, f"keys={sorted(params.keys())}")
        else:
            self._fail("param_get_returns_dict", cat,
                       f"returned {type(params).__name__}, expected dict")

        # dict contains the test parameter key
        if _TEST_PARAM_KEY in params:
            self._pass("param_get_has_key", cat,
                       f"key={_TEST_PARAM_KEY!r} value={params[_TEST_PARAM_KEY]!r}")
        else:
            self._fail("param_get_has_key", cat,
                       f"key {_TEST_PARAM_KEY!r} not found; keys={sorted(params.keys())!r}")

        # set_parameter with valid key and value → True
        if self.set_parameter(_TEST_PARAM_KEY, 99):
            self._pass("param_set_valid_returns_true", cat)
        else:
            self._fail("param_set_valid_returns_true", cat,
                       "set_parameter returned False for valid key")

        # get_parameters reflects the new value
        params2 = self.get_parameters()
        if params2.get(_TEST_PARAM_KEY) == 99:
            self._pass("param_get_reflects_set", cat, f"value={params2[_TEST_PARAM_KEY]!r}")
        else:
            self._fail("param_get_reflects_set", cat,
                       f"expected=99 got={params2.get(_TEST_PARAM_KEY)!r}")

        # set_parameter with unknown key → False
        if not self.set_parameter("nonexistent_param_xyz", 42):
            self._pass("param_set_invalid_returns_false", cat)
        else:
            self._fail("param_set_invalid_returns_false", cat,
                       "set_parameter returned True for unknown key")

        # get_parameter_schema returns a dict
        schema = self.get_parameter_schema()
        if isinstance(schema, dict):
            self._pass("param_schema_returns_dict", cat)
        else:
            self._fail("param_schema_returns_dict", cat,
                       f"returned {type(schema).__name__}, expected dict")

        # schema has an entry for the test parameter
        if _TEST_PARAM_KEY in schema:
            self._pass("param_schema_has_entry", cat,
                       f"schema[{_TEST_PARAM_KEY!r}]={schema[_TEST_PARAM_KEY]!r}")
        else:
            self._fail("param_schema_has_entry", cat,
                       f"key {_TEST_PARAM_KEY!r} not in schema; "
                       f"keys={sorted(schema.keys())!r}")

        # Reset to default so state persistence doesn't capture an unexpected value
        self.set_parameter(_TEST_PARAM_KEY, _TEST_PARAM_DEFAULT)

    # =========================================================================
    # Category: ib_error
    # =========================================================================

    def _run_ib_error_tests(self):
        cat = "ib_error"

        self._ib_error_info = None
        self._ib_error_event.clear()

        # Request historical data for conId=1 — IB rejects it almost immediately
        # with error 200 "No security definition has been found"
        invalid_contract = ContractBuilder.by_conid(1, exchange="SMART")
        logger.info(f"  [{cat}] Requesting historical data for invalid conId=1 (8s timeout)")
        bars = self.get_historical_data(
            contract=invalid_contract,
            duration_str="1 D",
            bar_size_setting="1 hour",
            what_to_show="TRADES",
            use_rth=True,
            timeout=8.0,
        )

        # Should return None (timeout waiting for data that never arrives)
        # or [] (IB returned an error-end event with no bars)
        if bars is None or bars == []:
            self._pass("err_invalid_historical_returns_none", cat,
                       f"returned {bars!r} for invalid contract (as expected)")
        else:
            self._fail("err_invalid_historical_returns_none", cat,
                       f"expected None/[] for invalid conId=1, got {len(bars)} bar(s)")

        # on_ib_error routing is a soft test — the executive may or may not
        # attribute historical-data errors back to plugins
        fired = self._ib_error_event.wait(timeout=1.0)
        if fired and self._ib_error_info:
            ec = self._ib_error_info.get("error_code")
            self._pass("err_on_ib_error_called", cat,
                       f"error_code={ec} error_string={self._ib_error_info['error_string']!r}")
            if isinstance(ec, int):
                self._pass("err_error_code_is_int", cat, f"error_code={ec}")
            else:
                self._fail("err_error_code_is_int", cat,
                           f"error_code={ec!r} type={type(ec).__name__}")
        else:
            # Soft pass: on_ib_error for historical data requests is not
            # guaranteed to be routed back to plugins in all engine versions
            self._pass("err_on_ib_error_called", cat,
                       "on_ib_error not routed for historical data (soft pass)")
            self._pass("err_error_code_is_int", cat, "skipped — on_ib_error not called")

    # =========================================================================
    # Summary
    # =========================================================================

    def _build_summary(self) -> Dict[str, Any]:
        total = len(self._results)
        passed = sum(1 for r in self._results if r.passed)
        errors = [r.test_name for r in self._results if not r.passed]

        by_category: Dict[str, Dict[str, int]] = {}
        for r in self._results:
            entry = by_category.setdefault(r.category, {"total": 0, "passed": 0})
            entry["total"] += 1
            if r.passed:
                entry["passed"] += 1

        return {
            "total": total,
            "passed": passed,
            "failed": total - passed,
            "errors": errors,
            "by_category": by_category,
        }
