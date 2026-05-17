"""
Pytest configuration for plugins/orders tests.

Sets up ibapi mocks and sys.path so the plugin can be imported without a
real IB TWS installation.  Path resolution:
    conftest.py -> tests/ -> orders/ -> plugins/ -> project_root/
"""
import sys
import pytest
from pathlib import Path
from unittest.mock import MagicMock

# Add project root so `import ib` and `import plugins` resolve correctly.
project_root = Path(__file__).resolve().parents[3]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# ---------------------------------------------------------------------------
# ibapi mock — must be installed before any plugin import.
# Mock ibapi and every submodule that the ib package imports transitively so
# we never fall through to the real pythonclient/ installation.
# EWrapper and EClient must be real classes (not MagicMock attributes) so
# that `class IBClient(EWrapper, EClient)` in ib/client.py resolves without
# a metaclass conflict.
# ---------------------------------------------------------------------------
_ibapi_mock = MagicMock()
_ibapi_mock.ticktype.TickTypeEnum.LAST = 4
_ibapi_mock.ticktype.TickTypeEnum.CLOSE = 9
_ibapi_mock.ticktype.TickTypeEnum.DELAYED_LAST = 68
_ibapi_mock.ticktype.TickTypeEnum.DELAYED_CLOSE = 75
_ibapi_mock.ticktype.TickTypeEnum.BID = 1
_ibapi_mock.ticktype.TickTypeEnum.ASK = 2
_ibapi_mock.ticktype.TickTypeEnum.BID_SIZE = 0
_ibapi_mock.ticktype.TickTypeEnum.ASK_SIZE = 3
_ibapi_mock.ticktype.TickTypeEnum.LAST_SIZE = 5
_ibapi_mock.ticktype.TickTypeEnum.VOLUME = 8
_ibapi_mock.ticktype.TickTypeEnum.DELAYED_BID_SIZE = 69
_ibapi_mock.ticktype.TickTypeEnum.DELAYED_ASK_SIZE = 70
_ibapi_mock.ticktype.TickTypeEnum.DELAYED_LAST_SIZE = 71
_ibapi_mock.ticktype.TickTypeEnum.DELAYED_VOLUME = 74


class _Contract:
    """Minimal IB Contract with independent per-instance state."""
    def __init__(self):
        self.symbol = ""
        self.secType = ""
        self.exchange = ""
        self.currency = ""
        self.conId = 0
        self.primaryExchange = ""
        self.localSymbol = ""
        self.tradingClass = ""
        self.lastTradeDateOrContractMonth = ""
        self.strike = 0.0
        self.right = ""
        self.multiplier = ""


class _EWrapper:
    """Minimal EWrapper stub — real class so IBClient can inherit from it."""
    def __init__(self): pass


class _EClient:
    """Minimal EClient stub — real class so IBClient can inherit from it."""
    def __init__(self, wrapper=None): pass


_contract_mock = MagicMock()
_contract_mock.Contract = _Contract

_wrapper_mock = MagicMock()
_wrapper_mock.EWrapper = _EWrapper

_client_mock = MagicMock()
_client_mock.EClient = _EClient

# Top-level ibapi package
sys.modules["ibapi"] = _ibapi_mock

# All submodules the ib package imports (directly or transitively).
# client and wrapper get special mocks so EClient/EWrapper are real classes.
_ibapi_submods = [
    "common", "contract", "order", "ticktype",
    "account_summary_tags", "execution", "decoder", "message", "comm",
    "server_versions", "tag_value", "order_cancel", "comboleg",
    "connection", "reader", "errors", "utils",
]
for _sub in _ibapi_submods:
    sys.modules.setdefault(f"ibapi.{_sub}", MagicMock())

sys.modules["ibapi.client"] = _client_mock
sys.modules["ibapi.wrapper"] = _wrapper_mock
# Override contract mock so instances have independent state
sys.modules["ibapi.contract"] = _contract_mock
sys.modules["ibapi.ticktype"] = _ibapi_mock.ticktype

# Pre-import ib.plugin_store (used by the autouse fixture below) so the ib
# package's __init__.py is processed once, with all mocks already in place.
import ib.plugin_store  # noqa: E402


# ---------------------------------------------------------------------------
# Plugin store isolation — a fresh SQLite DB for every test
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _isolated_plugin_store(tmp_path, monkeypatch):
    import ib.plugin_store as ps
    test_store = ps.PluginStore(db_path=tmp_path / "plugin_store.db")
    monkeypatch.setattr(ps, "_plugin_store", test_store)
    yield test_store
    monkeypatch.setattr(ps, "_plugin_store", None)
