"""
Pytest configuration for IB Portfolio tests.

This file ensures the package can be imported correctly during tests.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock

# Add the project root to Python path so we can import ib as a package
package_root = Path(__file__).parent.parent
if str(package_root) not in sys.path:
    sys.path.insert(0, str(package_root))


# Mock ibapi before any other imports
_ibapi_mock = MagicMock()
_ibapi_mock.ticktype.TickTypeEnum.LAST = 4
_ibapi_mock.ticktype.TickTypeEnum.CLOSE = 9
_ibapi_mock.ticktype.TickTypeEnum.DELAYED_LAST = 68
_ibapi_mock.ticktype.TickTypeEnum.DELAYED_CLOSE = 75
_ibapi_mock.ticktype.TickTypeEnum.BID = 1
_ibapi_mock.ticktype.TickTypeEnum.ASK = 2


# Create a proper Contract class so each instance has separate state
class Contract:
    """Mock IB Contract class"""
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


# Create contract module mock with the real Contract class
_contract_mock = MagicMock()
_contract_mock.Contract = Contract

sys.modules['ibapi'] = _ibapi_mock
sys.modules['ibapi.client'] = MagicMock()
sys.modules['ibapi.wrapper'] = MagicMock()
sys.modules['ibapi.common'] = MagicMock()
sys.modules['ibapi.contract'] = _contract_mock
sys.modules['ibapi.order'] = MagicMock()
sys.modules['ibapi.ticktype'] = _ibapi_mock.ticktype
sys.modules['ibapi.account_summary_tags'] = MagicMock()
sys.modules['ibapi.execution'] = MagicMock()

# Now we can import the package and create module aliases
# This allows `from main import ...` style imports to work
from ib import models, const, command_server

# execution_db only exists in the inner ib/ package, load it directly
import importlib.util
_ed_spec = importlib.util.spec_from_file_location(
    "execution_db", str(package_root / "ib" / "execution_db.py"))
execution_db = importlib.util.module_from_spec(_ed_spec)
_ed_spec.loader.exec_module(execution_db)

sys.modules['models'] = models
sys.modules['const'] = const
sys.modules['command_server'] = command_server
sys.modules['execution_db'] = execution_db

# Import modules that depend on ibapi mocks
from ib import client, portfolio, rebalancer, main
from ib import data_feed, enter_exit, order_reconciler, trading_engine
from ib import auth, connection_manager, contract_builder, order_builder, algo_params
from ib import rate_limiter, security_pool, plugin_executive

sys.modules['client'] = client
sys.modules['portfolio'] = portfolio
sys.modules['rebalancer'] = rebalancer
sys.modules['main'] = main
sys.modules['data_feed'] = data_feed
sys.modules['enter_exit'] = enter_exit
sys.modules['order_reconciler'] = order_reconciler
sys.modules['trading_engine'] = trading_engine
sys.modules['auth'] = auth
sys.modules['connection_manager'] = connection_manager
sys.modules['contract_builder'] = contract_builder
sys.modules['order_builder'] = order_builder
sys.modules['algo_params'] = algo_params
sys.modules['rate_limiter'] = rate_limiter
sys.modules['security_pool'] = security_pool
sys.modules['plugin_executive'] = plugin_executive

# ibctl is at root level only (not in inner ib/ package)
from ib import ibctl
sys.modules['ibctl'] = ibctl
