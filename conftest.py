"""
pytest configuration for ib package tests

Import bootstrap: tws-headless IS the `ib` package but is not named 'ib', so pytest
cannot import it as `ib` by walking sys.path normally. run_tests.sh works around this
by creating a temporary directory containing an `ib` symlink that points here, then
setting IB_PKG_PARENT to that directory.

This conftest registers `ib` (and bare-name aliases for submodules) so that both import
styles used across the test suite work:
  from ib.plugin_execution_log import X   (qualified)
  from models import X                    (bare, used in older tests)
"""
import sys
import os

_ib_parent = os.environ.get('IB_PKG_PARENT')
if not _ib_parent:
    raise RuntimeError(
        "IB_PKG_PARENT is not set. Run tests via run_tests.sh or set "
        "IB_PKG_PARENT to a directory that contains an 'ib' symlink "
        "pointing to this project root."
    )

if _ib_parent not in sys.path:
    sys.path.insert(0, _ib_parent)

import importlib

import ib  # noqa: F401  — registers ib and all submodules in sys.modules

# Alias each submodule under its bare name so tests that write
# `from models import X` work alongside `from ib.models import X`.
# Modules not exported from __init__.py (e.g. ibctl) are imported explicitly
# here so the alias is available before any test function runs.
for _name in [
    'models', 'const', 'algorithms', 'rebalancer', 'portfolio',
    'client', 'auth', 'rate_limiter', 'security_pool', 'command_server',
    'main', 'message_bus', 'plugin_loader', 'plugin_executive',
    'plugin_execution_log', 'plugin_performance', 'trading_engine',
    'connection_manager', 'data_feed', 'algorithm_runner', 'order_reconciler',
    'enter_exit', 'contract_builder', 'order_builder', 'algo_params',
    'ibctl',
]:
    _fq = f'ib.{_name}'
    if _fq not in sys.modules:
        try:
            importlib.import_module(_fq)
        except ImportError:
            pass
    if _fq in sys.modules and _name not in sys.modules:
        sys.modules[_name] = sys.modules[_fq]
