#!/usr/bin/env bash
#
# run_tests.sh — Test runner for the tws-headless / ib trading engine
#
# SYNOPSIS
#   ./run_tests.sh [pytest-args...]
#
# DESCRIPTION
#   Runs the full test suite (or a filtered subset) with the import environment
#   that the tests require.  All extra arguments are forwarded to pytest, so you
#   can use any standard pytest flag or test-path filter.
#
# IMPORT BOOTSTRAP
#   This project lives in the directory 'tws-headless', but the Python package
#   name used throughout the codebase (and in all imports) is 'ib'.  pytest
#   cannot resolve 'import ib' just by having the project root on sys.path —
#   the directory isn't named 'ib'.
#
#   This script creates a short-lived temporary directory, places an 'ib' symlink
#   inside it that points to the project root, then exports IB_PKG_PARENT so that
#   conftest.py can prepend that directory to sys.path.  The temp dir is cleaned
#   up automatically on exit regardless of success or failure.
#
# ROOTDIR
#   pytest is invoked with --rootdir=tests/ to prevent it from treating the project
#   root as a Python package and attempting to import __init__.py as a standalone
#   module (which would fail because __init__.py uses relative imports).
#
# IMPORT MODE
#   --import-mode=importlib prevents pytest from prepending test directories to
#   sys.path in a way that would shadow the 'ib' package registered by conftest.py.
#
# EXAMPLES
#   Run all tests:
#     ./run_tests.sh
#
#   Run a single test file:
#     ./run_tests.sh tests/test_plugin_execution_log.py
#
#   Run a single test class:
#     ./run_tests.sh tests/test_plugin_execution_log.py::TestExecutionLogReader
#
#   Run a single test:
#     ./run_tests.sh tests/test_models.py::TestPosition::test_default_values
#
#   Run with verbose output:
#     ./run_tests.sh -v
#
#   Run only fast tests (skip slow markers if defined):
#     ./run_tests.sh -m "not slow"
#
#   Show test coverage:
#     ./run_tests.sh --cov=. --cov-report=term-missing
#
#   Stop on first failure:
#     ./run_tests.sh -x
#
# KNOWN PRE-EXISTING FAILURES
#   None. The suite is green as of 2026-08-04 — a failure here is yours.
#
#   This section previously listed test_plugin_execution_log.py's commission
#   apportionment test (float precision) and "several" test_main.py tests as
#   expected failures. Both now pass, and the entries had outlived the problems
#   by long enough that the list was teaching readers to skim past real
#   breakage. Add something back only while it is actually failing, with the
#   reason and a way out.
#
# REQUIREMENTS
#   - Python 3.10+
#   - pytest (pip install pytest)
#   - ibapi  (pip install ibapi  or  pip install -e /path/to/TWS-API/source/pythonclient)
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---------------------------------------------------------------------------
# Temporary directory containing the 'ib' symlink
# ---------------------------------------------------------------------------
IB_TMP="$(mktemp -d)"
ln -s "$SCRIPT_DIR" "$IB_TMP/ib"

cleanup() {
    rm -rf "$IB_TMP"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Resolve the python / pytest binary
# ---------------------------------------------------------------------------
PYTHON="${PYTHON:-python3}"
PYTEST="$PYTHON -m pytest"

# ---------------------------------------------------------------------------
# Run pytest
# ---------------------------------------------------------------------------
# --rootdir=tests/
#   Sets pytest's rootdir to the tests sub-directory so pytest never tries to
#   import the project-root __init__.py as a standalone module during Package
#   node setup (which fails because of relative imports inside __init__.py).
#
# --import-mode=importlib
#   Uses importlib rather than sys.path prepending to import test modules,
#   which avoids shadowing the 'ib' package that conftest.py registers.
#
# IB_PKG_PARENT
#   Picked up by conftest.py to register the 'ib' package and bare-name
#   aliases for each submodule.

IB_PKG_PARENT="$IB_TMP" \
$PYTEST \
    --rootdir="$SCRIPT_DIR/tests" \
    --import-mode=importlib \
    "${@:-$SCRIPT_DIR/tests}"
