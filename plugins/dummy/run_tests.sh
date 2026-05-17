#!/usr/bin/env bash
# Run unit tests for this plugin.
# Usage: ./run_tests.sh [pytest-args...]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="${PYTHON:-python3}"

# cd to the plugin directory so pytest anchors on pytest.ini here
# rather than walking up to the project-root pyproject.toml.
cd "$SCRIPT_DIR"

exec $PYTHON -m pytest \
    --import-mode=importlib \
    "${@:-tests}"
