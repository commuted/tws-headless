#!/bin/bash
#
# start_trading.sh - Start the IB Trading Engine
#
# Usage:
#   ./start_trading.sh              # Default: paper trading on port 7497
#   ./start_trading.sh 4002         # Use IB Gateway paper port
#   ./start_trading.sh 7497 immediate  # Actually place paper orders
#   ./start_trading.sh 7497 dry_run /path/to/plugins  # Custom plugin directory
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export PORT="${1:-7497}"
export MODE="${2:-dry_run}"
export IB_PLUGIN_DIR="${3:-$SCRIPT_DIR/plugins}"

# Run from project root (where ib/ and plugins/ both live)
cd "$SCRIPT_DIR"
exec python3 -m ib.run_engine --plugins
