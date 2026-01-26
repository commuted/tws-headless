#!/usr/bin/env python3
"""
ibctl.py - Command-line client for IB Portfolio Rebalancer

Send commands to a running main.py instance via Unix socket.

Usage:
    ibctl status                          # Get portfolio status
    ibctl positions                       # List all positions
    ibctl summary                         # Executive account summary
    ibctl liquidate                       # Preview liquidation (dry run)
    ibctl liquidate --confirm             # Execute full liquidation
    ibctl sell SPY 10                     # Preview selling 10 shares of SPY
    ibctl sell SPY all --confirm          # Sell entire SPY position
    ibctl buy SPY 10 --confirm            # Buy 10 shares of SPY
    ibctl trade PLUGIN BUY SPY 100        # Preview plugin-attributed trade
    ibctl trade PLUGIN BUY SPY 100 --confirm  # Execute plugin-attributed trade
    ibctl plugin list                     # List all plugins
    ibctl plugin status NAME              # Get plugin status
    ibctl algo list                       # List all algorithms
    ibctl stop                            # Shutdown the server
"""

import argparse
import json
import sys
from .command_server import send_command, DEFAULT_SOCKET_PATH, CommandStatus


def format_result(result, verbose: bool = False):
    """Format the command result for display"""
    # Status indicator
    if result.status == CommandStatus.SUCCESS:
        status_str = "[OK]"
    elif result.status == CommandStatus.ERROR:
        status_str = "[ERROR]"
    else:
        status_str = "[PENDING]"

    print(f"{status_str} {result.message}")

    # Print data in verbose mode or for specific commands
    if verbose and result.data:
        print("\nData:")
        print(json.dumps(result.data, indent=2))
    elif result.data:
        # Special formatting for certain data types
        if "positions" in result.data:
            positions = result.data["positions"]
            if positions:
                print(f"\n{'Symbol':<8} {'Qty':>10} {'Price':>12} {'Value':>14} {'P&L':>12} {'Alloc':>8}")
                print("-" * 70)
                for p in positions:
                    print(f"{p['symbol']:<8} {p['quantity']:>10,.0f} "
                          f"${p['price']:>10,.2f} ${p['value']:>12,.2f} "
                          f"${p['pnl']:>10,.2f} {p['allocation']:>7.1f}%")


def main():
    parser = argparse.ArgumentParser(
        description="Command-line client for IB Portfolio Rebalancer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  status               Get portfolio status
  positions            List all positions with details
  summary [--json]     Executive account summary with plugin breakdown
  summary plugins      Show only plugin holdings
  summary unassigned   Show only unassigned holdings

  sell SYMBOL QTY      Sell shares (use 'all' for entire position, --confirm to execute)
  buy SYMBOL QTY       Buy shares (--confirm to execute)
  liquidate [SYMBOL]   Liquidate positions (add --confirm to execute)

  trade PLUGIN ACTION SYMBOL QTY [--confirm] [--reason "text"]
                       Execute trade with plugin attribution
                       ACTION: BUY or SELL

  plugin list          List all plugins
  plugin status NAME   Get plugin status
  plugin start NAME    Start a plugin
  plugin stop NAME     Stop a plugin
  plugin freeze NAME   Freeze a plugin (pause with state save)
  plugin resume NAME   Resume a frozen plugin
  plugin enable NAME   Enable plugin for execution
  plugin disable NAME  Disable plugin
  plugin trigger NAME  Manually trigger plugin run
  plugin params NAME   Get plugin parameters
  plugin param NAME KEY VALUE  Set plugin parameter

  algo list            List all algorithms
  algo status NAME     Get algorithm status
  algo enable NAME     Enable algorithm
  algo disable NAME    Disable algorithm
  algo trigger NAME    Manually trigger algorithm

  stop                 Shutdown the server gracefully

Examples:
  ibctl status
  ibctl positions
  ibctl summary
  ibctl summary --json
  ibctl liquidate
  ibctl liquidate --confirm
  ibctl sell SPY 10
  ibctl sell SPY all --confirm
  ibctl trade momentum_5day BUY SPY 100
  ibctl trade momentum_5day BUY SPY 100 --confirm
  ibctl trade manual SELL QQQ 50 --confirm --reason "Taking profits"
  ibctl plugin list
  ibctl plugin status momentum_5day
  ibctl plugin trigger momentum_5day
  ibctl algo list
  ibctl stop
        """,
    )

    parser.add_argument(
        "command",
        nargs="*",
        help="Command and arguments to send",
    )
    parser.add_argument(
        "--socket", "-s",
        default=DEFAULT_SOCKET_PATH,
        help=f"Socket path (default: {DEFAULT_SOCKET_PATH})",
    )
    parser.add_argument(
        "--timeout", "-t",
        type=float,
        default=10.0,
        help="Timeout in seconds (default: 10)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show detailed output",
    )
    parser.add_argument(
        "--json", "-j",
        action="store_true",
        help="Output raw JSON response",
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    # Build command string
    command_str = " ".join(args.command)

    # Send command
    result = send_command(
        command=command_str,
        socket_path=args.socket,
        timeout=args.timeout,
    )

    # Output result
    if args.json:
        print(json.dumps(result.to_dict(), indent=2))
    else:
        format_result(result, verbose=args.verbose)

    # Exit with error code if command failed
    if result.status == CommandStatus.ERROR:
        sys.exit(1)


if __name__ == "__main__":
    main()
