#!/usr/bin/env python3
"""
ibctl.py - Command-line client for IB Portfolio Rebalancer

Send commands to a running main.py or run_engine.py instance via Unix socket.

Usage:
    ./ibctl.py status                          # Get portfolio status
    ./ibctl.py positions                       # List all positions
    ./ibctl.py summary                         # Account summary with plugin breakdown
    ./ibctl.py summary --json                  # Account summary as JSON

    # Simple orders (market only)
    ./ibctl.py sell SPY 10                     # Preview selling 10 shares of SPY
    ./ibctl.py sell SPY all --confirm          # Sell entire SPY position
    ./ibctl.py buy SPY 10 --confirm            # Buy 10 shares of SPY
    ./ibctl.py liquidate --confirm             # Liquidate all positions

    # Advanced orders (all IB order types)
    ./ibctl.py order buy SPY 100               # Market order (dry run)
    ./ibctl.py order buy SPY 100 --confirm     # Market order (execute)
    ./ibctl.py order buy SPY 100 limit 450     # Limit order at $450
    ./ibctl.py order sell QQQ 50 stop 380      # Stop order at $380
    ./ibctl.py order buy AAPL 25 stop-limit 175 170   # Stop-limit order
    ./ibctl.py order sell MSFT 30 trail 2.00   # Trailing stop $2
    ./ibctl.py order sell MSFT 30 trail 1%     # Trailing stop 1%
    ./ibctl.py order buy SPY 100 moc           # Market on Close
    ./ibctl.py order sell QQQ 50 loc 380       # Limit on Close

    # Plugin-attributed trades
    ./ibctl.py trade PLUGIN BUY SPY 100        # Preview plugin-attributed trade
    ./ibctl.py trade PLUGIN BUY SPY 100 --confirm  # Execute trade

    # Internal transfers (bookkeeping only, no actual trades)
    ./ibctl.py transfer list _unassigned       # Show transferable assets
    ./ibctl.py transfer cash _unassigned momentum_5day 10000 --confirm
    ./ibctl.py transfer position _unassigned momentum_5day SPY 50 --confirm

    # Plugin/algorithm management
    ./ibctl.py plugin list                     # List all plugins
    ./ibctl.py plugin load PATH                # Load plugin from file
    ./ibctl.py plugin load PATH DESCRIPTOR     # Load with descriptor
    ./ibctl.py plugin unload NAME_OR_ID        # Unload a plugin
    ./ibctl.py plugin status NAME_OR_ID        # Get plugin status
    ./ibctl.py plugin dump NAME_OR_ID          # Dump positions & open orders
    ./ibctl.py pause                           # Pause execution
    ./ibctl.py resume                          # Resume execution
    ./ibctl.py stop                            # Shutdown the server
"""

import argparse
import json
import socket
import sys
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Any, Optional


# Default socket path - must match command_server.py
DEFAULT_SOCKET_PATH = "/tmp/ib_portfolio.sock"


class CommandStatus(Enum):
    """Command execution status"""
    SUCCESS = "success"
    ERROR = "error"
    PENDING = "pending"
    UNAUTHORIZED = "unauthorized"


@dataclass
class CommandResult:
    """Result of a command execution"""
    status: CommandStatus
    message: str = ""
    data: Dict[str, Any] = field(default_factory=dict)
    request_token: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "status": self.status.value,
            "message": self.message,
            "data": self.data,
        }
        if self.request_token is not None:
            result["request_token"] = self.request_token
        return result


def send_command(
    command: str,
    socket_path: str = DEFAULT_SOCKET_PATH,
    timeout: float = 10.0,
    token: Optional[str] = None,
    request_token: Optional[str] = None,
) -> CommandResult:
    """
    Send a command to the running server.

    Args:
        command: Command string to send
        socket_path: Path to Unix socket
        timeout: Connection timeout in seconds
        token: Authentication token (if server requires auth)
        request_token: Optional request token for tracking/dedup

    Returns:
        CommandResult from server
    """
    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(socket_path)
        sock.settimeout(timeout)

        # Build wire command: REQ first, then AUTH, then command
        full_command = command
        if token:
            full_command = f"AUTH {token} {full_command}"
        if request_token:
            full_command = f"REQ {request_token} {full_command}"

        # Send command
        sock.sendall((full_command + "\n").encode("utf-8"))

        # Receive response
        data = b""
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            data += chunk
            if b"\n" in data:
                break

        sock.close()

        # Parse response
        response = json.loads(data.decode("utf-8").strip())
        return CommandResult(
            status=CommandStatus(response.get("status", "error")),
            message=response.get("message", ""),
            data=response.get("data", {}),
            request_token=response.get("request_token"),
        )

    except FileNotFoundError:
        return CommandResult(
            status=CommandStatus.ERROR,
            message=f"Server not running (socket not found: {socket_path})",
        )
    except ConnectionRefusedError:
        return CommandResult(
            status=CommandStatus.ERROR,
            message="Connection refused - server may not be running",
        )
    except Exception as e:
        return CommandResult(
            status=CommandStatus.ERROR,
            message=f"Failed to send command: {e}",
        )


def format_result(result: CommandResult, verbose: bool = False):
    """Format the command result for display"""
    # Status indicator
    if result.status == CommandStatus.SUCCESS:
        status_str = "[OK]"
    elif result.status == CommandStatus.ERROR:
        status_str = "[ERROR]"
    elif result.status == CommandStatus.UNAUTHORIZED:
        status_str = "[UNAUTHORIZED]"
    else:
        status_str = "[PENDING]"

    print(f"{status_str} {result.message}")

    if result.request_token is not None:
        print(f"Request-Token: {result.request_token}")

    # Print data in verbose mode or for specific commands
    if verbose and result.data:
        print("\nData:")
        print(json.dumps(result.data, indent=2))
    elif result.data:
        # Special formatting for certain data types
        if "instance_id" in result.data and "plugin_name" in result.data:
            # Plugin load response
            print(f"\n  Plugin:      {result.data['plugin_name']}")
            print(f"  Instance ID: {result.data['instance_id']}")
            if result.data.get("descriptor") is not None:
                print(f"  Descriptor:  {result.data['descriptor']}")
            if result.data.get("path"):
                print(f"  Path:        {result.data['path']}")

        elif "open_orders" in result.data:
            # Plugin dump output
            cash = result.data.get("cash", 0.0)
            positions = result.data.get("positions", [])
            open_orders = result.data["open_orders"]

            print(f"\n  Cash: ${cash:,.2f}")

            if positions:
                print(f"\n  {'Symbol':<8} {'Qty':>10} {'Cost Basis':>12} {'Price':>12} {'Value':>14}")
                print(f"  {'-' * 60}")
                for p in positions:
                    print(f"  {p['symbol']:<8} {p['quantity']:>10,.0f} "
                          f"${p.get('cost_basis', 0):>10,.2f} "
                          f"${p.get('current_price', 0):>10,.2f} "
                          f"${p.get('market_value', 0):>12,.2f}")
            else:
                print("\n  Positions: (none)")

            if open_orders:
                print(f"\n  {'Order ID':>10} {'Symbol':<8} {'Action':<6} {'Qty':>8} {'Status':<10} {'Created'}")
                print(f"  {'-' * 70}")
                for o in open_orders:
                    created = o.get("created_at", "")[:19]
                    print(f"  {o['order_id']:>10} {o['symbol']:<8} {o['action']:<6} "
                          f"{o['quantity']:>8} {o['status']:<10} {created}")
            else:
                print("\n  Open orders: (none)")

        elif "positions" in result.data:
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
  summary [--json]     Account summary with plugin breakdown

  Simple orders (market only, requires existing position):
  sell SYMBOL QTY      Sell shares (use 'all' for entire position, --confirm to execute)
  buy SYMBOL QTY       Buy shares (--confirm to execute)
  liquidate [SYMBOL]   Liquidate positions (add --confirm to execute)

  Advanced orders (all IB order types):
  order ACTION SYMBOL QTY [TYPE] [options] [--confirm]
                       ACTION: buy or sell
                       TYPE: market (default), limit PRICE, stop PRICE,
                             stop-limit STOP LIMIT, trail AMOUNT|PERCENT,
                             moc, loc PRICE, moo, loo PRICE
                       --tif TIF: day (default), gtc, ioc, fok

  Plugin-attributed trades:
  trade PLUGIN ACTION SYMBOL QTY [--confirm] [--reason "text"]
                       Execute trade with plugin attribution

  Internal transfers (bookkeeping only, no actual trades):
  transfer cash FROM TO AMOUNT [--confirm]
                       Transfer cash between plugins
  transfer position FROM TO SYMBOL QTY [--confirm]
                       Transfer position between plugins
  transfer list PLUGIN Show transferable assets in a plugin

  Plugin commands (require server started with --plugins):
  All NAME_OR_ID args accept a plugin name or instance_id (UUID).
  Use instance_id to target a specific instance when multiple
  instances of the same plugin are loaded.

  plugin list                     List all plugins
  plugin load PATH [DESCRIPTOR]   Load a plugin from file (returns instance_id)
  plugin unload NAME_OR_ID        Unload a plugin
  plugin status NAME_OR_ID        Get plugin status
  plugin start NAME_OR_ID         Start a plugin
  plugin stop NAME_OR_ID          Stop a plugin
  plugin freeze NAME_OR_ID        Freeze a plugin (pause with state save)
  plugin resume NAME_OR_ID        Resume a frozen plugin
  plugin enable NAME_OR_ID        Enable plugin for execution
  plugin disable NAME_OR_ID       Disable plugin
  plugin trigger NAME_OR_ID       Manually trigger plugin run
  plugin dump NAME_OR_ID          Dump plugin positions and open orders

  algo list            List all algorithms
  algo status NAME     Get algorithm status
  algo enable NAME     Enable algorithm
  algo disable NAME    Disable algorithm
  algo trigger NAME    Manually trigger algorithm

  reconcile            Sync plugin holdings with IB account
  pause                Pause algorithm/plugin execution
  resume               Resume algorithm/plugin execution
  stop                 Shutdown the server gracefully

Examples:
  ./ibctl.py status
  ./ibctl.py positions
  ./ibctl.py summary --json
  ./ibctl.py order buy SPY 100                      # Market order (dry run)
  ./ibctl.py order buy SPY 100 --confirm            # Market order (execute)
  ./ibctl.py order buy SPY 100 limit 450.00         # Limit order
  ./ibctl.py order sell QQQ 50 stop 380             # Stop order
  ./ibctl.py order buy AAPL 25 stop-limit 175 170   # Stop-limit order
  ./ibctl.py order sell MSFT 30 trail 2.00          # Trailing stop $2
  ./ibctl.py order buy SPY 100 moc --confirm        # Market on Close
  ./ibctl.py order buy SPY 100 limit 450 --tif gtc  # Good till cancelled
  ./ibctl.py trade momentum_5day BUY SPY 100 --confirm
  ./ibctl.py transfer list _unassigned              # Show transferable assets
  ./ibctl.py transfer cash _unassigned momentum_5day 10000 --confirm
  ./ibctl.py transfer position _unassigned momentum_5day SPY 50 --confirm
  ./ibctl.py plugin list
  ./ibctl.py plugin load /path/to/my_strategy/plugin.py
  ./ibctl.py plugin load /path/to/plugin.py '{"symbol": "AAPL"}'
  ./ibctl.py plugin status momentum_5day
  ./ibctl.py plugin stop 78b052d2-17e7-4ee5-ac03-282e0cd05c2b
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
    parser.add_argument(
        "--token",
        help="Authentication token (if server requires auth)",
    )
    parser.add_argument(
        "--request-token",
        help="Request token for tracking/deduplication",
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
        token=args.token,
        request_token=args.request_token,
    )

    # Output result
    if args.json:
        print(json.dumps(result.to_dict(), indent=2))
    else:
        format_result(result, verbose=args.verbose)

    # Exit with error code if command failed
    if result.status in (CommandStatus.ERROR, CommandStatus.UNAUTHORIZED):
        sys.exit(1)


if __name__ == "__main__":
    main()
