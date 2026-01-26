#!/usr/bin/env python3
"""
ibctl.py - Command-line client for IB Portfolio Rebalancer

Send commands to a running main.py or run_engine.py instance via Unix socket.

Usage:
    ./ibctl.py status                          # Get portfolio status
    ./ibctl.py positions                       # List all positions
    ./ibctl.py summary                         # Executive account summary
    ./ibctl.py liquidate                       # Preview liquidation (dry run)
    ./ibctl.py liquidate --confirm             # Execute full liquidation
    ./ibctl.py sell SPY 10                     # Preview selling 10 shares of SPY
    ./ibctl.py sell SPY all --confirm          # Sell entire SPY position
    ./ibctl.py buy SPY 10 --confirm            # Buy 10 shares of SPY
    ./ibctl.py trade PLUGIN BUY SPY 100        # Preview plugin-attributed trade
    ./ibctl.py trade PLUGIN BUY SPY 100 --confirm  # Execute plugin-attributed trade
    ./ibctl.py plugin list                     # List all plugins
    ./ibctl.py plugin status NAME              # Get plugin status
    ./ibctl.py algo list                       # List all algorithms
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

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status.value,
            "message": self.message,
            "data": self.data,
        }


def send_command(
    command: str,
    socket_path: str = DEFAULT_SOCKET_PATH,
    timeout: float = 10.0,
    token: Optional[str] = None,
) -> CommandResult:
    """
    Send a command to the running server.

    Args:
        command: Command string to send
        socket_path: Path to Unix socket
        timeout: Connection timeout in seconds
        token: Authentication token (if server requires auth)

    Returns:
        CommandResult from server
    """
    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(socket_path)
        sock.settimeout(timeout)

        # Wrap command with auth token if provided
        if token:
            full_command = f"AUTH {token} {command}"
        else:
            full_command = command

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

  pause                Pause algorithm/plugin execution
  resume               Resume algorithm/plugin execution
  stop                 Shutdown the server gracefully

Examples:
  ./ibctl.py status
  ./ibctl.py positions
  ./ibctl.py summary
  ./ibctl.py summary --json
  ./ibctl.py liquidate
  ./ibctl.py liquidate --confirm
  ./ibctl.py sell SPY 10
  ./ibctl.py sell SPY all --confirm
  ./ibctl.py trade momentum_5day BUY SPY 100
  ./ibctl.py trade momentum_5day BUY SPY 100 --confirm
  ./ibctl.py trade manual SELL QQQ 50 --confirm --reason "Taking profits"
  ./ibctl.py plugin list
  ./ibctl.py plugin status momentum_5day
  ./ibctl.py plugin trigger momentum_5day
  ./ibctl.py algo list
  ./ibctl.py stop
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
