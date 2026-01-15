#!/usr/bin/env python3
"""
main.py - Entry point for IB Portfolio Rebalancer

Usage:
    python main.py                    # Show portfolio
    python main.py --stream           # Stream live tick prices
    python main.py --bars             # Stream 5-second OHLCV bars
    python main.py --rebalance        # Calculate rebalance
    python main.py --execute          # Execute rebalance (requires confirmation)
"""

import argparse
import atexit
import logging
import signal
import sys
import time
from datetime import datetime
from threading import Event
from typing import List, Optional

from portfolio import Portfolio
from rebalancer import (
    Rebalancer,
    RebalanceConfig,
    create_60_40_targets,
    create_three_fund_targets,
    create_equal_weight_targets,
)
from models import TargetAllocation, AssetType, RebalanceStrategy, Bar, OrderAction
from command_server import (
    CommandServer,
    CommandResult,
    CommandStatus,
    DEFAULT_SOCKET_PATH,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# =============================================================================
# Shutdown Management
# =============================================================================

class ShutdownManager:
    """
    Manages graceful shutdown on SIGINT/SIGTERM.

    Requires 3 SIGINT signals within 10 seconds to initiate shutdown.
    This prevents accidental shutdowns from stray Ctrl+C presses.
    """

    REQUIRED_SIGNALS = 3
    RESET_TIMEOUT = 10.0  # seconds

    def __init__(self):
        self._shutdown_event = Event()
        self._portfolio: Optional[Portfolio] = None
        self._original_sigint = None
        self._original_sigterm = None
        self._sigint_count = 0
        self._first_sigint_time: Optional[float] = None
        self._shutdown_initiated = False

    @property
    def should_shutdown(self) -> bool:
        """Check if shutdown has been requested"""
        return self._shutdown_event.is_set()

    def register_portfolio(self, portfolio: Portfolio):
        """Register the portfolio instance for cleanup"""
        self._portfolio = portfolio

    def install_handlers(self):
        """Install signal handlers for SIGINT and SIGTERM"""
        self._original_sigint = signal.signal(signal.SIGINT, self._signal_handler)
        self._original_sigterm = signal.signal(signal.SIGTERM, self._signal_handler)
        atexit.register(self._cleanup)
        print(f"Press Ctrl+C {self.REQUIRED_SIGNALS} times within {self.RESET_TIMEOUT:.0f}s to shutdown")

    def restore_handlers(self):
        """Restore original signal handlers"""
        if self._original_sigint:
            signal.signal(signal.SIGINT, self._original_sigint)
        if self._original_sigterm:
            signal.signal(signal.SIGTERM, self._original_sigterm)

    def _signal_handler(self, signum, frame):
        """Handle SIGINT/SIGTERM signals"""
        current_time = time.time()
        sig_name = "SIGINT" if signum == signal.SIGINT else "SIGTERM"

        # SIGTERM always triggers immediate shutdown
        if signum == signal.SIGTERM:
            print(f"\n{sig_name} received. Shutting down...")
            self._initiate_shutdown()
            return

        # Check if we need to reset the counter (timeout expired)
        if self._first_sigint_time is not None:
            elapsed = current_time - self._first_sigint_time
            if elapsed > self.RESET_TIMEOUT:
                # Reset counter - timeout expired
                self._sigint_count = 0
                self._first_sigint_time = None

        # Increment counter
        self._sigint_count += 1

        # Record first signal time
        if self._sigint_count == 1:
            self._first_sigint_time = current_time

        # Calculate remaining time
        elapsed = current_time - self._first_sigint_time
        remaining_time = max(0, self.RESET_TIMEOUT - elapsed)
        remaining_signals = self.REQUIRED_SIGNALS - self._sigint_count

        if remaining_signals > 0:
            # Not enough signals yet
            print(f"\n{sig_name} ({self._sigint_count}/{self.REQUIRED_SIGNALS}) - "
                  f"Press Ctrl+C {remaining_signals} more time(s) within {remaining_time:.1f}s to shutdown")
        else:
            # Enough signals - initiate shutdown
            print(f"\n{sig_name} ({self._sigint_count}/{self.REQUIRED_SIGNALS}) - "
                  f"Shutdown confirmed. Shutting down gracefully...")
            self._initiate_shutdown()

    def _initiate_shutdown(self):
        """Initiate the shutdown sequence"""
        if self._shutdown_initiated:
            # Already shutting down, force exit on additional signals
            print("\nShutdown already in progress. Force exit.")
            sys.exit(1)

        self._shutdown_initiated = True
        self._shutdown_event.set()
        self._cleanup()

    def _cleanup(self):
        """Perform cleanup operations"""
        if self._portfolio:
            try:
                self._portfolio.shutdown()
            except Exception as e:
                logger.debug(f"Error during portfolio shutdown: {e}")

    def wait(self, timeout: float = None) -> bool:
        """
        Wait for shutdown signal.

        Args:
            timeout: Maximum time to wait (None = forever)

        Returns:
            True if shutdown was signaled, False if timeout
        """
        return self._shutdown_event.wait(timeout=timeout)

    def wait_interruptible(self, duration: float = 0, poll_interval: float = 0.1):
        """
        Wait for specified duration or until shutdown.

        Args:
            duration: Time to wait in seconds (0 = until shutdown)
            poll_interval: How often to check for shutdown
        """
        if duration > 0:
            start = time.time()
            while not self.should_shutdown:
                elapsed = time.time() - start
                if elapsed >= duration:
                    break
                remaining = min(poll_interval, duration - elapsed)
                time.sleep(remaining)
        else:
            while not self.should_shutdown:
                time.sleep(poll_interval)


# Global shutdown manager instance
shutdown_manager = ShutdownManager()


# =============================================================================
# Command Handler
# =============================================================================

class CommandHandler:
    """
    Handles commands received from the command server.

    Provides command implementations for controlling the portfolio
    via external socket commands.
    """

    def __init__(self, portfolio: Portfolio, shutdown_mgr: ShutdownManager):
        self.portfolio = portfolio
        self.shutdown_mgr = shutdown_mgr
        self._liquidation_in_progress = False

    def register_commands(self, server: CommandServer):
        """Register all command handlers with the server"""
        server.register_handler("status", self.handle_status)
        server.register_handler("positions", self.handle_positions)
        server.register_handler("liquidate", self.handle_liquidate)
        server.register_handler("stop", self.handle_stop)
        server.register_handler("shutdown", self.handle_stop)
        server.register_handler("sell", self.handle_sell)
        server.register_handler("buy", self.handle_buy)

    def handle_status(self, args: List[str]) -> CommandResult:
        """Handle 'status' command - return portfolio status"""
        try:
            positions = self.portfolio.positions
            total_value = self.portfolio.total_value
            total_pnl = self.portfolio.total_pnl

            account = self.portfolio.get_account_summary()
            account_data = {}
            if account and account.is_valid:
                account_data = {
                    "account_id": account.account_id,
                    "net_liquidation": account.net_liquidation,
                    "available_funds": account.available_funds,
                    "buying_power": account.buying_power,
                }

            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=f"Portfolio: ${total_value:,.2f} ({len(positions)} positions, P&L: ${total_pnl:,.2f})",
                data={
                    "total_value": total_value,
                    "total_pnl": total_pnl,
                    "position_count": len(positions),
                    "connected": self.portfolio.connected,
                    "streaming": self.portfolio.is_streaming,
                    "bar_streaming": self.portfolio.is_bar_streaming,
                    "account": account_data,
                },
            )
        except Exception as e:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Failed to get status: {e}",
            )

    def handle_positions(self, args: List[str]) -> CommandResult:
        """Handle 'positions' command - return detailed position list"""
        try:
            positions = self.portfolio.positions
            pos_data = []
            for pos in sorted(positions, key=lambda p: p.market_value, reverse=True):
                pos_data.append({
                    "symbol": pos.symbol,
                    "quantity": pos.quantity,
                    "price": pos.current_price,
                    "value": pos.market_value,
                    "pnl": pos.unrealized_pnl,
                    "allocation": pos.allocation_pct,
                })

            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=f"{len(positions)} positions",
                data={"positions": pos_data},
            )
        except Exception as e:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Failed to get positions: {e}",
            )

    def handle_liquidate(self, args: List[str]) -> CommandResult:
        """
        Handle 'liquidate' command - sell all positions.

        Usage:
            liquidate           - Liquidate all positions
            liquidate SYMBOL    - Liquidate specific symbol
            liquidate --confirm - Execute without dry-run
        """
        try:
            if self._liquidation_in_progress:
                return CommandResult(
                    status=CommandStatus.ERROR,
                    message="Liquidation already in progress",
                )

            confirm = "--confirm" in args
            symbols = [a for a in args if not a.startswith("--")]

            positions = self.portfolio.positions
            if not positions:
                return CommandResult(
                    status=CommandStatus.SUCCESS,
                    message="No positions to liquidate",
                )

            # Filter to specific symbols if provided
            if symbols:
                positions = [p for p in positions if p.symbol in symbols]
                if not positions:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message=f"Symbol(s) not found: {', '.join(symbols)}",
                    )

            # Calculate what would be sold
            total_value = sum(p.market_value for p in positions)
            sell_list = [(p.symbol, p.quantity, p.market_value) for p in positions]

            if not confirm:
                # Dry run - just show what would happen
                sell_info = ", ".join(f"{s[0]}:{s[1]:.0f}" for s in sell_list)
                return CommandResult(
                    status=CommandStatus.SUCCESS,
                    message=f"Would sell: {sell_info} (${total_value:,.2f}). Use --confirm to execute.",
                    data={
                        "dry_run": True,
                        "positions": [{"symbol": s, "qty": q, "value": v} for s, q, v in sell_list],
                        "total_value": total_value,
                    },
                )

            # Execute liquidation
            self._liquidation_in_progress = True
            logger.warning(f"LIQUIDATION: Selling {len(positions)} positions (${total_value:,.2f})")

            order_ids = []
            errors = []

            for pos in positions:
                if pos.contract and pos.quantity > 0:
                    order_id = self.portfolio.place_market_order(
                        contract=pos.contract,
                        action="SELL",
                        quantity=pos.quantity,
                    )
                    if order_id:
                        order_ids.append(order_id)
                        logger.info(f"Liquidate: SELL {pos.quantity} {pos.symbol} (order {order_id})")
                    else:
                        errors.append(f"Failed to place order for {pos.symbol}")

            self._liquidation_in_progress = False

            if errors:
                return CommandResult(
                    status=CommandStatus.ERROR,
                    message=f"Partial liquidation: {len(order_ids)} orders placed, {len(errors)} failed",
                    data={"order_ids": order_ids, "errors": errors},
                )

            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=f"Liquidation initiated: {len(order_ids)} sell orders placed",
                data={"order_ids": order_ids, "total_value": total_value},
            )

        except Exception as e:
            self._liquidation_in_progress = False
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Liquidation failed: {e}",
            )

    def handle_sell(self, args: List[str]) -> CommandResult:
        """
        Handle 'sell' command - sell a specific position.

        Usage:
            sell SYMBOL QTY         - Sell QTY shares of SYMBOL
            sell SYMBOL all         - Sell entire position
            sell SYMBOL QTY --confirm  - Execute without dry-run
        """
        if len(args) < 2:
            return CommandResult(
                status=CommandStatus.ERROR,
                message="Usage: sell SYMBOL QTY [--confirm]",
            )

        symbol = args[0].upper()
        qty_arg = args[1].lower()
        confirm = "--confirm" in args

        pos = self.portfolio.get_position(symbol)
        if not pos:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"No position in {symbol}",
            )

        if qty_arg == "all":
            quantity = pos.quantity
        else:
            try:
                quantity = float(qty_arg)
            except ValueError:
                return CommandResult(
                    status=CommandStatus.ERROR,
                    message=f"Invalid quantity: {qty_arg}",
                )

        if quantity > pos.quantity:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Cannot sell {quantity}, only have {pos.quantity}",
            )

        est_value = quantity * pos.current_price

        if not confirm:
            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=f"Would sell {quantity:.0f} {symbol} (~${est_value:,.2f}). Use --confirm to execute.",
                data={"dry_run": True, "symbol": symbol, "quantity": quantity, "est_value": est_value},
            )

        order_id = self.portfolio.place_market_order(
            contract=pos.contract,
            action="SELL",
            quantity=quantity,
        )

        if order_id:
            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=f"Sell order placed: {quantity:.0f} {symbol} (order {order_id})",
                data={"order_id": order_id, "symbol": symbol, "quantity": quantity},
            )
        else:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Failed to place sell order for {symbol}",
            )

    def handle_buy(self, args: List[str]) -> CommandResult:
        """
        Handle 'buy' command - buy shares of a symbol.

        Usage:
            buy SYMBOL QTY --confirm  - Buy QTY shares of SYMBOL
        """
        if len(args) < 2:
            return CommandResult(
                status=CommandStatus.ERROR,
                message="Usage: buy SYMBOL QTY [--confirm]",
            )

        symbol = args[0].upper()
        confirm = "--confirm" in args

        try:
            quantity = float(args[1])
        except ValueError:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Invalid quantity: {args[1]}",
            )

        # Get contract from existing position or create new one
        pos = self.portfolio.get_position(symbol)
        if pos and pos.contract:
            contract = pos.contract
            est_price = pos.current_price
        else:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"No existing position for {symbol}. Cannot determine contract.",
            )

        est_value = quantity * est_price

        if not confirm:
            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=f"Would buy {quantity:.0f} {symbol} (~${est_value:,.2f}). Use --confirm to execute.",
                data={"dry_run": True, "symbol": symbol, "quantity": quantity, "est_value": est_value},
            )

        order_id = self.portfolio.place_market_order(
            contract=contract,
            action="BUY",
            quantity=quantity,
        )

        if order_id:
            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=f"Buy order placed: {quantity:.0f} {symbol} (order {order_id})",
                data={"order_id": order_id, "symbol": symbol, "quantity": quantity},
            )
        else:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Failed to place buy order for {symbol}",
            )

    def handle_stop(self, args: List[str]) -> CommandResult:
        """Handle 'stop' or 'shutdown' command - initiate graceful shutdown"""
        logger.info("Shutdown requested via command")
        self.shutdown_mgr._shutdown_event.set()
        self.shutdown_mgr._cleanup()

        return CommandResult(
            status=CommandStatus.SUCCESS,
            message="Shutdown initiated",
        )


# Global command server instance
command_server: Optional[CommandServer] = None


# =============================================================================
# Configuration
# =============================================================================

# Default connection settings
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 7497  # TWS paper trading
DEFAULT_CLIENT_ID = 1

# Example target allocations - customize for your portfolio
DEFAULT_TARGETS = create_three_fund_targets(
    us_pct=50.0,
    intl_pct=30.0,
    bond_pct=20.0,
)

# Alternative: Equal weight example
# DEFAULT_TARGETS = create_equal_weight_targets(["SPY", "QQQ", "IWM", "EFA", "BND"])


# =============================================================================
# Main Functions
# =============================================================================

def show_portfolio(portfolio: Portfolio) -> None:
    """Display current portfolio positions"""
    print("\n" + "=" * 80)
    print("PORTFOLIO POSITIONS")
    print("=" * 80)

    positions = portfolio.positions
    if not positions:
        print("No positions found")
        return

    # Sort by market value
    positions = sorted(positions, key=lambda p: p.market_value, reverse=True)

    # Header
    print(f"{'Symbol':<8} {'Type':<6} {'Qty':>10} {'Price':>12} "
          f"{'Value':>14} {'P&L':>12} {'Alloc':>8}")
    print("-" * 80)

    # Positions
    for pos in positions:
        print(f"{pos.symbol:<8} {pos.asset_type.value:<6} {pos.quantity:>10,.0f} "
              f"${pos.current_price:>10,.2f} ${pos.market_value:>12,.2f} "
              f"${pos.unrealized_pnl:>10,.2f} {pos.allocation_pct:>7.1f}%")

    # Summary
    print("-" * 80)
    total_value = portfolio.total_value
    total_pnl = portfolio.total_pnl
    print(f"{'TOTAL':<8} {'':<6} {'':>10} {'':>12} "
          f"${total_value:>12,.2f} ${total_pnl:>10,.2f} {'100.0':>7}%")
    print("=" * 80)

    # Account summary
    account = portfolio.get_account_summary()
    if account and account.is_valid:
        print(f"\nAccount: {account.account_id}")
        print(f"  Net Liquidation: ${account.net_liquidation:,.2f}")
        print(f"  Available Funds: ${account.available_funds:,.2f}")
        print(f"  Buying Power:    ${account.buying_power:,.2f}")


def show_targets(targets: List[TargetAllocation]) -> None:
    """Display target allocations"""
    print("\n" + "=" * 50)
    print("TARGET ALLOCATIONS")
    print("=" * 50)

    for target in targets:
        print(f"  {target.symbol:<8} {target.target_pct:>6.1f}%")

    print("-" * 50)
    print(f"  {'TOTAL':<8} {sum(t.target_pct for t in targets):>6.1f}%")
    print("=" * 50)


def stream_prices(portfolio: Portfolio, duration: int = 0) -> None:
    """
    Stream live prices for all portfolio positions.

    Args:
        portfolio: Connected portfolio instance
        duration: Duration in seconds (0 = until interrupted)
    """
    # Tick counter for stats
    tick_count = 0
    start_time = time.time()

    def on_tick(symbol: str, price: float, tick_type: str):
        """Callback for each price tick"""
        nonlocal tick_count
        tick_count += 1
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        pos = portfolio.get_position(symbol)
        pnl = pos.unrealized_pnl if pos else 0
        print(f"[{timestamp}] {symbol:8} {tick_type:12} ${price:>10.2f}  P&L: ${pnl:>10.2f}")

    print("\n" + "=" * 70)
    print("STREAMING PRICES (Ctrl+C to stop)")
    print("=" * 70)
    print(f"{'Time':<15} {'Symbol':<8} {'Type':<12} {'Price':>12}  {'P&L':>14}")
    print("-" * 70)

    # Start streaming
    portfolio.start_streaming(on_tick=on_tick, use_delayed=True)

    try:
        # Use global shutdown manager for interruptible wait
        shutdown_manager.wait_interruptible(duration=duration)
    finally:
        portfolio.stop_streaming()

    # Stats
    elapsed = time.time() - start_time
    if elapsed > 0:
        print("-" * 70)
        print(f"Streamed {tick_count} ticks in {elapsed:.1f}s "
              f"({tick_count/elapsed:.1f} ticks/sec)")
        print("=" * 70)


def stream_bars(portfolio: Portfolio, duration: int = 0) -> None:
    """
    Stream 5-second OHLCV bars for all portfolio positions.

    Args:
        portfolio: Connected portfolio instance
        duration: Duration in seconds (0 = until interrupted)
    """
    # Bar counter for stats
    bar_count = 0
    start_time = time.time()

    def on_bar(bar: Bar):
        """Callback for each new bar"""
        nonlocal bar_count
        bar_count += 1

        # Format timestamp to just time
        bar_time = bar.timestamp.split("T")[1] if "T" in bar.timestamp else bar.timestamp

        # Direction indicator
        direction = "+" if bar.is_bullish else "-"

        pos = portfolio.get_position(bar.symbol)
        pnl = pos.unrealized_pnl if pos else 0

        print(f"[{bar_time}] {bar.symbol:8} "
              f"O:{bar.open:>8.2f} H:{bar.high:>8.2f} L:{bar.low:>8.2f} C:{bar.close:>8.2f} "
              f"V:{bar.volume:>8} {direction} P&L: ${pnl:>10.2f}")

    print("\n" + "=" * 100)
    print("STREAMING 5-SECOND BARS (Ctrl+C to stop)")
    print("=" * 100)
    print(f"{'Time':<12} {'Symbol':<8} {'Open':>10} {'High':>10} {'Low':>10} "
          f"{'Close':>10} {'Volume':>10} {'':>3} {'P&L':>14}")
    print("-" * 100)

    # Start bar streaming
    portfolio.start_bar_streaming(on_bar=on_bar, what_to_show="TRADES", use_rth=False)

    try:
        # Use global shutdown manager for interruptible wait
        shutdown_manager.wait_interruptible(duration=duration)
    finally:
        portfolio.stop_bar_streaming()

    # Stats
    elapsed = time.time() - start_time
    num_positions = len(portfolio.positions) or 1  # Avoid division by zero
    if elapsed > 0:
        print("-" * 100)
        print(f"Streamed {bar_count} bars in {elapsed:.1f}s "
              f"({bar_count/elapsed:.2f} bars/sec, ~{bar_count/num_positions:.0f} per symbol)")
        print("=" * 100)


def calculate_rebalance(
    portfolio: Portfolio,
    targets: List[TargetAllocation],
    config: RebalanceConfig,
) -> None:
    """Calculate and display rebalancing trades"""
    rebalancer = Rebalancer(portfolio=portfolio, config=config)
    rebalancer.set_targets(targets)

    result = rebalancer.calculate(strategy=RebalanceStrategy.THRESHOLD)
    print(rebalancer.preview(result))


def execute_rebalance(
    portfolio: Portfolio,
    targets: List[TargetAllocation],
    config: RebalanceConfig,
) -> None:
    """Execute rebalancing trades (with confirmation)"""
    rebalancer = Rebalancer(portfolio=portfolio, config=config)
    rebalancer.set_targets(targets)

    result = rebalancer.calculate(strategy=RebalanceStrategy.THRESHOLD)
    print(rebalancer.preview(result))

    if not result.actionable_trades:
        print("No trades to execute.")
        return

    # Confirmation
    print("\n*** TRADE EXECUTION ***")
    print(f"This will execute {result.trade_count} trades.")

    if config.dry_run:
        print("(DRY RUN mode - trades will not actually execute)")

    response = input("\nProceed? (yes/no): ").strip().lower()

    if response == "yes":
        success = rebalancer.execute(result)
        if success:
            print("Trades submitted successfully")
        else:
            print("Trade execution failed or not implemented")
    else:
        print("Cancelled")


# =============================================================================
# CLI
# =============================================================================

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="IB Portfolio Rebalancer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                      Show current portfolio
  %(prog)s --stream             Stream live tick prices for all positions
  %(prog)s --bars               Stream 5-second OHLCV bars for all positions
  %(prog)s --stream --duration 60  Stream for 60 seconds
  %(prog)s --rebalance          Calculate rebalancing trades
  %(prog)s --rebalance --execute Execute trades (with confirmation)
  %(prog)s --threshold 3.0      Set drift threshold to 3%%
  %(prog)s --port 4002          Connect to IB Gateway paper trading
        """,
    )

    # Connection options
    parser.add_argument(
        "--host", default=DEFAULT_HOST,
        help=f"IB host (default: {DEFAULT_HOST})"
    )
    parser.add_argument(
        "--port", type=int, default=DEFAULT_PORT,
        help=f"IB port (default: {DEFAULT_PORT})"
    )
    parser.add_argument(
        "--client-id", type=int, default=DEFAULT_CLIENT_ID,
        help=f"Client ID (default: {DEFAULT_CLIENT_ID})"
    )

    # Actions
    parser.add_argument(
        "--stream", action="store_true",
        help="Stream live tick prices for all portfolio positions"
    )
    parser.add_argument(
        "--bars", action="store_true",
        help="Stream 5-second OHLCV bars for all portfolio positions"
    )
    parser.add_argument(
        "--duration", type=int, default=0,
        help="Stream duration in seconds (0 = until Ctrl+C)"
    )
    parser.add_argument(
        "--rebalance", action="store_true",
        help="Calculate rebalancing trades"
    )
    parser.add_argument(
        "--execute", action="store_true",
        help="Execute trades (requires --rebalance)"
    )

    # Rebalance options
    parser.add_argument(
        "--threshold", type=float, default=5.0,
        help="Drift threshold %% to trigger rebalance (default: 5.0)"
    )
    parser.add_argument(
        "--min-trade", type=float, default=100.0,
        help="Minimum trade value in dollars (default: 100)"
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=True,
        help="Don't actually execute trades (default: True)"
    )
    parser.add_argument(
        "--live", action="store_true",
        help="Execute real trades (disables dry-run)"
    )

    # Output options
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Verbose output"
    )
    parser.add_argument(
        "--quiet", "-q", action="store_true",
        help="Minimal output"
    )

    # Command server options
    parser.add_argument(
        "--socket", default=DEFAULT_SOCKET_PATH,
        help=f"Unix socket path for commands (default: {DEFAULT_SOCKET_PATH})"
    )
    parser.add_argument(
        "--no-server", action="store_true",
        help="Disable command server"
    )

    return parser.parse_args()


def main():
    """Main entry point"""
    global command_server

    args = parse_args()

    # Configure logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    elif args.quiet:
        logging.getLogger().setLevel(logging.WARNING)

    # Install signal handlers for graceful shutdown
    shutdown_manager.install_handlers()

    # Create rebalance config
    config = RebalanceConfig(
        drift_threshold_pct=args.threshold,
        min_trade_value=args.min_trade,
        dry_run=not args.live,
    )

    # Connect to IB
    logger.info(f"Connecting to IB at {args.host}:{args.port}...")
    portfolio = Portfolio(
        host=args.host,
        port=args.port,
        client_id=args.client_id,
    )

    # Register portfolio with shutdown manager for cleanup
    shutdown_manager.register_portfolio(portfolio)

    if not portfolio.connect():
        logger.error("Failed to connect to IB")
        sys.exit(1)

    try:
        # Check for early shutdown
        if shutdown_manager.should_shutdown:
            return

        # Load portfolio data
        logger.info("Loading portfolio data...")
        portfolio.load(fetch_prices=True, fetch_account=True)

        # Check for early shutdown
        if shutdown_manager.should_shutdown:
            return

        # Start command server (unless disabled)
        if not args.no_server:
            command_server = CommandServer(socket_path=args.socket)
            command_handler = CommandHandler(portfolio, shutdown_manager)
            command_handler.register_commands(command_server)
            if command_server.start():
                logger.info(f"Command server listening on {args.socket}")
            else:
                logger.warning("Failed to start command server")

        # Show portfolio
        show_portfolio(portfolio)

        # Streaming modes
        if args.stream:
            stream_prices(portfolio, duration=args.duration)

        elif args.bars:
            stream_bars(portfolio, duration=args.duration)

        # Rebalance actions
        elif args.rebalance:
            show_targets(DEFAULT_TARGETS)

            if args.execute:
                execute_rebalance(portfolio, DEFAULT_TARGETS, config)
            else:
                calculate_rebalance(portfolio, DEFAULT_TARGETS, config)

    except KeyboardInterrupt:
        # This may happen if signal handler hasn't fully processed
        logger.info("Interrupted")
    finally:
        # Stop command server
        if command_server:
            command_server.stop()
        portfolio.disconnect()
        shutdown_manager.restore_handlers()


if __name__ == "__main__":
    main()
