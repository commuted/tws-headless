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
from models import TargetAllocation, AssetType, RebalanceStrategy, Bar

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

    Provides a centralized way to handle shutdown signals and
    coordinate cleanup across the application.
    """

    def __init__(self):
        self._shutdown_event = Event()
        self._portfolio: Optional[Portfolio] = None
        self._original_sigint = None
        self._original_sigterm = None
        self._sigint_count = 0

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

    def restore_handlers(self):
        """Restore original signal handlers"""
        if self._original_sigint:
            signal.signal(signal.SIGINT, self._original_sigint)
        if self._original_sigterm:
            signal.signal(signal.SIGTERM, self._original_sigterm)

    def _signal_handler(self, signum, frame):
        """Handle SIGINT/SIGTERM signals"""
        self._sigint_count += 1
        sig_name = "SIGINT" if signum == signal.SIGINT else "SIGTERM"

        if self._sigint_count == 1:
            print(f"\n{sig_name} received. Shutting down gracefully...")
            self._shutdown_event.set()
            self._cleanup()
        elif self._sigint_count == 2:
            print("\nSecond interrupt received. Forcing disconnect...")
            if self._portfolio:
                try:
                    self._portfolio.disconnect()
                except Exception:
                    pass
        else:
            print("\nForce exit.")
            sys.exit(1)

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

    return parser.parse_args()


def main():
    """Main entry point"""
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
        portfolio.disconnect()
        shutdown_manager.restore_handlers()


if __name__ == "__main__":
    main()
