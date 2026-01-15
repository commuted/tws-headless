#!/usr/bin/env python3
"""
manage_allocations.py - CLI utility for managing algorithm allocations

A command-line interface for allocating portfolio positions between
trading algorithms using the IB Portfolio interface.

Usage:
    # View current status
    python manage_allocations.py status

    # Sync with IB portfolio
    python manage_allocations.py sync

    # Allocate positions
    python manage_allocations.py allocate momentum_5day SPY 100
    python manage_allocations.py allocate dummy BND 50

    # Transfer between algorithms
    python manage_allocations.py transfer momentum_5day dummy SPY 25

    # Auto-allocate all unallocated to an algorithm
    python manage_allocations.py auto-allocate momentum_5day

    # View algorithm holdings
    python manage_allocations.py show momentum_5day

    # Interactive mode
    python manage_allocations.py interactive

Examples:
    # Full workflow
    python manage_allocations.py sync
    python manage_allocations.py status
    python manage_allocations.py allocate momentum_5day SPY 100
    python manage_allocations.py allocate momentum_5day QQQ 50
    python manage_allocations.py allocate dummy BND 75
    python manage_allocations.py status
"""

import argparse
import logging
import sys
from typing import Optional

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_portfolio(host: str = "127.0.0.1", port: int = 7497, client_id: int = 1):
    """Connect to IB and return portfolio"""
    from portfolio import Portfolio

    portfolio = Portfolio(host=host, port=port, client_id=client_id)

    print(f"Connecting to IB at {host}:{port}...")
    if not portfolio.connect(timeout=10):
        print("Failed to connect to IB. Make sure TWS/Gateway is running.")
        return None

    print("Loading portfolio...")
    if not portfolio.load(timeout=30):
        print("Failed to load portfolio.")
        portfolio.disconnect()
        return None

    print(f"Loaded {len(portfolio.positions)} positions")
    return portfolio


def get_manager(portfolio=None, holdings_file: Optional[str] = None):
    """Get allocation manager"""
    from algorithms.allocation_manager import AllocationManager

    return AllocationManager(
        portfolio=portfolio,
        holdings_file=holdings_file,
    )


def cmd_status(args):
    """Show current allocation status"""
    manager = get_manager(holdings_file=args.holdings_file)

    if args.connect:
        portfolio = get_portfolio(args.host, args.port, args.client_id)
        if portfolio:
            manager.portfolio = portfolio
            manager.sync()
            portfolio.disconnect()

    print(manager.format_status())
    return 0


def cmd_sync(args):
    """Sync with IB portfolio"""
    portfolio = get_portfolio(args.host, args.port, args.client_id)
    if not portfolio:
        return 1

    manager = get_manager(portfolio, args.holdings_file)
    results = manager.sync()

    print("\nSync Results:")
    print(f"  Updated: {len(results.get('updated_positions', []))}")
    print(f"  New: {len(results.get('new_positions', []))}")
    print(f"  Removed: {len(results.get('removed_positions', []))}")

    if results.get('discrepancies'):
        print("\nDiscrepancies found:")
        for d in results['discrepancies']:
            print(f"  - {d}")

    manager.save()
    print("\nHoldings saved.")

    portfolio.disconnect()
    return 0


def cmd_allocate(args):
    """Allocate position to algorithm"""
    portfolio = None
    if args.connect:
        portfolio = get_portfolio(args.host, args.port, args.client_id)
        if not portfolio:
            return 1

    manager = get_manager(portfolio, args.holdings_file)

    if portfolio:
        manager.sync()

    result = manager.allocate(args.algorithm, args.symbol, args.quantity)

    if result.success:
        print(f"SUCCESS: {result.message}")
        print(f"  Previous: {result.previous_quantity}")
        print(f"  New: {result.quantity}")
        manager.save()
    else:
        print(f"FAILED: {result.message}")

    if portfolio:
        portfolio.disconnect()

    return 0 if result.success else 1


def cmd_deallocate(args):
    """Deallocate position from algorithm"""
    manager = get_manager(holdings_file=args.holdings_file)

    result = manager.deallocate(args.algorithm, args.symbol, args.quantity)

    if result.success:
        print(f"SUCCESS: {result.message}")
        print(f"  Previous: {result.previous_quantity}")
        print(f"  Remaining: {result.quantity}")
        manager.save()
    else:
        print(f"FAILED: {result.message}")

    return 0 if result.success else 1


def cmd_transfer(args):
    """Transfer allocation between algorithms"""
    manager = get_manager(holdings_file=args.holdings_file)

    result = manager.transfer(
        args.from_algorithm,
        args.to_algorithm,
        args.symbol,
        args.quantity,
    )

    if result.success:
        print(f"SUCCESS: {result.message}")
        manager.save()
    else:
        print(f"FAILED: {result.message}")

    return 0 if result.success else 1


def cmd_auto_allocate(args):
    """Auto-allocate all unallocated to an algorithm"""
    portfolio = None
    if args.connect:
        portfolio = get_portfolio(args.host, args.port, args.client_id)
        if not portfolio:
            return 1

    manager = get_manager(portfolio, args.holdings_file)

    if portfolio:
        manager.sync()

    results = manager.auto_allocate(args.algorithm, include_cash=not args.no_cash)

    print(f"\nAuto-allocated to {args.algorithm}:")
    print(f"  Positions: {len(results['positions_allocated'])}")

    for pos in results['positions_allocated']:
        print(f"    {pos['symbol']}: {pos['quantity']:.0f} (${pos['value']:,.2f})")

    print(f"  Cash: ${results['cash_allocated']:,.2f}")

    if results['errors']:
        print("\nErrors:")
        for err in results['errors']:
            print(f"  - {err}")

    manager.save()
    print("\nHoldings saved.")

    if portfolio:
        portfolio.disconnect()

    return 0


def cmd_distribute(args):
    """Distribute unallocated equally among algorithms"""
    manager = get_manager(holdings_file=args.holdings_file)

    algorithms = args.algorithms.split(',')
    results = manager.distribute_equally(algorithms, args.symbol)

    print(f"\nDistributed to: {', '.join(algorithms)}")
    print(f"  Distributions: {len(results['distributions'])}")

    for d in results['distributions']:
        print(f"    {d['algorithm']}: {d['symbol']} x {d['quantity']:.0f}")

    if results['errors']:
        print("\nErrors:")
        for err in results['errors']:
            print(f"  - {err}")

    manager.save()
    return 0


def cmd_show(args):
    """Show algorithm holdings"""
    manager = get_manager(holdings_file=args.holdings_file)

    summary = manager.get_algorithm_summary(args.algorithm)
    holdings = summary['holdings']
    weights = summary['weights']

    print(f"\n{'=' * 60}")
    print(f"ALGORITHM: {args.algorithm}")
    print(f"{'=' * 60}")
    print(f"Total Value: ${holdings['total_value']:,.2f}")
    print(f"Cash: ${holdings['cash']:,.2f}")
    print(f"Position Value: ${holdings['position_value']:,.2f}")

    if holdings['positions']:
        print(f"\n{'-' * 60}")
        print("POSITIONS")
        print(f"{'-' * 60}")
        print(f"{'Symbol':<8} {'Qty':>10} {'Price':>10} {'Value':>12} {'Weight':>8}")

        for pos in holdings['positions']:
            weight = weights.get(pos['symbol'], 0)
            print(
                f"{pos['symbol']:<8} {pos['quantity']:>10.0f} "
                f"${pos['current_price']:>9.2f} ${pos['market_value']:>11,.2f} "
                f"{weight:>7.1f}%"
            )

    if weights.get('_CASH', 0) > 0:
        print(f"\nCash Weight: {weights['_CASH']:.1f}%")

    print(f"{'=' * 60}\n")
    return 0


def cmd_allocate_cash(args):
    """Allocate cash to algorithm"""
    manager = get_manager(holdings_file=args.holdings_file)

    result = manager.allocate_cash(args.algorithm, args.amount)

    if result.success:
        print(f"SUCCESS: {result.message}")
        manager.save()
    else:
        print(f"FAILED: {result.message}")

    return 0 if result.success else 1


def cmd_transfer_cash(args):
    """Transfer cash between algorithms"""
    manager = get_manager(holdings_file=args.holdings_file)

    result = manager.transfer_cash(
        args.from_algorithm,
        args.to_algorithm,
        args.amount,
    )

    if result.success:
        print(f"SUCCESS: {result.message}")
        manager.save()
    else:
        print(f"FAILED: {result.message}")

    return 0 if result.success else 1


def cmd_interactive(args):
    """Interactive allocation mode"""
    portfolio = get_portfolio(args.host, args.port, args.client_id)
    if not portfolio:
        return 1

    manager = get_manager(portfolio, args.holdings_file)
    manager.sync()

    print("\n" + "=" * 60)
    print("INTERACTIVE ALLOCATION MODE")
    print("=" * 60)
    print("Commands:")
    print("  status              - Show current allocations")
    print("  sync                - Resync with IB")
    print("  alloc <algo> <sym> <qty> - Allocate position")
    print("  dealloc <algo> <sym> <qty> - Deallocate position")
    print("  transfer <from> <to> <sym> <qty> - Transfer")
    print("  auto <algo>         - Auto-allocate all to algorithm")
    print("  show <algo>         - Show algorithm holdings")
    print("  cash <algo> <amt>   - Allocate cash")
    print("  save                - Save holdings")
    print("  quit                - Exit")
    print("=" * 60 + "\n")

    while True:
        try:
            line = input("alloc> ").strip()
            if not line:
                continue

            parts = line.split()
            cmd = parts[0].lower()

            if cmd == 'quit' or cmd == 'exit' or cmd == 'q':
                print("Saving and exiting...")
                manager.save()
                break

            elif cmd == 'status':
                print(manager.format_status())

            elif cmd == 'sync':
                results = manager.sync()
                print(f"Synced: {len(results.get('updated_positions', []))} updated")

            elif cmd == 'alloc' and len(parts) >= 4:
                algo, sym, qty = parts[1], parts[2], float(parts[3])
                result = manager.allocate(algo, sym, qty)
                print(f"{'SUCCESS' if result.success else 'FAILED'}: {result.message}")

            elif cmd == 'dealloc' and len(parts) >= 4:
                algo, sym, qty = parts[1], parts[2], float(parts[3])
                result = manager.deallocate(algo, sym, qty)
                print(f"{'SUCCESS' if result.success else 'FAILED'}: {result.message}")

            elif cmd == 'transfer' and len(parts) >= 5:
                from_algo, to_algo, sym, qty = parts[1], parts[2], parts[3], float(parts[4])
                result = manager.transfer(from_algo, to_algo, sym, qty)
                print(f"{'SUCCESS' if result.success else 'FAILED'}: {result.message}")

            elif cmd == 'auto' and len(parts) >= 2:
                algo = parts[1]
                results = manager.auto_allocate(algo)
                print(f"Auto-allocated {len(results['positions_allocated'])} positions")

            elif cmd == 'show' and len(parts) >= 2:
                algo = parts[1]
                summary = manager.get_algorithm_summary(algo)
                holdings = summary['holdings']
                print(f"\n{algo}: ${holdings['total_value']:,.2f}")
                print(f"  Cash: ${holdings['cash']:,.2f}")
                for pos in holdings['positions']:
                    print(f"  {pos['symbol']}: {pos['quantity']:.0f} @ ${pos['current_price']:.2f}")

            elif cmd == 'cash' and len(parts) >= 3:
                algo, amount = parts[1], float(parts[2])
                result = manager.allocate_cash(algo, amount)
                print(f"{'SUCCESS' if result.success else 'FAILED'}: {result.message}")

            elif cmd == 'save':
                manager.save()
                print("Holdings saved.")

            elif cmd == 'help':
                print("Commands: status, sync, alloc, dealloc, transfer, auto, show, cash, save, quit")

            else:
                print(f"Unknown command or invalid arguments: {line}")
                print("Type 'help' for available commands")

        except KeyboardInterrupt:
            print("\nSaving and exiting...")
            manager.save()
            break
        except Exception as e:
            print(f"Error: {e}")

    portfolio.disconnect()
    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Manage algorithm allocations for IB Portfolio",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s status                           Show current allocation status
  %(prog)s sync                             Sync with IB portfolio
  %(prog)s allocate momentum_5day SPY 100   Allocate 100 SPY to momentum_5day
  %(prog)s transfer mom dummy SPY 25        Transfer 25 SPY from mom to dummy
  %(prog)s auto-allocate momentum_5day      Auto-allocate all to momentum_5day
  %(prog)s show momentum_5day               Show algorithm holdings
  %(prog)s interactive                      Enter interactive mode
        """,
    )

    # Global arguments
    parser.add_argument(
        '--host', default='127.0.0.1',
        help='IB Gateway/TWS host (default: 127.0.0.1)'
    )
    parser.add_argument(
        '--port', type=int, default=7497,
        help='IB Gateway/TWS port (default: 7497 for TWS paper)'
    )
    parser.add_argument(
        '--client-id', type=int, default=1,
        help='IB client ID (default: 1)'
    )
    parser.add_argument(
        '--holdings-file', '-f',
        help='Path to shared holdings JSON file'
    )
    parser.add_argument(
        '--verbose', '-v', action='store_true',
        help='Enable verbose logging'
    )

    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Status command
    status_parser = subparsers.add_parser('status', help='Show allocation status')
    status_parser.add_argument(
        '--connect', '-c', action='store_true',
        help='Connect to IB and sync before showing status'
    )
    status_parser.set_defaults(func=cmd_status)

    # Sync command
    sync_parser = subparsers.add_parser('sync', help='Sync with IB portfolio')
    sync_parser.set_defaults(func=cmd_sync)

    # Allocate command
    alloc_parser = subparsers.add_parser('allocate', help='Allocate position to algorithm')
    alloc_parser.add_argument('algorithm', help='Algorithm name')
    alloc_parser.add_argument('symbol', help='Position symbol')
    alloc_parser.add_argument('quantity', type=float, help='Quantity to allocate')
    alloc_parser.add_argument(
        '--connect', '-c', action='store_true',
        help='Connect to IB and sync first'
    )
    alloc_parser.set_defaults(func=cmd_allocate)

    # Deallocate command
    dealloc_parser = subparsers.add_parser('deallocate', help='Deallocate from algorithm')
    dealloc_parser.add_argument('algorithm', help='Algorithm name')
    dealloc_parser.add_argument('symbol', help='Position symbol')
    dealloc_parser.add_argument('quantity', type=float, help='Quantity to deallocate')
    dealloc_parser.set_defaults(func=cmd_deallocate)

    # Transfer command
    transfer_parser = subparsers.add_parser('transfer', help='Transfer between algorithms')
    transfer_parser.add_argument('from_algorithm', help='Source algorithm')
    transfer_parser.add_argument('to_algorithm', help='Destination algorithm')
    transfer_parser.add_argument('symbol', help='Position symbol')
    transfer_parser.add_argument('quantity', type=float, help='Quantity to transfer')
    transfer_parser.set_defaults(func=cmd_transfer)

    # Auto-allocate command
    auto_parser = subparsers.add_parser('auto-allocate', help='Auto-allocate to algorithm')
    auto_parser.add_argument('algorithm', help='Algorithm to receive allocations')
    auto_parser.add_argument(
        '--no-cash', action='store_true',
        help='Do not allocate cash'
    )
    auto_parser.add_argument(
        '--connect', '-c', action='store_true',
        help='Connect to IB and sync first'
    )
    auto_parser.set_defaults(func=cmd_auto_allocate)

    # Distribute command
    dist_parser = subparsers.add_parser('distribute', help='Distribute equally')
    dist_parser.add_argument('algorithms', help='Comma-separated algorithm names')
    dist_parser.add_argument('--symbol', '-s', help='Specific symbol (default: all)')
    dist_parser.set_defaults(func=cmd_distribute)

    # Show command
    show_parser = subparsers.add_parser('show', help='Show algorithm holdings')
    show_parser.add_argument('algorithm', help='Algorithm name')
    show_parser.set_defaults(func=cmd_show)

    # Allocate cash command
    cash_parser = subparsers.add_parser('allocate-cash', help='Allocate cash')
    cash_parser.add_argument('algorithm', help='Algorithm name')
    cash_parser.add_argument('amount', type=float, help='Amount to allocate')
    cash_parser.set_defaults(func=cmd_allocate_cash)

    # Transfer cash command
    tcash_parser = subparsers.add_parser('transfer-cash', help='Transfer cash')
    tcash_parser.add_argument('from_algorithm', help='Source algorithm')
    tcash_parser.add_argument('to_algorithm', help='Destination algorithm')
    tcash_parser.add_argument('amount', type=float, help='Amount to transfer')
    tcash_parser.set_defaults(func=cmd_transfer_cash)

    # Interactive command
    interactive_parser = subparsers.add_parser('interactive', help='Interactive mode')
    interactive_parser.set_defaults(func=cmd_interactive)

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if not args.command:
        parser.print_help()
        return 1

    return args.func(args)


if __name__ == '__main__':
    sys.exit(main())
