#!/usr/bin/env python3
"""
run_engine.py - Start the IB Trading Engine

Full-featured entry point for continuous algorithmic trading with:
- Socket command interface (ibctl.py compatible)
- Plugin execution
- Real-time market data streaming
- Order execution with reconciliation

Usage:
    python3 -m ib.run_engine              # From parent directory
    ./start_trading.sh                     # Via shell script

Options via environment variables:
    PORT=7497 MODE=dry_run python3 -m ib.run_engine

Command line options:
    python3 -m ib.run_engine --port 4002 --mode immediate
    python3 -m ib.run_engine --no-server   # Disable socket server
"""

import argparse
import asyncio
import logging
import sys
import os
from pathlib import Path
from typing import Optional, List

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("trading")


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="TWS Headless - Continuous algorithmic trading engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Environment variables (override defaults):
  PORT              IB Gateway/TWS port (default: 7497)
  MODE              Order mode: dry_run, immediate, queued (default: dry_run)
  MARKET_DATA_TYPE  IB market data type: 1=live, 3=delayed (default: auto)

Examples:
  python3 -m ib.run_engine
  python3 -m ib.run_engine --port 4002 --mode immediate
  PORT=4002 MODE=immediate python3 -m ib.run_engine
  ./start_trading.sh 4002 immediate
        """,
    )

    # Connection options
    parser.add_argument(
        "--host", default="127.0.0.1",
        help="IB host (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--port", type=int, default=None,
        help="IB port (default: from PORT env or 7497)"
    )
    parser.add_argument(
        "--client-id", type=int, default=1,
        help="Client ID (default: 1)"
    )

    # Order mode
    parser.add_argument(
        "--mode", choices=["dry_run", "immediate", "queued"], default=None,
        help="Order execution mode (default: from MODE env or dry_run)"
    )

    parser.add_argument(
        "--plugin-dir",
        default=None,
        help="Plugin directory path (default: from IB_PLUGIN_DIR env or ./plugins)",
    )

    # Market data type
    parser.add_argument(
        "--market-data-type", type=int, choices=[1, 2, 3, 4], default=None,
        help="IB market data type: 1=live, 2=frozen, 3=delayed, 4=delayed-frozen "
             "(default: auto-detect from account — paper→3, live→1). "
             "Use 1 if your paper account has live data shared from a live account."
    )

    # Command server options
    parser.add_argument(
        "--socket", default="/tmp/tws_headless.sock",
        help="Unix socket path for commands (default: /tmp/tws_headless.sock)"
    )
    parser.add_argument(
        "--no-server", action="store_true",
        help="Disable command server"
    )

    # Logging
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
    args = parse_args()

    # Configure logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    elif args.quiet:
        logging.getLogger().setLevel(logging.WARNING)

    # Set plugin directory: CLI arg > env var > default (./plugins relative to project root)
    if args.plugin_dir:
        os.environ["IB_PLUGIN_DIR"] = str(Path(args.plugin_dir).resolve())
    elif not os.environ.get("IB_PLUGIN_DIR"):
        os.environ["IB_PLUGIN_DIR"] = str(Path(__file__).parent.parent / "plugins")

    # Get config from environment, then override with command line
    port = args.port or int(os.environ.get("PORT", "7497"))
    mode = args.mode or os.environ.get("MODE", "dry_run")
    mdt_env = os.environ.get("MARKET_DATA_TYPE")
    market_data_type = args.market_data_type or (int(mdt_env) if mdt_env else None)

    logger.info("=" * 60)
    logger.info("IB Trading Engine")
    logger.info("=" * 60)
    logger.info(f"Port: {port}")
    logger.info(f"Order Mode: {mode}")
    logger.info(f"Market Data Type: {market_data_type if market_data_type else 'auto'}")
    logger.info(f"Socket: {args.socket if not args.no_server else 'disabled'}")
    logger.info("Engine Mode: Plugin Executive")
    logger.info("=" * 60)

    # Import components
    from .trading_engine import TradingEngine, EngineConfig, EngineState
    from .plugin_executive import OrderExecutionMode
    from .data_feed import DataType
    from .command_server import CommandServer, CommandResult, CommandStatus

    # Map mode string to enum
    mode_map = {
        "dry_run": OrderExecutionMode.DRY_RUN,
        "immediate": OrderExecutionMode.IMMEDIATE,
        "queued": OrderExecutionMode.QUEUED,
    }

    # Create engine config
    config = EngineConfig(
        host=args.host,
        port=port,
        client_id=args.client_id,
        order_mode=mode_map.get(mode, OrderExecutionMode.DRY_RUN),
        enable_message_bus=True,
        market_data_type=market_data_type,
    )

    # Create engine
    logger.info("Creating engine...")
    engine = TradingEngine(config)

    # Create command handler and server
    command_server: Optional[CommandServer] = None
    if not args.no_server:
        command_server = CommandServer(socket_path=args.socket)
        handler = EngineCommandHandler(engine)
        handler.register_commands(command_server)

    # Load orders system plugin for socket order execution
    try:
        from plugins.orders import OrdersPlugin
        from .plugin_executive import ExecutionMode

        orders_plugin = OrdersPlugin(
            portfolio=engine.portfolio,
            message_bus=engine.message_bus if hasattr(engine, 'message_bus') else None,
        )
        if engine.plugin_executive:
            engine.plugin_executive.register_plugin(
                orders_plugin,
                execution_mode=ExecutionMode.MANUAL,
                enabled=True,
            )
            logger.info(f"Added system plugin: {orders_plugin.name}")
    except ImportError as e:
        logger.warning(f"Could not load orders plugin: {e}")

    # Load example plugin
    try:
        from plugins.momentum_5day import create_default_momentum_5day

        plugin = create_default_momentum_5day()
        if engine.plugin_executive:
            engine.plugin_executive.register_plugin(plugin, enabled=True)
            logger.info(f"Added plugin: {plugin.name}")
    except ImportError as e:
        logger.warning(f"Could not load example plugin: {e}")

    # Load paper test feeds plugin
    try:
        from plugins.paper_tests.paper_test_feeds import PaperTestFeedsPlugin
        from .plugin_executive import ExecutionMode

        paper_test_feeds = PaperTestFeedsPlugin(
            portfolio=engine.portfolio,
            message_bus=engine.message_bus if hasattr(engine, "message_bus") else None,
        )
        if engine.plugin_executive:
            engine.plugin_executive.register_plugin(
                paper_test_feeds,
                execution_mode=ExecutionMode.MANUAL,
                enabled=True,
            )
            logger.info(f"Added plugin: {paper_test_feeds.name}")
    except ImportError as e:
        logger.warning(f"Could not load paper_test_feeds plugin: {e}")

    # Callbacks
    def on_started():
        logger.info("Engine started successfully")

        # Configure per-account state before reconciliation and auto-reload
        if engine.portfolio and engine.portfolio.managed_accounts:
            account_id = engine.portfolio.managed_accounts[0]
            logger.info(f"Active account: {account_id}")
            from .plugin_store import configure_plugin_store
            configure_plugin_store(account_id)
            if engine.plugin_executive:
                engine.plugin_executive.set_account(account_id)

        if command_server:
            if command_server.start():
                logger.info(f"Command server listening on {args.socket}")
            else:
                logger.warning("Failed to start command server")

        # Reconcile plugin holdings with account on startup
        if engine.plugin_executive:
            logger.info("Reconciling plugin holdings with account...")
            report = engine.plugin_executive.reconcile_with_account()
            if report.get("discrepancies"):
                formatted = engine.plugin_executive.format_reconciliation_report(report)
                for line in formatted.split("\n"):
                    logger.info(line)
            else:
                logger.info("Reconciliation complete: holdings match account")

        # Auto-reload plugins from last session.
        # Run in a thread so the asyncio event loop stays free for IB socket
        # I/O — plugin.start() may call get_historical_data() which blocks on
        # threading.Event.wait(), and AsyncIBTransport needs the loop to deliver
        # those responses.
        if engine.plugin_executive:
            pe = engine.plugin_executive

            async def _reload_plugins():
                result = await asyncio.to_thread(pe.reload_registered_plugins)
                if result["reloaded"]:
                    logger.info(f"Auto-reloaded plugins: {result['reloaded']}")
                if result["failed"]:
                    logger.warning(f"Failed to reload plugins: {result['failed']}")

            asyncio.get_event_loop().create_task(_reload_plugins())

        logger.info("Streaming data - press Ctrl+C 3x to stop")

    def on_stopped():
        logger.info("Engine stopped")
        if command_server:
            command_server.stop()

    def on_signal(name, signal):
        logger.info(f"SIGNAL [{name}]: {signal.action} {signal.quantity} {signal.symbol} - {signal.reason}")

    def on_bar(symbol, bar, data_type):
        if data_type == DataType.BAR_1MIN:
            logger.debug(f"[{symbol}] O={bar.open:.2f} H={bar.high:.2f} L={bar.low:.2f} C={bar.close:.2f} V={bar.volume}")

    def on_error(err):
        logger.error(f"Engine error: {err}")

    engine.on_started = on_started
    engine.on_stopped = on_stopped
    engine.on_signal = on_signal
    engine.on_bar = on_bar
    engine.on_error = on_error

    # Start engine
    logger.info("Connecting to IB...")

    async def _run():
        if await engine.start():
            await engine.run_forever()
            logger.info("Engine stopped.")
            return 0
        else:
            logger.error("Failed to start engine")
            return 1

    return asyncio.run(_run())


_STATE_DISPLAY = {"started": "running", "loaded": "idle"}


def _display_state(raw: str) -> str:
    return _STATE_DISPLAY.get(raw, raw)


class EngineCommandHandler:
    """
    Command handler for the trading engine.

    Provides socket commands for controlling the engine via ibctl.py.
    """

    def __init__(self, engine):
        self.engine = engine
        self._liquidation_in_progress = False

    def register_commands(self, server: 'CommandServer'):
        """Register all command handlers with the server"""
        from .command_server import CommandServer

        server.register_handler("status", self.handle_status)
        server.register_handler("positions", self.handle_positions)
        server.register_handler("summary", self.handle_summary)
        server.register_handler("liquidate", self.handle_liquidate)
        server.register_handler("stop", self.handle_stop)
        server.register_handler("shutdown", self.handle_stop)
        server.register_handler("sell", self.handle_sell)
        server.register_handler("buy", self.handle_buy)
        server.register_handler("trade", self.handle_trade)
        server.register_handler("pause", self.handle_pause)
        server.register_handler("resume", self.handle_resume)
        server.register_handler("order", self.handle_order)
        server.register_handler("transfer", self.handle_transfer)
        server.register_handler("reconcile", self.handle_reconcile)
        server.register_handler("historical", self.handle_historical)

        server.register_handler("plugin", self.handle_plugin)

    def handle_status(self, args: List[str]):
        """Handle 'status' command"""
        from .command_server import CommandResult, CommandStatus

        try:
            status = self.engine.get_status()
            portfolio = self.engine.portfolio

            message = (
                f"Engine: {status['state']} | "
                f"Connected: {status['connected']} | "
                f"Positions: {status['portfolio']['positions'] if status['portfolio'] else 0}"
            )

            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=message,
                data=status,
            )
        except Exception as e:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Failed to get status: {e}",
            )

    def handle_positions(self, args: List[str]):
        """Handle 'positions' command"""
        from .command_server import CommandResult, CommandStatus

        try:
            positions = self.engine.portfolio.positions
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

    def handle_summary(self, args: List[str]):
        """Handle 'summary' command - account summary with plugin breakdown"""
        from .command_server import CommandResult, CommandStatus
        import json

        try:
            output_json = "--json" in args
            portfolio = self.engine.portfolio

            account = portfolio.get_account_summary()
            total_value = portfolio.total_value
            total_pnl = portfolio.total_pnl
            positions = portfolio.positions

            # Get cash from account or plugin executive
            cash_balance = 0.0
            if account and account.is_valid:
                cash_balance = account.available_funds or 0.0

            account_data = {
                "total_value": total_value,
                "cash": cash_balance,
                "positions_value": total_value - cash_balance,
                "total_pnl": total_pnl,
            }
            if account and account.is_valid:
                account_data.update({
                    "account_id": account.account_id,
                    "net_liquidation": account.net_liquidation,
                    "available_funds": account.available_funds,
                    "buying_power": account.buying_power,
                })

            # Get plugin holdings breakdown if using plugin executive
            plugin_holdings = {}
            unassigned = None
            if self.engine.plugin_executive:
                # Sync unassigned holdings with current portfolio state FIRST
                self.engine.plugin_executive.sync_unassigned_holdings()

                # Then get holdings summary (now includes synced unassigned data)
                holdings_summary = self.engine.plugin_executive.get_holdings_summary()
                plugin_holdings = holdings_summary.get("plugins", {})
                unassigned = holdings_summary.get("unassigned")

            data = {
                "account": account_data,
                "portfolio": {
                    "total_value": total_value,
                    "cash": cash_balance,
                    "total_pnl": total_pnl,
                    "position_count": len(positions),
                },
                "plugins": plugin_holdings,
                "unassigned": unassigned,
            }

            if output_json:
                message = json.dumps(data, indent=2)
            else:
                lines = [
                    "=" * 50,
                    "ACCOUNT SUMMARY",
                    "=" * 50,
                    f"Total Value:    ${total_value:>12,.2f}",
                    f"Cash:           ${cash_balance:>12,.2f}",
                    f"Positions Value: ${total_value - cash_balance:>11,.2f}",
                    f"Total P&L:      ${total_pnl:>12,.2f}",
                    f"Positions:      {len(positions):>12}",
                ]
                if account_data.get("buying_power"):
                    lines.append(f"Buying Power:   ${account_data.get('buying_power', 0):>12,.2f}")

                # Show plugin breakdown if available
                if plugin_holdings:
                    lines.append("")
                    lines.append("PLUGIN HOLDINGS:")
                    lines.append("-" * 50)
                    for name, info in plugin_holdings.items():
                        plugin_value = info.get("total_value", 0.0)
                        plugin_cash = info.get("cash", 0.0)
                        lines.append(f"  {name}: ${plugin_value:,.2f} (cash: ${plugin_cash:,.2f})")

                # Show unassigned
                if unassigned:
                    lines.append("")
                    lines.append("UNASSIGNED:")
                    lines.append("-" * 50)
                    unassigned_cash = unassigned.get("cash", 0.0)
                    unassigned_value = unassigned.get("total_value", 0.0)
                    unassigned_positions = unassigned.get("positions", [])
                    lines.append(f"  Cash: ${unassigned_cash:,.2f}")
                    lines.append(f"  Positions: {len(unassigned_positions)} (${unassigned_value - unassigned_cash:,.2f})")

                lines.append("=" * 50)
                message = "\n".join(lines)

            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=message,
                data=data,
            )
        except Exception as e:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Failed to get summary: {e}",
            )

    def _get_plugin_holdings(self):
        """Get holdings for all registered plugins"""
        if not self.engine.plugin_executive:
            return {}

        try:
            return self.engine.plugin_executive.get_holdings_summary()
        except Exception as e:
            logger.warning(f"Failed to get plugin holdings: {e}")
            return {}

    def handle_liquidate(self, args: List[str]):
        """Handle 'liquidate' command"""
        from .command_server import CommandResult, CommandStatus

        if self._liquidation_in_progress:
            return CommandResult(
                status=CommandStatus.ERROR,
                message="Liquidation already in progress",
            )

        confirm = "--confirm" in args
        symbols = [a for a in args if not a.startswith("--")]

        portfolio = self.engine.portfolio
        positions = portfolio.positions

        if not positions:
            return CommandResult(
                status=CommandStatus.SUCCESS,
                message="No positions to liquidate",
            )

        if symbols:
            positions = [p for p in positions if p.symbol in symbols]
            if not positions:
                return CommandResult(
                    status=CommandStatus.ERROR,
                    message=f"Symbol(s) not found: {', '.join(symbols)}",
                )

        total_value = sum(p.market_value for p in positions)
        sell_list = [(p.symbol, p.quantity, p.market_value) for p in positions]

        if not confirm:
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
        order_ids = []
        errors = []

        for pos in positions:
            if pos.contract and pos.quantity > 0:
                order_id = portfolio.place_market_order(
                    contract=pos.contract,
                    action="SELL",
                    quantity=pos.quantity,
                )
                if order_id:
                    order_ids.append(order_id)
                else:
                    errors.append(f"Failed to place order for {pos.symbol}")

        self._liquidation_in_progress = False

        if errors:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Partial liquidation: {len(order_ids)} orders, {len(errors)} failed",
                data={"order_ids": order_ids, "errors": errors},
            )

        return CommandResult(
            status=CommandStatus.SUCCESS,
            message=f"Liquidation initiated: {len(order_ids)} sell orders",
            data={"order_ids": order_ids, "total_value": total_value},
        )

    def handle_sell(self, args: List[str]):
        """Handle 'sell' command"""
        from .command_server import CommandResult, CommandStatus

        if len(args) < 2:
            return CommandResult(
                status=CommandStatus.ERROR,
                message="Usage: sell SYMBOL QTY [--confirm]",
            )

        symbol = args[0].upper()
        qty_arg = args[1].lower()
        confirm = "--confirm" in args

        portfolio = self.engine.portfolio
        pos = portfolio.get_position(symbol)
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

        order_id = portfolio.place_market_order(
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

    def handle_buy(self, args: List[str]):
        """Handle 'buy' command"""
        from .command_server import CommandResult, CommandStatus

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

        portfolio = self.engine.portfolio
        pos = portfolio.get_position(symbol)
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

        order_id = portfolio.place_market_order(
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

    def handle_trade(self, args: List[str]):
        """Handle 'trade' command - execute trade with plugin attribution"""
        from .command_server import CommandResult, CommandStatus

        if not self.engine.plugin_executive:
            return CommandResult(
                status=CommandStatus.ERROR,
                message="Trade command requires plugin executive ",
            )

        if len(args) < 4:
            return CommandResult(
                status=CommandStatus.ERROR,
                message="Usage: trade PLUGIN ACTION SYMBOL QTY [--confirm] [--reason \"text\"]",
            )

        plugin_name = args[0]
        action = args[1].upper()
        symbol = args[2].upper()

        try:
            quantity = int(args[3])
        except ValueError:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Invalid quantity: {args[3]}. Must be an integer.",
            )

        confirm = "--confirm" in args
        reason = "manual_trade"

        for i, arg in enumerate(args):
            if arg == "--reason" and i + 1 < len(args):
                reason = args[i + 1]
                break

        if action not in ("BUY", "SELL"):
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Invalid action: {action}. Must be BUY or SELL.",
            )

        if quantity <= 0:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Invalid quantity: {quantity}. Must be positive.",
            )

        try:
            dry_run = not confirm
            success, order_id, message = self.engine.plugin_executive.execute_manual_trade(
                plugin_name=plugin_name,
                symbol=symbol,
                action=action,
                quantity=quantity,
                reason=reason,
                dry_run=dry_run,
            )

            if success:
                return CommandResult(
                    status=CommandStatus.SUCCESS,
                    message=message,
                    data={
                        "plugin": plugin_name,
                        "action": action,
                        "symbol": symbol,
                        "quantity": quantity,
                        "order_id": order_id,
                        "dry_run": dry_run,
                    },
                )
            else:
                return CommandResult(
                    status=CommandStatus.ERROR,
                    message=message,
                )
        except Exception as e:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Trade failed: {e}",
            )

    def handle_order(self, args: List[str]):
        """
        Handle 'order' command - execute any IB order type.

        Usage:
            order ACTION SYMBOL QTY [TYPE] [options] [--confirm]

        Examples:
            order buy SPY 100                      # Market order (dry run)
            order buy SPY 100 --confirm            # Market order (execute)
            order buy SPY 100 limit 450.00         # Limit order
            order sell QQQ 50 stop 380.00          # Stop order
            order buy AAPL 25 stop-limit 175 170   # Stop-limit order
            order sell MSFT 30 trail 2.00          # Trailing stop $2
            order sell MSFT 30 trail 1%            # Trailing stop 1%
            order buy SPY 100 moc                  # Market on Close
            order sell QQQ 50 loc 380.00           # Limit on Close
        """
        from .command_server import CommandResult, CommandStatus
        from plugins.orders import OrdersPlugin, OrderType, TimeInForce

        # Get orders plugin
        orders_plugin = None
        if self.engine.plugin_executive:
            from plugins.orders.plugin import ORDERS_PLUGIN_NAME
            config = self.engine.plugin_executive._plugins.get(ORDERS_PLUGIN_NAME)
            if config:
                orders_plugin = config.plugin

        if not orders_plugin:
            # Try to execute directly through portfolio if no plugin
            return self._handle_order_direct(args)

        # Parse arguments
        if len(args) < 3:
            from plugins.orders.plugin import get_order_help
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Usage: order ACTION SYMBOL QTY [TYPE] [options] [--confirm]\n\n{get_order_help()}",
            )

        action = args[0].upper()
        symbol = args[1].upper()

        try:
            quantity = float(args[2])
        except ValueError:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Invalid quantity: {args[2]}",
            )

        # Parse remaining arguments
        confirm = "--confirm" in args
        remaining = [a for a in args[3:] if a != "--confirm"]

        # Default order type is market
        order_type = OrderType.MARKET
        limit_price = None
        stop_price = None
        trail_amount = None
        trail_percent = None
        tif = TimeInForce.DAY

        i = 0
        while i < len(remaining):
            arg = remaining[i].lower()

            # Check for --tif option
            if arg == "--tif" and i + 1 < len(remaining):
                tif_parsed = orders_plugin.parse_tif(remaining[i + 1])
                if tif_parsed:
                    tif = tif_parsed
                i += 2
                continue

            # Check for order type
            parsed_type = orders_plugin.parse_order_type(arg)
            if parsed_type:
                order_type = parsed_type

                # Handle types that need prices
                if order_type == OrderType.LIMIT:
                    if i + 1 < len(remaining):
                        try:
                            limit_price = float(remaining[i + 1])
                            i += 1
                        except ValueError:
                            pass

                elif order_type == OrderType.STOP:
                    if i + 1 < len(remaining):
                        try:
                            stop_price = float(remaining[i + 1])
                            i += 1
                        except ValueError:
                            pass

                elif order_type == OrderType.STOP_LIMIT:
                    # Expects: stop-limit STOP_PRICE LIMIT_PRICE
                    if i + 2 < len(remaining):
                        try:
                            stop_price = float(remaining[i + 1])
                            limit_price = float(remaining[i + 2])
                            i += 2
                        except ValueError:
                            pass

                elif order_type in (OrderType.TRAILING_STOP, OrderType.TRAILING_STOP_LIMIT):
                    # Expects: trail AMOUNT or trail PERCENT%
                    if i + 1 < len(remaining):
                        trail_str = remaining[i + 1]
                        if trail_str.endswith('%'):
                            try:
                                trail_percent = float(trail_str[:-1])
                            except ValueError:
                                pass
                        else:
                            try:
                                trail_amount = float(trail_str)
                            except ValueError:
                                pass
                        i += 1
                    # For trail limit, also get limit price
                    if order_type == OrderType.TRAILING_STOP_LIMIT and i + 1 < len(remaining):
                        try:
                            limit_price = float(remaining[i + 1])
                            i += 1
                        except ValueError:
                            pass

                elif order_type in (OrderType.LIMIT_ON_CLOSE, OrderType.LIMIT_ON_OPEN):
                    if i + 1 < len(remaining):
                        try:
                            limit_price = float(remaining[i + 1])
                            i += 1
                        except ValueError:
                            pass

            i += 1

        # Execute the order
        try:
            success, order_id, message = orders_plugin.execute_order(
                symbol=symbol,
                action=action,
                quantity=quantity,
                order_type=order_type,
                limit_price=limit_price,
                stop_price=stop_price,
                trail_amount=trail_amount,
                trail_percent=trail_percent,
                tif=tif,
                dry_run=not confirm,
            )

            if success:
                return CommandResult(
                    status=CommandStatus.SUCCESS,
                    message=message,
                    data={
                        "action": action,
                        "symbol": symbol,
                        "quantity": quantity,
                        "order_type": order_type.name,
                        "order_id": order_id,
                        "dry_run": not confirm,
                    },
                )
            else:
                return CommandResult(
                    status=CommandStatus.ERROR,
                    message=message,
                )

        except Exception as e:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Order failed: {e}",
            )

    def _handle_order_direct(self, args: List[str]):
        """Handle order command directly through portfolio (fallback)"""
        from .command_server import CommandResult, CommandStatus

        if len(args) < 3:
            return CommandResult(
                status=CommandStatus.ERROR,
                message="Usage: order ACTION SYMBOL QTY [limit PRICE] [stop PRICE] [--confirm]",
            )

        action = args[0].upper()
        symbol = args[1].upper()

        try:
            quantity = float(args[2])
        except ValueError:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Invalid quantity: {args[2]}",
            )

        confirm = "--confirm" in args

        # Parse order type from remaining args
        order_type = "MKT"
        limit_price = 0.0
        stop_price = 0.0

        remaining = [a for a in args[3:] if a != "--confirm"]
        i = 0
        while i < len(remaining):
            arg = remaining[i].lower()
            if arg == "limit" and i + 1 < len(remaining):
                order_type = "LMT"
                try:
                    limit_price = float(remaining[i + 1])
                except ValueError:
                    pass
                i += 2
            elif arg == "stop" and i + 1 < len(remaining):
                order_type = "STP"
                try:
                    stop_price = float(remaining[i + 1])
                except ValueError:
                    pass
                i += 2
            elif arg == "stop-limit" and i + 2 < len(remaining):
                order_type = "STP LMT"
                try:
                    stop_price = float(remaining[i + 1])
                    limit_price = float(remaining[i + 2])
                except ValueError:
                    pass
                i += 3
            else:
                i += 1

        # Get contract
        portfolio = self.engine.portfolio
        pos = portfolio.get_position(symbol)
        if pos and pos.contract:
            contract = pos.contract
        else:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"No existing position for {symbol}. Cannot determine contract.",
            )

        # Build description
        desc = f"{action} {quantity:.0f} {symbol} {order_type}"
        if limit_price > 0:
            desc += f" @ ${limit_price:.2f}"
        if stop_price > 0:
            desc += f" stop ${stop_price:.2f}"

        if not confirm:
            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=f"[DRY RUN] Would place: {desc}. Use --confirm to execute.",
                data={"dry_run": True},
            )

        order_id = portfolio.place_order(
            contract=contract,
            action=action,
            quantity=quantity,
            order_type=order_type,
            limit_price=limit_price,
            stop_price=stop_price,
        )

        if order_id:
            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=f"[EXECUTED] Order {order_id}: {desc}",
                data={"order_id": order_id},
            )
        else:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Failed to place order: {desc}",
            )

    def handle_transfer(self, args: List[str]):
        """
        Handle 'transfer' command - move cash or positions between plugins.

        This is internal bookkeeping only - no actual trades are placed.

        Usage:
            transfer cash FROM_PLUGIN TO_PLUGIN AMOUNT [--confirm]
            transfer position FROM_PLUGIN TO_PLUGIN SYMBOL QTY [--confirm]
            transfer list PLUGIN                  # Show transferable assets

        Examples:
            transfer cash _unassigned momentum_5day 10000
            transfer position _unassigned momentum_5day SPY 100
            transfer list _unassigned
        """
        from .command_server import CommandResult, CommandStatus

        if not self.engine.plugin_executive:
            return CommandResult(
                status=CommandStatus.ERROR,
                message="Transfer command requires plugin executive ",
            )

        if not args:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=(
                    "Usage:\n"
                    "  transfer cash FROM TO AMOUNT [--confirm]\n"
                    "  transfer position FROM TO SYMBOL QTY [--confirm]\n"
                    "  transfer list PLUGIN"
                ),
            )

        subcommand = args[0].lower()
        pe = self.engine.plugin_executive

        if subcommand == "list":
            # List transferable assets from a plugin
            if len(args) < 2:
                return CommandResult(
                    status=CommandStatus.ERROR,
                    message="Usage: transfer list PLUGIN",
                )

            plugin_name = args[1]
            cash = pe.get_transferable_cash(plugin_name)
            positions = pe.get_transferable_positions(plugin_name)

            lines = [f"Transferable from '{plugin_name}':", f"  Cash: ${cash:,.2f}"]
            if positions:
                lines.append("  Positions:")
                for p in positions:
                    lines.append(f"    {p['symbol']}: {p['quantity']:.2f} (${p['value']:,.2f})")
            else:
                lines.append("  Positions: (none)")

            return CommandResult(
                status=CommandStatus.SUCCESS,
                message="\n".join(lines),
                data={"cash": cash, "positions": positions},
            )

        elif subcommand == "cash":
            # Transfer cash between plugins
            if len(args) < 4:
                return CommandResult(
                    status=CommandStatus.ERROR,
                    message="Usage: transfer cash FROM TO AMOUNT [--confirm]",
                )

            from_plugin = args[1]
            to_plugin = args[2]
            confirm = "--confirm" in args

            try:
                amount = float(args[3])
            except ValueError:
                return CommandResult(
                    status=CommandStatus.ERROR,
                    message=f"Invalid amount: {args[3]}",
                )

            if not confirm:
                return CommandResult(
                    status=CommandStatus.SUCCESS,
                    message=f"[DRY RUN] Would transfer ${amount:,.2f} from '{from_plugin}' to '{to_plugin}'. Use --confirm to execute.",
                    data={"dry_run": True, "from": from_plugin, "to": to_plugin, "amount": amount},
                )

            success, message = pe.transfer_cash(from_plugin, to_plugin, amount)
            return CommandResult(
                status=CommandStatus.SUCCESS if success else CommandStatus.ERROR,
                message=message,
                data={"from": from_plugin, "to": to_plugin, "amount": amount} if success else {},
            )

        elif subcommand == "position":
            # Transfer position between plugins
            if len(args) < 5:
                return CommandResult(
                    status=CommandStatus.ERROR,
                    message="Usage: transfer position FROM TO SYMBOL QTY [--confirm]",
                )

            from_plugin = args[1]
            to_plugin = args[2]
            symbol = args[3].upper()
            confirm = "--confirm" in args

            try:
                quantity = float(args[4])
            except ValueError:
                return CommandResult(
                    status=CommandStatus.ERROR,
                    message=f"Invalid quantity: {args[4]}",
                )

            if not confirm:
                return CommandResult(
                    status=CommandStatus.SUCCESS,
                    message=f"[DRY RUN] Would transfer {quantity:.2f} {symbol} from '{from_plugin}' to '{to_plugin}'. Use --confirm to execute.",
                    data={"dry_run": True, "from": from_plugin, "to": to_plugin, "symbol": symbol, "quantity": quantity},
                )

            success, message = pe.transfer_position(from_plugin, to_plugin, symbol, quantity)
            return CommandResult(
                status=CommandStatus.SUCCESS if success else CommandStatus.ERROR,
                message=message,
                data={"from": from_plugin, "to": to_plugin, "symbol": symbol, "quantity": quantity} if success else {},
            )

        else:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Unknown transfer subcommand: {subcommand}. Use 'cash', 'position', or 'list'.",
            )

    def handle_reconcile(self, args: List[str]):
        """
        Handle 'reconcile' command - sync plugin holdings with account.

        Compares plugin holdings against actual account positions and cash.
        Reports discrepancies and adjusts plugin holdings to match reality.

        Usage:
            reconcile              # Run reconciliation and show report
            reconcile --json       # Output report as JSON
        """
        from .command_server import CommandResult, CommandStatus
        import json

        if not self.engine.plugin_executive:
            return CommandResult(
                status=CommandStatus.ERROR,
                message="Reconcile command requires plugin executive ",
            )

        output_json = "--json" in args

        try:
            pe = self.engine.plugin_executive
            report = pe.reconcile_with_account()

            if output_json:
                message = json.dumps(report, indent=2)
            else:
                message = pe.format_reconciliation_report(report)

            discrepancy_count = len(report.get("discrepancies", []))
            adjustment_count = len(report.get("adjustments", []))

            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=message,
                data={
                    "discrepancies": discrepancy_count,
                    "adjustments": adjustment_count,
                    "report": report,
                },
            )

        except Exception as e:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Reconciliation failed: {e}",
            )

    def handle_historical(self, args: List[str]):
        """
        Handle 'historical' command — fetch bar data from IB and return as JSON.

        Usage:
            historical fetch SYMBOL [options]

        Options:
            --bar-size X     Bar width with spaces replaced by _: 5_mins, 1_hour,
                             1_day (default: 1_day)
            --duration X     How far back: 1_D, 1_W, 1_M, 1_Y (default: 1_W)
            --end DATETIME   End date as YYYYMMDD-HH:MM:SS UTC (default: now)
            --what X         TRADES, MIDPOINT, BID, ASK (default: TRADES)
            --no-rth         Include extended-hours bars
            --type X         Contract type: etf (default), stock, forex

        Returns bars as JSON with keys:
            symbol, bar_size, duration, what_to_show, use_rth,
            fetch_start_utc, fetch_end_utc, bars (list of dicts), count
        """
        import threading
        from datetime import datetime, timezone, timedelta
        from .command_server import CommandResult, CommandStatus

        if not args or args[0].lower() != "fetch":
            return CommandResult(
                status=CommandStatus.ERROR,
                message=(
                    "Usage: historical fetch SYMBOL [--bar-size X] [--duration X] "
                    "[--end DATETIME] [--what X] [--no-rth] [--type etf|stock|forex]"
                ),
            )

        if len(args) < 2:
            return CommandResult(
                status=CommandStatus.ERROR,
                message="historical fetch requires a SYMBOL argument",
            )

        symbol = args[1].upper()

        # Parse flags
        bar_size       = "1 day"
        duration       = "1 W"
        end_date_time  = ""
        what_to_show   = "TRADES"
        use_rth        = True
        contract_type  = "etf"

        i = 2
        while i < len(args):
            a = args[i]
            if a == "--bar-size" and i + 1 < len(args):
                bar_size = args[i + 1].replace("_", " ")
                i += 2
            elif a == "--duration" and i + 1 < len(args):
                duration = args[i + 1].replace("_", " ")
                i += 2
            elif a == "--end" and i + 1 < len(args):
                end_date_time = args[i + 1]
                i += 2
            elif a == "--what" and i + 1 < len(args):
                what_to_show = args[i + 1]
                i += 2
            elif a == "--no-rth":
                use_rth = False
                i += 1
            elif a == "--type" and i + 1 < len(args):
                contract_type = args[i + 1].lower()
                i += 2
            else:
                i += 1

        portfolio = self.engine.portfolio
        if not portfolio or not portfolio.connected:
            return CommandResult(
                status=CommandStatus.ERROR,
                message="Not connected to IB",
            )

        # Build contract
        from .contract_builder import ContractBuilder
        if contract_type == "stock":
            contract = ContractBuilder.us_stock(symbol)
        elif contract_type == "forex":
            contract = ContractBuilder.forex(symbol)
        else:
            contract = ContractBuilder.etf(symbol)

        # Blocking fetch
        done   = threading.Event()
        holder: dict = {}

        def on_end(bars, start, end):
            holder["bars"] = bars
            done.set()

        fetch_start_utc = datetime.now(timezone.utc)
        portfolio.request_historical_data(
            contract=contract,
            end_date_time=end_date_time,
            duration_str=duration,
            bar_size_setting=bar_size,
            what_to_show=what_to_show,
            use_rth=use_rth,
            on_end=on_end,
        )

        if not done.wait(120.0):
            return CommandResult(
                status=CommandStatus.ERROR,
                message="Timeout waiting for historical data (120s)",
            )

        fetch_end_utc = datetime.now(timezone.utc)
        bars = holder.get("bars") or []

        bar_dicts = [
            {
                "date":      str(b.date),
                "open":      float(b.open),
                "high":      float(b.high),
                "low":       float(b.low),
                "close":     float(b.close),
                "volume":    int(b.volume),
                "wap":       float(getattr(b, "wap",      0.0)),
                "bar_count": int(getattr(b, "barCount",   0)),
            }
            for b in bars
        ]

        return CommandResult(
            status=CommandStatus.SUCCESS,
            message=f"Fetched {len(bar_dicts)} bars for {symbol} ({bar_size}, {duration})",
            data={
                "symbol":          symbol,
                "bar_size":        bar_size,
                "duration":        duration,
                "what_to_show":    what_to_show,
                "use_rth":         use_rth,
                "fetch_start_utc": fetch_start_utc.strftime("%Y-%m-%dT%H:%M:%S"),
                "fetch_end_utc":   fetch_end_utc.strftime("%Y-%m-%dT%H:%M:%S"),
                "bars":            bar_dicts,
                "count":           len(bar_dicts),
            },
        )

    def handle_pause(self, args: List[str]):
        """Handle 'pause' command - pause algorithm/plugin execution"""
        from .command_server import CommandResult, CommandStatus

        self.engine.pause()
        return CommandResult(
            status=CommandStatus.SUCCESS,
            message="Engine paused (data still flowing)",
        )

    def handle_resume(self, args: List[str]):
        """Handle 'resume' command - resume algorithm/plugin execution"""
        from .command_server import CommandResult, CommandStatus

        self.engine.resume()
        return CommandResult(
            status=CommandStatus.SUCCESS,
            message="Engine resumed",
        )

    def handle_stop(self, args: List[str]):
        """Handle 'stop' or 'shutdown' command"""
        import asyncio
        from .command_server import CommandResult, CommandStatus

        logger.info("Shutdown requested via command")
        loop = getattr(self.engine, "_loop", None)
        if loop is not None and loop.is_running():
            asyncio.run_coroutine_threadsafe(self.engine.stop(), loop)
        else:
            # Fallback: set shutdown event directly if loop isn't captured yet
            self.engine._shutdown_event.set()

        return CommandResult(
            status=CommandStatus.SUCCESS,
            message="Shutdown initiated",
        )

    def handle_plugin(self, args: List[str]):
        """Handle 'plugin' command - plugin control"""
        from .command_server import CommandResult, CommandStatus

        if not self.engine.plugin_executive:
            return CommandResult(
                status=CommandStatus.ERROR,
                message="Plugin executive not available ",
            )

        if not args:
            return CommandResult(
                status=CommandStatus.ERROR,
                message="Usage: plugin <list|status|start|stop|freeze|resume|trigger|dump> [name]",
            )

        subcommand = args[0].lower()
        subargs = args[1:]
        pe = self.engine.plugin_executive

        if subcommand == "list":
            plugin_names = pe.plugins  # List of plugin instance IDs
            status_list = {}
            lines = []
            for iid in sorted(plugin_names):
                status = pe.get_plugin_status(iid)
                if status:
                    is_system = status.get("is_system_plugin", False)
                    state = _display_state(status["state"])
                    enabled = status["enabled"]
                    plugin_name = status["name"]
                    slot = status["slot"]
                    short_id = iid[:8]

                    # Display label: "name" or "name:slot" if slot differs
                    label = plugin_name if slot == plugin_name else f"{plugin_name}:{slot}"

                    # Format: label  short-uuid  [STATE] (system)
                    parts = [f"  {label:<24} {short_id}  [{state}]"]
                    if is_system:
                        parts.append("(system)")
                    lines.append(" ".join(parts))

                    status_list[iid] = {
                        "name": plugin_name,
                        "slot": slot,
                        "state": state,
                        "is_system_plugin": is_system,
                        "enabled": enabled,
                        "run_count": status["run_count"],
                    }

            n = len(plugin_names)
            message = f"{n} plugin{'s' if n != 1 else ''}:\n" + "\n".join(lines)
            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=message,
                data={"plugins": status_list},
            )

        elif subcommand == "status" and subargs:
            status = pe.get_plugin_status(subargs[0])
            if status:
                return CommandResult(
                    status=CommandStatus.SUCCESS,
                    message=f"Plugin '{subargs[0]}': {_display_state(status['state'])}  (id: {status.get('instance_id', 'n/a')})",
                    data=status,
                )
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Plugin '{subargs[0]}' not found",
            )

        elif subcommand == "start" and subargs:
            if pe.start_plugin(subargs[0]):
                return CommandResult(
                    status=CommandStatus.SUCCESS,
                    message=f"Plugin '{subargs[0]}' started",
                )
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Failed to start plugin '{subargs[0]}'",
            )

        elif subcommand == "stop" and subargs:
            if pe.stop_plugin(subargs[0]):
                return CommandResult(
                    status=CommandStatus.SUCCESS,
                    message=f"Plugin '{subargs[0]}' stopped",
                )
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Failed to stop plugin '{subargs[0]}'",
            )

        elif subcommand == "freeze" and subargs:
            if pe.freeze_plugin(subargs[0]):
                return CommandResult(
                    status=CommandStatus.SUCCESS,
                    message=f"Plugin '{subargs[0]}' frozen",
                )
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Failed to freeze plugin '{subargs[0]}'",
            )

        elif subcommand == "resume" and subargs:
            if pe.resume_plugin(subargs[0]):
                return CommandResult(
                    status=CommandStatus.SUCCESS,
                    message=f"Plugin '{subargs[0]}' resumed",
                )
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Failed to resume plugin '{subargs[0]}'",
            )

        elif subcommand == "enable" and subargs:
            if pe.enable_plugin(subargs[0], True):
                return CommandResult(
                    status=CommandStatus.SUCCESS,
                    message=f"Plugin '{subargs[0]}' enabled",
                )
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Plugin '{subargs[0]}' not found",
            )

        elif subcommand == "disable" and subargs:
            if pe.enable_plugin(subargs[0], False):
                return CommandResult(
                    status=CommandStatus.SUCCESS,
                    message=f"Plugin '{subargs[0]}' disabled",
                )
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Plugin '{subargs[0]}' not found",
            )

        elif subcommand == "trigger" and subargs:
            result = pe.trigger_plugin(subargs[0])
            if result:
                signals_count = len(result.actionable_signals) if result.signals else 0
                return CommandResult(
                    status=CommandStatus.SUCCESS,
                    message=f"Plugin '{subargs[0]}' triggered: {signals_count} signals",
                )
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Plugin '{subargs[0]}' not found",
            )

        elif subcommand == "dump":
            if not subargs:
                return CommandResult(
                    status=CommandStatus.ERROR,
                    message="Usage: plugin dump NAME_OR_ID",
                )
            iid, config = pe._resolve_plugin(subargs[0])
            if not config:
                return CommandResult(
                    status=CommandStatus.ERROR,
                    message=f"Plugin '{subargs[0]}' not found",
                )

            plugin = config.plugin
            holdings = plugin.get_effective_holdings()

            open_orders = []
            for oid, po in pe._pending_orders.items():
                if po.plugin_name == plugin.name:
                    open_orders.append({
                        "order_id": oid,
                        "symbol": po.signal.symbol,
                        "action": po.signal.action,
                        "quantity": po.signal.quantity,
                        "status": po.status,
                        "created_at": po.created_at.isoformat(),
                    })

            name = plugin.slot
            data = {
                "plugin": name,
                "cash": holdings.get("cash", 0.0),
                "positions": holdings.get("positions", []),
                "open_orders": open_orders,
            }

            # Build text summary
            lines = [f"Plugin '{name}' dump:"]
            lines.append(f"  Cash: ${data['cash']:,.2f}")
            if data["positions"]:
                lines.append(f"  Positions ({len(data['positions'])}):")
                for p in data["positions"]:
                    lines.append(
                        f"    {p['symbol']:<8} {p['quantity']:>10,.0f} "
                        f"cost=${p.get('cost_basis', 0):>10,.2f} "
                        f"price=${p.get('current_price', 0):>10,.2f} "
                        f"value=${p.get('market_value', 0):>12,.2f}"
                    )
            else:
                lines.append("  Positions: (none)")
            if open_orders:
                lines.append(f"  Open orders ({len(open_orders)}):")
                for o in open_orders:
                    lines.append(
                        f"    #{o['order_id']} {o['action']} "
                        f"{o['symbol']} x{o['quantity']} [{o['status']}]"
                    )
            else:
                lines.append("  Open orders: (none)")

            return CommandResult(
                status=CommandStatus.SUCCESS,
                message="\n".join(lines),
                data=data,
            )

        elif subcommand == "request" and len(subargs) >= 2:
            name = subargs[0]
            request_type = subargs[1]
            payload = {}
            if len(subargs) >= 3:
                import json as _json
                try:
                    payload = _json.loads(subargs[2])
                except Exception as e:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message=f"Invalid JSON payload: {e}",
                    )

            _, config = pe._resolve_plugin(name)
            if not config:
                return CommandResult(
                    status=CommandStatus.ERROR,
                    message=f"Plugin '{name}' not found",
                )

            try:
                result = config.plugin.handle_request(request_type, payload)
            except Exception as e:
                return CommandResult(
                    status=CommandStatus.ERROR,
                    message=f"Plugin request error: {e}",
                )

            if result.get("success"):
                return CommandResult(
                    status=CommandStatus.SUCCESS,
                    message=result.get("message", f"Plugin '{name}' handled '{request_type}'"),
                    data=result.get("data", {}),
                )
            return CommandResult(
                status=CommandStatus.ERROR,
                message=result.get("message", f"Plugin '{name}' returned failure for '{request_type}'"),
                data=result.get("data", {}),
            )

        elif subcommand == "load" and subargs:
            module_or_path = subargs[0]

            # Normalise: convert filesystem path to dotted module name
            if "/" in module_or_path or module_or_path.endswith(".py"):
                from pathlib import Path as _Path
                p = _Path(module_or_path.rstrip("/"))
                if p.suffix == ".py":
                    p = p.parent           # strip plugin.py → dir
                parts: List[str] = []
                while p.name:
                    parts.insert(0, p.name)
                    parent = p.parent
                    if not (parent / "__init__.py").exists():
                        break
                    p = parent
                module_name = ".".join(parts)
            else:
                module_name = module_or_path

            try:
                import importlib as _il
                # Always evict the module (and any submodules) from sys.modules
                # so that each "plugin load" picks up the latest code on disk.
                _prefix = module_name + "."
                _stale = [k for k in sys.modules if k == module_name or k.startswith(_prefix)]
                for _k in _stale:
                    del sys.modules[_k]
                module = _il.import_module(module_name)

                from plugins.base import PluginBase
                plugin_class = None
                for _attr_name, _obj in vars(module).items():
                    if (
                        isinstance(_obj, type)
                        and issubclass(_obj, PluginBase)
                        and _obj is not PluginBase
                        and not _attr_name.startswith("_")
                    ):
                        plugin_class = _obj
                        break

                if plugin_class is None:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message=f"No PluginBase subclass found in '{module_name}'",
                    )

                plugin_instance = plugin_class(
                    portfolio=self.engine.portfolio,
                    message_bus=(
                        self.engine.message_bus
                        if hasattr(self.engine, "message_bus")
                        else None
                    ),
                )

                from .plugin_executive import ExecutionMode
                from pathlib import Path as _Path2
                pe.register_plugin(
                    plugin_instance,
                    execution_mode=ExecutionMode.MANUAL,
                    enabled=True,
                )

                # Record source_file so the registry can persist lifecycle status
                cfg = pe._plugins.get(plugin_instance.instance_id)
                if cfg and hasattr(module, "__file__") and module.__file__:
                    src = _Path2(module.__file__)
                    # Resolve plugin.py → its parent package directory
                    if src.name == "plugin.py" or src.stem == src.parent.name:
                        src = src.parent
                    cfg.source_file = src

                return CommandResult(
                    status=CommandStatus.SUCCESS,
                    message=f"Loaded plugin '{plugin_instance.name}' from {module_name}",
                    data={
                        "plugin_name": plugin_instance.name,
                        "instance_id": plugin_instance.name,
                        "module": module_name,
                    },
                )

            except Exception as exc:
                return CommandResult(
                    status=CommandStatus.ERROR,
                    message=f"Failed to load plugin from '{module_or_path}': {exc}",
                )

        elif subcommand == "unload" and subargs:
            name = subargs[0]
            if pe.unload_plugin(name):
                return CommandResult(
                    status=CommandStatus.SUCCESS,
                    message=f"Plugin '{name}' unloaded",
                )
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Plugin '{name}' not found or could not be unloaded",
            )

        elif subcommand == "instruments" and subargs:
            from plugins.base import PluginInstrument
            isub = subargs[0].lower()
            iargs = subargs[1:]

            if isub == "list" and iargs:
                name = iargs[0]
                instruments = pe.get_plugin_instruments(name)
                if instruments is None:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message=f"Plugin '{name}' not found",
                    )
                _, config = pe._resolve_plugin(name)
                compliance = getattr(config.plugin, "INSTRUMENT_COMPLIANCE", False) if config else False
                return CommandResult(
                    status=CommandStatus.SUCCESS,
                    message=f"{len(instruments)} instrument(s) for '{name}'",
                    data={
                        "instruments": [i.to_dict() for i in instruments],
                        "compliance": compliance,
                    },
                )

            elif isub == "add" and len(iargs) >= 2:
                name, symbol = iargs[0], iargs[1].upper()
                flags = iargs[2:]
                # Parse optional flags
                inst_name = symbol
                weight = 0.0
                min_weight = 0.0
                max_weight = 100.0
                exchange = "SMART"
                currency = "USD"
                sec_type = "STK"
                enabled = True
                i = 0
                while i < len(flags):
                    f = flags[i]
                    if f == "--name" and i + 1 < len(flags):
                        inst_name = flags[i + 1]; i += 2
                    elif f == "--weight" and i + 1 < len(flags):
                        weight = float(flags[i + 1]); i += 2
                    elif f == "--min-weight" and i + 1 < len(flags):
                        min_weight = float(flags[i + 1]); i += 2
                    elif f == "--max-weight" and i + 1 < len(flags):
                        max_weight = float(flags[i + 1]); i += 2
                    elif f == "--exchange" and i + 1 < len(flags):
                        exchange = flags[i + 1]; i += 2
                    elif f == "--currency" and i + 1 < len(flags):
                        currency = flags[i + 1]; i += 2
                    elif f == "--sec-type" and i + 1 < len(flags):
                        sec_type = flags[i + 1]; i += 2
                    elif f == "--disabled":
                        enabled = False; i += 1
                    else:
                        i += 1
                inst = PluginInstrument(
                    symbol=symbol, name=inst_name,
                    weight=weight, min_weight=min_weight, max_weight=max_weight,
                    enabled=enabled, exchange=exchange, currency=currency,
                    sec_type=sec_type,
                )
                if pe.add_plugin_instrument(name, inst):
                    return CommandResult(
                        status=CommandStatus.SUCCESS,
                        message=f"Instrument '{symbol}' added to '{name}'",
                    )
                return CommandResult(
                    status=CommandStatus.ERROR,
                    message=f"Plugin '{name}' not found",
                )

            elif isub == "remove" and len(iargs) >= 2:
                name, symbol = iargs[0], iargs[1].upper()
                if pe.remove_plugin_instrument(name, symbol):
                    return CommandResult(
                        status=CommandStatus.SUCCESS,
                        message=f"Instrument '{symbol}' removed from '{name}'",
                    )
                return CommandResult(
                    status=CommandStatus.ERROR,
                    message=f"Plugin '{name}' not found or symbol '{symbol}' not registered",
                )

            elif isub in ("enable", "disable") and len(iargs) >= 2:
                name, symbol = iargs[0], iargs[1].upper()
                enabled = (isub == "enable")
                if pe.set_plugin_instrument_enabled(name, symbol, enabled):
                    state = "enabled" if enabled else "disabled"
                    return CommandResult(
                        status=CommandStatus.SUCCESS,
                        message=f"Instrument '{symbol}' {state} in '{name}'",
                    )
                return CommandResult(
                    status=CommandStatus.ERROR,
                    message=f"Plugin '{name}' not found or symbol '{symbol}' not registered",
                )

            elif isub == "clear" and iargs:
                name = iargs[0]
                if pe.clear_plugin_instruments(name):
                    return CommandResult(
                        status=CommandStatus.SUCCESS,
                        message=f"All instruments cleared from '{name}'",
                    )
                return CommandResult(
                    status=CommandStatus.ERROR,
                    message=f"Plugin '{name}' not found",
                )

            elif isub == "reload" and iargs:
                name = iargs[0]
                count = pe.reload_plugin_instruments(name)
                if count is not None:
                    return CommandResult(
                        status=CommandStatus.SUCCESS,
                        message=f"Reloaded {count} instrument(s) for '{name}'",
                    )
                return CommandResult(
                    status=CommandStatus.ERROR,
                    message=f"Plugin '{name}' not found",
                )

            return CommandResult(
                status=CommandStatus.ERROR,
                message=(
                    "Usage: plugin instruments list|add|remove|enable|disable|clear|reload NAME [SYM] [flags]"
                ),
            )

        return CommandResult(
            status=CommandStatus.ERROR,
            message=f"Unknown plugin subcommand: {subcommand}",
        )


if __name__ == "__main__":
    sys.exit(main())
