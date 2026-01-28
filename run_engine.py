#!/usr/bin/env python3
"""
run_engine.py - Start the IB Trading Engine

Full-featured entry point for continuous algorithmic trading with:
- Socket command interface (ibctl.py compatible)
- Plugin/algorithm execution
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
    python3 -m ib.run_engine --plugins     # Use plugin executive instead of algorithm runner
"""

import argparse
import logging
import sys
import os
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
        description="IB Trading Engine - Continuous algorithmic trading",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Environment variables (override defaults):
  PORT    IB Gateway/TWS port (default: 7497)
  MODE    Order mode: dry_run, immediate, queued (default: dry_run)

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

    # Engine mode
    parser.add_argument(
        "--plugins", action="store_true",
        help="Use plugin executive instead of algorithm runner"
    )

    # Command server options
    parser.add_argument(
        "--socket", default="/tmp/ib_portfolio.sock",
        help="Unix socket path for commands (default: /tmp/ib_portfolio.sock)"
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

    # Get config from environment, then override with command line
    port = args.port or int(os.environ.get("PORT", "7497"))
    mode = args.mode or os.environ.get("MODE", "dry_run")

    logger.info("=" * 60)
    logger.info("IB Trading Engine")
    logger.info("=" * 60)
    logger.info(f"Port: {port}")
    logger.info(f"Order Mode: {mode}")
    logger.info(f"Socket: {args.socket if not args.no_server else 'disabled'}")
    logger.info(f"Engine Mode: {'Plugin Executive' if args.plugins else 'Algorithm Runner'}")
    logger.info("=" * 60)

    # Import components
    from .trading_engine import TradingEngine, EngineConfig, EngineState
    from .algorithm_runner import OrderExecutionMode
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
        use_plugin_executive=args.plugins,
        enable_message_bus=args.plugins,
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

    # Setup example algorithm (if not using plugins)
    if not args.plugins:
        try:
            from .algorithms.base import AlgorithmInstrument
            from .algorithms import DummyAlgorithm

            algo = DummyAlgorithm()
            algo.add_instrument(AlgorithmInstrument(symbol="SPY", name="SPDR S&P 500 ETF"))
            algo.add_instrument(AlgorithmInstrument(symbol="QQQ", name="Invesco QQQ Trust"))
            algo._loaded = True

            engine.add_algorithm(algo)
            logger.info(f"Added algorithm: {algo.name}")
        except ImportError as e:
            logger.warning(f"Could not load example algorithm: {e}")

    # Callbacks
    def on_started():
        logger.info("Engine started successfully")
        if command_server:
            if command_server.start():
                logger.info(f"Command server listening on {args.socket}")
            else:
                logger.warning("Failed to start command server")
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
    if engine.start():
        engine.run_forever()
    else:
        logger.error("Failed to start engine")
        return 1

    logger.info("Engine stopped.")
    return 0


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

        # Always register plugin and algo commands - they return helpful
        # errors if the feature isn't enabled
        server.register_handler("plugin", self.handle_plugin)
        server.register_handler("algo", self.handle_algo)

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
                holdings_summary = self.engine.plugin_executive.get_holdings_summary()
                plugin_holdings = holdings_summary.get("plugins", {})
                unassigned = holdings_summary.get("unassigned")

                # Sync unassigned holdings with current portfolio state
                self.engine.plugin_executive.sync_unassigned_holdings()

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
                message="Trade command requires plugin executive (use --plugins flag)",
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
        from .command_server import CommandResult, CommandStatus

        logger.info("Shutdown requested via command")
        self.engine.stop()

        return CommandResult(
            status=CommandStatus.SUCCESS,
            message="Shutdown initiated",
        )

    def handle_algo(self, args: List[str]):
        """Handle 'algo' command - algorithm control"""
        from .command_server import CommandResult, CommandStatus

        if not self.engine.runner:
            return CommandResult(
                status=CommandStatus.ERROR,
                message="Algorithm runner not available",
            )

        if not args:
            return CommandResult(
                status=CommandStatus.ERROR,
                message="Usage: algo <list|status|enable|disable|trigger> [name]",
            )

        subcommand = args[0].lower()
        subargs = args[1:]

        if subcommand == "list":
            algos = self.engine.runner.algorithms
            status_list = {}
            for name in algos:
                status = self.engine.runner.get_algorithm_status(name)
                if status:
                    status_list[name] = {
                        "enabled": status["enabled"],
                        "run_count": status["run_count"],
                    }
            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=f"{len(algos)} algorithms",
                data={"algorithms": status_list},
            )

        elif subcommand == "status" and subargs:
            status = self.engine.runner.get_algorithm_status(subargs[0])
            if status:
                return CommandResult(
                    status=CommandStatus.SUCCESS,
                    message=f"Algorithm '{subargs[0]}'",
                    data=status,
                )
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Algorithm '{subargs[0]}' not found",
            )

        elif subcommand == "enable" and subargs:
            self.engine.runner.enable_algorithm(subargs[0], True)
            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=f"Algorithm '{subargs[0]}' enabled",
            )

        elif subcommand == "disable" and subargs:
            self.engine.runner.enable_algorithm(subargs[0], False)
            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=f"Algorithm '{subargs[0]}' disabled",
            )

        elif subcommand == "trigger" and subargs:
            result = self.engine.runner.trigger_algorithm(subargs[0])
            if result:
                return CommandResult(
                    status=CommandStatus.SUCCESS,
                    message=f"Algorithm '{subargs[0]}' triggered: {result.signals_count} signals",
                )
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Algorithm '{subargs[0]}' not found",
            )

        return CommandResult(
            status=CommandStatus.ERROR,
            message=f"Unknown algo subcommand: {subcommand}",
        )

    def handle_plugin(self, args: List[str]):
        """Handle 'plugin' command - plugin control"""
        from .command_server import CommandResult, CommandStatus

        if not self.engine.plugin_executive:
            return CommandResult(
                status=CommandStatus.ERROR,
                message="Plugin executive not available (use --plugins flag)",
            )

        if not args:
            return CommandResult(
                status=CommandStatus.ERROR,
                message="Usage: plugin <list|status|start|stop|freeze|resume|trigger> [name]",
            )

        subcommand = args[0].lower()
        subargs = args[1:]
        pe = self.engine.plugin_executive

        if subcommand == "list":
            plugins = pe.plugins
            status_list = {}
            for name in plugins:
                status = pe.get_plugin_status(name)
                if status:
                    status_list[name] = {
                        "state": status["state"],
                        "enabled": status["enabled"],
                        "run_count": status["run_count"],
                    }
            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=f"{len(plugins)} plugins",
                data={"plugins": status_list},
            )

        elif subcommand == "status" and subargs:
            status = pe.get_plugin_status(subargs[0])
            if status:
                return CommandResult(
                    status=CommandStatus.SUCCESS,
                    message=f"Plugin '{subargs[0]}': {status['state']}",
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

        return CommandResult(
            status=CommandStatus.ERROR,
            message=f"Unknown plugin subcommand: {subcommand}",
        )


if __name__ == "__main__":
    sys.exit(main())
