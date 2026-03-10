#!/usr/bin/env python3
"""
main.py - Entry point for TWS Headless

Usage:
    python main.py                    # Show portfolio
    python main.py --stream           # Stream live tick prices
    python main.py --bars             # Stream 5-second OHLCV bars
    python main.py --rebalance        # Calculate rebalance
    python main.py --execute          # Execute rebalance (requires confirmation)
"""

import argparse
import asyncio
import atexit
import logging
import signal
import sys
import time
from datetime import datetime
from threading import Event
from typing import Any, Dict, List, Optional

from .portfolio import Portfolio
from .rebalancer import (
    Rebalancer,
    RebalanceConfig,
    create_60_40_targets,
    create_three_fund_targets,
    create_equal_weight_targets,
)
from .models import TargetAllocation, AssetType, RebalanceStrategy, Bar, OrderAction
from .command_server import (
    CommandServer,
    CommandResult,
    CommandStatus,
    DEFAULT_SOCKET_PATH,
)
from .execution_db import (
    ExecutionDatabase,
    ExecutionRecord,
    CommissionRecord,
    get_execution_db,
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
        print(f"Shutdown requires {self.REQUIRED_SIGNALS}x Ctrl+C within {self.RESET_TIMEOUT:.0f} seconds")

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
            print(f"\nCtrl+C [{self._sigint_count}/{self.REQUIRED_SIGNALS}] "
                  f"Press {remaining_signals} more time(s) within {remaining_time:.1f}s to confirm shutdown")
        else:
            # Enough signals - initiate shutdown
            print(f"\nCtrl+C [{self._sigint_count}/{self.REQUIRED_SIGNALS}] "
                  f"Shutdown confirmed. Exiting gracefully...")
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

    async def wait_interruptible(self, duration: float = 0, poll_interval: float = 0.1):
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
                await asyncio.sleep(remaining)
        else:
            while not self.should_shutdown:
                await asyncio.sleep(poll_interval)


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

    def __init__(self, portfolio: Portfolio, shutdown_mgr: ShutdownManager, plugin_executive=None):
        self.portfolio = portfolio
        self.shutdown_mgr = shutdown_mgr
        self.plugin_executive = plugin_executive
        self._liquidation_in_progress = False

    def set_plugin_executive(self, executive):
        """Set the plugin executive for plugin commands"""
        self.plugin_executive = executive

    def register_commands(self, server: CommandServer):
        """Register all command handlers with the server"""
        server.register_handler("status", self.handle_status)
        server.register_handler("positions", self.handle_positions)
        server.register_handler("summary", self.handle_summary)
        server.register_handler("liquidate", self.handle_liquidate)
        server.register_handler("stop", self.handle_stop)
        server.register_handler("shutdown", self.handle_stop)
        server.register_handler("sell", self.handle_sell)
        server.register_handler("buy", self.handle_buy)
        server.register_handler("trade", self.handle_trade)
        server.register_handler("plugin", self.handle_plugin)
        server.register_handler("db", self.handle_db)

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

    # =========================================================================
    # Plugin Commands
    # =========================================================================

    def handle_plugin(self, args: List[str]) -> CommandResult:
        """
        Handle 'plugin' command - plugin lifecycle control.

        Usage:
            plugin list                               - List all plugins
            plugin load <path[=slot]>                 - Load plugin from file (optional slot name)
            plugin unload <name>                      - Unload plugin
            plugin status <name>                      - Get plugin status
            plugin start <name>                       - Start plugin
            plugin stop <name>                        - Stop plugin
            plugin freeze <name>                      - Freeze plugin
            plugin resume <name>                      - Resume plugin
            plugin enable <name>                      - Enable plugin for execution
            plugin disable <name>                     - Disable plugin
            plugin request <name> <type> <json>       - Send typed request to plugin
            plugin message <name> <json>              - Send arbitrary message to plugin
            plugin help <name>                        - Show plugin CLI help
            plugin param <name> <key> <value>         - Set plugin parameter
            plugin params <name>                      - Get plugin parameters
            plugin feeds                              - List MessageBus channels
            plugin history <channel> [count]          - Get channel message history
            plugin dump <name>                        - Dump positions and open orders
            plugin departures [--clear]               - Show departure status board
            plugin instruments list <name>            - List instruments for a plugin
            plugin instruments add <name> <symbol> [options]  - Add/update instrument
            plugin instruments remove <name> <symbol> - Remove instrument
            plugin instruments enable <name> <symbol> - Enable instrument
            plugin instruments disable <name> <symbol>- Disable instrument
            plugin instruments clear <name>           - Remove all instruments
            plugin instruments reload <name>          - Re-read instruments from SQLite
        """
        if self.plugin_executive is None:
            return CommandResult(
                status=CommandStatus.ERROR,
                message="Plugin executive not configured",
            )

        if not args:
            return CommandResult(
                status=CommandStatus.ERROR,
                message="Usage: plugin <command> [args]. Use 'plugin list' to see plugins.",
            )

        subcommand = args[0].lower()
        subargs = args[1:]

        try:
            if subcommand == "list":
                return self._plugin_list()
            elif subcommand == "load":
                if not subargs:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message="Usage: plugin load <path> [descriptor]",
                    )
                descriptor = subargs[1] if len(subargs) > 1 else None
                return self._plugin_load(subargs[0], descriptor=descriptor)
            elif subcommand == "unload":
                if not subargs:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message="Usage: plugin unload <name>",
                    )
                return self._plugin_unload(subargs[0])
            elif subcommand == "status":
                if not subargs:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message="Usage: plugin status <name>",
                    )
                return self._plugin_status(subargs[0])
            elif subcommand == "start":
                if not subargs:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message="Usage: plugin start <name>",
                    )
                return self._plugin_start(subargs[0])
            elif subcommand == "stop":
                if not subargs:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message="Usage: plugin stop <name>",
                    )
                return self._plugin_stop(subargs[0])
            elif subcommand == "freeze":
                if not subargs:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message="Usage: plugin freeze <name>",
                    )
                return self._plugin_freeze(subargs[0])
            elif subcommand == "resume":
                if not subargs:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message="Usage: plugin resume <name>",
                    )
                return self._plugin_resume(subargs[0])
            elif subcommand == "enable":
                if not subargs:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message="Usage: plugin enable <name>",
                    )
                return self._plugin_enable(subargs[0], True)
            elif subcommand == "disable":
                if not subargs:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message="Usage: plugin disable <name>",
                    )
                return self._plugin_enable(subargs[0], False)
            elif subcommand == "request":
                if len(subargs) < 2:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message="Usage: plugin request <name> <type> [json_payload]",
                    )
                payload = subargs[2] if len(subargs) > 2 else "{}"
                return self._plugin_request(subargs[0], subargs[1], payload)
            elif subcommand == "message":
                if not subargs:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message="Usage: plugin message <name> <json_payload>",
                    )
                payload = subargs[1] if len(subargs) > 1 else "{}"
                return self._plugin_message(subargs[0], payload)
            elif subcommand == "help":
                if not subargs:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message="Usage: plugin help <name>",
                    )
                return self._plugin_help(subargs[0])
            elif subcommand == "param":
                if len(subargs) < 3:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message="Usage: plugin param <name> <key> <value>",
                    )
                return self._plugin_set_param(subargs[0], subargs[1], subargs[2])
            elif subcommand == "params":
                if not subargs:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message="Usage: plugin params <name>",
                    )
                return self._plugin_get_params(subargs[0])
            elif subcommand == "feeds":
                return self._plugin_feeds()
            elif subcommand == "history":
                if not subargs:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message="Usage: plugin history <channel> [count]",
                    )
                count = int(subargs[1]) if len(subargs) > 1 else 10
                return self._plugin_history(subargs[0], count)
            elif subcommand == "reset-cb":
                if not subargs:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message="Usage: plugin reset-cb <name>",
                    )
                return self._plugin_reset_cb(subargs[0])
            elif subcommand == "trigger":
                if not subargs:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message="Usage: plugin trigger <name>",
                    )
                return self._plugin_trigger(subargs[0])
            elif subcommand == "dump":
                if not subargs:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message="Usage: plugin dump <name>",
                    )
                return self._plugin_dump(subargs[0])
            elif subcommand == "departures":
                clear = "--clear" in subargs
                return self._plugin_departures(clear)
            elif subcommand == "instruments":
                return self._plugin_instruments(subargs)
            elif subcommand == "export":
                if not subargs:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message="Usage: plugin export <slot> [file]",
                    )
                fp = subargs[1] if len(subargs) > 1 else None
                return self._plugin_export(subargs[0], fp)
            elif subcommand == "import":
                if not subargs:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message="Usage: plugin import <file>",
                    )
                return self._plugin_import(subargs[0])
            else:
                return CommandResult(
                    status=CommandStatus.ERROR,
                    message=f"Unknown plugin subcommand: {subcommand}",
                )
        except Exception as e:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Plugin command failed: {e}",
            )

    def _plugin_list(self) -> CommandResult:
        """List all registered plugins"""
        plugins = self.plugin_executive.plugins
        all_status = {}
        for name in plugins:
            status = self.plugin_executive.get_plugin_status(name)
            if status:
                all_status[name] = {
                    "state": status["state"],
                    "enabled": status["enabled"],
                    "circuit_breaker_state": status["circuit_breaker"]["state"],
                    "run_count": status["run_count"],
                }

        return CommandResult(
            status=CommandStatus.SUCCESS,
            message=f"{len(plugins)} plugins registered",
            data={"plugins": all_status},
        )

    def _plugin_load(self, path_spec: str, descriptor: Any = None) -> CommandResult:
        """Load a plugin from file, optionally passing a descriptor.

        path_spec may be ``/path/to/plugin.py=my_slot`` to assign a stable
        instance storage key independent of the plugin class name.
        """
        # Parse optional =slot suffix
        slot = None
        if "=" in path_spec:
            path, slot = path_spec.split("=", 1)
            slot = slot.strip() or None
        else:
            path = path_spec

        result = self.plugin_executive.load_plugin_from_file(
            path, descriptor=descriptor, slot=slot
        )
        if result:
            slot_display = result["slot"]
            name_display = result["plugin_name"]
            label = slot_display if slot_display != name_display else name_display
            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=f"Plugin '{label}' loaded "
                        f"(instance_id={result['instance_id'][:8]})",
                data={
                    "plugin_name": result["plugin_name"],
                    "slot": result["slot"],
                    "instance_id": result["instance_id"],
                    "descriptor": result["descriptor"],
                    "path": path,
                },
            )
        return CommandResult(
            status=CommandStatus.ERROR,
            message=f"Failed to load plugin from {path}",
        )

    def _plugin_unload(self, name: str) -> CommandResult:
        """Unload a plugin"""
        if self.plugin_executive.unload_plugin(name):
            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=f"Plugin '{name}' unloaded",
            )
        return CommandResult(
            status=CommandStatus.ERROR,
            message=f"Failed to unload plugin '{name}'",
        )

    def _plugin_status(self, name: str) -> CommandResult:
        """Get detailed plugin status"""
        status = self.plugin_executive.get_plugin_status(name)
        if status is None:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Plugin '{name}' not found",
            )

        return CommandResult(
            status=CommandStatus.SUCCESS,
            message=f"Plugin '{name}': {status['state']}",
            data=status,
        )

    def _plugin_start(self, name: str) -> CommandResult:
        """Start a plugin"""
        if self.plugin_executive.start_plugin(name):
            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=f"Plugin '{name}' started",
            )
        return CommandResult(
            status=CommandStatus.ERROR,
            message=f"Failed to start plugin '{name}'",
        )

    def _plugin_stop(self, name: str) -> CommandResult:
        """Stop a plugin"""
        if self.plugin_executive.stop_plugin(name):
            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=f"Plugin '{name}' stopped",
            )
        return CommandResult(
            status=CommandStatus.ERROR,
            message=f"Failed to stop plugin '{name}'",
        )

    def _plugin_freeze(self, name: str) -> CommandResult:
        """Freeze a plugin"""
        if self.plugin_executive.freeze_plugin(name):
            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=f"Plugin '{name}' frozen",
            )
        return CommandResult(
            status=CommandStatus.ERROR,
            message=f"Failed to freeze plugin '{name}'",
        )

    def _plugin_resume(self, name: str) -> CommandResult:
        """Resume a frozen plugin"""
        if self.plugin_executive.resume_plugin(name):
            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=f"Plugin '{name}' resumed",
            )
        return CommandResult(
            status=CommandStatus.ERROR,
            message=f"Failed to resume plugin '{name}'",
        )

    def _plugin_enable(self, name: str, enabled: bool) -> CommandResult:
        """Enable or disable a plugin for continuous execution"""
        if self.plugin_executive.enable_plugin(name, enabled):
            action = "enabled" if enabled else "disabled"
            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=f"Plugin '{name}' {action}",
            )
        return CommandResult(
            status=CommandStatus.ERROR,
            message=f"Plugin '{name}' not found",
        )

    def _plugin_request(self, name: str, request_type: str, payload_json: str) -> CommandResult:
        """Send a custom request to a plugin"""
        import json
        try:
            payload = json.loads(payload_json)
        except json.JSONDecodeError as e:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Invalid JSON payload: {e}",
            )

        response = self.plugin_executive.send_request(name, request_type, payload)

        if response.get("success"):
            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=response.get("message", "Request successful"),
                data=response.get("data", {}),
            )
        return CommandResult(
            status=CommandStatus.ERROR,
            message=response.get("message", "Request failed"),
        )

    def _plugin_instruments(self, args: List[str]) -> CommandResult:
        """Handle 'plugin instruments <sub> ...' commands."""
        import json as _json
        if not args:
            return CommandResult(
                status=CommandStatus.ERROR,
                message="Usage: plugin instruments list|add|remove|enable|disable|clear|reload <name> [symbol] [options]",
            )

        sub = args[0].lower()
        rest = args[1:]

        if sub == "list":
            if not rest:
                return CommandResult(status=CommandStatus.ERROR,
                                     message="Usage: plugin instruments list <name>")
            instruments = self.plugin_executive.get_plugin_instruments(rest[0])
            if instruments is None:
                return CommandResult(status=CommandStatus.ERROR,
                                     message=f"Plugin '{rest[0]}' not found")
            data = [i.to_dict() for i in instruments]
            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=f"{len(data)} instrument(s) for '{rest[0]}'",
                data={"instruments": data},
            )

        if sub == "add":
            # plugin instruments add <name> <symbol> [--name NAME] [--weight W]
            # [--exchange X] [--currency C] [--sec-type T] [--disabled]
            if len(rest) < 2:
                return CommandResult(status=CommandStatus.ERROR,
                                     message="Usage: plugin instruments add <name> <symbol> [options]")
            plugin_name, symbol = rest[0], rest[1].upper()
            flags = rest[2:]

            def _flag(key, default=None):
                for i, f in enumerate(flags):
                    if f == key and i + 1 < len(flags):
                        return flags[i + 1]
                return default

            from plugins.base import PluginInstrument
            inst = PluginInstrument(
                symbol=symbol,
                name=_flag("--name", symbol),
                weight=float(_flag("--weight", 0.0)),
                min_weight=float(_flag("--min-weight", 0.0)),
                max_weight=float(_flag("--max-weight", 100.0)),
                enabled="--disabled" not in flags,
                exchange=_flag("--exchange", "SMART"),
                currency=_flag("--currency", "USD"),
                sec_type=_flag("--sec-type", "STK"),
            )
            if self.plugin_executive.add_plugin_instrument(plugin_name, inst):
                return CommandResult(
                    status=CommandStatus.SUCCESS,
                    message=f"Instrument '{symbol}' added to '{plugin_name}'",
                    data=inst.to_dict(),
                )
            return CommandResult(status=CommandStatus.ERROR,
                                 message=f"Plugin '{plugin_name}' not found")

        if sub == "remove":
            if len(rest) < 2:
                return CommandResult(status=CommandStatus.ERROR,
                                     message="Usage: plugin instruments remove <name> <symbol>")
            plugin_name, symbol = rest[0], rest[1].upper()
            if self.plugin_executive.remove_plugin_instrument(plugin_name, symbol):
                return CommandResult(status=CommandStatus.SUCCESS,
                                     message=f"Instrument '{symbol}' removed from '{plugin_name}'")
            return CommandResult(status=CommandStatus.ERROR,
                                 message=f"Plugin '{plugin_name}' or symbol '{symbol}' not found")

        if sub in ("enable", "disable"):
            if len(rest) < 2:
                return CommandResult(status=CommandStatus.ERROR,
                                     message=f"Usage: plugin instruments {sub} <name> <symbol>")
            plugin_name, symbol = rest[0], rest[1].upper()
            enabled = sub == "enable"
            if self.plugin_executive.set_plugin_instrument_enabled(plugin_name, symbol, enabled):
                action = "enabled" if enabled else "disabled"
                return CommandResult(status=CommandStatus.SUCCESS,
                                     message=f"Instrument '{symbol}' {action} in '{plugin_name}'")
            return CommandResult(status=CommandStatus.ERROR,
                                 message=f"Plugin '{plugin_name}' or symbol '{symbol}' not found")

        if sub == "clear":
            if not rest:
                return CommandResult(status=CommandStatus.ERROR,
                                     message="Usage: plugin instruments clear <name>")
            if self.plugin_executive.clear_plugin_instruments(rest[0]):
                return CommandResult(status=CommandStatus.SUCCESS,
                                     message=f"All instruments cleared from '{rest[0]}'")
            return CommandResult(status=CommandStatus.ERROR,
                                 message=f"Plugin '{rest[0]}' not found")

        if sub == "reload":
            if not rest:
                return CommandResult(status=CommandStatus.ERROR,
                                     message="Usage: plugin instruments reload <name>")
            count = self.plugin_executive.reload_plugin_instruments(rest[0])
            if count is None:
                return CommandResult(status=CommandStatus.ERROR,
                                     message=f"Plugin '{rest[0]}' not found")
            return CommandResult(status=CommandStatus.SUCCESS,
                                 message=f"Reloaded {count} instrument(s) for '{rest[0]}'")

        return CommandResult(status=CommandStatus.ERROR,
                             message=f"Unknown instruments subcommand: {sub}")

    def _plugin_message(self, name: str, payload_json: str) -> CommandResult:
        """Send an arbitrary JSON message to a plugin (routes to handle_request type='message')."""
        import json
        try:
            payload = json.loads(payload_json)
        except json.JSONDecodeError as e:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Invalid JSON payload: {e}",
            )

        response = self.plugin_executive.send_request(name, "message", payload)

        if response.get("success"):
            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=response.get("message", "Message delivered"),
                data=response.get("data", {}),
            )
        return CommandResult(
            status=CommandStatus.ERROR,
            message=response.get("message", "Plugin did not handle message"),
            data=response.get("data", {}),
        )

    def _plugin_help(self, name: str) -> CommandResult:
        """Return CLI help text from a plugin's cli_help() method."""
        help_text = self.plugin_executive.send_help(name)
        if help_text is None:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Plugin '{name}' not found",
            )
        return CommandResult(
            status=CommandStatus.SUCCESS,
            message=help_text,
        )

    def _plugin_set_param(self, name: str, key: str, value: str) -> CommandResult:
        """Set a plugin parameter"""
        # Try to parse value as number or boolean
        parsed_value: any = value
        if value.lower() in ("true", "false"):
            parsed_value = value.lower() == "true"
        else:
            try:
                parsed_value = float(value)
                if parsed_value.is_integer():
                    parsed_value = int(parsed_value)
            except ValueError:
                pass

        if self.plugin_executive.set_plugin_parameter(name, key, parsed_value):
            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=f"Parameter '{key}' set to '{parsed_value}' for '{name}'",
            )
        return CommandResult(
            status=CommandStatus.ERROR,
            message=f"Failed to set parameter for '{name}'",
        )

    def _plugin_get_params(self, name: str) -> CommandResult:
        """Get plugin parameters"""
        params = self.plugin_executive.get_plugin_parameters(name)
        if params is None:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Plugin '{name}' not found",
            )

        return CommandResult(
            status=CommandStatus.SUCCESS,
            message=f"{len(params)} parameters",
            data={"parameters": params},
        )

    def _plugin_feeds(self) -> CommandResult:
        """List all MessageBus channels"""
        feeds = self.plugin_executive.list_feeds()
        return CommandResult(
            status=CommandStatus.SUCCESS,
            message=f"{len(feeds)} channels",
            data={"channels": feeds},
        )

    def _plugin_history(self, channel: str, count: int) -> CommandResult:
        """Get message history for a channel"""
        history = self.plugin_executive.get_feed_history(channel, count=count)
        return CommandResult(
            status=CommandStatus.SUCCESS,
            message=f"{len(history)} messages from '{channel}'",
            data={"messages": history},
        )

    def _plugin_reset_cb(self, name: str) -> CommandResult:
        """Reset plugin circuit breaker"""
        if self.plugin_executive.reset_circuit_breaker(name):
            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=f"Circuit breaker reset for '{name}'",
            )
        return CommandResult(
            status=CommandStatus.ERROR,
            message=f"Failed to reset circuit breaker for '{name}'",
        )

    def _plugin_trigger(self, name: str) -> CommandResult:
        """Manually trigger a plugin"""
        result = self.plugin_executive.trigger_plugin(name)
        if result is None:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Plugin '{name}' not found",
            )

        signals_count = len(result.actionable_signals) if result.signals else 0
        return CommandResult(
            status=CommandStatus.SUCCESS if result.success else CommandStatus.ERROR,
            message=f"Plugin '{name}' triggered: {signals_count} actionable signals",
            data={
                "success": result.success,
                "signals_count": signals_count,
                "error": result.error,
            },
        )

    def _plugin_dump(self, name: str) -> CommandResult:
        """Dump plugin positions and open orders"""
        pe = self.plugin_executive
        if name not in pe._plugins:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Plugin '{name}' not found",
            )

        plugin = pe._plugins[name].plugin
        holdings = plugin.get_effective_holdings()

        open_orders = []
        for oid, po in pe._pending_orders.items():
            if po.plugin_name == name:
                open_orders.append({
                    "order_id": oid,
                    "symbol": po.signal.symbol,
                    "action": po.signal.action,
                    "quantity": po.signal.quantity,
                    "status": po.status,
                    "created_at": po.created_at.isoformat(),
                })

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

    def _plugin_departures(self, clear: bool = False) -> CommandResult:
        """Get departure status messages from unloaded plugins"""
        departures = self.plugin_executive.get_departures(clear=clear)
        count = len(departures)
        msg = f"{count} departure(s)"
        if clear and count > 0:
            msg += " (cleared)"
        return CommandResult(
            status=CommandStatus.SUCCESS,
            message=msg,
            data={"departures": departures},
        )

    def _plugin_export(self, slot: str, filepath: Optional[str] = None) -> CommandResult:
        """Export a plugin instance to a portable JSON document."""
        import json as _json
        data = self.plugin_executive.export_plugin(slot)
        if data is None:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"No registry entry found for slot '{slot}'",
            )
        if filepath:
            try:
                with open(filepath, "w") as f:
                    _json.dump(data, f, indent=2, default=str)
                return CommandResult(
                    status=CommandStatus.SUCCESS,
                    message=f"Exported slot '{slot}' to {filepath}",
                    data=data,
                )
            except Exception as e:
                return CommandResult(
                    status=CommandStatus.ERROR,
                    message=f"Failed to write export file: {e}",
                )
        return CommandResult(
            status=CommandStatus.SUCCESS,
            message=f"Exported slot '{slot}'",
            data=data,
        )

    def _plugin_import(self, filepath: str) -> CommandResult:
        """Import a plugin instance from a portable JSON document."""
        import json as _json
        try:
            with open(filepath) as f:
                data = _json.load(f)
        except Exception as e:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Failed to read import file: {e}",
            )
        if self.plugin_executive.import_plugin(data):
            slot = data.get("slot", "?")
            return CommandResult(
                status=CommandStatus.SUCCESS,
                message=(
                    f"Imported slot '{slot}' — use 'plugin load' to activate"
                ),
                data={"slot": slot},
            )
        return CommandResult(
            status=CommandStatus.ERROR,
            message="Import failed — missing 'slot' or 'class_path' in document",
        )

    def handle_db(self, args: List[str]) -> CommandResult:
        """
        Handle 'db' command - execution database operations.

        Usage:
            db status                           - Show database stats
            db executions [SYMBOL] [--limit N]  - List executions
            db commissions [--limit N]          - List commissions
            db summary SYMBOL                   - Position summary from executions
            db insert exec <json>               - Insert execution record
            db insert comm <json>               - Insert commission record
            db sql <query>                      - Execute raw SQL (read-only)
        """
        if not args:
            return CommandResult(
                status=CommandStatus.ERROR,
                message="Usage: db <command> [args]. Commands: status, executions, commissions, summary, insert, sql",
            )

        subcommand = args[0].lower()
        subargs = args[1:]

        try:
            db = get_execution_db()

            if subcommand == "status":
                return self._db_status(db)

            elif subcommand == "executions":
                symbol = None
                limit = 50
                skip_next = False
                for i, arg in enumerate(subargs):
                    if skip_next:
                        skip_next = False
                        continue
                    if arg == "--limit" and i + 1 < len(subargs):
                        limit = int(subargs[i + 1])
                        skip_next = True
                    elif not arg.startswith("--"):
                        symbol = arg
                return self._db_executions(db, symbol, limit)

            elif subcommand == "commissions":
                limit = 50
                for i, arg in enumerate(subargs):
                    if arg == "--limit" and i + 1 < len(subargs):
                        limit = int(subargs[i + 1])
                return self._db_commissions(db, limit)

            elif subcommand == "summary":
                if not subargs:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message="Usage: db summary <symbol>",
                    )
                return self._db_summary(db, subargs[0])

            elif subcommand == "insert":
                if len(subargs) < 2:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message="Usage: db insert <exec|comm> <json>",
                    )
                record_type = subargs[0].lower()
                json_data = " ".join(subargs[1:])
                return self._db_insert(db, record_type, json_data)

            elif subcommand == "sql":
                if not subargs:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message="Usage: db sql <query>",
                    )
                query = " ".join(subargs)
                return self._db_sql(db, query)

            else:
                return CommandResult(
                    status=CommandStatus.ERROR,
                    message=f"Unknown db command: {subcommand}",
                )

        except Exception as e:
            logger.error(f"Database command failed: {e}")
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Database error: {e}",
            )

    def _db_status(self, db: ExecutionDatabase) -> CommandResult:
        """Get database status and statistics"""
        exec_count = db.get_execution_count()
        comm_count = db.get_commission_count()
        total_comm = db.get_total_commission()

        return CommandResult(
            status=CommandStatus.SUCCESS,
            message=f"Executions: {exec_count}, Commissions: {comm_count}, Total fees: ${total_comm:.2f}",
            data={
                "execution_count": exec_count,
                "commission_count": comm_count,
                "total_commission": total_comm,
                "db_path": str(db.db_path),
            },
        )

    def _db_executions(self, db: ExecutionDatabase, symbol: Optional[str], limit: int) -> CommandResult:
        """List executions from database"""
        if symbol:
            executions = db.get_executions_by_symbol(symbol)[:limit]
        else:
            executions = db.get_all_executions(limit=limit)

        exec_list = [e.to_dict() for e in executions]

        return CommandResult(
            status=CommandStatus.SUCCESS,
            message=f"{len(executions)} executions" + (f" for {symbol}" if symbol else ""),
            data={"executions": exec_list},
        )

    def _db_commissions(self, db: ExecutionDatabase, limit: int) -> CommandResult:
        """List commissions from database"""
        import sqlite3

        try:
            with sqlite3.connect(db.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT c.*, e.symbol, e.side, e.shares
                    FROM commissions c
                    LEFT JOIN executions e ON c.exec_id = e.exec_id
                    ORDER BY c.timestamp DESC
                    LIMIT ?
                """, (limit,))
                rows = cursor.fetchall()

                comm_list = []
                for row in rows:
                    comm_list.append({
                        "exec_id": row["exec_id"],
                        "symbol": row["symbol"],
                        "side": row["side"],
                        "shares": row["shares"],
                        "commission": row["commission"],
                        "currency": row["currency"],
                        "realized_pnl": row["realized_pnl"],
                        "timestamp": row["timestamp"],
                    })

                return CommandResult(
                    status=CommandStatus.SUCCESS,
                    message=f"{len(comm_list)} commission records",
                    data={"commissions": comm_list},
                )
        except Exception as e:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Failed to get commissions: {e}",
            )

    def _db_summary(self, db: ExecutionDatabase, symbol: str) -> CommandResult:
        """Get position summary from executions"""
        summary = db.get_position_summary(symbol)
        if not summary:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"No executions found for {symbol}",
            )

        cost_basis = db.get_cost_basis(symbol)

        return CommandResult(
            status=CommandStatus.SUCCESS,
            message=f"{symbol}: {summary['net_position']:.0f} shares, avg cost ${summary['avg_buy_price']:.2f}",
            data={
                "summary": summary,
                "cost_basis": cost_basis,
            },
        )

    def _db_insert(self, db: ExecutionDatabase, record_type: str, json_data: str) -> CommandResult:
        """Insert a record into the database"""
        import json

        try:
            data = json.loads(json_data)
        except json.JSONDecodeError as e:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Invalid JSON: {e}",
            )

        if record_type == "exec":
            # Parse and insert execution record
            required = ["exec_id", "order_id", "symbol", "sec_type", "shares", "avg_price", "side"]
            for field in required:
                if field not in data:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message=f"Missing required field: {field}",
                    )

            record = ExecutionRecord(
                exec_id=data["exec_id"],
                order_id=int(data["order_id"]),
                symbol=data["symbol"],
                sec_type=data["sec_type"],
                exchange=data.get("exchange", ""),
                currency=data.get("currency", "USD"),
                local_symbol=data.get("local_symbol", ""),
                shares=float(data["shares"]),
                cum_qty=float(data.get("cum_qty", data["shares"])),
                avg_price=float(data["avg_price"]),
                side=data["side"],
                account=data.get("account", ""),
                timestamp=datetime.fromisoformat(data["timestamp"]) if "timestamp" in data else datetime.now(),
            )

            success = db.insert_execution(record)
            return CommandResult(
                status=CommandStatus.SUCCESS if success else CommandStatus.ERROR,
                message=f"Execution {'inserted' if success else 'already exists'}: {record.exec_id}",
            )

        elif record_type == "comm":
            # Parse and insert commission record
            required = ["exec_id", "commission"]
            for field in required:
                if field not in data:
                    return CommandResult(
                        status=CommandStatus.ERROR,
                        message=f"Missing required field: {field}",
                    )

            record = CommissionRecord(
                exec_id=data["exec_id"],
                commission=float(data["commission"]),
                currency=data.get("currency", "USD"),
                realized_pnl=float(data["realized_pnl"]) if "realized_pnl" in data else None,
                timestamp=datetime.fromisoformat(data["timestamp"]) if "timestamp" in data else datetime.now(),
            )

            success = db.insert_commission(record)
            return CommandResult(
                status=CommandStatus.SUCCESS if success else CommandStatus.ERROR,
                message=f"Commission {'inserted' if success else 'already exists'}: {record.exec_id}",
            )

        else:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Unknown record type: {record_type}. Use 'exec' or 'comm'.",
            )

    def _db_sql(self, db: ExecutionDatabase, query: str) -> CommandResult:
        """Execute raw SQL query (read-only)"""
        import sqlite3

        # Security: only allow SELECT queries
        query_lower = query.strip().lower()
        if not query_lower.startswith("select"):
            return CommandResult(
                status=CommandStatus.ERROR,
                message="Only SELECT queries are allowed for safety",
            )

        try:
            with sqlite3.connect(db.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(query)
                rows = cursor.fetchall()

                # Convert rows to list of dicts
                results = []
                for row in rows:
                    results.append(dict(row))

                return CommandResult(
                    status=CommandStatus.SUCCESS,
                    message=f"{len(results)} rows returned",
                    data={"rows": results, "query": query},
                )
        except Exception as e:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"SQL error: {e}",
            )

    def handle_summary(self, args: List[str]) -> CommandResult:
        """
        Handle 'summary' command - executive account summary with holdings by plugin.

        Usage:
            summary              - Full account summary with plugin breakdown
            summary --json       - Output as formatted JSON
            summary plugins      - Show only plugin holdings
            summary unassigned   - Show only unassigned holdings
        """
        try:
            output_json = "--json" in args
            args = [a for a in args if a != "--json"]

            subcommand = args[0].lower() if args else "full"

            if subcommand == "full":
                return self._summary_full(output_json)
            elif subcommand == "plugins":
                return self._summary_plugins_only(output_json)
            elif subcommand == "unassigned":
                return self._summary_unassigned_only(output_json)
            else:
                return CommandResult(
                    status=CommandStatus.ERROR,
                    message="Usage: summary [full|plugins|unassigned] [--json]",
                )
        except Exception as e:
            logger.error(f"Summary command failed: {e}")
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Summary failed: {e}",
            )

    def _summary_full(self, output_json: bool = False) -> CommandResult:
        """Generate full executive account summary"""
        # Get account info
        account = self.portfolio.get_account_summary()
        positions = self.portfolio.positions
        total_portfolio_value = self.portfolio.total_value
        total_pnl = self.portfolio.total_pnl

        # Build account summary
        account_data = {}
        if account and account.is_valid:
            account_data = {
                "account_id": account.account_id,
                "net_liquidation": account.net_liquidation,
                "available_funds": account.available_funds,
                "buying_power": account.buying_power,
                "cash": getattr(account, "total_cash", 0.0),
            }

        # Get plugin holdings
        plugin_holdings = self._get_plugin_holdings()

        # Calculate unassigned value
        total_plugin_value = sum(p["total_value"] for p in plugin_holdings.values())
        unassigned_value = total_portfolio_value - total_plugin_value

        # Build position map for unassigned calculation
        position_map = {p.symbol: p for p in positions}
        plugin_positions = set()
        for plugin_data in plugin_holdings.values():
            for pos in plugin_data.get("positions", []):
                plugin_positions.add(pos["symbol"])

        # Find unassigned positions
        unassigned_positions = []
        for symbol, pos in position_map.items():
            if symbol not in plugin_positions:
                unassigned_positions.append({
                    "symbol": symbol,
                    "quantity": pos.quantity,
                    "market_value": pos.market_value,
                    "unrealized_pnl": pos.unrealized_pnl,
                })

        # Build response data
        data = {
            "account": account_data,
            "portfolio": {
                "total_value": total_portfolio_value,
                "total_pnl": total_pnl,
                "position_count": len(positions),
            },
            "plugins": plugin_holdings,
            "plugin_total_value": total_plugin_value,
            "unassigned": {
                "value": unassigned_value,
                "positions": unassigned_positions,
            },
        }

        if output_json:
            import json
            message = json.dumps(data, indent=2)
        else:
            # Build human-readable summary
            lines = []
            lines.append("=" * 70)
            lines.append("EXECUTIVE ACCOUNT SUMMARY")
            lines.append("=" * 70)

            if account_data:
                lines.append(f"\nAccount: {account_data.get('account_id', 'N/A')}")
                lines.append(f"  Net Liquidation:  ${account_data.get('net_liquidation', 0):>15,.2f}")
                lines.append(f"  Available Funds:  ${account_data.get('available_funds', 0):>15,.2f}")
                lines.append(f"  Buying Power:     ${account_data.get('buying_power', 0):>15,.2f}")

            lines.append(f"\nPortfolio Summary:")
            lines.append(f"  Total Value:      ${total_portfolio_value:>15,.2f}")
            lines.append(f"  Total P&L:        ${total_pnl:>15,.2f}")
            lines.append(f"  Positions:        {len(positions):>15}")

            lines.append("\n" + "-" * 70)
            lines.append("HOLDINGS BY PLUGIN")
            lines.append("-" * 70)

            if plugin_holdings:
                for name, holdings in sorted(plugin_holdings.items()):
                    pct = (holdings["total_value"] / total_portfolio_value * 100) if total_portfolio_value else 0
                    lines.append(f"\n  {name}:")
                    lines.append(f"    Total Value:    ${holdings['total_value']:>15,.2f}  ({pct:>5.1f}%)")
                    lines.append(f"    Cash:           ${holdings['cash']:>15,.2f}")
                    if holdings.get("positions"):
                        lines.append(f"    Positions:")
                        for pos in holdings["positions"]:
                            lines.append(f"      {pos['symbol']:>6}: {pos['quantity']:>8} shares  ${pos['market_value']:>12,.2f}")
            else:
                lines.append("\n  No plugins with holdings registered")

            lines.append("\n" + "-" * 70)
            lines.append("UNASSIGNED HOLDINGS")
            lines.append("-" * 70)

            unassigned_pct = (unassigned_value / total_portfolio_value * 100) if total_portfolio_value else 0
            lines.append(f"\n  Unassigned Value: ${unassigned_value:>15,.2f}  ({unassigned_pct:>5.1f}%)")

            if unassigned_positions:
                lines.append(f"  Positions not claimed by any plugin:")
                for pos in unassigned_positions:
                    lines.append(f"    {pos['symbol']:>6}: {pos['quantity']:>8} shares  ${pos['market_value']:>12,.2f}")
            else:
                lines.append("  All positions are assigned to plugins")

            lines.append("\n" + "=" * 70)
            message = "\n".join(lines)

        return CommandResult(
            status=CommandStatus.SUCCESS,
            message=message,
            data=data,
        )

    def _summary_plugins_only(self, output_json: bool = False) -> CommandResult:
        """Show only plugin holdings"""
        plugin_holdings = self._get_plugin_holdings()
        total_value = sum(p["total_value"] for p in plugin_holdings.values())

        data = {
            "plugins": plugin_holdings,
            "total_value": total_value,
        }

        if output_json:
            import json
            message = json.dumps(data, indent=2)
        else:
            lines = ["PLUGIN HOLDINGS", "-" * 50]
            for name, holdings in sorted(plugin_holdings.items()):
                lines.append(f"\n{name}:")
                lines.append(f"  Value: ${holdings['total_value']:,.2f}")
                lines.append(f"  Cash:  ${holdings['cash']:,.2f}")
                for pos in holdings.get("positions", []):
                    lines.append(f"  {pos['symbol']}: {pos['quantity']} @ ${pos['market_value']:,.2f}")
            lines.append(f"\nTotal Plugin Value: ${total_value:,.2f}")
            message = "\n".join(lines)

        return CommandResult(
            status=CommandStatus.SUCCESS,
            message=message,
            data=data,
        )

    def _summary_unassigned_only(self, output_json: bool = False) -> CommandResult:
        """Show only unassigned holdings"""
        positions = self.portfolio.positions
        total_value = self.portfolio.total_value
        plugin_holdings = self._get_plugin_holdings()

        # Calculate assigned positions
        plugin_positions = set()
        plugin_value = sum(p["total_value"] for p in plugin_holdings.values())
        for plugin_data in plugin_holdings.values():
            for pos in plugin_data.get("positions", []):
                plugin_positions.add(pos["symbol"])

        # Find unassigned
        unassigned_positions = []
        for pos in positions:
            if pos.symbol not in plugin_positions:
                unassigned_positions.append({
                    "symbol": pos.symbol,
                    "quantity": pos.quantity,
                    "market_value": pos.market_value,
                    "unrealized_pnl": pos.unrealized_pnl,
                })

        unassigned_value = total_value - plugin_value

        data = {
            "unassigned_value": unassigned_value,
            "positions": unassigned_positions,
        }

        if output_json:
            import json
            message = json.dumps(data, indent=2)
        else:
            lines = ["UNASSIGNED HOLDINGS", "-" * 50]
            lines.append(f"Unassigned Value: ${unassigned_value:,.2f}")
            if unassigned_positions:
                lines.append("\nPositions:")
                for pos in unassigned_positions:
                    lines.append(f"  {pos['symbol']}: {pos['quantity']} shares @ ${pos['market_value']:,.2f}")
            else:
                lines.append("\nNo unassigned positions")
            message = "\n".join(lines)

        return CommandResult(
            status=CommandStatus.SUCCESS,
            message=message,
            data=data,
        )

    def handle_trade(self, args: List[str]) -> CommandResult:
        """
        Handle 'trade' command - execute trade with plugin attribution.

        Usage:
            trade PLUGIN ACTION SYMBOL QTY [--confirm] [--reason "text"]

        Examples:
            trade momentum_5day BUY SPY 100              # Dry run
            trade momentum_5day BUY SPY 100 --confirm    # Execute
            trade manual SELL QQQ 50 --confirm --reason "Taking profits"
        """
        if self.plugin_executive is None:
            return CommandResult(
                status=CommandStatus.ERROR,
                message="Plugin executive not configured",
            )

        if len(args) < 4:
            return CommandResult(
                status=CommandStatus.ERROR,
                message="Usage: trade PLUGIN ACTION SYMBOL QTY [--confirm] [--reason \"text\"]",
            )

        # Parse arguments
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

        # Parse flags
        confirm = "--confirm" in args
        reason = "manual_trade"

        # Parse --reason flag
        for i, arg in enumerate(args):
            if arg == "--reason" and i + 1 < len(args):
                reason = args[i + 1]
                break

        # Validate action
        if action not in ("BUY", "SELL"):
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Invalid action: {action}. Must be BUY or SELL.",
            )

        # Validate quantity
        if quantity <= 0:
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Invalid quantity: {quantity}. Must be positive.",
            )

        try:
            # Execute the trade through plugin executive
            dry_run = not confirm
            success, order_id, message = self.plugin_executive.execute_manual_trade(
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
            logger.error(f"Trade command failed: {e}")
            return CommandResult(
                status=CommandStatus.ERROR,
                message=f"Trade failed: {e}",
            )

    def _get_plugin_holdings(self) -> Dict[str, Dict]:
        """Get holdings for all registered plugins"""
        holdings = {}

        if not self.plugin_executive:
            return holdings

        # Get all registered plugins
        try:
            all_status = self.plugin_executive.get_all_plugin_status()
            for name, status in all_status.items():
                plugin_config = self.plugin_executive._plugins.get(name)
                if plugin_config and plugin_config.plugin:
                    plugin = plugin_config.plugin
                    effective_holdings = plugin.get_effective_holdings()

                    holdings[name] = {
                        "state": status.get("state", "unknown"),
                        "total_value": effective_holdings.get("total_value", 0.0),
                        "cash": effective_holdings.get("cash", 0.0),
                        "positions": effective_holdings.get("positions", []),
                    }
        except Exception as e:
            logger.warning(f"Failed to get plugin holdings: {e}")

        return holdings

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
DEFAULT_PORT_PAPER = 7497  # TWS paper trading
DEFAULT_PORT_LIVE = 7496   # TWS live trading
DEFAULT_PORT = DEFAULT_PORT_PAPER
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


async def stream_prices(portfolio: Portfolio, duration: int = 0) -> None:
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
        await shutdown_manager.wait_interruptible(duration=duration)
    finally:
        portfolio.stop_streaming()

    # Stats
    elapsed = time.time() - start_time
    if elapsed > 0:
        print("-" * 70)
        print(f"Streamed {tick_count} ticks in {elapsed:.1f}s "
              f"({tick_count/elapsed:.1f} ticks/sec)")
        print("=" * 70)


async def stream_bars(portfolio: Portfolio, duration: int = 0) -> None:
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
        await shutdown_manager.wait_interruptible(duration=duration)
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
        description="TWS Headless",
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
        "--port", type=int, default=None,
        help=f"IB port (default: {DEFAULT_PORT_PAPER} paper, {DEFAULT_PORT_LIVE} live)"
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
        help=f"Connect to live trading (port {DEFAULT_PORT_LIVE}) and execute real trades"
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

    # Determine port: explicit --port overrides, otherwise use --live flag
    if args.port is not None:
        port = args.port
    elif args.live:
        port = DEFAULT_PORT_LIVE
    else:
        port = DEFAULT_PORT_PAPER

    # Connect to IB
    logger.info(f"Connecting to IB at {args.host}:{port}...")
    portfolio = Portfolio(
        host=args.host,
        port=port,
        client_id=args.client_id,
    )

    # Register portfolio with shutdown manager for cleanup
    shutdown_manager.register_portfolio(portfolio)

    async def _run():
        command_server = None
        if not await portfolio.connect():
            logger.error("Failed to connect to IB")
            sys.exit(1)

        try:
            # Check for early shutdown
            if shutdown_manager.should_shutdown:
                return

            # Load portfolio data
            logger.info("Loading portfolio data...")
            await portfolio.load(fetch_prices=True, fetch_account=True)

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
                await stream_prices(portfolio, duration=args.duration)

            elif args.bars:
                await stream_bars(portfolio, duration=args.duration)

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
            await portfolio.disconnect()
            shutdown_manager.restore_handlers()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
