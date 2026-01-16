#!/usr/bin/env python3
"""
run_engine.py - Start the IB Trading Engine

Usage:
    python3 -m ib.run_engine              # From parent directory
    ./start_trading.sh                     # Via shell script

Options via environment variables:
    PORT=7497 MODE=dry_run python3 -m ib.run_engine
"""

import logging
import sys
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("trading")


def main():
    # Get config from environment
    port = int(os.environ.get("PORT", "7497"))
    mode = os.environ.get("MODE", "dry_run")

    logger.info("=" * 50)
    logger.info("IB Trading Engine")
    logger.info("=" * 50)
    logger.info(f"Port: {port}")
    logger.info(f"Order Mode: {mode}")
    logger.info("=" * 50)

    from .trading_engine import create_engine
    from .data_feed import DataType
    from .algorithms.base import AlgorithmInstrument
    from .algorithms import DummyAlgorithm

    # Create engine
    logger.info(f"Creating engine...")
    engine = create_engine(port=port, order_mode=mode)

    # Setup algorithm
    algo = DummyAlgorithm()
    algo.add_instrument(AlgorithmInstrument(symbol="SPY", name="SPDR S&P 500 ETF"))
    algo.add_instrument(AlgorithmInstrument(symbol="QQQ", name="Invesco QQQ Trust"))
    algo._loaded = True

    engine.add_algorithm(algo)

    # Callbacks
    def on_started():
        logger.info("Engine started successfully")
        logger.info("Streaming data - press Ctrl+C to stop")

    def on_signal(name, signal):
        logger.info(f"SIGNAL [{name}]: {signal.action} {signal.quantity} {signal.symbol} - {signal.reason}")

    def on_bar(symbol, bar, data_type):
        if data_type == DataType.BAR_1MIN:
            logger.info(f"[{symbol}] O={bar.open:.2f} H={bar.high:.2f} L={bar.low:.2f} C={bar.close:.2f} V={bar.volume}")

    def on_error(err):
        logger.error(f"Error: {err}")

    engine.on_started = on_started
    engine.on_signal = on_signal
    engine.on_bar = on_bar
    engine.on_error = on_error

    # Start
    logger.info("Connecting to IB...")
    if engine.start():
        engine.run_forever()
    else:
        logger.error("Failed to start engine")
        return 1

    logger.info("Engine stopped.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
