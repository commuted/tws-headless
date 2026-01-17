"""
plugins - Plugin system for trading algorithms

This module provides the plugin infrastructure including:
- PluginBase: Abstract base class for all plugins
- PluginState: Plugin lifecycle states
- Data classes for holdings, instruments, signals, and results
"""

from .base import (
    # Main classes
    PluginBase,
    PluginState,
    # Data classes
    HoldingPosition,
    Holdings,
    PluginInstrument,
    TradeSignal,
    PluginResult,
    # Backward compatibility aliases
    AlgorithmInstrument,
    AlgorithmResult,
)

__all__ = [
    # Main classes
    "PluginBase",
    "PluginState",
    # Data classes
    "HoldingPosition",
    "Holdings",
    "PluginInstrument",
    "TradeSignal",
    "PluginResult",
    # Backward compatibility
    "AlgorithmInstrument",
    "AlgorithmResult",
]
