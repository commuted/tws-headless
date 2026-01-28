"""
plugins - Plugin system for trading algorithms

This module provides the plugin infrastructure including:
- PluginBase: Abstract base class for all plugins
- PluginState: Plugin lifecycle states
- UnassignedPlugin: System plugin for unattributed positions and cash
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

from .unassigned import UnassignedPlugin
from .unassigned.plugin import UNASSIGNED_PLUGIN_NAME

__all__ = [
    # Main classes
    "PluginBase",
    "PluginState",
    # System plugins
    "UnassignedPlugin",
    "UNASSIGNED_PLUGIN_NAME",
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
