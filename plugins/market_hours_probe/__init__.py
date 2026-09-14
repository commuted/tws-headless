"""Read-only probe plugin for exercising ib/market_calendar.py against live IB."""

from .plugin import MarketHoursProbe

__all__ = ["MarketHoursProbe"]
