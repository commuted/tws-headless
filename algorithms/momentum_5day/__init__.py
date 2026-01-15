"""
Momentum 5-Day Reallocation Algorithm

A basic reallocation algorithm that uses 5 days of daily bars
to make momentum-based allocation decisions.
"""

from .algorithm import Momentum5DayAlgorithm

__all__ = ["Momentum5DayAlgorithm"]
