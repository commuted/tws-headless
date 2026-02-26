"""
feed_test_specs.py - Extensible feed type definitions and test pair configurations

Defines what feed types exist, how to test them, and which pairs to test together.
Adding a new feed type = add enum value + factory function + append to DEFAULT_TEST_PAIRS.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import List

from ibapi.contract import Contract

from ib.contract_builder import ContractBuilder


class FeedType(Enum):
    """All supported feed types for testing"""
    FOREX = "forex"
    STOCK = "stock"
    FUTURE = "future"
    OPTION = "option"
    INDEX = "index"
    BOND = "bond"
    CRYPTO = "crypto"


@dataclass
class FeedTestSpec:
    """Everything needed to test one feed subscription"""
    feed_type: FeedType
    symbol: str
    contract: Contract
    what_to_show: str = "TRADES"
    # reqRealTimeBars ignores reqMarketDataType(3); TRADES bars need a live
    # equity subscription even when delayed ticks work.  Set bar_what_to_show
    # to override independently (e.g. MIDPOINT works on paper accounts).
    bar_what_to_show: str = ""  # empty → falls back to what_to_show
    use_rth: bool = True
    tick_timeout: float = 15.0
    bar_timeout: float = 12.0
    min_tick_count: int = 1
    description: str = ""

    def __post_init__(self):
        if not self.description:
            self.description = f"{self.feed_type.value} {self.symbol}"
        if not self.bar_what_to_show:
            self.bar_what_to_show = self.what_to_show


@dataclass
class FeedTestPair:
    """Two FeedTestSpecs to test simultaneously"""
    name: str
    spec_a: FeedTestSpec
    spec_b: FeedTestSpec


def forex_spec() -> FeedTestSpec:
    """Create a test spec for EUR.USD forex pair"""
    return FeedTestSpec(
        feed_type=FeedType.FOREX,
        symbol="EUR.USD",
        contract=ContractBuilder.forex("EUR", "USD"),
        what_to_show="MIDPOINT",
        use_rth=False,
        description="Forex EUR.USD",
    )


def stock_spec() -> FeedTestSpec:
    """Create a test spec for SPY stock."""
    return FeedTestSpec(
        feed_type=FeedType.STOCK,
        symbol="SPY",
        contract=ContractBuilder.us_stock("SPY"),
        what_to_show="TRADES",
        use_rth=True,
        description="Stock SPY",
    )


DEFAULT_TEST_PAIRS: List[FeedTestPair] = [
    FeedTestPair("Forex + Stock", forex_spec(), stock_spec()),
]
