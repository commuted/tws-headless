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
    tick_timeout: float = 30.0
    bar_timeout: float = 12.0
    min_tick_count: int = 1
    description: str = ""
    # If True, a bar-test timeout is treated as PASS (skipped) because
    # reqRealTimeBars only delivers data during market hours.
    bar_market_hours_only: bool = False

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
    """Create a test spec for SPY stock.

    primaryExch="ARCA" is required for unambiguous live TRADES data.
    Without it, IB may return a contract-ambiguous warning and no data.

    Tick test uses TRADES (delayed mode works for ticks).
    Bar test uses MIDPOINT: reqRealTimeBars with TRADES requires a live
    equity data subscription even in live mode, while MIDPOINT works on
    paper accounts with no additional subscription.

    bar_market_hours_only=True: reqRealTimeBars delivers nothing outside
    market hours regardless of what_to_show; treat timeout as skipped.
    """
    return FeedTestSpec(
        feed_type=FeedType.STOCK,
        symbol="SPY",
        contract=ContractBuilder.us_stock("SPY", primary_exchange="ARCA"),
        what_to_show="TRADES",
        bar_what_to_show="MIDPOINT",
        use_rth=True,
        bar_market_hours_only=True,
        description="Stock SPY",
    )


def forex_spec_2() -> FeedTestSpec:
    """GBP.USD forex pair — used by the second concurrent feed plugin.

    Different from forex_spec() (EUR.USD) so that both concurrent plugins
    subscribe to distinct IB request IDs and avoid IB repricing the same
    contract from two independent subscriptions.
    """
    return FeedTestSpec(
        feed_type=FeedType.FOREX,
        symbol="GBP.USD",
        contract=ContractBuilder.forex("GBP", "USD"),
        what_to_show="MIDPOINT",
        use_rth=False,
        description="Forex GBP.USD",
    )


def stock_spec_2() -> FeedTestSpec:
    """QQQ stock spec — used by the second concurrent feed plugin.

    Different from stock_spec() (SPY) so both concurrent plugins subscribe
    to distinct symbols and IB does not reprice the same contract from two
    independent reqMarketData calls.  QQQ has a typical 5-min range of
    0.15–0.25%, similar to SPY, so the feed validation criteria are
    equally meaningful.
    """
    return FeedTestSpec(
        feed_type=FeedType.STOCK,
        symbol="QQQ",
        contract=ContractBuilder.us_stock("QQQ", primary_exchange="NASDAQ"),
        what_to_show="TRADES",
        bar_what_to_show="MIDPOINT",
        use_rth=True,
        bar_market_hours_only=True,
        description="Stock QQQ",
    )


DEFAULT_TEST_PAIRS: List[FeedTestPair] = [
    FeedTestPair("Forex + Stock", forex_spec(), stock_spec()),
]

# Used by the second concurrent plugin (paper_test_feeds_2) so both plugins
# subscribe to distinct symbols and avoid IB repricing collisions.
DEFAULT_TEST_PAIRS_2: List[FeedTestPair] = [
    FeedTestPair("Forex + Stock (concurrent)", forex_spec_2(), stock_spec_2()),
]
