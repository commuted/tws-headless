"""
paper_test_feeds_2/plugin.py - Second concurrent instance of the feed test.

Subscribes to DIFFERENT symbols than paper_test_feeds (GBP.USD + QQQ instead
of EUR.USD + SPY) so that both concurrent plugins request distinct IB contracts.
Subscribing two plugins to the same symbol concurrently can cause IB to
reprice one of the requests; using distinct symbols avoids this entirely
while still exercising the StreamManager's ref-counting paths independently.
"""

from pathlib import Path
from typing import Optional

from plugins.paper_tests.paper_test_feeds.feed_test_specs import DEFAULT_TEST_PAIRS_2
from plugins.paper_tests.paper_test_feeds.plugin import PaperTestFeedsPlugin


class PaperTestFeeds2Plugin(PaperTestFeedsPlugin):
    """
    Concurrent second instance of the feed test plugin.

    Uses GBP.USD + QQQ (distinct from EUR.USD + SPY used by paper_test_feeds)
    to avoid IB repricing when both plugins subscribe simultaneously.
    Exercises the StreamManager's subscription lifecycle with a fresh set of
    contracts independently of the first instance.

    Run via: plugin request paper_test_feeds_2 run_tests
    """

    # Override to use distinct symbols from the first concurrent instance
    TEST_PAIRS = DEFAULT_TEST_PAIRS_2

    def __init__(
        self,
        base_path: Optional[Path] = None,
        portfolio=None,
        shared_holdings=None,
        message_bus=None,
    ):
        super().__init__(
            base_path, portfolio, shared_holdings, message_bus,
            name="paper_test_feeds_2",
        )

    @property
    def description(self) -> str:
        return (
            "Paper Test Feeds 2: concurrent second instance — verifies that "
            "shared StreamManager subscriptions deliver data to both plugins "
            "independently and that ref-counting prevents premature cancellation."
        )
