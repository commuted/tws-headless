"""
paper_test_feeds_2/plugin.py - Second concurrent instance of the feed test.

Subscribes to the same symbols as paper_test_feeds simultaneously to verify
that the StreamManager correctly shares subscriptions: both plugins receive
all ticks and bars, and neither cancellation affects the other's stream.
"""

from pathlib import Path
from typing import Optional

from plugins.paper_tests.paper_test_feeds.plugin import PaperTestFeedsPlugin


class PaperTestFeeds2Plugin(PaperTestFeedsPlugin):
    """
    Concurrent second instance of the feed test plugin.

    Identical logic to paper_test_feeds; runs at the same time to exercise
    the StreamManager's shared-subscription and ref-counting paths.

    Run via: plugin request paper_test_feeds_2 run_tests
    """

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
