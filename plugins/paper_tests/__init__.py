"""
paper_tests - Paper trading test plugins
"""

from .paper_test_feeds import PaperTestFeedsPlugin
from .paper_test_feeds_2 import PaperTestFeeds2Plugin
from .paper_test_historical import PaperTestHistoricalPlugin

__all__ = ["PaperTestFeedsPlugin", "PaperTestFeeds2Plugin", "PaperTestHistoricalPlugin"]
