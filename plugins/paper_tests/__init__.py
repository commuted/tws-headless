"""
paper_tests - Paper trading test plugins
"""

from .paper_test_feeds import PaperTestFeedsPlugin
from .paper_test_historical import PaperTestHistoricalPlugin

__all__ = ["PaperTestFeedsPlugin", "PaperTestHistoricalPlugin"]
