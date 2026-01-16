"""
const.py - IB API Constants

Constants from the official Interactive Brokers API for compatibility.
"""

import sys
import math
from decimal import Decimal

# Invalid/unset ID constant
NO_VALID_ID = -1

# Maximum message length (16MB - 1 byte)
MAX_MSG_LEN = 0xFFFFFF

# Sentinel values for unset fields
UNSET_INTEGER = 2**31 - 1
UNSET_LONG = 2**63 - 1
UNSET_DOUBLE = float(sys.float_info.max)
UNSET_DECIMAL = Decimal(2**127 - 1)

# Special double values
DOUBLE_INFINITY = math.inf

# Origin constants
CUSTOMER = 0
FIRM = 1
UNKNOWN = 2

# Auction strategy constants
AUCTION_UNSET = 0
AUCTION_MATCH = 1
AUCTION_IMPROVEMENT = 2
AUCTION_TRANSPARENT = 3

# Special offset constant for compete orders
COMPETE_AGAINST_BEST_OFFSET_UP_TO_MID = DOUBLE_INFINITY
