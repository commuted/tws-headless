"""
pytest configuration for ib package tests
"""

import sys
from pathlib import Path

# Add the parent directory to path so we can import ib as a package
parent_dir = Path(__file__).parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

# Now re-import ib to ensure it's imported as a package
import ib  # noqa: F401
