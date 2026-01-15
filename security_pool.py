"""
security_pool.py - Tradeable securities pool management

Provides a structured pool of approved tradeable securities organized by
asset category and sub-category. Securities can be loaded from a JSON file
at startup or on command.
"""

import json
import logging
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import List, Dict, Optional, Set, Iterator

from ibapi.contract import Contract

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================

class AssetCategory(Enum):
    """Major asset categories"""
    EQUITY = "equity"
    FIXED_INCOME = "fixed_income"
    COMMODITIES = "commodities"
    REAL_ESTATE = "real_estate"
    CURRENCIES = "currencies"
    CASH = "cash"


class EquitySubCategory(Enum):
    """Equity sub-categories"""
    LARGE_CAP_GROWTH = "large_cap_growth"
    LARGE_CAP_VALUE = "large_cap_value"
    SMALL_CAP = "small_cap"
    EMERGING_MARKETS = "emerging_markets"


class FixedIncomeSubCategory(Enum):
    """Fixed income sub-categories"""
    GOVERNMENT = "government"
    HIGH_YIELD = "high_yield"
    INTERNATIONAL = "international"


class CommoditiesSubCategory(Enum):
    """Commodities sub-categories"""
    ENERGY = "energy"
    PRECIOUS_METALS = "precious_metals"
    INDUSTRIAL_METALS = "industrial_metals"
    AGRICULTURE = "agriculture"


class RealEstateSubCategory(Enum):
    """Real estate sub-categories"""
    RESIDENTIAL = "residential"
    INDUSTRIAL = "industrial"
    RETAIL = "retail"
    OFFICE = "office"


class CurrenciesSubCategory(Enum):
    """Currencies sub-categories"""
    MAJORS_USD = "majors_usd"
    MAJORS_EUR = "majors_eur"
    EMERGING_MARKETS = "emerging_markets"


class CashSubCategory(Enum):
    """Cash/money market sub-categories"""
    TREASURY_BILLS = "treasury_bills"
    COMMERCIAL_PAPER = "commercial_paper"
    MONEY_MARKET = "money_market"


# Mapping from category to sub-category enum
SUBCATEGORY_MAP = {
    AssetCategory.EQUITY: EquitySubCategory,
    AssetCategory.FIXED_INCOME: FixedIncomeSubCategory,
    AssetCategory.COMMODITIES: CommoditiesSubCategory,
    AssetCategory.REAL_ESTATE: RealEstateSubCategory,
    AssetCategory.CURRENCIES: CurrenciesSubCategory,
    AssetCategory.CASH: CashSubCategory,
}


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class Security:
    """
    Represents a tradeable security in the pool.

    Attributes:
        symbol: Trading symbol (e.g., "SPY")
        name: Full name/description
        category: Major asset category
        sub_category: Sub-category within the major category
        exchange: Primary exchange (default "SMART")
        currency: Trading currency (default "USD")
        sec_type: Security type (default "STK" for stocks/ETFs)
        enabled: Whether security is enabled for trading
        notes: Optional notes about the security
    """
    symbol: str
    name: str
    category: AssetCategory
    sub_category: str  # String to allow any sub-category enum value
    exchange: str = "SMART"
    currency: str = "USD"
    sec_type: str = "STK"
    enabled: bool = True
    notes: str = ""

    def to_contract(self) -> Contract:
        """Convert to IB Contract object"""
        contract = Contract()
        contract.symbol = self.symbol
        contract.secType = self.sec_type
        contract.exchange = self.exchange
        contract.currency = self.currency
        return contract

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        return {
            "symbol": self.symbol,
            "name": self.name,
            "category": self.category.value,
            "sub_category": self.sub_category,
            "exchange": self.exchange,
            "currency": self.currency,
            "sec_type": self.sec_type,
            "enabled": self.enabled,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "Security":
        """Create Security from dictionary"""
        return cls(
            symbol=data["symbol"],
            name=data["name"],
            category=AssetCategory(data["category"]),
            sub_category=data["sub_category"],
            exchange=data.get("exchange", "SMART"),
            currency=data.get("currency", "USD"),
            sec_type=data.get("sec_type", "STK"),
            enabled=data.get("enabled", True),
            notes=data.get("notes", ""),
        )


@dataclass
class CategoryInfo:
    """Information about an asset category"""
    category: AssetCategory
    display_name: str
    description: str
    sub_categories: List[str] = field(default_factory=list)

    @property
    def sub_category_enum(self):
        """Get the sub-category enum for this category"""
        return SUBCATEGORY_MAP.get(self.category)


# =============================================================================
# Security Pool
# =============================================================================

class SecurityPool:
    """
    Manages a pool of approved tradeable securities.

    Securities are organized by category and sub-category for easy
    filtering and selection during portfolio construction and rebalancing.

    Usage:
        # Load from default file
        pool = SecurityPool()
        pool.load()

        # Or load from specific file
        pool = SecurityPool("/path/to/instruments.json")
        pool.load()

        # Get securities by category
        equities = pool.get_by_category(AssetCategory.EQUITY)

        # Get specific security
        spy = pool.get("SPY")

        # Get contract for trading
        contract = pool.get_contract("VUG")
    """

    # Default instruments file location (same directory as this module)
    DEFAULT_INSTRUMENTS_FILE = Path(__file__).parent / "instruments.json"

    def __init__(self, instruments_file: Optional[str] = None):
        """
        Initialize the security pool.

        Args:
            instruments_file: Path to JSON file with instruments.
                            Uses default if not specified.
        """
        self._instruments_file = Path(instruments_file) if instruments_file else self.DEFAULT_INSTRUMENTS_FILE
        self._securities: Dict[str, Security] = {}
        self._by_category: Dict[AssetCategory, List[Security]] = {cat: [] for cat in AssetCategory}
        self._by_sub_category: Dict[str, List[Security]] = {}
        self._loaded = False
        self._category_info: Dict[AssetCategory, CategoryInfo] = {}

        # Initialize category info
        self._init_category_info()

    def _init_category_info(self):
        """Initialize category metadata"""
        self._category_info = {
            AssetCategory.EQUITY: CategoryInfo(
                category=AssetCategory.EQUITY,
                display_name="Equity",
                description="Stock market investments including domestic and international equities",
                sub_categories=[e.value for e in EquitySubCategory],
            ),
            AssetCategory.FIXED_INCOME: CategoryInfo(
                category=AssetCategory.FIXED_INCOME,
                display_name="Fixed Income",
                description="Bond and debt instruments including government and corporate bonds",
                sub_categories=[e.value for e in FixedIncomeSubCategory],
            ),
            AssetCategory.COMMODITIES: CategoryInfo(
                category=AssetCategory.COMMODITIES,
                display_name="Commodities",
                description="Physical goods including energy, metals, and agricultural products",
                sub_categories=[e.value for e in CommoditiesSubCategory],
            ),
            AssetCategory.REAL_ESTATE: CategoryInfo(
                category=AssetCategory.REAL_ESTATE,
                display_name="Real Estate",
                description="Real estate investment trusts and property-related securities",
                sub_categories=[e.value for e in RealEstateSubCategory],
            ),
            AssetCategory.CURRENCIES: CategoryInfo(
                category=AssetCategory.CURRENCIES,
                display_name="Currencies",
                description="Foreign exchange and currency-related instruments",
                sub_categories=[e.value for e in CurrenciesSubCategory],
            ),
            AssetCategory.CASH: CategoryInfo(
                category=AssetCategory.CASH,
                display_name="Cash/Money Market",
                description="Short-term, highly liquid investments",
                sub_categories=[e.value for e in CashSubCategory],
            ),
        }

    @property
    def loaded(self) -> bool:
        """Whether securities have been loaded"""
        return self._loaded

    @property
    def count(self) -> int:
        """Total number of securities in the pool"""
        return len(self._securities)

    @property
    def enabled_count(self) -> int:
        """Number of enabled securities"""
        return sum(1 for s in self._securities.values() if s.enabled)

    def load(self, reload: bool = False) -> int:
        """
        Load securities from the instruments file.

        Args:
            reload: If True, reload even if already loaded

        Returns:
            Number of securities loaded
        """
        if self._loaded and not reload:
            logger.debug("Securities already loaded, use reload=True to reload")
            return self.count

        if not self._instruments_file.exists():
            logger.warning(f"Instruments file not found: {self._instruments_file}")
            return 0

        try:
            with open(self._instruments_file, "r") as f:
                data = json.load(f)

            # Clear existing data
            self._securities.clear()
            self._by_category = {cat: [] for cat in AssetCategory}
            self._by_sub_category.clear()

            # Load securities
            securities_data = data.get("securities", [])
            for sec_data in securities_data:
                try:
                    security = Security.from_dict(sec_data)
                    self._add_security(security)
                except Exception as e:
                    logger.warning(f"Failed to load security {sec_data.get('symbol', 'unknown')}: {e}")

            self._loaded = True
            logger.info(f"Loaded {self.count} securities from {self._instruments_file}")
            return self.count

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in instruments file: {e}")
            return 0
        except Exception as e:
            logger.error(f"Failed to load instruments file: {e}")
            return 0

    def _add_security(self, security: Security):
        """Add a security to the pool"""
        self._securities[security.symbol] = security
        self._by_category[security.category].append(security)

        if security.sub_category not in self._by_sub_category:
            self._by_sub_category[security.sub_category] = []
        self._by_sub_category[security.sub_category].append(security)

    def save(self, file_path: Optional[str] = None):
        """
        Save securities to a JSON file.

        Args:
            file_path: Path to save to. Uses instruments file if not specified.
        """
        path = Path(file_path) if file_path else self._instruments_file

        data = {
            "version": "1.0",
            "description": "Approved tradeable securities pool",
            "securities": [s.to_dict() for s in self._securities.values()],
        }

        with open(path, "w") as f:
            json.dump(data, f, indent=2)

        logger.info(f"Saved {self.count} securities to {path}")

    # =========================================================================
    # Lookup Methods
    # =========================================================================

    def get(self, symbol: str) -> Optional[Security]:
        """
        Get a security by symbol.

        Args:
            symbol: Trading symbol

        Returns:
            Security if found, None otherwise
        """
        return self._securities.get(symbol.upper())

    def get_contract(self, symbol: str) -> Optional[Contract]:
        """
        Get an IB Contract for a symbol.

        Args:
            symbol: Trading symbol

        Returns:
            Contract if security found, None otherwise
        """
        security = self.get(symbol)
        return security.to_contract() if security else None

    def contains(self, symbol: str) -> bool:
        """Check if a symbol is in the pool"""
        return symbol.upper() in self._securities

    def is_approved(self, symbol: str) -> bool:
        """Check if a symbol is approved (in pool and enabled)"""
        security = self.get(symbol)
        return security is not None and security.enabled

    # =========================================================================
    # Filtering Methods
    # =========================================================================

    def get_by_category(
        self,
        category: AssetCategory,
        enabled_only: bool = True,
    ) -> List[Security]:
        """
        Get all securities in a category.

        Args:
            category: Asset category to filter by
            enabled_only: If True, only return enabled securities

        Returns:
            List of securities in the category
        """
        securities = self._by_category.get(category, [])
        if enabled_only:
            return [s for s in securities if s.enabled]
        return list(securities)

    def get_by_sub_category(
        self,
        sub_category: str,
        enabled_only: bool = True,
    ) -> List[Security]:
        """
        Get all securities in a sub-category.

        Args:
            sub_category: Sub-category to filter by
            enabled_only: If True, only return enabled securities

        Returns:
            List of securities in the sub-category
        """
        securities = self._by_sub_category.get(sub_category, [])
        if enabled_only:
            return [s for s in securities if s.enabled]
        return list(securities)

    def get_symbols(
        self,
        category: Optional[AssetCategory] = None,
        enabled_only: bool = True,
    ) -> List[str]:
        """
        Get list of symbols, optionally filtered by category.

        Args:
            category: Optional category filter
            enabled_only: If True, only return enabled securities

        Returns:
            List of trading symbols
        """
        if category:
            securities = self.get_by_category(category, enabled_only)
        elif enabled_only:
            securities = [s for s in self._securities.values() if s.enabled]
        else:
            securities = list(self._securities.values())

        return [s.symbol for s in securities]

    def get_contracts(
        self,
        category: Optional[AssetCategory] = None,
        enabled_only: bool = True,
    ) -> List[Contract]:
        """
        Get list of IB Contracts, optionally filtered by category.

        Args:
            category: Optional category filter
            enabled_only: If True, only return enabled securities

        Returns:
            List of IB Contract objects
        """
        if category:
            securities = self.get_by_category(category, enabled_only)
        elif enabled_only:
            securities = [s for s in self._securities.values() if s.enabled]
        else:
            securities = list(self._securities.values())

        return [s.to_contract() for s in securities]

    # =========================================================================
    # Category Information
    # =========================================================================

    def get_categories(self) -> List[AssetCategory]:
        """Get list of all asset categories"""
        return list(AssetCategory)

    def get_category_info(self, category: AssetCategory) -> CategoryInfo:
        """Get information about a category"""
        return self._category_info[category]

    def get_sub_categories(self, category: AssetCategory) -> List[str]:
        """Get sub-categories for a category"""
        return self._category_info[category].sub_categories

    def category_summary(self) -> Dict[str, int]:
        """
        Get a summary of securities per category.

        Returns:
            Dict mapping category name to count of enabled securities
        """
        return {
            cat.value: len(self.get_by_category(cat, enabled_only=True))
            for cat in AssetCategory
        }

    # =========================================================================
    # Management Methods
    # =========================================================================

    def add(self, security: Security) -> bool:
        """
        Add a security to the pool.

        Args:
            security: Security to add

        Returns:
            True if added, False if symbol already exists
        """
        if security.symbol in self._securities:
            logger.warning(f"Security {security.symbol} already in pool")
            return False

        self._add_security(security)
        return True

    def remove(self, symbol: str) -> bool:
        """
        Remove a security from the pool.

        Args:
            symbol: Symbol to remove

        Returns:
            True if removed, False if not found
        """
        symbol = symbol.upper()
        if symbol not in self._securities:
            return False

        security = self._securities.pop(symbol)
        self._by_category[security.category].remove(security)
        self._by_sub_category[security.sub_category].remove(security)
        return True

    def enable(self, symbol: str) -> bool:
        """Enable a security for trading"""
        security = self.get(symbol)
        if security:
            security.enabled = True
            return True
        return False

    def disable(self, symbol: str) -> bool:
        """Disable a security from trading"""
        security = self.get(symbol)
        if security:
            security.enabled = False
            return True
        return False

    # =========================================================================
    # Iteration Support
    # =========================================================================

    def __iter__(self) -> Iterator[Security]:
        """Iterate over all securities"""
        return iter(self._securities.values())

    def __len__(self) -> int:
        """Return total count of securities"""
        return len(self._securities)

    def __contains__(self, symbol: str) -> bool:
        """Check if symbol in pool"""
        return self.contains(symbol)

    def __getitem__(self, symbol: str) -> Security:
        """Get security by symbol, raises KeyError if not found"""
        security = self.get(symbol)
        if security is None:
            raise KeyError(f"Symbol {symbol} not found in pool")
        return security

    # =========================================================================
    # String Representation
    # =========================================================================

    def __repr__(self) -> str:
        return f"SecurityPool(count={self.count}, enabled={self.enabled_count}, loaded={self._loaded})"

    def summary(self) -> str:
        """Get a formatted summary of the pool"""
        lines = [
            f"Security Pool: {self.count} securities ({self.enabled_count} enabled)",
            "-" * 50,
        ]

        for category in AssetCategory:
            securities = self.get_by_category(category, enabled_only=False)
            enabled = sum(1 for s in securities if s.enabled)
            info = self._category_info[category]
            lines.append(f"{info.display_name}: {enabled}/{len(securities)}")

            # Group by sub-category
            by_sub = {}
            for s in securities:
                if s.sub_category not in by_sub:
                    by_sub[s.sub_category] = []
                by_sub[s.sub_category].append(s)

            for sub_cat, secs in by_sub.items():
                symbols = ", ".join(s.symbol for s in secs if s.enabled)
                if symbols:
                    lines.append(f"  {sub_cat}: {symbols}")

        return "\n".join(lines)


# =============================================================================
# Convenience Functions
# =============================================================================

def load_security_pool(file_path: Optional[str] = None) -> SecurityPool:
    """
    Load and return a security pool.

    Args:
        file_path: Optional path to instruments file

    Returns:
        Loaded SecurityPool instance
    """
    pool = SecurityPool(file_path)
    pool.load()
    return pool


def create_default_instruments_file():
    """Create the default instruments.json file with representative ETFs"""
    pool = SecurityPool()

    # Equity
    pool.add(Security("VUG", "Vanguard Growth ETF", AssetCategory.EQUITY, EquitySubCategory.LARGE_CAP_GROWTH.value,
                      notes="Large-cap US growth stocks"))
    pool.add(Security("VTV", "Vanguard Value ETF", AssetCategory.EQUITY, EquitySubCategory.LARGE_CAP_VALUE.value,
                      notes="Large-cap US value stocks"))
    pool.add(Security("IJR", "iShares Core S&P Small-Cap ETF", AssetCategory.EQUITY, EquitySubCategory.SMALL_CAP.value,
                      notes="US small-cap stocks"))
    pool.add(Security("VWO", "Vanguard FTSE Emerging Markets ETF", AssetCategory.EQUITY, EquitySubCategory.EMERGING_MARKETS.value,
                      notes="Emerging market equities"))

    # Fixed Income
    pool.add(Security("GOVT", "iShares U.S. Treasury Bond ETF", AssetCategory.FIXED_INCOME, FixedIncomeSubCategory.GOVERNMENT.value,
                      notes="US Treasury bonds across maturities"))
    pool.add(Security("HYG", "iShares iBoxx $ High Yield Corporate Bond ETF", AssetCategory.FIXED_INCOME, FixedIncomeSubCategory.HIGH_YIELD.value,
                      notes="High yield corporate bonds"))
    pool.add(Security("BNDX", "Vanguard Total International Bond ETF", AssetCategory.FIXED_INCOME, FixedIncomeSubCategory.INTERNATIONAL.value,
                      notes="International investment-grade bonds"))

    # Commodities
    pool.add(Security("USO", "United States Oil Fund", AssetCategory.COMMODITIES, CommoditiesSubCategory.ENERGY.value,
                      notes="Crude oil futures"))
    pool.add(Security("GLD", "SPDR Gold Trust", AssetCategory.COMMODITIES, CommoditiesSubCategory.PRECIOUS_METALS.value,
                      notes="Physical gold"))
    pool.add(Security("DBB", "Invesco DB Base Metals Fund", AssetCategory.COMMODITIES, CommoditiesSubCategory.INDUSTRIAL_METALS.value,
                      notes="Industrial base metals (aluminum, copper, zinc)"))
    pool.add(Security("DBA", "Invesco DB Agriculture Fund", AssetCategory.COMMODITIES, CommoditiesSubCategory.AGRICULTURE.value,
                      notes="Agricultural commodities"))

    # Real Estate
    pool.add(Security("REZ", "iShares Residential and Multisector Real Estate ETF", AssetCategory.REAL_ESTATE, RealEstateSubCategory.RESIDENTIAL.value,
                      notes="Residential REITs"))
    pool.add(Security("INDS", "Pacer Industrial Real Estate ETF", AssetCategory.REAL_ESTATE, RealEstateSubCategory.INDUSTRIAL.value,
                      notes="Industrial REITs"))
    pool.add(Security("RTH", "VanEck Retail ETF", AssetCategory.REAL_ESTATE, RealEstateSubCategory.RETAIL.value,
                      notes="Retail sector"))
    pool.add(Security("SRVR", "Pacer Data & Infrastructure Real Estate ETF", AssetCategory.REAL_ESTATE, RealEstateSubCategory.OFFICE.value,
                      notes="Data centers and infrastructure REITs"))

    # Currencies
    pool.add(Security("UUP", "Invesco DB US Dollar Index Bullish Fund", AssetCategory.CURRENCIES, CurrenciesSubCategory.MAJORS_USD.value,
                      notes="Long USD vs major currencies"))
    pool.add(Security("FXE", "Invesco CurrencyShares Euro Trust", AssetCategory.CURRENCIES, CurrenciesSubCategory.MAJORS_EUR.value,
                      notes="Euro currency"))
    pool.add(Security("CEW", "WisdomTree Emerging Currency Strategy Fund", AssetCategory.CURRENCIES, CurrenciesSubCategory.EMERGING_MARKETS.value,
                      notes="Emerging market currencies"))

    # Cash/Money Market
    pool.add(Security("BIL", "SPDR Bloomberg 1-3 Month T-Bill ETF", AssetCategory.CASH, CashSubCategory.TREASURY_BILLS.value,
                      notes="Short-term Treasury bills"))
    pool.add(Security("FLOT", "iShares Floating Rate Bond ETF", AssetCategory.CASH, CashSubCategory.COMMERCIAL_PAPER.value,
                      notes="Floating rate investment-grade bonds"))
    pool.add(Security("JPST", "JPMorgan Ultra-Short Income ETF", AssetCategory.CASH, CashSubCategory.MONEY_MARKET.value,
                      notes="Ultra-short duration bonds"))

    pool.save()
    return pool
