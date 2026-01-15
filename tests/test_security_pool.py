"""
Unit tests for security_pool.py

Tests the tradeable securities pool management module.
"""

import json
import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

from security_pool import (
    AssetCategory,
    EquitySubCategory,
    FixedIncomeSubCategory,
    CommoditiesSubCategory,
    RealEstateSubCategory,
    CurrenciesSubCategory,
    CashSubCategory,
    SUBCATEGORY_MAP,
    Security,
    CategoryInfo,
    SecurityPool,
    load_security_pool,
    create_default_instruments_file,
)


# =============================================================================
# Enum Tests
# =============================================================================

class TestAssetCategory:
    """Tests for AssetCategory enum"""

    def test_equity_value(self):
        assert AssetCategory.EQUITY.value == "equity"

    def test_fixed_income_value(self):
        assert AssetCategory.FIXED_INCOME.value == "fixed_income"

    def test_commodities_value(self):
        assert AssetCategory.COMMODITIES.value == "commodities"

    def test_real_estate_value(self):
        assert AssetCategory.REAL_ESTATE.value == "real_estate"

    def test_currencies_value(self):
        assert AssetCategory.CURRENCIES.value == "currencies"

    def test_cash_value(self):
        assert AssetCategory.CASH.value == "cash"

    def test_all_categories_count(self):
        assert len(AssetCategory) == 6


class TestEquitySubCategory:
    """Tests for EquitySubCategory enum"""

    def test_large_cap_growth(self):
        assert EquitySubCategory.LARGE_CAP_GROWTH.value == "large_cap_growth"

    def test_large_cap_value(self):
        assert EquitySubCategory.LARGE_CAP_VALUE.value == "large_cap_value"

    def test_small_cap(self):
        assert EquitySubCategory.SMALL_CAP.value == "small_cap"

    def test_emerging_markets(self):
        assert EquitySubCategory.EMERGING_MARKETS.value == "emerging_markets"


class TestFixedIncomeSubCategory:
    """Tests for FixedIncomeSubCategory enum"""

    def test_government(self):
        assert FixedIncomeSubCategory.GOVERNMENT.value == "government"

    def test_high_yield(self):
        assert FixedIncomeSubCategory.HIGH_YIELD.value == "high_yield"

    def test_international(self):
        assert FixedIncomeSubCategory.INTERNATIONAL.value == "international"


class TestCommoditiesSubCategory:
    """Tests for CommoditiesSubCategory enum"""

    def test_energy(self):
        assert CommoditiesSubCategory.ENERGY.value == "energy"

    def test_precious_metals(self):
        assert CommoditiesSubCategory.PRECIOUS_METALS.value == "precious_metals"

    def test_industrial_metals(self):
        assert CommoditiesSubCategory.INDUSTRIAL_METALS.value == "industrial_metals"

    def test_agriculture(self):
        assert CommoditiesSubCategory.AGRICULTURE.value == "agriculture"


class TestRealEstateSubCategory:
    """Tests for RealEstateSubCategory enum"""

    def test_residential(self):
        assert RealEstateSubCategory.RESIDENTIAL.value == "residential"

    def test_industrial(self):
        assert RealEstateSubCategory.INDUSTRIAL.value == "industrial"

    def test_retail(self):
        assert RealEstateSubCategory.RETAIL.value == "retail"

    def test_office(self):
        assert RealEstateSubCategory.OFFICE.value == "office"


class TestCurrenciesSubCategory:
    """Tests for CurrenciesSubCategory enum"""

    def test_majors_usd(self):
        assert CurrenciesSubCategory.MAJORS_USD.value == "majors_usd"

    def test_majors_eur(self):
        assert CurrenciesSubCategory.MAJORS_EUR.value == "majors_eur"

    def test_emerging_markets(self):
        assert CurrenciesSubCategory.EMERGING_MARKETS.value == "emerging_markets"


class TestCashSubCategory:
    """Tests for CashSubCategory enum"""

    def test_treasury_bills(self):
        assert CashSubCategory.TREASURY_BILLS.value == "treasury_bills"

    def test_commercial_paper(self):
        assert CashSubCategory.COMMERCIAL_PAPER.value == "commercial_paper"

    def test_money_market(self):
        assert CashSubCategory.MONEY_MARKET.value == "money_market"


class TestSubcategoryMap:
    """Tests for SUBCATEGORY_MAP"""

    def test_equity_maps_to_equity_subcategory(self):
        assert SUBCATEGORY_MAP[AssetCategory.EQUITY] == EquitySubCategory

    def test_fixed_income_maps_correctly(self):
        assert SUBCATEGORY_MAP[AssetCategory.FIXED_INCOME] == FixedIncomeSubCategory

    def test_commodities_maps_correctly(self):
        assert SUBCATEGORY_MAP[AssetCategory.COMMODITIES] == CommoditiesSubCategory

    def test_real_estate_maps_correctly(self):
        assert SUBCATEGORY_MAP[AssetCategory.REAL_ESTATE] == RealEstateSubCategory

    def test_currencies_maps_correctly(self):
        assert SUBCATEGORY_MAP[AssetCategory.CURRENCIES] == CurrenciesSubCategory

    def test_cash_maps_correctly(self):
        assert SUBCATEGORY_MAP[AssetCategory.CASH] == CashSubCategory


# =============================================================================
# Security Tests
# =============================================================================

class TestSecurity:
    """Tests for Security dataclass"""

    def test_create_basic_security(self):
        security = Security(
            symbol="SPY",
            name="SPDR S&P 500 ETF",
            category=AssetCategory.EQUITY,
            sub_category="large_cap_growth",
        )
        assert security.symbol == "SPY"
        assert security.name == "SPDR S&P 500 ETF"
        assert security.category == AssetCategory.EQUITY
        assert security.sub_category == "large_cap_growth"

    def test_default_values(self):
        security = Security(
            symbol="SPY",
            name="SPDR S&P 500 ETF",
            category=AssetCategory.EQUITY,
            sub_category="large_cap_growth",
        )
        assert security.exchange == "SMART"
        assert security.currency == "USD"
        assert security.sec_type == "STK"
        assert security.enabled is True
        assert security.notes == ""

    def test_custom_exchange_currency(self):
        security = Security(
            symbol="EWJ",
            name="iShares MSCI Japan ETF",
            category=AssetCategory.EQUITY,
            sub_category="emerging_markets",
            exchange="ARCA",
            currency="USD",
        )
        assert security.exchange == "ARCA"
        assert security.currency == "USD"

    def test_to_contract(self):
        security = Security(
            symbol="VUG",
            name="Vanguard Growth ETF",
            category=AssetCategory.EQUITY,
            sub_category="large_cap_growth",
        )
        contract = security.to_contract()
        assert contract.symbol == "VUG"
        assert contract.secType == "STK"
        assert contract.exchange == "SMART"
        assert contract.currency == "USD"

    def test_to_dict(self):
        security = Security(
            symbol="GLD",
            name="SPDR Gold Trust",
            category=AssetCategory.COMMODITIES,
            sub_category="precious_metals",
            notes="Physical gold ETF",
        )
        data = security.to_dict()
        assert data["symbol"] == "GLD"
        assert data["name"] == "SPDR Gold Trust"
        assert data["category"] == "commodities"
        assert data["sub_category"] == "precious_metals"
        assert data["notes"] == "Physical gold ETF"

    def test_from_dict(self):
        data = {
            "symbol": "HYG",
            "name": "High Yield Bond ETF",
            "category": "fixed_income",
            "sub_category": "high_yield",
            "exchange": "SMART",
            "currency": "USD",
            "sec_type": "STK",
            "enabled": True,
            "notes": "Corporate high yield",
        }
        security = Security.from_dict(data)
        assert security.symbol == "HYG"
        assert security.category == AssetCategory.FIXED_INCOME
        assert security.sub_category == "high_yield"
        assert security.notes == "Corporate high yield"

    def test_from_dict_minimal(self):
        data = {
            "symbol": "BND",
            "name": "Vanguard Total Bond",
            "category": "fixed_income",
            "sub_category": "government",
        }
        security = Security.from_dict(data)
        assert security.symbol == "BND"
        assert security.exchange == "SMART"
        assert security.enabled is True

    def test_roundtrip_dict_conversion(self):
        original = Security(
            symbol="USO",
            name="United States Oil Fund",
            category=AssetCategory.COMMODITIES,
            sub_category="energy",
            notes="Oil futures ETF",
        )
        data = original.to_dict()
        restored = Security.from_dict(data)
        assert restored.symbol == original.symbol
        assert restored.name == original.name
        assert restored.category == original.category
        assert restored.sub_category == original.sub_category
        assert restored.notes == original.notes


class TestCategoryInfo:
    """Tests for CategoryInfo dataclass"""

    def test_create_category_info(self):
        info = CategoryInfo(
            category=AssetCategory.EQUITY,
            display_name="Equity",
            description="Stock investments",
            sub_categories=["large_cap_growth", "large_cap_value"],
        )
        assert info.category == AssetCategory.EQUITY
        assert info.display_name == "Equity"
        assert len(info.sub_categories) == 2

    def test_sub_category_enum_property(self):
        info = CategoryInfo(
            category=AssetCategory.EQUITY,
            display_name="Equity",
            description="Stocks",
        )
        assert info.sub_category_enum == EquitySubCategory


# =============================================================================
# SecurityPool Tests - Initialization
# =============================================================================

class TestSecurityPoolInit:
    """Tests for SecurityPool initialization"""

    def test_init_default(self):
        pool = SecurityPool()
        assert pool.count == 0
        assert pool.loaded is False

    def test_init_custom_file(self):
        pool = SecurityPool("/custom/path/instruments.json")
        assert str(pool._instruments_file) == "/custom/path/instruments.json"

    def test_init_category_info(self):
        pool = SecurityPool()
        assert len(pool._category_info) == 6
        assert AssetCategory.EQUITY in pool._category_info


# =============================================================================
# SecurityPool Tests - Loading
# =============================================================================

class TestSecurityPoolLoad:
    """Tests for SecurityPool.load method"""

    def test_load_from_default_file(self):
        pool = SecurityPool()
        count = pool.load()
        assert count > 0
        assert pool.loaded is True

    def test_load_populates_securities(self):
        pool = SecurityPool()
        pool.load()
        assert "VUG" in pool
        assert "GLD" in pool

    def test_load_idempotent_without_reload(self):
        pool = SecurityPool()
        count1 = pool.load()
        count2 = pool.load()
        assert count1 == count2
        assert pool.loaded is True

    def test_load_with_reload(self):
        pool = SecurityPool()
        pool.load()
        # Add a security
        pool.add(Security("TEST", "Test", AssetCategory.EQUITY, "large_cap_growth"))
        assert "TEST" in pool
        # Reload should clear it
        pool.load(reload=True)
        assert "TEST" not in pool

    def test_load_missing_file(self):
        pool = SecurityPool("/nonexistent/file.json")
        count = pool.load()
        assert count == 0

    def test_load_invalid_json(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("not valid json {{{")
            temp_path = f.name

        pool = SecurityPool(temp_path)
        count = pool.load()
        assert count == 0

        Path(temp_path).unlink()

    def test_load_custom_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({
                "version": "1.0",
                "securities": [
                    {
                        "symbol": "TEST1",
                        "name": "Test Security 1",
                        "category": "equity",
                        "sub_category": "large_cap_growth",
                    },
                    {
                        "symbol": "TEST2",
                        "name": "Test Security 2",
                        "category": "fixed_income",
                        "sub_category": "government",
                    },
                ]
            }, f)
            temp_path = f.name

        pool = SecurityPool(temp_path)
        count = pool.load()
        assert count == 2
        assert "TEST1" in pool
        assert "TEST2" in pool

        Path(temp_path).unlink()


# =============================================================================
# SecurityPool Tests - Lookup Methods
# =============================================================================

class TestSecurityPoolLookup:
    """Tests for SecurityPool lookup methods"""

    @pytest.fixture
    def loaded_pool(self):
        pool = SecurityPool()
        pool.load()
        return pool

    def test_get_existing_symbol(self, loaded_pool):
        security = loaded_pool.get("VUG")
        assert security is not None
        assert security.symbol == "VUG"

    def test_get_nonexistent_symbol(self, loaded_pool):
        security = loaded_pool.get("NOTREAL")
        assert security is None

    def test_get_case_insensitive(self, loaded_pool):
        security = loaded_pool.get("vug")
        assert security is not None
        assert security.symbol == "VUG"

    def test_get_contract(self, loaded_pool):
        contract = loaded_pool.get_contract("GLD")
        assert contract is not None
        assert contract.symbol == "GLD"
        assert contract.secType == "STK"

    def test_get_contract_nonexistent(self, loaded_pool):
        contract = loaded_pool.get_contract("NOTREAL")
        assert contract is None

    def test_contains(self, loaded_pool):
        assert loaded_pool.contains("VUG")
        assert loaded_pool.contains("vug")
        assert not loaded_pool.contains("NOTREAL")

    def test_is_approved_enabled(self, loaded_pool):
        assert loaded_pool.is_approved("VUG")

    def test_is_approved_disabled(self, loaded_pool):
        loaded_pool.disable("VUG")
        assert not loaded_pool.is_approved("VUG")

    def test_is_approved_nonexistent(self, loaded_pool):
        assert not loaded_pool.is_approved("NOTREAL")


# =============================================================================
# SecurityPool Tests - Filtering Methods
# =============================================================================

class TestSecurityPoolFiltering:
    """Tests for SecurityPool filtering methods"""

    @pytest.fixture
    def loaded_pool(self):
        pool = SecurityPool()
        pool.load()
        return pool

    def test_get_by_category_equity(self, loaded_pool):
        equities = loaded_pool.get_by_category(AssetCategory.EQUITY)
        assert len(equities) > 0
        assert all(s.category == AssetCategory.EQUITY for s in equities)

    def test_get_by_category_fixed_income(self, loaded_pool):
        bonds = loaded_pool.get_by_category(AssetCategory.FIXED_INCOME)
        assert len(bonds) > 0
        assert all(s.category == AssetCategory.FIXED_INCOME for s in bonds)

    def test_get_by_category_enabled_only(self, loaded_pool):
        # Disable one equity
        loaded_pool.disable("VUG")
        equities = loaded_pool.get_by_category(AssetCategory.EQUITY, enabled_only=True)
        symbols = [s.symbol for s in equities]
        assert "VUG" not in symbols

    def test_get_by_category_include_disabled(self, loaded_pool):
        loaded_pool.disable("VUG")
        equities = loaded_pool.get_by_category(AssetCategory.EQUITY, enabled_only=False)
        symbols = [s.symbol for s in equities]
        assert "VUG" in symbols

    def test_get_by_sub_category(self, loaded_pool):
        growth = loaded_pool.get_by_sub_category("large_cap_growth")
        assert len(growth) > 0
        assert all(s.sub_category == "large_cap_growth" for s in growth)

    def test_get_by_sub_category_empty(self, loaded_pool):
        result = loaded_pool.get_by_sub_category("nonexistent_subcategory")
        assert result == []

    def test_get_symbols_all(self, loaded_pool):
        symbols = loaded_pool.get_symbols()
        assert len(symbols) > 0
        assert "VUG" in symbols

    def test_get_symbols_by_category(self, loaded_pool):
        symbols = loaded_pool.get_symbols(category=AssetCategory.COMMODITIES)
        assert "GLD" in symbols
        assert "VUG" not in symbols

    def test_get_contracts_all(self, loaded_pool):
        contracts = loaded_pool.get_contracts()
        assert len(contracts) > 0
        assert all(c.secType == "STK" for c in contracts)

    def test_get_contracts_by_category(self, loaded_pool):
        contracts = loaded_pool.get_contracts(category=AssetCategory.CASH)
        assert len(contracts) > 0
        symbols = [c.symbol for c in contracts]
        assert "BIL" in symbols


# =============================================================================
# SecurityPool Tests - Category Information
# =============================================================================

class TestSecurityPoolCategories:
    """Tests for SecurityPool category information methods"""

    @pytest.fixture
    def loaded_pool(self):
        pool = SecurityPool()
        pool.load()
        return pool

    def test_get_categories(self, loaded_pool):
        categories = loaded_pool.get_categories()
        assert len(categories) == 6
        assert AssetCategory.EQUITY in categories

    def test_get_category_info(self, loaded_pool):
        info = loaded_pool.get_category_info(AssetCategory.EQUITY)
        assert info.display_name == "Equity"
        assert len(info.sub_categories) > 0

    def test_get_sub_categories(self, loaded_pool):
        sub_cats = loaded_pool.get_sub_categories(AssetCategory.EQUITY)
        assert "large_cap_growth" in sub_cats
        assert "large_cap_value" in sub_cats

    def test_category_summary(self, loaded_pool):
        summary = loaded_pool.category_summary()
        assert "equity" in summary
        assert summary["equity"] > 0
        assert "fixed_income" in summary


# =============================================================================
# SecurityPool Tests - Management Methods
# =============================================================================

class TestSecurityPoolManagement:
    """Tests for SecurityPool management methods"""

    @pytest.fixture
    def loaded_pool(self):
        pool = SecurityPool()
        pool.load()
        return pool

    def test_add_new_security(self, loaded_pool):
        initial_count = loaded_pool.count
        security = Security(
            symbol="NEW1",
            name="New Security",
            category=AssetCategory.EQUITY,
            sub_category="large_cap_growth",
        )
        result = loaded_pool.add(security)
        assert result is True
        assert loaded_pool.count == initial_count + 1
        assert "NEW1" in loaded_pool

    def test_add_duplicate_fails(self, loaded_pool):
        security = Security(
            symbol="VUG",
            name="Duplicate",
            category=AssetCategory.EQUITY,
            sub_category="large_cap_growth",
        )
        result = loaded_pool.add(security)
        assert result is False

    def test_remove_existing(self, loaded_pool):
        initial_count = loaded_pool.count
        result = loaded_pool.remove("VUG")
        assert result is True
        assert loaded_pool.count == initial_count - 1
        assert "VUG" not in loaded_pool

    def test_remove_nonexistent(self, loaded_pool):
        result = loaded_pool.remove("NOTREAL")
        assert result is False

    def test_enable_security(self, loaded_pool):
        loaded_pool.disable("VUG")
        assert not loaded_pool.get("VUG").enabled
        result = loaded_pool.enable("VUG")
        assert result is True
        assert loaded_pool.get("VUG").enabled

    def test_enable_nonexistent(self, loaded_pool):
        result = loaded_pool.enable("NOTREAL")
        assert result is False

    def test_disable_security(self, loaded_pool):
        assert loaded_pool.get("VUG").enabled
        result = loaded_pool.disable("VUG")
        assert result is True
        assert not loaded_pool.get("VUG").enabled

    def test_disable_nonexistent(self, loaded_pool):
        result = loaded_pool.disable("NOTREAL")
        assert result is False


# =============================================================================
# SecurityPool Tests - Iteration
# =============================================================================

class TestSecurityPoolIteration:
    """Tests for SecurityPool iteration support"""

    @pytest.fixture
    def loaded_pool(self):
        pool = SecurityPool()
        pool.load()
        return pool

    def test_iter(self, loaded_pool):
        securities = list(loaded_pool)
        assert len(securities) > 0
        assert all(isinstance(s, Security) for s in securities)

    def test_len(self, loaded_pool):
        assert len(loaded_pool) == loaded_pool.count

    def test_contains_operator(self, loaded_pool):
        assert "VUG" in loaded_pool
        assert "NOTREAL" not in loaded_pool

    def test_getitem(self, loaded_pool):
        security = loaded_pool["VUG"]
        assert security.symbol == "VUG"

    def test_getitem_raises_keyerror(self, loaded_pool):
        with pytest.raises(KeyError):
            _ = loaded_pool["NOTREAL"]


# =============================================================================
# SecurityPool Tests - Save
# =============================================================================

class TestSecurityPoolSave:
    """Tests for SecurityPool.save method"""

    def test_save_creates_file(self):
        pool = SecurityPool()
        pool.add(Security("TEST1", "Test 1", AssetCategory.EQUITY, "large_cap_growth"))
        pool.add(Security("TEST2", "Test 2", AssetCategory.FIXED_INCOME, "government"))

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            temp_path = f.name

        pool.save(temp_path)

        assert Path(temp_path).exists()

        with open(temp_path) as f:
            data = json.load(f)

        assert "securities" in data
        assert len(data["securities"]) == 2

        Path(temp_path).unlink()

    def test_save_load_roundtrip(self):
        pool1 = SecurityPool()
        pool1.add(Security("AAA", "Security A", AssetCategory.EQUITY, "large_cap_growth"))
        pool1.add(Security("BBB", "Security B", AssetCategory.CASH, "money_market"))

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            temp_path = f.name

        pool1.save(temp_path)

        pool2 = SecurityPool(temp_path)
        pool2.load()

        assert pool2.count == 2
        assert "AAA" in pool2
        assert "BBB" in pool2

        Path(temp_path).unlink()


# =============================================================================
# SecurityPool Tests - Properties
# =============================================================================

class TestSecurityPoolProperties:
    """Tests for SecurityPool properties"""

    def test_count_empty(self):
        pool = SecurityPool("/nonexistent/path.json")
        assert pool.count == 0

    def test_count_after_load(self):
        pool = SecurityPool()
        pool.load()
        assert pool.count > 0

    def test_enabled_count(self):
        pool = SecurityPool()
        pool.load()
        initial_enabled = pool.enabled_count
        pool.disable("VUG")
        assert pool.enabled_count == initial_enabled - 1

    def test_loaded_property(self):
        pool = SecurityPool()
        assert pool.loaded is False
        pool.load()
        assert pool.loaded is True


# =============================================================================
# SecurityPool Tests - String Representation
# =============================================================================

class TestSecurityPoolStringRep:
    """Tests for SecurityPool string representation"""

    def test_repr(self):
        pool = SecurityPool()
        pool.load()
        repr_str = repr(pool)
        assert "SecurityPool" in repr_str
        assert "count=" in repr_str
        assert "enabled=" in repr_str

    def test_summary(self):
        pool = SecurityPool()
        pool.load()
        summary = pool.summary()
        assert "Security Pool" in summary
        assert "Equity" in summary
        assert "Fixed Income" in summary


# =============================================================================
# Convenience Function Tests
# =============================================================================

class TestConvenienceFunctions:
    """Tests for module-level convenience functions"""

    def test_load_security_pool(self):
        pool = load_security_pool()
        assert pool.loaded is True
        assert pool.count > 0

    def test_load_security_pool_custom_path(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({
                "version": "1.0",
                "securities": [
                    {
                        "symbol": "XYZ",
                        "name": "XYZ Fund",
                        "category": "equity",
                        "sub_category": "large_cap_growth",
                    }
                ]
            }, f)
            temp_path = f.name

        pool = load_security_pool(temp_path)
        assert pool.count == 1
        assert "XYZ" in pool

        Path(temp_path).unlink()

    def test_create_default_instruments_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file = Path(temp_dir) / "test_instruments.json"

            # Patch the default file path
            with patch.object(SecurityPool, 'DEFAULT_INSTRUMENTS_FILE', temp_file):
                pool = create_default_instruments_file()

            assert pool.count == 21  # All the ETFs we defined
            assert "VUG" in pool
            assert "GLD" in pool
            assert "BIL" in pool


# =============================================================================
# Integration Tests
# =============================================================================

class TestSecurityPoolIntegration:
    """Integration tests for SecurityPool"""

    def test_full_workflow(self):
        """Test complete workflow: load, filter, modify, save"""
        # Load
        pool = SecurityPool()
        pool.load()
        initial_count = pool.count

        # Filter by category
        equities = pool.get_by_category(AssetCategory.EQUITY)
        assert len(equities) == 4  # VUG, VTV, IJR, VWO

        # Disable some
        pool.disable("VUG")
        pool.disable("VTV")
        equities_enabled = pool.get_by_category(AssetCategory.EQUITY, enabled_only=True)
        assert len(equities_enabled) == 2

        # Add new
        pool.add(Security("SPY", "SPDR S&P 500", AssetCategory.EQUITY, "large_cap_growth"))
        assert pool.count == initial_count + 1

        # Get contracts for trading
        contracts = pool.get_contracts(category=AssetCategory.COMMODITIES)
        assert len(contracts) == 4
        assert all(c.symbol in ["USO", "GLD", "DBB", "DBA"] for c in contracts)

    def test_category_coverage(self):
        """Verify all categories have securities"""
        pool = SecurityPool()
        pool.load()

        for category in AssetCategory:
            securities = pool.get_by_category(category)
            assert len(securities) > 0, f"No securities in category {category.value}"

    def test_all_expected_etfs_present(self):
        """Verify all expected ETFs are loaded"""
        pool = SecurityPool()
        pool.load()

        expected_etfs = [
            # Equity
            "VUG", "VTV", "IJR", "VWO",
            # Fixed Income
            "GOVT", "HYG", "BNDX",
            # Commodities
            "USO", "GLD", "DBB", "DBA",
            # Real Estate
            "REZ", "INDS", "RTH", "SRVR",
            # Currencies
            "UUP", "FXE", "CEW",
            # Cash
            "BIL", "FLOT", "JPST",
        ]

        for symbol in expected_etfs:
            assert symbol in pool, f"Expected ETF {symbol} not found in pool"
