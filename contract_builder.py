"""
contract_builder.py - Contract factory methods for all asset types

Based on the official IB API Testbed patterns. Provides static factory methods
for creating properly configured Contract objects for different security types.
"""

from typing import List, Optional
from ibapi.contract import Contract, ComboLeg


class ContractBuilder:
    """Factory methods for creating IB Contract objects."""

    # =========================================================================
    # Stock Contracts
    # =========================================================================

    @staticmethod
    def stock(symbol: str, exchange: str = "SMART", currency: str = "USD",
              primary_exchange: str = "") -> Contract:
        """
        Create a stock contract.

        Args:
            symbol: Stock ticker symbol (e.g., "AAPL", "SPY")
            exchange: Exchange to route to (default: "SMART")
            currency: Currency of the stock (default: "USD")
            primary_exchange: Primary listing exchange (optional, e.g., "NASDAQ", "NYSE")

        Returns:
            Configured Contract object
        """
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.currency = currency
        contract.exchange = exchange
        if primary_exchange:
            contract.primaryExchange = primary_exchange
        return contract

    @staticmethod
    def us_stock(symbol: str, primary_exchange: str = "") -> Contract:
        """Create a US stock contract with SMART routing."""
        return ContractBuilder.stock(symbol, "SMART", "USD", primary_exchange)

    @staticmethod
    def european_stock(symbol: str, currency: str = "EUR",
                       primary_exchange: str = "") -> Contract:
        """Create a European stock contract."""
        return ContractBuilder.stock(symbol, "SMART", currency, primary_exchange)

    @staticmethod
    def etf(symbol: str, exchange: str = "SMART", currency: str = "USD") -> Contract:
        """Create an ETF contract (same as stock but clarifies intent)."""
        return ContractBuilder.stock(symbol, exchange, currency)

    # =========================================================================
    # Option Contracts
    # =========================================================================

    @staticmethod
    def option(symbol: str, expiry: str, strike: float, right: str,
               exchange: str = "SMART", currency: str = "USD",
               multiplier: str = "100", trading_class: str = "") -> Contract:
        """
        Create an option contract.

        Args:
            symbol: Underlying symbol
            expiry: Expiration date (YYYYMMDD) or month (YYYYMM)
            strike: Strike price
            right: "C" for call, "P" for put
            exchange: Exchange (default: "SMART")
            currency: Currency (default: "USD")
            multiplier: Contract multiplier (default: "100")
            trading_class: Trading class (optional, for ambiguous contracts)

        Returns:
            Configured Contract object
        """
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "OPT"
        contract.exchange = exchange
        contract.currency = currency
        contract.lastTradeDateOrContractMonth = expiry
        contract.strike = strike
        contract.right = right
        contract.multiplier = multiplier
        if trading_class:
            contract.tradingClass = trading_class
        return contract

    @staticmethod
    def option_by_local_symbol(local_symbol: str, exchange: str,
                                currency: str = "USD") -> Contract:
        """
        Create an option contract using its local symbol.

        Args:
            local_symbol: Exchange-specific option symbol
            exchange: Exchange where the option trades
            currency: Currency (default: "USD")

        Returns:
            Configured Contract object
        """
        contract = Contract()
        contract.localSymbol = local_symbol
        contract.secType = "OPT"
        contract.exchange = exchange
        contract.currency = currency
        return contract

    @staticmethod
    def option_chain_query(symbol: str, exchange: str = "SMART",
                           currency: str = "USD") -> Contract:
        """
        Create an ambiguous option contract for querying the full option chain.

        Use with reqContractDetails() to get all options for an underlying.

        Args:
            symbol: Underlying symbol
            exchange: Exchange (default: "SMART")
            currency: Currency (default: "USD")

        Returns:
            Ambiguous Contract for chain queries
        """
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "OPT"
        contract.exchange = exchange
        contract.currency = currency
        return contract

    # =========================================================================
    # Futures Contracts
    # =========================================================================

    @staticmethod
    def future(symbol: str, expiry: str, exchange: str,
               currency: str = "USD", multiplier: str = "") -> Contract:
        """
        Create a futures contract.

        Args:
            symbol: Futures symbol (e.g., "ES", "NQ", "GC")
            expiry: Expiration month (YYYYMM) or date (YYYYMMDD)
            exchange: Exchange (e.g., "CME", "NYMEX", "EUREX")
            currency: Currency (default: "USD")
            multiplier: Contract multiplier (optional)

        Returns:
            Configured Contract object
        """
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "FUT"
        contract.exchange = exchange
        contract.currency = currency
        contract.lastTradeDateOrContractMonth = expiry
        if multiplier:
            contract.multiplier = multiplier
        return contract

    @staticmethod
    def future_by_local_symbol(local_symbol: str, exchange: str,
                                currency: str = "USD") -> Contract:
        """
        Create a futures contract using its local symbol.

        Args:
            local_symbol: Exchange-specific futures symbol (e.g., "ESH4")
            exchange: Exchange where the future trades
            currency: Currency (default: "USD")

        Returns:
            Configured Contract object
        """
        contract = Contract()
        contract.localSymbol = local_symbol
        contract.secType = "FUT"
        contract.exchange = exchange
        contract.currency = currency
        return contract

    @staticmethod
    def continuous_future(symbol: str, exchange: str,
                          currency: str = "USD") -> Contract:
        """
        Create a continuous futures contract for charting/data.

        Args:
            symbol: Futures symbol (e.g., "ES", "GC")
            exchange: Exchange
            currency: Currency (default: "USD")

        Returns:
            Continuous futures Contract
        """
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "CONTFUT"
        contract.exchange = exchange
        contract.currency = currency
        return contract

    # =========================================================================
    # Forex Contracts
    # =========================================================================

    @staticmethod
    def forex(base_currency: str, quote_currency: str,
              exchange: str = "IDEALPRO") -> Contract:
        """
        Create a forex (CASH) contract.

        Args:
            base_currency: Base currency (e.g., "EUR", "GBP")
            quote_currency: Quote currency (e.g., "USD", "JPY")
            exchange: Exchange (default: "IDEALPRO")

        Returns:
            Configured Contract object
        """
        contract = Contract()
        contract.symbol = base_currency
        contract.secType = "CASH"
        contract.currency = quote_currency
        contract.exchange = exchange
        return contract

    # =========================================================================
    # Index Contracts
    # =========================================================================

    @staticmethod
    def index(symbol: str, exchange: str, currency: str = "USD") -> Contract:
        """
        Create an index contract.

        Args:
            symbol: Index symbol (e.g., "SPX", "NDX", "DAX")
            exchange: Exchange (e.g., "CBOE", "EUREX")
            currency: Currency (default: "USD")

        Returns:
            Configured Contract object
        """
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "IND"
        contract.currency = currency
        contract.exchange = exchange
        return contract

    # =========================================================================
    # Bond Contracts
    # =========================================================================

    @staticmethod
    def bond_by_cusip(cusip: str, exchange: str = "SMART",
                       currency: str = "USD") -> Contract:
        """
        Create a bond contract using CUSIP.

        Args:
            cusip: CUSIP identifier
            exchange: Exchange (default: "SMART")
            currency: Currency (default: "USD")

        Returns:
            Configured Contract object
        """
        contract = Contract()
        contract.symbol = cusip
        contract.secType = "BOND"
        contract.exchange = exchange
        contract.currency = currency
        return contract

    @staticmethod
    def bond_by_conid(con_id: int, exchange: str = "SMART") -> Contract:
        """
        Create a bond contract using contract ID.

        Args:
            con_id: IB contract ID
            exchange: Exchange (default: "SMART")

        Returns:
            Configured Contract object
        """
        contract = Contract()
        contract.conId = con_id
        contract.exchange = exchange
        return contract

    # =========================================================================
    # Other Instrument Types
    # =========================================================================

    @staticmethod
    def cfd(symbol: str, sec_type: str = "STK", exchange: str = "SMART",
            currency: str = "USD") -> Contract:
        """
        Create a CFD contract.

        Args:
            symbol: Underlying symbol
            sec_type: Underlying type ("STK" for stock CFD, etc.)
            exchange: Exchange (default: "SMART")
            currency: Currency (default: "USD")

        Returns:
            Configured Contract object
        """
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "CFD"
        contract.exchange = exchange
        contract.currency = currency
        return contract

    @staticmethod
    def commodity(symbol: str, exchange: str = "SMART",
                  currency: str = "USD") -> Contract:
        """
        Create a commodity contract (e.g., precious metals).

        Args:
            symbol: Commodity symbol (e.g., "XAUUSD" for gold)
            exchange: Exchange (default: "SMART")
            currency: Currency (default: "USD")

        Returns:
            Configured Contract object
        """
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "CMDTY"
        contract.exchange = exchange
        contract.currency = currency
        return contract

    @staticmethod
    def crypto(symbol: str, exchange: str = "PAXOS",
               currency: str = "USD") -> Contract:
        """
        Create a cryptocurrency contract.

        Args:
            symbol: Crypto symbol (e.g., "BTC", "ETH")
            exchange: Exchange (default: "PAXOS")
            currency: Quote currency (default: "USD")

        Returns:
            Configured Contract object
        """
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "CRYPTO"
        contract.exchange = exchange
        contract.currency = currency
        return contract

    @staticmethod
    def mutual_fund(symbol: str, exchange: str = "FUNDSERV",
                    currency: str = "USD") -> Contract:
        """
        Create a mutual fund contract.

        Args:
            symbol: Fund symbol
            exchange: Exchange (default: "FUNDSERV")
            currency: Currency (default: "USD")

        Returns:
            Configured Contract object
        """
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "FUND"
        contract.exchange = exchange
        contract.currency = currency
        return contract

    @staticmethod
    def warrant(symbol: str, expiry: str, strike: float, right: str,
                exchange: str, currency: str = "EUR",
                multiplier: str = "0.01") -> Contract:
        """
        Create a warrant contract.

        Args:
            symbol: Underlying symbol
            expiry: Expiration date (YYYYMMDD)
            strike: Strike price
            right: "C" for call, "P" for put
            exchange: Exchange
            currency: Currency (default: "EUR")
            multiplier: Contract multiplier (default: "0.01")

        Returns:
            Configured Contract object
        """
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "WAR"
        contract.exchange = exchange
        contract.currency = currency
        contract.lastTradeDateOrContractMonth = expiry
        contract.strike = strike
        contract.right = right
        contract.multiplier = multiplier
        return contract

    @staticmethod
    def futures_on_options(symbol: str, expiry: str, strike: float, right: str,
                           exchange: str, currency: str = "USD",
                           multiplier: str = "") -> Contract:
        """
        Create a futures option (FOP) contract.

        Args:
            symbol: Underlying futures symbol
            expiry: Expiration date (YYYYMMDD)
            strike: Strike price
            right: "C" for call, "P" for put
            exchange: Exchange
            currency: Currency (default: "USD")
            multiplier: Contract multiplier (optional)

        Returns:
            Configured Contract object
        """
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "FOP"
        contract.exchange = exchange
        contract.currency = currency
        contract.lastTradeDateOrContractMonth = expiry
        contract.strike = strike
        contract.right = right
        if multiplier:
            contract.multiplier = multiplier
        return contract

    # =========================================================================
    # Contract by Identifier
    # =========================================================================

    @staticmethod
    def by_conid(con_id: int, sec_type: str = "", exchange: str = "") -> Contract:
        """
        Create a contract using IB contract ID.

        Note: When using conId, provide minimal other attributes to avoid conflicts.

        Args:
            con_id: IB contract ID
            sec_type: Security type (optional)
            exchange: Exchange (optional)

        Returns:
            Configured Contract object
        """
        contract = Contract()
        contract.conId = con_id
        if sec_type:
            contract.secType = sec_type
        if exchange:
            contract.exchange = exchange
        return contract

    @staticmethod
    def by_isin(isin: str, exchange: str = "SMART", currency: str = "USD",
                sec_type: str = "STK") -> Contract:
        """
        Create a contract using ISIN.

        Args:
            isin: ISIN identifier
            exchange: Exchange (default: "SMART")
            currency: Currency (default: "USD")
            sec_type: Security type (default: "STK")

        Returns:
            Configured Contract object
        """
        contract = Contract()
        contract.secIdType = "ISIN"
        contract.secId = isin
        contract.exchange = exchange
        contract.currency = currency
        contract.secType = sec_type
        return contract

    @staticmethod
    def by_figi(figi: str, exchange: str = "SMART") -> Contract:
        """
        Create a contract using FIGI.

        Args:
            figi: FIGI identifier
            exchange: Exchange (default: "SMART")

        Returns:
            Configured Contract object
        """
        contract = Contract()
        contract.secIdType = "FIGI"
        contract.secId = figi
        contract.exchange = exchange
        return contract

    # =========================================================================
    # Combination/Spread Contracts
    # =========================================================================

    @staticmethod
    def combo(symbol: str, legs: List[ComboLeg], exchange: str = "SMART",
              currency: str = "USD") -> Contract:
        """
        Create a combination (spread) contract.

        Args:
            symbol: Combined symbol (e.g., "AAPL,MSFT" or underlying)
            legs: List of ComboLeg objects defining the spread
            exchange: Exchange (default: "SMART")
            currency: Currency (default: "USD")

        Returns:
            Configured Contract object
        """
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "BAG"
        contract.currency = currency
        contract.exchange = exchange
        contract.comboLegs = legs
        return contract

    @staticmethod
    def create_combo_leg(con_id: int, ratio: int, action: str,
                         exchange: str) -> ComboLeg:
        """
        Create a combo leg for spread orders.

        Args:
            con_id: Contract ID of the leg
            ratio: Ratio for the leg
            action: "BUY" or "SELL"
            exchange: Exchange for execution

        Returns:
            Configured ComboLeg object
        """
        leg = ComboLeg()
        leg.conId = con_id
        leg.ratio = ratio
        leg.action = action
        leg.exchange = exchange
        return leg

    @staticmethod
    def stock_spread(leg1_conid: int, leg1_action: str,
                     leg2_conid: int, leg2_action: str,
                     symbol: str = "", exchange: str = "SMART",
                     currency: str = "USD") -> Contract:
        """
        Create a two-leg stock spread (pair trade).

        Args:
            leg1_conid: Contract ID of first leg
            leg1_action: "BUY" or "SELL" for first leg
            leg2_conid: Contract ID of second leg
            leg2_action: "BUY" or "SELL" for second leg
            symbol: Combined symbol (optional)
            exchange: Exchange (default: "SMART")
            currency: Currency (default: "USD")

        Returns:
            Configured spread Contract
        """
        leg1 = ContractBuilder.create_combo_leg(leg1_conid, 1, leg1_action, exchange)
        leg2 = ContractBuilder.create_combo_leg(leg2_conid, 1, leg2_action, exchange)
        return ContractBuilder.combo(symbol, [leg1, leg2], exchange, currency)

    @staticmethod
    def option_spread(leg1_conid: int, leg1_action: str,
                      leg2_conid: int, leg2_action: str,
                      symbol: str, exchange: str,
                      currency: str = "USD") -> Contract:
        """
        Create a two-leg option spread.

        Args:
            leg1_conid: Contract ID of first option
            leg1_action: "BUY" or "SELL" for first leg
            leg2_conid: Contract ID of second option
            leg2_action: "BUY" or "SELL" for second leg
            symbol: Underlying symbol
            exchange: Exchange
            currency: Currency (default: "USD")

        Returns:
            Configured spread Contract
        """
        leg1 = ContractBuilder.create_combo_leg(leg1_conid, 1, leg1_action, exchange)
        leg2 = ContractBuilder.create_combo_leg(leg2_conid, 1, leg2_action, exchange)
        return ContractBuilder.combo(symbol, [leg1, leg2], exchange, currency)

    @staticmethod
    def futures_spread(leg1_conid: int, leg1_action: str,
                       leg2_conid: int, leg2_action: str,
                       symbol: str, exchange: str,
                       currency: str = "USD") -> Contract:
        """
        Create a two-leg futures calendar spread.

        Args:
            leg1_conid: Contract ID of near-month future
            leg1_action: "BUY" or "SELL" for first leg
            leg2_conid: Contract ID of far-month future
            leg2_action: "BUY" or "SELL" for second leg
            symbol: Futures symbol
            exchange: Exchange
            currency: Currency (default: "USD")

        Returns:
            Configured spread Contract
        """
        leg1 = ContractBuilder.create_combo_leg(leg1_conid, 1, leg1_action, exchange)
        leg2 = ContractBuilder.create_combo_leg(leg2_conid, 1, leg2_action, exchange)
        return ContractBuilder.combo(symbol, [leg1, leg2], exchange, currency)

    # =========================================================================
    # News Contracts
    # =========================================================================

    @staticmethod
    def news_feed(provider_code: str, exchange: str = "") -> Contract:
        """
        Create a news feed contract.

        Args:
            provider_code: News provider code (e.g., "BRF", "BZ", "FLY")
            exchange: Exchange (default: same as provider_code)

        Returns:
            Configured Contract object
        """
        contract = Contract()
        contract.secType = "NEWS"
        contract.exchange = exchange or provider_code
        return contract

    @staticmethod
    def broadtape_news(provider: str, feed: str) -> Contract:
        """
        Create a broadtape news feed contract.

        Args:
            provider: News provider (e.g., "BRF", "BZ", "FLY")
            feed: Feed identifier (e.g., "BRF_ALL", "BZ_ALL")

        Returns:
            Configured Contract object
        """
        contract = Contract()
        contract.symbol = f"{provider}:{feed}"
        contract.secType = "NEWS"
        contract.exchange = provider
        return contract

    # =========================================================================
    # Algo Venue Contracts
    # =========================================================================

    @staticmethod
    def jefferies_stock(symbol: str, currency: str = "USD") -> Contract:
        """Create a stock contract for Jefferies algo routing."""
        contract = ContractBuilder.stock(symbol, "JEFFALGO", currency)
        return contract

    @staticmethod
    def csfb_stock(symbol: str, currency: str = "USD") -> Contract:
        """Create a stock contract for CSFB algo routing."""
        contract = ContractBuilder.stock(symbol, "CSFBALGO", currency)
        return contract

    @staticmethod
    def ibkrats_stock(symbol: str, currency: str = "USD") -> Contract:
        """Create a stock contract for IBKRATS routing."""
        contract = ContractBuilder.stock(symbol, "IBKRATS", currency)
        return contract
