"""
algo_params.py - Algorithm parameter helpers

Based on the official IB API Testbed patterns. Provides methods to configure
algorithmic order parameters for TWAP, VWAP, Adaptive, and other IB algos.
"""

from ibapi.order import Order
from ibapi.tag_value import TagValue


class AlgoParams:
    """Helper methods for configuring IB algorithmic orders."""

    # =========================================================================
    # Time-Weighted Algorithms
    # =========================================================================

    @staticmethod
    def fill_twap(order: Order, strategy_type: str, start_time: str,
                  end_time: str, allow_past_end_time: bool = False) -> None:
        """
        Configure TWAP (Time-Weighted Average Price) algorithm.

        Args:
            order: Order to configure
            strategy_type: Strategy type parameter
            start_time: Start time (format: HHmmss or HH:mm:ss)
            end_time: End time (format: HHmmss or HH:mm:ss)
            allow_past_end_time: Allow execution after end time
        """
        order.algoStrategy = "Twap"
        order.algoParams = [
            TagValue("strategyType", strategy_type),
            TagValue("startTime", start_time),
            TagValue("endTime", end_time),
            TagValue("allowPastEndTime", int(allow_past_end_time)),
        ]

    @staticmethod
    def fill_vwap(order: Order, max_pct_vol: float, start_time: str,
                  end_time: str, allow_past_end_time: bool = False,
                  no_take_liq: bool = False) -> None:
        """
        Configure VWAP (Volume-Weighted Average Price) algorithm.

        Args:
            order: Order to configure
            max_pct_vol: Maximum percentage of volume (0.0-1.0)
            start_time: Start time (format: HHmmss or HH:mm:ss)
            end_time: End time (format: HHmmss or HH:mm:ss)
            allow_past_end_time: Allow execution after end time
            no_take_liq: Don't take liquidity
        """
        order.algoStrategy = "Vwap"
        order.algoParams = [
            TagValue("maxPctVol", max_pct_vol),
            TagValue("startTime", start_time),
            TagValue("endTime", end_time),
            TagValue("allowPastEndTime", int(allow_past_end_time)),
            TagValue("noTakeLiq", int(no_take_liq)),
        ]

    # =========================================================================
    # Volume-Based Algorithms
    # =========================================================================

    @staticmethod
    def fill_pct_vol(order: Order, pct_vol: float, start_time: str,
                     end_time: str, no_take_liq: bool = False) -> None:
        """
        Configure Percentage of Volume algorithm.

        Args:
            order: Order to configure
            pct_vol: Target percentage of volume (0.0-1.0)
            start_time: Start time
            end_time: End time
            no_take_liq: Don't take liquidity
        """
        order.algoStrategy = "PctVol"
        order.algoParams = [
            TagValue("pctVol", pct_vol),
            TagValue("startTime", start_time),
            TagValue("endTime", end_time),
            TagValue("noTakeLiq", int(no_take_liq)),
        ]

    @staticmethod
    def fill_price_variant_pct_vol(order: Order, pct_vol: float,
                                   delta_pct_vol: float, min_pct_vol_4px: float,
                                   max_pct_vol_4px: float, start_time: str,
                                   end_time: str, no_take_liq: bool = False) -> None:
        """Configure Price Variant Percentage of Volume algorithm."""
        order.algoStrategy = "PctVolPx"
        order.algoParams = [
            TagValue("pctVol", pct_vol),
            TagValue("deltaPctVol", delta_pct_vol),
            TagValue("minPctVol4Px", min_pct_vol_4px),
            TagValue("maxPctVol4Px", max_pct_vol_4px),
            TagValue("startTime", start_time),
            TagValue("endTime", end_time),
            TagValue("noTakeLiq", int(no_take_liq)),
        ]

    @staticmethod
    def fill_size_variant_pct_vol(order: Order, start_pct_vol: float,
                                  end_pct_vol: float, start_time: str,
                                  end_time: str, no_take_liq: bool = False) -> None:
        """Configure Size Variant Percentage of Volume algorithm."""
        order.algoStrategy = "PctVolSz"
        order.algoParams = [
            TagValue("startPctVol", start_pct_vol),
            TagValue("endPctVol", end_pct_vol),
            TagValue("startTime", start_time),
            TagValue("endTime", end_time),
            TagValue("noTakeLiq", int(no_take_liq)),
        ]

    @staticmethod
    def fill_time_variant_pct_vol(order: Order, start_pct_vol: float,
                                  end_pct_vol: float, start_time: str,
                                  end_time: str, no_take_liq: bool = False) -> None:
        """Configure Time Variant Percentage of Volume algorithm."""
        order.algoStrategy = "PctVolTm"
        order.algoParams = [
            TagValue("startPctVol", start_pct_vol),
            TagValue("endPctVol", end_pct_vol),
            TagValue("startTime", start_time),
            TagValue("endTime", end_time),
            TagValue("noTakeLiq", int(no_take_liq)),
        ]

    # =========================================================================
    # Arrival Price Algorithms
    # =========================================================================

    @staticmethod
    def fill_arrival_price(order: Order, max_pct_vol: float, risk_aversion: str,
                           start_time: str, end_time: str,
                           force_completion: bool = False,
                           allow_past_time: bool = False) -> None:
        """
        Configure Arrival Price algorithm.

        Args:
            order: Order to configure
            max_pct_vol: Maximum percentage of volume
            risk_aversion: "VeryLow", "Low", "Medium", "High", "VeryHigh"
            start_time: Start time
            end_time: End time
            force_completion: Force completion by end time
            allow_past_time: Allow execution after end time
        """
        order.algoStrategy = "ArrivalPx"
        order.algoParams = [
            TagValue("maxPctVol", max_pct_vol),
            TagValue("riskAversion", risk_aversion),
            TagValue("startTime", start_time),
            TagValue("endTime", end_time),
            TagValue("forceCompletion", int(force_completion)),
            TagValue("allowPastEndTime", int(allow_past_time)),
        ]

    # =========================================================================
    # Impact Algorithms
    # =========================================================================

    @staticmethod
    def fill_min_impact(order: Order, max_pct_vol: float) -> None:
        """
        Configure Minimum Impact algorithm.

        Args:
            order: Order to configure
            max_pct_vol: Maximum percentage of volume
        """
        order.algoStrategy = "MinImpact"
        order.algoParams = [
            TagValue("maxPctVol", max_pct_vol),
        ]

    @staticmethod
    def fill_balance_impact_risk(order: Order, max_pct_vol: float,
                                 risk_aversion: str,
                                 force_completion: bool = False) -> None:
        """
        Configure Balance Impact Risk algorithm.

        Args:
            order: Order to configure
            max_pct_vol: Maximum percentage of volume
            risk_aversion: "VeryLow", "Low", "Medium", "High", "VeryHigh"
            force_completion: Force completion
        """
        order.algoStrategy = "BalanceImpactRisk"
        order.algoParams = [
            TagValue("maxPctVol", max_pct_vol),
            TagValue("riskAversion", risk_aversion),
            TagValue("forceCompletion", int(force_completion)),
        ]

    # =========================================================================
    # Dark Pool / Execution Algorithms
    # =========================================================================

    @staticmethod
    def fill_dark_ice(order: Order, display_size: int, start_time: str,
                      end_time: str, allow_past_end_time: bool = False) -> None:
        """
        Configure DarkIce algorithm.

        Args:
            order: Order to configure
            display_size: Size to display
            start_time: Start time
            end_time: End time
            allow_past_end_time: Allow execution after end time
        """
        order.algoStrategy = "DarkIce"
        order.algoParams = [
            TagValue("displaySize", display_size),
            TagValue("startTime", start_time),
            TagValue("endTime", end_time),
            TagValue("allowPastEndTime", int(allow_past_end_time)),
        ]

    @staticmethod
    def fill_close_price(order: Order, max_pct_vol: float, risk_aversion: str,
                         start_time: str, force_completion: bool = False) -> None:
        """
        Configure Close Price algorithm.

        Args:
            order: Order to configure
            max_pct_vol: Maximum percentage of volume
            risk_aversion: "VeryLow", "Low", "Medium", "High", "VeryHigh"
            start_time: Start time
            force_completion: Force completion
        """
        order.algoStrategy = "ClosePx"
        order.algoParams = [
            TagValue("maxPctVol", max_pct_vol),
            TagValue("riskAversion", risk_aversion),
            TagValue("startTime", start_time),
            TagValue("forceCompletion", int(force_completion)),
        ]

    # =========================================================================
    # Adaptive Algorithm
    # =========================================================================

    @staticmethod
    def fill_adaptive(order: Order, priority: str = "Normal") -> None:
        """
        Configure Adaptive algorithm.

        Args:
            order: Order to configure
            priority: "Patient", "Normal", or "Urgent"
        """
        order.algoStrategy = "Adaptive"
        order.algoParams = [
            TagValue("adaptivePriority", priority),
        ]

    # =========================================================================
    # Accumulate/Distribute Algorithm
    # =========================================================================

    @staticmethod
    def fill_accumulate_distribute(order: Order, component_size: int,
                                   time_between_orders: int,
                                   randomize_time_20: bool = False,
                                   randomize_size_55: bool = False,
                                   give_up: int = 0,
                                   catch_up: bool = False,
                                   wait_for_fill: bool = False,
                                   start_time: str = "",
                                   end_time: str = "") -> None:
        """
        Configure Accumulate/Distribute algorithm.

        Args:
            order: Order to configure
            component_size: Size of each component order
            time_between_orders: Seconds between orders
            randomize_time_20: Randomize time by +/-20%
            randomize_size_55: Randomize size by +/-55%
            give_up: Give up after N seconds
            catch_up: Catch up on missed orders
            wait_for_fill: Wait for fill before next order
            start_time: Active time start
            end_time: Active time end
        """
        order.algoStrategy = "AD"
        order.algoParams = [
            TagValue("componentSize", component_size),
            TagValue("timeBetweenOrders", time_between_orders),
            TagValue("randomizeTime20", int(randomize_time_20)),
            TagValue("randomizeSize55", int(randomize_size_55)),
            TagValue("giveUp", give_up),
            TagValue("catchUp", int(catch_up)),
            TagValue("waitForFill", int(wait_for_fill)),
            TagValue("activeTimeStart", start_time),
            TagValue("activeTimeEnd", end_time),
        ]

    # =========================================================================
    # Scale Order Parameters
    # =========================================================================

    @staticmethod
    def fill_scale_params(order: Order, init_level_size: int,
                          subs_level_size: int, random_percent: bool,
                          price_increment: float, price_adjust_value: float = 0,
                          price_adjust_interval: int = 0,
                          profit_offset: float = 0,
                          auto_reset: bool = False,
                          init_position: int = 0,
                          init_fill_qty: int = 0) -> None:
        """
        Configure scale order parameters.

        Args:
            order: Order to configure
            init_level_size: Initial component size
            subs_level_size: Subsequent component size
            random_percent: Randomize size by +/-55%
            price_increment: Price increment per level
            price_adjust_value: Auto-adjust starting price by
            price_adjust_interval: Auto-adjust interval (seconds)
            profit_offset: Profit taking offset
            auto_reset: Restore size after profit
            init_position: Initial position
            init_fill_qty: Filled initial size
        """
        order.scaleInitLevelSize = init_level_size
        order.scaleSubsLevelSize = subs_level_size
        order.scaleRandomPercent = random_percent
        order.scalePriceIncrement = price_increment
        order.scalePriceAdjustValue = price_adjust_value
        order.scalePriceAdjustInterval = price_adjust_interval
        order.scaleProfitOffset = profit_offset
        order.scaleAutoReset = auto_reset
        order.scaleInitPosition = init_position
        order.scaleInitFillQty = init_fill_qty

    # =========================================================================
    # Third-Party Algo Providers
    # =========================================================================

    @staticmethod
    def fill_jefferies_vwap(order: Order, start_time: str, end_time: str,
                            relative_limit: float = 0, max_volume_rate: float = 0,
                            exclude_auctions: str = "",
                            trigger_price: float = 0, wow_price: float = 0,
                            min_fill_size: int = 0, wow_order_pct: float = 0,
                            wow_mode: str = "", is_buy_back: bool = False,
                            wow_reference: str = "") -> None:
        """
        Configure Jefferies VWAP algorithm.

        Note: Requires direct routing to "JEFFALGO" exchange.
        """
        order.algoStrategy = "VWAP"
        order.algoParams = [
            TagValue("StartTime", start_time),
            TagValue("EndTime", end_time),
            TagValue("RelativeLimit", relative_limit),
            TagValue("MaxVolumeRate", max_volume_rate),
            TagValue("ExcludeAuctions", exclude_auctions),
            TagValue("TriggerPrice", trigger_price),
            TagValue("WowPrice", wow_price),
            TagValue("MinFillSize", min_fill_size),
            TagValue("WowOrderPct", wow_order_pct),
            TagValue("WowMode", wow_mode),
            TagValue("IsBuyBack", int(is_buy_back)),
            TagValue("WowReference", wow_reference),
        ]

    @staticmethod
    def fill_csfb_inline(order: Order, start_time: str, end_time: str,
                         exec_style: str = "", min_percent: int = 0,
                         max_percent: int = 0, display_size: int = 0,
                         auction: str = "", block_finder: bool = False,
                         block_price: float = 0, min_block_size: int = 0,
                         max_block_size: int = 0, i_would_price: float = 0) -> None:
        """
        Configure CSFB Inline algorithm.

        Note: Requires direct routing to "CSFBALGO" exchange.
        """
        order.algoStrategy = "INLINE"
        order.algoParams = [
            TagValue("StartTime", start_time),
            TagValue("EndTime", end_time),
            TagValue("ExecStyle", exec_style),
            TagValue("MinPercent", min_percent),
            TagValue("MaxPercent", max_percent),
            TagValue("DisplaySize", display_size),
            TagValue("Auction", auction),
            TagValue("BlockFinder", int(block_finder)),
            TagValue("BlockPrice", block_price),
            TagValue("MinBlockSize", min_block_size),
            TagValue("MaxBlockSize", max_block_size),
            TagValue("IWouldPrice", i_would_price),
        ]

    @staticmethod
    def fill_qbalgo_strobe(order: Order, start_time: str, end_time: str,
                           benchmark: str = "", percent_volume: float = 0,
                           no_clean_up: bool = False) -> None:
        """
        Configure QB Algo Strobe algorithm.

        Note: Requires direct routing to "QBALGO" exchange.
        """
        order.algoStrategy = "STROBE"
        order.algoParams = [
            TagValue("StartTime", start_time),
            TagValue("EndTime", end_time),
            TagValue("Benchmark", benchmark),
            TagValue("PercentVolume", str(percent_volume)),
            TagValue("NoCleanUp", int(no_clean_up)),
        ]


# Convenience aliases
TWAP = AlgoParams.fill_twap
VWAP = AlgoParams.fill_vwap
Adaptive = AlgoParams.fill_adaptive
PctVol = AlgoParams.fill_pct_vol
ArrivalPrice = AlgoParams.fill_arrival_price
MinImpact = AlgoParams.fill_min_impact
DarkIce = AlgoParams.fill_dark_ice
ClosePx = AlgoParams.fill_close_price
