"""
rate_limiter.py - Rate limiting for IB order placement

Implements token bucket rate limiting to comply with IB's API rate limits.
Default conservative limit: 10 orders per second.

Usage:
    from rate_limiter import OrderRateLimiter

    limiter = OrderRateLimiter(orders_per_second=10.0)

    # In order execution loop:
    if limiter.acquire(timeout=5.0):
        execute_order()
    else:
        handle_rate_limit_exceeded()
"""

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class RateLimiterConfig:
    """Configuration for token bucket rate limiter"""

    max_rate: float = 10.0  # Maximum operations per second
    bucket_size: int = 10  # Burst capacity (max tokens)
    refill_interval: float = 0.1  # Seconds between refill checks


@dataclass
class RateLimiterStats:
    """Statistics for rate limiter"""

    requests_allowed: int = 0
    requests_delayed: int = 0
    requests_rejected: int = 0
    total_delay_ms: float = 0.0
    last_request_time: Optional[str] = None
    started_at: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        avg_delay = (
            self.total_delay_ms / self.requests_delayed
            if self.requests_delayed > 0
            else 0.0
        )
        return {
            "requests_allowed": self.requests_allowed,
            "requests_delayed": self.requests_delayed,
            "requests_rejected": self.requests_rejected,
            "total_delay_ms": round(self.total_delay_ms, 2),
            "avg_delay_ms": round(avg_delay, 2),
            "last_request_time": self.last_request_time,
            "started_at": self.started_at,
        }


class RateLimiter:
    """
    Thread-safe token bucket rate limiter.

    The token bucket algorithm allows bursts up to bucket_size while
    maintaining an average rate of max_rate operations per second.

    Tokens are continuously added at a rate of max_rate per second,
    up to a maximum of bucket_size tokens.
    """

    def __init__(self, config: Optional[RateLimiterConfig] = None):
        """
        Initialize rate limiter.

        Args:
            config: Rate limiter configuration (uses defaults if None)
        """
        self.config = config or RateLimiterConfig()
        self._tokens: float = float(self.config.bucket_size)
        self._last_refill: float = time.monotonic()
        self._lock = threading.Lock()
        self._stats = RateLimiterStats(started_at=datetime.now().isoformat())

    def _refill(self) -> None:
        """Refill tokens based on elapsed time since last refill"""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._last_refill = now

        # Add tokens based on elapsed time
        tokens_to_add = elapsed * self.config.max_rate
        self._tokens = min(self._tokens + tokens_to_add, self.config.bucket_size)

    def try_acquire(self) -> bool:
        """
        Try to acquire a token without blocking.

        Returns:
            True if token acquired, False if not available
        """
        with self._lock:
            self._refill()

            if self._tokens >= 1.0:
                self._tokens -= 1.0
                self._stats.requests_allowed += 1
                self._stats.last_request_time = datetime.now().isoformat()
                return True

            return False

    def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Acquire a token for a rate-limited operation.

        Args:
            blocking: If True, block until token available
            timeout: Maximum time to wait in seconds (None = wait forever)

        Returns:
            True if token acquired, False if timeout or non-blocking and unavailable
        """
        if not blocking:
            return self.try_acquire()

        start_time = time.monotonic()
        waited = False

        while True:
            with self._lock:
                self._refill()

                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    self._stats.requests_allowed += 1
                    self._stats.last_request_time = datetime.now().isoformat()

                    if waited:
                        delay_ms = (time.monotonic() - start_time) * 1000
                        self._stats.requests_delayed += 1
                        self._stats.total_delay_ms += delay_ms

                    return True

                # Calculate wait time for next token
                tokens_needed = 1.0 - self._tokens
                wait_time = tokens_needed / self.config.max_rate

            # Check timeout
            if timeout is not None:
                elapsed = time.monotonic() - start_time
                if elapsed >= timeout:
                    with self._lock:
                        self._stats.requests_rejected += 1
                    return False
                # Don't wait longer than remaining timeout
                wait_time = min(wait_time, timeout - elapsed)

            # Wait for tokens to refill
            waited = True
            time.sleep(min(wait_time, self.config.refill_interval))

    @property
    def available_tokens(self) -> float:
        """Get current available tokens (may be stale)"""
        with self._lock:
            self._refill()
            return self._tokens

    @property
    def stats(self) -> Dict[str, Any]:
        """Get rate limiter statistics"""
        with self._lock:
            return self._stats.to_dict()

    def reset_stats(self) -> None:
        """Reset statistics counters"""
        with self._lock:
            self._stats = RateLimiterStats(started_at=datetime.now().isoformat())


class OrderRateLimiter:
    """
    Rate limiter specialized for IB order placement.

    Wraps the generic RateLimiter with order-specific tracking
    and IB-compliant defaults.

    IB Rate Limits (reference):
    - 50 messages per second (general API limit)
    - Order-specific limits vary by account type

    Default conservative limit: 10 orders per second
    """

    def __init__(
        self,
        orders_per_second: float = 10.0,
        burst_size: int = 10,
    ):
        """
        Initialize order rate limiter.

        Args:
            orders_per_second: Maximum orders per second (default: 10)
            burst_size: Maximum burst capacity (default: 10)
        """
        self._limiter = RateLimiter(
            RateLimiterConfig(
                max_rate=orders_per_second,
                bucket_size=burst_size,
            )
        )

        # Order-specific statistics
        self._order_stats = {
            "orders_submitted": 0,
            "orders_rate_limited": 0,
            "orders_rejected": 0,
        }
        self._lock = threading.Lock()

        logger.info(
            f"Order rate limiter initialized: {orders_per_second} orders/sec, "
            f"burst size {burst_size}"
        )

    def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Acquire permission to submit an order.

        Args:
            blocking: If True, wait for rate limit clearance
            timeout: Maximum time to wait in seconds

        Returns:
            True if order can be submitted, False if rate limited
        """
        result = self._limiter.acquire(blocking=blocking, timeout=timeout)

        with self._lock:
            if result:
                self._order_stats["orders_submitted"] += 1
                # Check if we had to wait (was rate limited)
                if self._limiter._stats.requests_delayed > 0:
                    prev_delayed = getattr(self, "_prev_delayed", 0)
                    if self._limiter._stats.requests_delayed > prev_delayed:
                        self._order_stats["orders_rate_limited"] += 1
                    self._prev_delayed = self._limiter._stats.requests_delayed
            else:
                self._order_stats["orders_rejected"] += 1

        return result

    def try_acquire(self) -> bool:
        """
        Try to acquire order permission without blocking.

        Returns:
            True if order can be submitted immediately
        """
        return self.acquire(blocking=False)

    @property
    def available_capacity(self) -> float:
        """Get available order capacity (tokens)"""
        return self._limiter.available_tokens

    @property
    def stats(self) -> Dict[str, Any]:
        """Get combined statistics"""
        with self._lock:
            limiter_stats = self._limiter.stats
            return {
                **limiter_stats,
                **self._order_stats,
                "orders_per_second_limit": self._limiter.config.max_rate,
                "burst_size": self._limiter.config.bucket_size,
            }

    def reset_stats(self) -> None:
        """Reset all statistics"""
        self._limiter.reset_stats()
        with self._lock:
            self._order_stats = {
                "orders_submitted": 0,
                "orders_rate_limited": 0,
                "orders_rejected": 0,
            }
            self._prev_delayed = 0

    @property
    def orders_per_second(self) -> float:
        """Get configured orders per second limit"""
        return self._limiter.config.max_rate

    @property
    def burst_size(self) -> int:
        """Get configured burst size"""
        return self._limiter.config.bucket_size
