"""
Tests for rate_limiter.py - Token bucket rate limiting
"""

import pytest
import time
import threading
from datetime import datetime

from rate_limiter import (
    RateLimiter,
    RateLimiterConfig,
    RateLimiterStats,
    OrderRateLimiter,
)


class TestRateLimiterConfig:
    """Tests for RateLimiterConfig dataclass"""

    def test_default_values(self):
        """Test default configuration values"""
        config = RateLimiterConfig()

        assert config.max_rate == 10.0
        assert config.bucket_size == 10
        assert config.refill_interval == 0.1

    def test_custom_values(self):
        """Test custom configuration"""
        config = RateLimiterConfig(
            max_rate=50.0,
            bucket_size=20,
            refill_interval=0.05,
        )

        assert config.max_rate == 50.0
        assert config.bucket_size == 20
        assert config.refill_interval == 0.05


class TestRateLimiterStats:
    """Tests for RateLimiterStats dataclass"""

    def test_default_values(self):
        """Test default stats values"""
        stats = RateLimiterStats()

        assert stats.requests_allowed == 0
        assert stats.requests_delayed == 0
        assert stats.requests_rejected == 0
        assert stats.total_delay_ms == 0.0

    def test_to_dict(self):
        """Test stats serialization"""
        stats = RateLimiterStats(
            requests_allowed=100,
            requests_delayed=10,
            requests_rejected=5,
            total_delay_ms=500.0,
            started_at="2024-01-01T00:00:00",
        )

        d = stats.to_dict()

        assert d["requests_allowed"] == 100
        assert d["requests_delayed"] == 10
        assert d["requests_rejected"] == 5
        assert d["total_delay_ms"] == 500.0
        assert d["avg_delay_ms"] == 50.0  # 500 / 10

    def test_avg_delay_zero_delayed(self):
        """Test avg delay when no requests delayed"""
        stats = RateLimiterStats(requests_delayed=0)
        d = stats.to_dict()

        assert d["avg_delay_ms"] == 0.0


class TestRateLimiter:
    """Tests for RateLimiter class"""

    def test_initialization(self):
        """Test rate limiter initialization"""
        limiter = RateLimiter()

        assert limiter.available_tokens == 10.0
        assert limiter.config.max_rate == 10.0

    def test_custom_config(self):
        """Test rate limiter with custom config"""
        config = RateLimiterConfig(max_rate=5.0, bucket_size=3)
        limiter = RateLimiter(config)

        assert limiter.available_tokens == 3.0
        assert limiter.config.max_rate == 5.0

    def test_try_acquire_success(self):
        """Test non-blocking acquire success"""
        limiter = RateLimiter()

        result = limiter.try_acquire()

        assert result is True
        assert limiter.available_tokens == pytest.approx(9.0, abs=0.01)

    def test_try_acquire_exhausts_tokens(self):
        """Test acquiring all tokens"""
        config = RateLimiterConfig(max_rate=100.0, bucket_size=3)
        limiter = RateLimiter(config)

        # Acquire all tokens
        assert limiter.try_acquire() is True
        assert limiter.try_acquire() is True
        assert limiter.try_acquire() is True

        # Should fail - no tokens left
        assert limiter.try_acquire() is False

    def test_token_refill(self):
        """Test token refill over time"""
        config = RateLimiterConfig(max_rate=100.0, bucket_size=5)
        limiter = RateLimiter(config)

        # Exhaust all tokens
        for _ in range(5):
            limiter.try_acquire()

        assert limiter.available_tokens < 1.0

        # Wait for refill (at 100/sec, should get ~10 tokens in 100ms)
        time.sleep(0.1)

        # Should have refilled
        assert limiter.available_tokens >= 1.0

    def test_acquire_blocking(self):
        """Test blocking acquire waits for token"""
        config = RateLimiterConfig(max_rate=100.0, bucket_size=1)
        limiter = RateLimiter(config)

        # Take the only token
        limiter.try_acquire()

        # Blocking acquire should wait and succeed
        start = time.monotonic()
        result = limiter.acquire(blocking=True, timeout=1.0)
        elapsed = time.monotonic() - start

        assert result is True
        assert elapsed >= 0.005  # Should have waited some time

    def test_acquire_timeout(self):
        """Test acquire respects timeout"""
        config = RateLimiterConfig(max_rate=1.0, bucket_size=1)  # 1 per second
        limiter = RateLimiter(config)

        # Take the only token
        limiter.try_acquire()

        # Should timeout before getting another token
        start = time.monotonic()
        result = limiter.acquire(blocking=True, timeout=0.1)
        elapsed = time.monotonic() - start

        assert result is False
        assert elapsed >= 0.1
        assert elapsed < 0.5  # Should not wait much longer than timeout

    def test_acquire_non_blocking(self):
        """Test non-blocking acquire with parameter"""
        config = RateLimiterConfig(max_rate=100.0, bucket_size=1)
        limiter = RateLimiter(config)

        limiter.try_acquire()

        result = limiter.acquire(blocking=False)

        assert result is False

    def test_stats_tracking(self):
        """Test statistics are tracked correctly"""
        config = RateLimiterConfig(max_rate=100.0, bucket_size=5)
        limiter = RateLimiter(config)

        # Successful acquires
        for _ in range(3):
            limiter.try_acquire()

        stats = limiter.stats

        assert stats["requests_allowed"] == 3
        assert stats["started_at"] is not None

    def test_stats_rejected_tracking(self):
        """Test rejected requests are tracked"""
        config = RateLimiterConfig(max_rate=1.0, bucket_size=1)
        limiter = RateLimiter(config)

        limiter.try_acquire()

        # This should be rejected (timeout immediately)
        limiter.acquire(blocking=True, timeout=0.01)

        stats = limiter.stats

        assert stats["requests_rejected"] == 1

    def test_reset_stats(self):
        """Test stats reset"""
        limiter = RateLimiter()

        for _ in range(5):
            limiter.try_acquire()

        limiter.reset_stats()

        stats = limiter.stats
        assert stats["requests_allowed"] == 0

    def test_bucket_size_limit(self):
        """Test tokens don't exceed bucket size"""
        config = RateLimiterConfig(max_rate=1000.0, bucket_size=5)
        limiter = RateLimiter(config)

        # Wait to let tokens accumulate
        time.sleep(0.1)

        # Should still be capped at bucket size
        assert limiter.available_tokens <= 5.0

    def test_thread_safety(self):
        """Test concurrent access to rate limiter"""
        config = RateLimiterConfig(max_rate=1000.0, bucket_size=100)
        limiter = RateLimiter(config)
        results = []
        errors = []

        def acquire_tokens(count):
            try:
                for _ in range(count):
                    results.append(limiter.try_acquire())
            except Exception as e:
                errors.append(e)

        # Launch multiple threads
        threads = [
            threading.Thread(target=acquire_tokens, args=(20,))
            for _ in range(5)
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(results) == 100

        # Should have exactly bucket_size successes initially
        # (plus any that refilled during execution)
        successes = sum(1 for r in results if r)
        assert successes >= 100  # All should succeed at 1000/sec


class TestOrderRateLimiter:
    """Tests for OrderRateLimiter class"""

    def test_initialization(self):
        """Test order rate limiter initialization"""
        limiter = OrderRateLimiter()

        assert limiter.orders_per_second == 10.0
        assert limiter.burst_size == 10

    def test_custom_limits(self):
        """Test custom order limits"""
        limiter = OrderRateLimiter(
            orders_per_second=25.0,
            burst_size=15,
        )

        assert limiter.orders_per_second == 25.0
        assert limiter.burst_size == 15

    def test_acquire_success(self):
        """Test successful order acquisition"""
        limiter = OrderRateLimiter(orders_per_second=100.0, burst_size=10)

        result = limiter.acquire()

        assert result is True
        assert limiter.stats["orders_submitted"] == 1

    def test_try_acquire(self):
        """Test non-blocking order acquisition"""
        limiter = OrderRateLimiter(orders_per_second=100.0, burst_size=2)

        assert limiter.try_acquire() is True
        assert limiter.try_acquire() is True
        assert limiter.try_acquire() is False  # Exhausted

    def test_available_capacity(self):
        """Test available capacity tracking"""
        limiter = OrderRateLimiter(orders_per_second=100.0, burst_size=5)

        initial = limiter.available_capacity
        limiter.acquire()
        after = limiter.available_capacity

        assert initial == pytest.approx(5.0, abs=0.01)
        assert after == pytest.approx(4.0, abs=0.01)

    def test_stats(self):
        """Test order statistics"""
        limiter = OrderRateLimiter(orders_per_second=100.0, burst_size=10)

        for _ in range(5):
            limiter.acquire()

        stats = limiter.stats

        assert stats["orders_submitted"] == 5
        assert stats["orders_per_second_limit"] == 100.0
        assert stats["burst_size"] == 10

    def test_stats_rejected(self):
        """Test rejected order tracking"""
        limiter = OrderRateLimiter(orders_per_second=1.0, burst_size=1)

        limiter.acquire()  # Take the only token
        limiter.acquire(blocking=True, timeout=0.01)  # Should be rejected

        stats = limiter.stats

        assert stats["orders_rejected"] == 1

    def test_reset_stats(self):
        """Test stats reset"""
        limiter = OrderRateLimiter(orders_per_second=100.0, burst_size=10)

        for _ in range(5):
            limiter.acquire()

        limiter.reset_stats()

        stats = limiter.stats
        assert stats["orders_submitted"] == 0
        assert stats["requests_allowed"] == 0

    def test_rate_limiting_effective(self):
        """Test that rate limiting actually limits rate"""
        limiter = OrderRateLimiter(orders_per_second=50.0, burst_size=5)

        # Submit 10 orders with blocking
        start = time.monotonic()
        for _ in range(10):
            limiter.acquire(blocking=True, timeout=1.0)
        elapsed = time.monotonic() - start

        # At 50/sec with burst of 5, submitting 10 orders should take
        # at least ~0.1 seconds (5 burst + 5 at 50/sec = 5/50 = 0.1s)
        assert elapsed >= 0.08  # Allow some tolerance

    def test_burst_handling(self):
        """Test burst capacity allows initial burst"""
        limiter = OrderRateLimiter(orders_per_second=10.0, burst_size=5)

        # Should be able to acquire 5 instantly (burst)
        start = time.monotonic()
        for _ in range(5):
            assert limiter.try_acquire() is True
        elapsed = time.monotonic() - start

        # Burst should be nearly instant
        assert elapsed < 0.01

        # 6th should fail without waiting
        assert limiter.try_acquire() is False


class TestRateLimiterIntegration:
    """Integration tests for rate limiting"""

    def test_sustained_rate(self):
        """Test sustained rate over longer period"""
        limiter = OrderRateLimiter(orders_per_second=100.0, burst_size=10)

        # Submit 50 orders
        start = time.monotonic()
        for _ in range(50):
            limiter.acquire(blocking=True, timeout=5.0)
        elapsed = time.monotonic() - start

        # At 100/sec, 50 orders should take ~0.4-0.5 seconds
        # (10 burst + 40 at 100/sec = 0.4s minimum)
        assert elapsed >= 0.35
        assert elapsed < 1.0

    def test_multiple_limiters_independent(self):
        """Test multiple limiters operate independently"""
        limiter1 = OrderRateLimiter(orders_per_second=100.0, burst_size=5)
        limiter2 = OrderRateLimiter(orders_per_second=100.0, burst_size=5)

        # Exhaust limiter1
        for _ in range(5):
            limiter1.try_acquire()

        # limiter2 should still have capacity
        assert limiter1.try_acquire() is False
        assert limiter2.try_acquire() is True

    def test_concurrent_order_submission(self):
        """Test concurrent order submission through rate limiter"""
        limiter = OrderRateLimiter(orders_per_second=100.0, burst_size=20)
        results = []
        errors = []

        def submit_orders(count):
            try:
                for _ in range(count):
                    result = limiter.acquire(blocking=True, timeout=5.0)
                    results.append(result)
            except Exception as e:
                errors.append(e)

        # 5 threads each submitting 10 orders
        threads = [
            threading.Thread(target=submit_orders, args=(10,))
            for _ in range(5)
        ]

        start = time.monotonic()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        elapsed = time.monotonic() - start

        assert len(errors) == 0
        assert len(results) == 50
        assert all(results)  # All should succeed
        assert limiter.stats["orders_submitted"] == 50

        # At 100/sec with burst 20, 50 orders should take ~0.3s
        assert elapsed >= 0.25
