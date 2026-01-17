"""
Tests for message_bus.py - Pub/Sub message bus
"""

import pytest
import time
import threading
from datetime import datetime, timedelta

from ib.message_bus import (
    MessageBus,
    Message,
    MessageMetadata,
    ChannelInfo,
    Subscription,
)


class TestMessageMetadata:
    """Tests for MessageMetadata dataclass"""

    def test_create_metadata(self):
        """Test creating metadata"""
        ts = datetime.now()
        metadata = MessageMetadata(
            timestamp=ts,
            source_plugin="test_plugin",
            message_type="signal",
            sequence_number=42,
        )

        assert metadata.timestamp == ts
        assert metadata.source_plugin == "test_plugin"
        assert metadata.message_type == "signal"
        assert metadata.sequence_number == 42

    def test_to_dict(self):
        """Test metadata to_dict"""
        ts = datetime(2026, 1, 17, 10, 30, 0)
        metadata = MessageMetadata(
            timestamp=ts,
            source_plugin="test",
            message_type="data",
            sequence_number=1,
        )

        d = metadata.to_dict()
        assert d["source_plugin"] == "test"
        assert d["message_type"] == "data"
        assert d["sequence_number"] == 1
        assert "2026-01-17" in d["timestamp"]


class TestMessage:
    """Tests for Message dataclass"""

    def test_create_message(self):
        """Test creating a message"""
        metadata = MessageMetadata(
            timestamp=datetime.now(),
            source_plugin="pub",
            message_type="data",
            sequence_number=1,
        )
        message = Message(
            channel="test_channel",
            payload={"value": 42},
            metadata=metadata,
        )

        assert message.channel == "test_channel"
        assert message.payload["value"] == 42

    def test_to_dict(self):
        """Test message to_dict"""
        metadata = MessageMetadata(
            timestamp=datetime.now(),
            source_plugin="pub",
            message_type="signal",
            sequence_number=5,
        )
        message = Message(
            channel="signals",
            payload={"action": "BUY"},
            metadata=metadata,
        )

        d = message.to_dict()
        assert d["channel"] == "signals"
        assert d["payload"]["action"] == "BUY"
        assert d["metadata"]["source_plugin"] == "pub"


class TestChannelInfo:
    """Tests for ChannelInfo dataclass"""

    def test_create_channel_info(self):
        """Test creating channel info"""
        info = ChannelInfo(name="test_channel")

        assert info.name == "test_channel"
        assert info.description == ""
        assert info.publishers == set()
        assert info.subscribers == set()
        assert info.message_count == 0
        assert info.last_message_at is None

    def test_to_dict(self):
        """Test channel info to_dict"""
        info = ChannelInfo(
            name="test",
            description="Test channel",
            publishers={"pub1", "pub2"},
            subscribers={"sub1"},
            message_count=100,
            last_message_at=datetime(2026, 1, 17, 10, 0, 0),
        )

        d = info.to_dict()
        assert d["name"] == "test"
        assert d["description"] == "Test channel"
        assert "pub1" in d["publishers"]
        assert "sub1" in d["subscribers"]
        assert d["message_count"] == 100


class TestMessageBusBasic:
    """Basic MessageBus tests"""

    def test_create_bus(self):
        """Test creating a MessageBus"""
        bus = MessageBus()
        assert bus is not None

    def test_create_bus_with_history_limit(self):
        """Test creating bus with custom history limit"""
        bus = MessageBus(max_message_history=100)
        assert bus._max_history == 100

    def test_publish_creates_channel(self):
        """Test that publish auto-creates channel"""
        bus = MessageBus()

        bus.publish("new_channel", {"data": 1}, "publisher")

        channels = bus.list_channels()
        channel_names = [c.name for c in channels]
        assert "new_channel" in channel_names

    def test_publish_updates_channel_stats(self):
        """Test that publish updates channel statistics"""
        bus = MessageBus()

        bus.publish("test", {"data": 1}, "pub1")
        bus.publish("test", {"data": 2}, "pub2")

        info = bus.get_channel("test")
        assert info.message_count == 2
        assert "pub1" in info.publishers
        assert "pub2" in info.publishers
        assert info.last_message_at is not None


class TestMessageBusSubscription:
    """Tests for subscription functionality"""

    def test_subscribe_creates_channel(self):
        """Test that subscribe auto-creates channel"""
        bus = MessageBus()

        bus.subscribe("new_channel", lambda m: None, "subscriber")

        info = bus.get_channel("new_channel")
        assert info is not None
        assert "subscriber" in info.subscribers

    def test_subscribe_receives_messages(self):
        """Test that subscribers receive published messages"""
        bus = MessageBus()
        received = []

        def callback(message):
            received.append(message)

        bus.subscribe("test", callback, "sub")
        bus.publish("test", {"value": 42}, "pub")

        assert len(received) == 1
        assert received[0].payload["value"] == 42

    def test_message_metadata_populated(self):
        """Test that message metadata is properly populated"""
        bus = MessageBus()
        received = []

        bus.subscribe("test", lambda m: received.append(m), "sub")
        bus.publish("test", {}, "pub", message_type="signal")

        msg = received[0]
        assert msg.metadata.source_plugin == "pub"
        assert msg.metadata.message_type == "signal"
        assert msg.metadata.sequence_number == 1
        assert msg.metadata.timestamp is not None

    def test_sequence_numbers_increment(self):
        """Test that sequence numbers increment per channel"""
        bus = MessageBus()
        received = []

        bus.subscribe("test", lambda m: received.append(m), "sub")

        for i in range(5):
            bus.publish("test", {"i": i}, "pub")

        assert len(received) == 5
        for i, msg in enumerate(received):
            assert msg.metadata.sequence_number == i + 1

    def test_unsubscribe_stops_delivery(self):
        """Test that unsubscribe stops message delivery"""
        bus = MessageBus()
        received = []

        bus.subscribe("test", lambda m: received.append(m), "sub")
        bus.publish("test", {"before": True}, "pub")

        bus.unsubscribe("test", "sub")
        bus.publish("test", {"after": True}, "pub")

        assert len(received) == 1
        assert received[0].payload.get("before") is True

    def test_unsubscribe_returns_false_if_not_subscribed(self):
        """Test unsubscribe returns False if not subscribed"""
        bus = MessageBus()

        result = bus.unsubscribe("nonexistent", "sub")
        assert result is False

    def test_unsubscribe_all(self):
        """Test unsubscribing from all channels"""
        bus = MessageBus()

        bus.subscribe("channel1", lambda m: None, "sub")
        bus.subscribe("channel2", lambda m: None, "sub")
        bus.subscribe("channel3", lambda m: None, "sub")

        count = bus.unsubscribe_all("sub")

        assert count == 3
        for channel in ["channel1", "channel2", "channel3"]:
            info = bus.get_channel(channel)
            assert "sub" not in info.subscribers


class TestMessageBusMultipleSubscribers:
    """Tests for multiple subscribers"""

    def test_multiple_subscribers_same_channel(self):
        """Test multiple subscribers receive same message"""
        bus = MessageBus()
        r1, r2, r3 = [], [], []

        bus.subscribe("test", lambda m: r1.append(m), "sub1")
        bus.subscribe("test", lambda m: r2.append(m), "sub2")
        bus.subscribe("test", lambda m: r3.append(m), "sub3")

        bus.publish("test", {"data": 1}, "pub")

        assert len(r1) == 1
        assert len(r2) == 1
        assert len(r3) == 1

    def test_subscriber_update_callback(self):
        """Test re-subscribing updates callback"""
        bus = MessageBus()
        r1, r2 = [], []

        bus.subscribe("test", lambda m: r1.append(m), "sub")
        bus.publish("test", {"first": True}, "pub")

        bus.subscribe("test", lambda m: r2.append(m), "sub")
        bus.publish("test", {"second": True}, "pub")

        assert len(r1) == 1
        assert len(r2) == 1


class TestMessageBusChannelManagement:
    """Tests for channel management"""

    def test_create_channel_explicit(self):
        """Test explicitly creating a channel"""
        bus = MessageBus()

        result = bus.create_channel("my_channel", "My description")

        assert result is True
        info = bus.get_channel("my_channel")
        assert info.description == "My description"

    def test_create_channel_already_exists(self):
        """Test creating channel that already exists"""
        bus = MessageBus()

        bus.create_channel("test")
        result = bus.create_channel("test")

        assert result is False

    def test_delete_channel(self):
        """Test deleting a channel"""
        bus = MessageBus()

        bus.create_channel("to_delete")
        bus.subscribe("to_delete", lambda m: None, "sub")
        bus.publish("to_delete", {}, "pub")

        result = bus.delete_channel("to_delete")

        assert result is True
        assert bus.get_channel("to_delete") is None

    def test_delete_nonexistent_channel(self):
        """Test deleting channel that doesn't exist"""
        bus = MessageBus()

        result = bus.delete_channel("nonexistent")

        assert result is False

    def test_list_channels(self):
        """Test listing all channels"""
        bus = MessageBus()

        bus.create_channel("channel1")
        bus.create_channel("channel2")
        bus.create_channel("channel3")

        channels = bus.list_channels()
        names = [c.name for c in channels]

        assert len(channels) == 3
        assert "channel1" in names
        assert "channel2" in names
        assert "channel3" in names


class TestMessageBusHistory:
    """Tests for message history"""

    def test_get_history(self):
        """Test getting message history"""
        bus = MessageBus()

        for i in range(10):
            bus.publish("test", {"i": i}, "pub")

        history = bus.get_history("test")

        assert len(history) == 10
        assert history[0].payload["i"] == 0
        assert history[-1].payload["i"] == 9

    def test_get_history_with_count(self):
        """Test getting limited history"""
        bus = MessageBus()

        for i in range(10):
            bus.publish("test", {"i": i}, "pub")

        history = bus.get_history("test", count=3)

        assert len(history) == 3
        assert history[-1].payload["i"] == 9

    def test_get_history_with_since(self):
        """Test getting history since timestamp"""
        bus = MessageBus()

        bus.publish("test", {"early": True}, "pub")
        time.sleep(0.01)
        since = datetime.now()
        time.sleep(0.01)
        bus.publish("test", {"late": True}, "pub")

        history = bus.get_history("test", since=since)

        assert len(history) == 1
        assert history[0].payload.get("late") is True

    def test_history_limit_enforced(self):
        """Test that history limit is enforced"""
        bus = MessageBus(max_message_history=5)

        for i in range(10):
            bus.publish("test", {"i": i}, "pub")

        history = bus.get_history("test")

        assert len(history) == 5
        assert history[0].payload["i"] == 5  # Oldest kept

    def test_clear_history_single_channel(self):
        """Test clearing history for single channel"""
        bus = MessageBus()

        bus.publish("channel1", {}, "pub")
        bus.publish("channel2", {}, "pub")

        bus.clear_history("channel1")

        assert len(bus.get_history("channel1")) == 0
        assert len(bus.get_history("channel2")) == 1

    def test_clear_all_history(self):
        """Test clearing all history"""
        bus = MessageBus()

        bus.publish("channel1", {}, "pub")
        bus.publish("channel2", {}, "pub")

        bus.clear_history()

        assert len(bus.get_history("channel1")) == 0
        assert len(bus.get_history("channel2")) == 0


class TestMessageBusStats:
    """Tests for statistics"""

    def test_get_stats(self):
        """Test getting statistics"""
        bus = MessageBus()

        stats = bus.get_stats()

        assert "messages_published" in stats
        assert "messages_delivered" in stats
        assert "delivery_errors" in stats
        assert "channels" in stats

    def test_stats_updated_on_publish(self):
        """Test that stats are updated on publish"""
        bus = MessageBus()

        bus.subscribe("test", lambda m: None, "sub")

        for i in range(5):
            bus.publish("test", {"i": i}, "pub")

        stats = bus.get_stats()
        assert stats["messages_published"] == 5
        assert stats["messages_delivered"] == 5

    def test_stats_track_errors(self):
        """Test that stats track delivery errors"""
        bus = MessageBus()

        def bad_callback(m):
            raise Exception("Callback error")

        bus.subscribe("test", bad_callback, "sub")
        bus.publish("test", {}, "pub")

        stats = bus.get_stats()
        assert stats["delivery_errors"] == 1

    def test_reset_stats(self):
        """Test resetting statistics"""
        bus = MessageBus()

        bus.subscribe("test", lambda m: None, "sub")
        bus.publish("test", {}, "pub")

        bus.reset_stats()

        stats = bus.get_stats()
        assert stats["messages_published"] == 0
        assert stats["messages_delivered"] == 0


class TestMessageBusThreadSafety:
    """Tests for thread safety"""

    def test_concurrent_publish(self):
        """Test concurrent publishing is thread-safe"""
        bus = MessageBus()
        received = []
        lock = threading.Lock()

        def callback(m):
            with lock:
                received.append(m)

        bus.subscribe("test", callback, "sub")

        def publish_batch(publisher_id):
            for i in range(100):
                bus.publish("test", {"pub": publisher_id, "i": i}, f"pub{publisher_id}")

        threads = [
            threading.Thread(target=publish_batch, args=(i,)) for i in range(5)
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(received) == 500  # 5 publishers * 100 messages

    def test_concurrent_subscribe_unsubscribe(self):
        """Test concurrent subscribe/unsubscribe is thread-safe"""
        bus = MessageBus()
        errors = []

        def subscribe_unsubscribe(sub_id):
            try:
                for _ in range(50):
                    bus.subscribe("test", lambda m: None, f"sub{sub_id}")
                    bus.unsubscribe("test", f"sub{sub_id}")
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=subscribe_unsubscribe, args=(i,)) for i in range(10)
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0

    def test_callback_errors_dont_affect_others(self):
        """Test that one callback error doesn't affect other subscribers"""
        bus = MessageBus()
        r1, r2 = [], []

        def bad_callback(m):
            raise Exception("Error!")

        def good_callback(m):
            r2.append(m)

        bus.subscribe("test", bad_callback, "bad_sub")
        bus.subscribe("test", good_callback, "good_sub")

        bus.publish("test", {"data": 1}, "pub")

        # Good subscriber should still receive message
        assert len(r2) == 1


class TestMessageBusPatterns:
    """Tests for common usage patterns"""

    def test_signal_channel_pattern(self):
        """Test using signal channel pattern"""
        bus = MessageBus()
        signals = []

        # Simulate momentum plugin publishing signals
        bus.subscribe("momentum_signals", lambda m: signals.append(m), "consumer")

        bus.publish(
            "momentum_signals",
            {"symbol": "SPY", "direction": "bullish", "strength": 0.8},
            "momentum_plugin",
            message_type="signal",
        )

        assert len(signals) == 1
        assert signals[0].payload["symbol"] == "SPY"
        assert signals[0].metadata.message_type == "signal"

    def test_indicator_channel_pattern(self):
        """Test using indicator channel pattern"""
        bus = MessageBus()
        indicators = []

        bus.subscribe("indicators_rsi", lambda m: indicators.append(m), "strategy")

        bus.publish(
            "indicators_rsi",
            {"symbol": "AAPL", "value": 65.3, "period": 14},
            "rsi_calculator",
        )

        assert len(indicators) == 1
        assert indicators[0].payload["value"] == 65.3

    def test_synthetic_ticker_pattern(self):
        """Test using synthetic ticker pattern"""
        bus = MessageBus()
        spreads = []

        bus.subscribe("synthetic_spy_qqq_spread", lambda m: spreads.append(m), "arb")

        bus.publish(
            "synthetic_spy_qqq_spread",
            {"value": 0.05, "spy_price": 450.0, "qqq_price": 380.0},
            "spread_calculator",
        )

        assert len(spreads) == 1
        assert spreads[0].payload["value"] == 0.05
