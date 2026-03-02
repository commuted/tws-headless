"""
message_bus.py - Pub/Sub message bus for inter-plugin communication

Provides named channels for topic-based messaging between plugins.
Plugins communicate ONLY via MessageBus - no direct memory access.
"""

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


@dataclass
class MessageMetadata:
    """Metadata attached to every message"""

    timestamp: datetime
    source_plugin: str
    message_type: str
    sequence_number: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "source_plugin": self.source_plugin,
            "message_type": self.message_type,
            "sequence_number": self.sequence_number,
        }


@dataclass
class Message:
    """A message on a channel"""

    channel: str
    payload: Any
    metadata: MessageMetadata

    def to_dict(self) -> Dict[str, Any]:
        return {
            "channel": self.channel,
            "payload": self.payload,
            "metadata": self.metadata.to_dict(),
        }


@dataclass
class ChannelInfo:
    """Information about a channel"""

    name: str
    description: str = ""
    publishers: Set[str] = field(default_factory=set)
    subscribers: Set[str] = field(default_factory=set)
    message_count: int = 0
    last_message_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "publishers": sorted(self.publishers),
            "subscribers": sorted(self.subscribers),
            "message_count": self.message_count,
            "last_message_at": (
                self.last_message_at.isoformat() if self.last_message_at else None
            ),
        }


@dataclass
class Subscription:
    """A subscription to a channel"""

    channel: str
    subscriber: str
    callback: Callable[[Message], None]
    created_at: datetime = field(default_factory=datetime.now)


class MessageBus:
    """
    Pub/Sub message bus for inter-plugin communication.

    Provides:
    - Named channels for topic-based messaging
    - Message metadata (timestamp, source, type, sequence number)
    - Channel discovery for debugging/monitoring
    - Message history per channel
    - Thread-safe operations

    Plugins communicate ONLY via MessageBus - no direct memory access.

    Usage:
        bus = MessageBus()

        # Publisher plugin
        bus.publish("momentum_signals", {"symbol": "SPY", "signal": 0.8}, "momentum_plugin")

        # Subscriber plugin
        def on_signal(message: Message):
            print(f"Got signal: {message.payload}")

        bus.subscribe("momentum_signals", on_signal, "consumer_plugin")

    Standard Channel Patterns:
        {plugin}_signals  - Trading signals from a plugin
        {plugin}_metrics  - Performance metrics
        indicators_{name} - Indicator values (RSI, MACD, etc.)
        synthetic_{name}  - Synthetic tickers/spreads
        backtest_{id}     - Backtest data streams
        alerts            - System-wide alerts
    """

    def __init__(self, max_message_history: int = 1000):
        """
        Initialize the MessageBus.

        Args:
            max_message_history: Maximum messages to keep in history per channel
        """
        self._channels: Dict[str, ChannelInfo] = {}
        self._subscriptions: Dict[str, List[Subscription]] = defaultdict(list)
        self._sequence_numbers: Dict[str, int] = defaultdict(int)
        self._message_history: Dict[str, List[Message]] = defaultdict(list)
        self._max_history = max_message_history

        # Statistics
        self._stats = {
            "messages_published": 0,
            "messages_delivered": 0,
            "delivery_errors": 0,
        }

    def publish(
        self,
        channel: str,
        payload: Any,
        publisher: str,
        message_type: str = "data",
    ) -> bool:
        """
        Publish a message to a channel.

        Args:
            channel: Channel name (e.g., "momentum_signals", "indicators_rsi")
            payload: Message payload (any JSON-serializable data)
            publisher: Name of the publishing plugin
            message_type: Type of message (data, signal, alert, metric, state)

        Returns:
            True if published (even if no subscribers)
        """
        # Ensure channel exists
        if channel not in self._channels:
            self._channels[channel] = ChannelInfo(name=channel)

        channel_info = self._channels[channel]
        channel_info.publishers.add(publisher)

        # Create message with metadata
        self._sequence_numbers[channel] += 1
        metadata = MessageMetadata(
            timestamp=datetime.now(),
            source_plugin=publisher,
            message_type=message_type,
            sequence_number=self._sequence_numbers[channel],
        )

        message = Message(
            channel=channel,
            payload=payload,
            metadata=metadata,
        )

        # Update channel stats
        channel_info.message_count += 1
        channel_info.last_message_at = metadata.timestamp

        # Store in history
        history = self._message_history[channel]
        history.append(message)
        if len(history) > self._max_history:
            self._message_history[channel] = history[-self._max_history :]

        # Get subscribers (copy list to avoid issues during iteration)
        subscribers = list(self._subscriptions.get(channel, []))
        self._stats["messages_published"] += 1

        # Deliver to subscribers OUTSIDE the lock to prevent deadlocks
        for sub in subscribers:
            try:
                sub.callback(message)
                self._stats["messages_delivered"] += 1
            except Exception as e:
                self._stats["delivery_errors"] += 1
                logger.error(
                    f"Error delivering message to '{sub.subscriber}' "
                    f"on channel '{channel}': {e}"
                )

        return True

    def subscribe(
        self,
        channel: str,
        callback: Callable[[Message], None],
        subscriber: str,
    ) -> bool:
        """
        Subscribe to a channel.

        Args:
            channel: Channel name
            callback: Function called for each message (receives Message object)
            subscriber: Name of subscribing plugin

        Returns:
            True if subscribed successfully
        """
        # Ensure channel exists
        if channel not in self._channels:
            self._channels[channel] = ChannelInfo(name=channel)

        # Check if already subscribed
        for sub in self._subscriptions[channel]:
            if sub.subscriber == subscriber:
                # Update callback
                sub.callback = callback
                logger.debug(
                    f"'{subscriber}' updated subscription to channel '{channel}'"
                )
                return True

        # Add new subscription
        subscription = Subscription(
            channel=channel,
            subscriber=subscriber,
            callback=callback,
        )
        self._subscriptions[channel].append(subscription)
        self._channels[channel].subscribers.add(subscriber)

        logger.debug(f"'{subscriber}' subscribed to channel '{channel}'")
        return True

    def unsubscribe(self, channel: str, subscriber: str) -> bool:
        """
        Unsubscribe from a channel.

        Args:
            channel: Channel name
            subscriber: Name of subscribing plugin

        Returns:
            True if unsubscribed (False if wasn't subscribed)
        """
        if channel not in self._subscriptions:
            return False

        original_count = len(self._subscriptions[channel])
        self._subscriptions[channel] = [
            s for s in self._subscriptions[channel] if s.subscriber != subscriber
        ]

        if channel in self._channels:
            self._channels[channel].subscribers.discard(subscriber)

        removed = len(self._subscriptions[channel]) < original_count
        if removed:
            logger.debug(f"'{subscriber}' unsubscribed from channel '{channel}'")

        return removed

    def unsubscribe_all(self, subscriber: str) -> int:
        """
        Unsubscribe from all channels.

        Args:
            subscriber: Name of subscribing plugin

        Returns:
            Number of channels unsubscribed from
        """
        count = 0
        for channel in list(self._subscriptions.keys()):
            if self.unsubscribe(channel, subscriber):
                count += 1
        return count

    def list_channels(self) -> List[ChannelInfo]:
        """
        List all available channels with info.

        Returns:
            List of ChannelInfo objects
        """
        return list(self._channels.values())

    def get_channel(self, channel: str) -> Optional[ChannelInfo]:
        """
        Get info for a specific channel.

        Args:
            channel: Channel name

        Returns:
            ChannelInfo or None if channel doesn't exist
        """
        return self._channels.get(channel)

    def get_history(
        self,
        channel: str,
        count: int = 100,
        since: Optional[datetime] = None,
    ) -> List[Message]:
        """
        Get message history for a channel.

        Args:
            channel: Channel name
            count: Maximum messages to return
            since: Only messages after this timestamp

        Returns:
            List of Message objects (most recent last)
        """
        messages = self._message_history.get(channel, [])

        if since:
            messages = [m for m in messages if m.metadata.timestamp >= since]

        return list(messages[-count:])

    def create_channel(
        self,
        name: str,
        description: str = "",
    ) -> bool:
        """
        Create a channel explicitly (optional - channels auto-create on publish).

        Args:
            name: Channel name
            description: Human-readable description

        Returns:
            True if created (False if already exists)
        """
        if name in self._channels:
            return False

        self._channels[name] = ChannelInfo(name=name, description=description)
        logger.debug(f"Created channel '{name}'")
        return True

    def delete_channel(self, name: str) -> bool:
        """
        Delete a channel and all its subscriptions.

        Args:
            name: Channel name

        Returns:
            True if deleted (False if didn't exist)
        """
        if name not in self._channels:
            return False

        del self._channels[name]
        self._subscriptions.pop(name, None)
        self._message_history.pop(name, None)
        self._sequence_numbers.pop(name, None)

        logger.debug(f"Deleted channel '{name}'")
        return True

    def clear_history(self, channel: Optional[str] = None) -> bool:
        """
        Clear message history.

        Args:
            channel: Channel to clear (None = clear all)

        Returns:
            True if cleared
        """
        if channel:
            if channel in self._message_history:
                self._message_history[channel] = []
                return True
            return False
        else:
            self._message_history.clear()
            return True

    def get_stats(self) -> Dict[str, Any]:
        """
        Get MessageBus statistics.

        Returns:
            Dict with stats (messages_published, messages_delivered, etc.)
        """
        return {
            **self._stats,
            "channels": len(self._channels),
            "total_subscriptions": sum(
                len(subs) for subs in self._subscriptions.values()
            ),
            "history_size": sum(
                len(msgs) for msgs in self._message_history.values()
            ),
        }

    def reset_stats(self):
        """Reset statistics counters."""
        self._stats = {
            "messages_published": 0,
            "messages_delivered": 0,
            "delivery_errors": 0,
        }
