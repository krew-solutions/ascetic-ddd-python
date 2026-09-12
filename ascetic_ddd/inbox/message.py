"""Inbox message - structure for incoming integration messages."""

from dataclasses import dataclass, field
from typing import Any


__all__ = (
    'InboxMessage',
)


@dataclass
class InboxMessage:
    """Incoming integration message for the Inbox pattern.

    Attributes:
        tenant_id: Tenant identifier. Extracted from payload. Use 1 if tenant is not used.
        stream_type: Type of the event stream. Can be one of:
            - bounded_context_name.aggregate_name extracted from payload
            - topic/channel name
        stream_id: Identifier of the stream. Can be one of:
            - aggregate.id.internal_id extracted from payload for composite aggregate.id
            - aggregate.id extracted from payload for primitive aggregate.id
            - partition key of topic/channel
        stream_position: Position in the stream (monotonically increasing). Can be one of:
            - aggregate.version from payload
            - position/offset of topic/channel
        uri: Routing URI (e.g., 'kafka://orders', 'amqp://exchange/key'). Can be one of:
            - bus_type://topic_or_channel_name
            - bus_type://topic_or_channel_name/partition_key
        payload: The message as it came off the wire: serialized bytes.
        metadata: Optional event metadata (may contain message_id, causal_dependencies, etc.).
        partition_key: Key for worker distribution (computed by strategy, auto-assigned).
        received_position: Position when message was received (auto-assigned by DB).
        processed_position: Position when message was processed (None if not processed).

    See for more info:
        - ascetic_ddd/seedwork/domain/aggregate/event_meta.py
        - ascetic_ddd/seedwork/domain/aggregate/causal_dependency.py

    TODO: Use abstract stream_id (stored as jsonb)
    similar to ascetic_ddd.seedwork.infrastructure.repository.stream_id.StreamId
    instead of three fields?
    """
    tenant_id: Any
    stream_type: str
    stream_id: dict[str, Any]
    stream_position: int
    uri: str
    payload: bytes
    metadata: dict[str, Any] | None = None
    received_position: int | None = None
    processed_position: int | None = None

    @property
    def causal_dependencies(self) -> list[dict[str, Any]]:
        """Get causal dependencies from metadata.

        Returns:
            List of dependency descriptors, each containing:
            - tenant_id, stream_type, stream_id, stream_position
        """
        if self.metadata is None:
            return []
        return self.metadata.get('causal_dependencies', [])

    @property
    def message_id(self) -> str | None:
        """Get message_id from metadata if present."""
        if self.metadata is None:
            return None
        return self.metadata.get('message_id')
