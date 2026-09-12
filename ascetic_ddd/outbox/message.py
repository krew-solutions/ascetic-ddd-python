"""Outbox message - structure for outgoing integration messages."""

from dataclasses import dataclass
from typing import Any


__all__ = (
    'OutboxMessage',
)


@dataclass
class OutboxMessage:
    """Outgoing integration message for the Outbox pattern.

    Attributes:
        uri: Routing address (e.g., 'kafka://orders', 'amqp://exchange/key').
        payload: The message as it goes on the wire: serialized (and encrypted where
            required) before it reaches the outbox; the dispatcher relays bytes.
        metadata: Message metadata (must contain 'message_id' for idempotency).
        created_at: Timestamp when message was created (auto-assigned by DB).
        position: Position in the outbox (auto-assigned by DB).
        transaction_id: PostgreSQL transaction ID (auto-assigned by pg_current_xact_id()).
    """
    uri: str
    payload: bytes
    metadata: dict[str, Any]  # Required: must contain 'message_id'
    created_at: str | None = None
    position: int | None = None
    transaction_id: int | None = None
