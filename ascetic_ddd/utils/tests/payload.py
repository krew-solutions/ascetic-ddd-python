"""Payload helpers for inbox and outbox tests: the stored payload is bytes."""

import json
from typing import Any

__all__ = ('json_payload', 'decode_payload')


def json_payload(obj: dict[str, Any]) -> bytes:
    """Encode a test payload as the bytes the outbox and inbox store."""
    return json.dumps(obj).encode()


def decode_payload(payload: bytes) -> dict[str, Any]:
    """Read a stored payload back as JSON."""
    return json.loads(payload)
