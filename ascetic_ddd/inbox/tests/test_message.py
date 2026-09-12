"""Tests for InboxMessage class."""

import unittest

from ascetic_ddd.inbox.message import InboxMessage
from ascetic_ddd.utils.tests.payload import json_payload


class InboxMessageTestCase(unittest.TestCase):
    """Test cases for InboxMessage."""

    def test_create_message(self):
        """InboxMessage can be created with required fields."""
        message = InboxMessage(
            tenant_id="tenant1",
            stream_type="Order",
            stream_id={"id": "order-123"},
            stream_position=1,
            uri="kafka://orders",
            payload=json_payload({"amount": 100}),
        )

        self.assertEqual(message.tenant_id, "tenant1")
        self.assertEqual(message.stream_type, "Order")
        self.assertEqual(message.stream_id, {"id": "order-123"})
        self.assertEqual(message.stream_position, 1)
        self.assertEqual(message.uri, "kafka://orders")
        self.assertEqual(message.payload, json_payload({"amount": 100}))
        self.assertIsNone(message.metadata)
        self.assertIsNone(message.received_position)
        self.assertIsNone(message.processed_position)

    def test_create_message_with_metadata(self):
        """InboxMessage can be created with metadata."""
        message = InboxMessage(
            tenant_id="tenant1",
            stream_type="Order",
            stream_id={"id": "order-123"},
            stream_position=1,
            uri="kafka://orders",
            payload=json_payload({"amount": 100}),
            metadata={"message_id": "uuid-123", "timestamp": "2024-01-01T00:00:00Z"},
        )

        self.assertEqual(message.metadata["message_id"], "uuid-123")

    def test_causal_dependencies_empty_when_no_metadata(self):
        """causal_dependencies returns empty list when no metadata."""
        message = InboxMessage(
            tenant_id="tenant1",
            stream_type="Order",
            stream_id={"id": "order-123"},
            stream_position=1,
            uri="kafka://orders",
            payload=json_payload({}),
        )

        self.assertEqual(message.causal_dependencies, [])

    def test_causal_dependencies_empty_when_not_present(self):
        """causal_dependencies returns empty list when not in metadata."""
        message = InboxMessage(
            tenant_id="tenant1",
            stream_type="Order",
            stream_id={"id": "order-123"},
            stream_position=1,
            uri="kafka://orders",
            payload=json_payload({}),
            metadata={"message_id": "uuid-123"},
        )

        self.assertEqual(message.causal_dependencies, [])

    def test_causal_dependencies_returns_list(self):
        """causal_dependencies returns the list from metadata."""
        deps = [
            {"tenant_id": "tenant1", "stream_type": "User", "stream_id": {"id": "user-1"}, "stream_position": 5},
            {"tenant_id": "tenant1", "stream_type": "Product", "stream_id": {"id": "prod-1"}, "stream_position": 3},
        ]
        message = InboxMessage(
            tenant_id="tenant1",
            stream_type="Order",
            stream_id={"id": "order-123"},
            stream_position=1,
            uri="kafka://orders",
            payload=json_payload({}),
            metadata={"causal_dependencies": deps},
        )

        self.assertEqual(message.causal_dependencies, deps)
        self.assertEqual(len(message.causal_dependencies), 2)

    def test_message_id_none_when_no_metadata(self):
        """message_id returns None when no metadata."""
        message = InboxMessage(
            tenant_id="tenant1",
            stream_type="Order",
            stream_id={"id": "order-123"},
            stream_position=1,
            uri="kafka://orders",
            payload=json_payload({}),
        )

        self.assertIsNone(message.message_id)

    def test_message_id_returns_value(self):
        """message_id returns the value from metadata."""
        message = InboxMessage(
            tenant_id="tenant1",
            stream_type="Order",
            stream_id={"id": "order-123"},
            stream_position=1,
            uri="kafka://orders",
            payload=json_payload({}),
            metadata={"message_id": "uuid-456"},
        )

        self.assertEqual(message.message_id, "uuid-456")

    def test_received_and_processed_positions(self):
        """InboxMessage can store received and processed positions."""
        message = InboxMessage(
            tenant_id="tenant1",
            stream_type="Order",
            stream_id={"id": "order-123"},
            stream_position=1,
            uri="kafka://orders",
            payload=json_payload({}),
            received_position=100,
            processed_position=50,
        )

        self.assertEqual(message.received_position, 100)
        self.assertEqual(message.processed_position, 50)


if __name__ == '__main__':
    unittest.main()
