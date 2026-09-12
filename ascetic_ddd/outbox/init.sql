-- Transactional Outbox Pattern - PostgreSQL Schema
--
-- Key Design Decisions:
-- 1. transaction_id (xid8) + position for correct ordering
-- 2. Visibility rule: only read committed transactions
-- 3. Consumer groups for multiple independent consumers
-- 4. FOR UPDATE locking for concurrent dispatcher safety
-- 5. URI-based routing and position tracking

-- =============================================================================
-- OUTBOX TABLE
-- =============================================================================
-- Stores messages within the same transaction as business state changes.
-- Messages are only visible to dispatchers after the transaction commits.

CREATE TABLE IF NOT EXISTS outbox (
    -- Auto-incrementing position within the outbox
    -- Note: BIGSERIAL doesn't guarantee ordering in concurrent transactions!
    -- That's why we also capture transaction_id.
    "position" BIGSERIAL,

    -- Routing URI (e.g., 'kafka://orders', 'amqp://exchange/key', 'sb://./queue')
    -- Used for routing to different transports/topics
    "uri" VARCHAR(255) NOT NULL,

    -- Message payload as it goes on the wire: serialized (and encrypted where
    -- required) before it reaches the outbox; the dispatcher relays bytes
    "payload" BYTEA NOT NULL,

    -- Message metadata (must contain 'message_id' for idempotency)
    -- Also: correlation_id, causation_id, aggregate info, etc.
    "metadata" JSONB NOT NULL,

    -- Timestamp when the message was created
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- PostgreSQL transaction ID (xid8 type)
    -- Captured using pg_current_xact_id() at insert time
    -- Used for correct ordering across concurrent transactions
    "transaction_id" xid8 NOT NULL,

    -- Primary key: (transaction_id, position)
    -- This allows efficient queries for messages after a given position
    -- and ensures uniqueness within a transaction
    PRIMARY KEY ("transaction_id", "position")
);

-- Index for efficient queries by position alone (for simple sequential reads)
CREATE INDEX IF NOT EXISTS outbox_position_idx ON outbox ("position");

-- Index for queries filtering by uri
CREATE INDEX IF NOT EXISTS outbox_uri_idx ON outbox ("uri");

-- Unique index on message_id from metadata for idempotency
-- Consumers should use metadata->>'message_id' to detect and ignore duplicates
CREATE UNIQUE INDEX IF NOT EXISTS outbox_message_id_uniq ON outbox (((metadata->>'message_id')::uuid));


-- =============================================================================
-- OUTBOX OFFSETS TABLE (Consumer Groups)
-- =============================================================================
-- Tracks the position of each consumer group in the outbox.
-- Supports multiple independent consumers reading the same outbox.
-- Position is tracked per (consumer_group, uri) for selective subscription.

CREATE TABLE IF NOT EXISTS outbox_offsets (
    -- Consumer group identifier (empty string for default)
    "consumer_group" VARCHAR(255) NOT NULL,

    -- URI filter for selective subscription
    -- Empty string means "all URIs" (default behavior)
    -- Specific URI (e.g., 'kafka://orders') means track position for that URI only
    "uri" VARCHAR(255) NOT NULL DEFAULT '',

    -- Last acknowledged offset
    -- Consumer has processed all messages up to and including this offset
    "offset_acked" BIGINT NOT NULL DEFAULT 0,

    -- Last processed transaction ID
    -- Used together with offset_acked to determine the exact position
    "last_processed_transaction_id" xid8 NOT NULL DEFAULT '0',

    -- Timestamp of last position update
    "updated_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Composite primary key: (consumer_group, uri)
    -- Allows tracking position per consumer group per uri
    PRIMARY KEY ("consumer_group", "uri")
);


-- =============================================================================
-- USAGE NOTES
-- =============================================================================
--
-- PUBLISHING (within business transaction):
-- -----------------------------------------
-- INSERT INTO outbox (uri, payload, metadata, transaction_id)
-- VALUES ($1, $2, $3, pg_current_xact_id());
--
-- Example:
-- INSERT INTO outbox (uri, payload, metadata, transaction_id)
-- VALUES (
--     'kafka://orders',
--     '{"type": "OrderCreated", "order_id": "123", "amount": 100}'::bytea,
--     '{"message_id": "550e8400-e29b-41d4-a716-446655440000"}'::jsonb,
--     pg_current_xact_id()
-- );
--
--
-- DISPATCHING (reading unprocessed messages):
-- -------------------------------------------
-- The key is to only read messages from COMMITTED transactions.
-- pg_snapshot_xmin(pg_current_snapshot()) returns the oldest transaction ID
-- that is still in progress. Any transaction_id below this is guaranteed
-- to be committed and visible.
--
-- Without URI filter (all messages):
-- WITH last_processed AS (
--     SELECT offset_acked, last_processed_transaction_id
--     FROM outbox_offsets
--     WHERE consumer_group = $1 AND uri = ''
--     FOR UPDATE  -- Lock to prevent concurrent reads
-- )
-- SELECT "position", transaction_id, uri, payload, metadata
-- FROM outbox
-- WHERE (
--     (transaction_id = (SELECT last_processed_transaction_id FROM last_processed)
--      AND "position" > (SELECT offset_acked FROM last_processed))
--     OR
--     (transaction_id > (SELECT last_processed_transaction_id FROM last_processed))
-- )
-- AND transaction_id < pg_snapshot_xmin(pg_current_snapshot())
-- ORDER BY transaction_id ASC, "position" ASC
-- LIMIT 100;
--
-- With URI filter (specific uri only):
-- WITH last_processed AS (
--     SELECT offset_acked, last_processed_transaction_id
--     FROM outbox_offsets
--     WHERE consumer_group = $1 AND uri = $2
--     FOR UPDATE
-- )
-- SELECT "position", transaction_id, uri, payload, metadata
-- FROM outbox
-- WHERE (
--     (transaction_id = (SELECT last_processed_transaction_id FROM last_processed)
--      AND "position" > (SELECT offset_acked FROM last_processed))
--     OR
--     (transaction_id > (SELECT last_processed_transaction_id FROM last_processed))
-- )
-- AND transaction_id < pg_snapshot_xmin(pg_current_snapshot())
-- AND uri = $2  -- Filter by uri
-- ORDER BY transaction_id ASC, "position" ASC
-- LIMIT 100;
--
--
-- ACKNOWLEDGING (updating consumer position):
-- -------------------------------------------
-- INSERT INTO outbox_offsets (consumer_group, uri, offset_acked, last_processed_transaction_id, updated_at)
-- VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
-- ON CONFLICT (consumer_group, uri) DO UPDATE SET
--     offset_acked = EXCLUDED.offset_acked,
--     last_processed_transaction_id = EXCLUDED.last_processed_transaction_id,
--     updated_at = EXCLUDED.updated_at;
--
--
-- INITIALIZING CONSUMER GROUP:
-- ----------------------------
-- Before first dispatch, ensure the consumer group exists with zero position.
-- This is required for FOR UPDATE locking to work.
--
-- INSERT INTO outbox_offsets (consumer_group, uri, offset_acked, last_processed_transaction_id)
-- VALUES ($1, $2, 0, '0')
-- ON CONFLICT DO NOTHING;
--
--
-- CLEANUP (archiving old messages):
-- ---------------------------------
-- Find the minimum position across all consumer groups:
--
-- SELECT MIN(last_processed_transaction_id) as min_txid, MIN(offset_acked) as min_offset
-- FROM outbox_offsets;
--
-- Delete messages before that position:
--
-- DELETE FROM outbox
-- WHERE transaction_id < $min_txid
--    OR (transaction_id = $min_txid AND "position" <= $min_offset);
--
--
-- =============================================================================
-- URI-BASED POSITION TRACKING
-- =============================================================================
--
-- The uri column in outbox_offsets allows selective subscription:
--
-- 1. uri = '' (empty string): Track position for ALL messages
--    - Default behavior, backwards compatible
--    - Consumer sees all messages regardless of their uri
--
-- 2. uri = 'kafka://orders': Track position for this URI only
--    - Consumer only sees messages with uri = 'kafka://orders'
--    - Position is tracked independently from other URIs
--
-- Example: Same consumer group, different URIs
-- (consumer_group='notifications', uri='') -> tracks all
-- (consumer_group='notifications', uri='kafka://orders') -> tracks orders only
-- (consumer_group='notifications', uri='kafka://users') -> tracks users only
--
-- This is equivalent to Watermill's table-per-topic approach,
-- but without creating separate tables.
--
--
-- =============================================================================
-- WHY transaction_id (xid8)?
-- =============================================================================
--
-- Problem with SERIAL alone:
-- TX1 starts, gets position=1
-- TX2 starts, gets position=2
-- TX2 commits
-- Consumer reads position > 0, sees message 2
-- TX1 commits
-- Message 1 is now visible but consumer already moved past it!
--
-- Solution with transaction_id:
-- Only read messages where transaction_id < pg_snapshot_xmin(pg_current_snapshot())
-- This ensures we only see messages from fully committed transactions.
-- Within a transaction, messages are ordered by position.
-- Across transactions, they're ordered by transaction_id.
--
-- Reference: https://event-driven.io/en/ordering_in_postgres_outbox/
