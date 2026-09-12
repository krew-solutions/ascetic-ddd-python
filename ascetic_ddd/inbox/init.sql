CREATE SEQUENCE IF NOT EXISTS inbox_received_position_seq;

CREATE TABLE inbox (
    tenant_id varchar(128) NOT NULL,
    stream_type varchar(128) NOT NULL,
    stream_id jsonb NOT NULL,
    stream_position integer NOT NULL,
    uri varchar(255) NOT NULL,
    payload bytea NOT NULL,
    metadata jsonb NULL,
    received_position bigint NOT NULL UNIQUE DEFAULT nextval('inbox_received_position_seq'),
    processed_position bigint NULL,
    CONSTRAINT inbox_pk PRIMARY KEY (tenant_id, stream_type, stream_id, stream_position)
);

CREATE INDEX inbox__received_position_idx ON inbox(received_position);
CREATE INDEX inbox__processed_position_idx ON inbox(processed_position) WHERE processed_position IS NULL;
CREATE UNIQUE INDEX inbox__message_id_uniq ON inbox( ((metadata->>'message_id')::uuid) );
