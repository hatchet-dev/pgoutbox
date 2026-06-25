-- +goose Up
-- +goose StatementBegin
CREATE INDEX messages_topic_inserted_at_idx ON messages (topic, inserted_at);

CREATE TABLE maintenance_leases (
    topic       TEXT        NOT NULL,
    holder_id   UUID        NOT NULL,
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT maintenance_leases_pkey PRIMARY KEY (topic)
);

CREATE TABLE topics (
    topic                         TEXT        NOT NULL,
    expiration_nanos              BIGINT      NULL,
    exclusive_consumer_id         UUID        NULL,
    exclusive_consumer_expires_at TIMESTAMPTZ NULL,
    CONSTRAINT topics_pkey PRIMARY KEY (topic)
);

CREATE FUNCTION topics_insert_trigger_fn()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    new_topics text[];
BEGIN
    -- Read-before-write: collect only topics not already in the table.
    -- TG_TABLE_SCHEMA is used with %I so this works for any schema without
    -- touching search_path (no side-effects on the calling transaction).
    -- array_agg returns NULL when every topic in this batch already exists,
    -- allowing the early-return below to skip the INSERT entirely.
    EXECUTE format(
        $q$SELECT array_agg(DISTINCT i.topic)
           FROM inserted AS i
           WHERE NOT EXISTS (
               SELECT 1 FROM %I.topics AS t WHERE t.topic = i.topic
           )$q$,
        TG_TABLE_SCHEMA
    ) INTO new_topics;

    IF new_topics IS NULL THEN
        RETURN NULL;
    END IF;

    -- Register new topics with no expiration; expiration_nanos is set
    -- separately via WithTopicExpiration + Start.
    EXECUTE format(
        $q$INSERT INTO %I.topics (topic, expiration_nanos)
           SELECT unnest($1), NULL::bigint
           ON CONFLICT (topic) DO NOTHING$q$,
        TG_TABLE_SCHEMA
    ) USING new_topics;

    RETURN NULL;
END;
$$;

CREATE TRIGGER messages_topics_sync
    AFTER INSERT ON messages
    REFERENCING NEW TABLE AS inserted
    FOR EACH STATEMENT
    EXECUTE FUNCTION topics_insert_trigger_fn();
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TRIGGER IF EXISTS messages_topics_sync ON messages;
DROP FUNCTION IF EXISTS topics_insert_trigger_fn();
DROP TABLE IF EXISTS topics;
DROP TABLE IF EXISTS maintenance_leases;
DROP INDEX IF EXISTS messages_topic_inserted_at_idx;
-- +goose StatementEnd
