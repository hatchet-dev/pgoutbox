CREATE TABLE messages (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    topic TEXT NOT NULL,
    payload JSONB NOT NULL,
    CONSTRAINT v1_message_pkey PRIMARY KEY (topic, id)
);

ALTER TABLE messages SET (
    autovacuum_vacuum_scale_factor = '0.1',
    autovacuum_analyze_scale_factor='0.05',
    autovacuum_vacuum_threshold='25',
    autovacuum_analyze_threshold='25',
    autovacuum_vacuum_cost_delay='10',
    autovacuum_vacuum_cost_limit='1000'
);

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
    last_inserted_at              TIMESTAMPTZ NULL,
    CONSTRAINT topics_pkey PRIMARY KEY (topic)
);

CREATE FUNCTION topics_insert_trigger_fn()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    new_topics text[];
BEGIN
    -- Bump activity for this batch's topics, at most once per 30s window per
    -- topic, bounding write churn on the topics table regardless of insert
    -- rate. FOR UPDATE SKIP LOCKED: the winning producer's row lock is held
    -- until its transaction commits, so at window rollover concurrent
    -- producers skip the bump instead of queueing behind it — and producers
    -- never block behind a ProcessMessages flush, which holds the topic row
    -- lock for its whole transaction. On a continuously-flushed topic that
    -- means every trigger bump can be skipped; the flush path compensates by
    -- bumping activity itself (BumpTopicActivityIfStale) under the very lock
    -- that causes the skips.
    EXECUTE format(
        $q$UPDATE %I.topics t
           SET last_inserted_at = now()
           WHERE t.topic IN (
               SELECT t2.topic FROM %I.topics t2
               WHERE t2.topic IN (SELECT DISTINCT i.topic FROM inserted AS i)
                 AND (t2.last_inserted_at IS NULL
                      OR t2.last_inserted_at < now() - interval '30 seconds')
               FOR UPDATE SKIP LOCKED
           )$q$,
        TG_TABLE_SCHEMA, TG_TABLE_SCHEMA
    );

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
        $q$INSERT INTO %I.topics (topic, expiration_nanos, last_inserted_at)
           SELECT unnest($1), NULL::bigint, now()
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

CREATE TABLE consumer_sessions (
    consumer_id                  UUID        NOT NULL,
    expires_at                   TIMESTAMPTZ NOT NULL,
    maintains_default_expiration BOOLEAN     NOT NULL DEFAULT FALSE,
    CONSTRAINT consumer_sessions_pkey PRIMARY KEY (consumer_id)
);