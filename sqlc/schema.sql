CREATE TABLE messages (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    topic TEXT NOT NULL,
    payload JSONB NOT NULL,
    CONSTRAINT v1_message_pkey PRIMARY KEY (topic, id)
);

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