CREATE TABLE messages (
    id BIGINT NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    topic TEXT NOT NULL,
    payload JSONB NOT NULL,
    CONSTRAINT messages_pkey PRIMARY KEY (topic, id)
) PARTITION BY LIST (topic);

CREATE TABLE topic_meta (
    topic TEXT PRIMARY KEY,
    next_id BIGINT NOT NULL DEFAULT 1,
    acked_id BIGINT NOT NULL DEFAULT 0,
    partition_size BIGINT NOT NULL,
    partition_count INT NOT NULL,
    fill_seq_name TEXT NOT NULL,
    lease_holder TEXT,
    lease_expires_at TIMESTAMPTZ,
    lease_high_id BIGINT,
    writes_since_resize BIGINT NOT NULL DEFAULT 0,
    acks_since_resize BIGINT NOT NULL DEFAULT 0,
    last_write_at TIMESTAMPTZ,
    last_process_at TIMESTAMPTZ,
    resized_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE topic_partitions (
    topic TEXT NOT NULL REFERENCES topic_meta (topic) ON DELETE CASCADE,
    partition_index INT NOT NULL,
    relname TEXT NOT NULL,
    id_from BIGINT NOT NULL,
    id_to BIGINT NOT NULL,
    partition_size BIGINT NOT NULL,
    high_water_id BIGINT NOT NULL DEFAULT 0,
    status TEXT NOT NULL CHECK (
        status IN ('future', 'active', 'sealed', 'dropped')
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (topic, partition_index),
    CONSTRAINT topic_partitions_relname_key UNIQUE (relname)
);

CREATE INDEX topic_partitions_topic_status_idx ON topic_partitions (topic, status);

ALTER TABLE topic_meta SET (
    fillfactor = 70,
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 5,
    autovacuum_analyze_scale_factor = 0.0,
    autovacuum_analyze_threshold = 5,
    autovacuum_vacuum_cost_delay = 0,
    autovacuum_vacuum_cost_limit = 1000
);

ALTER TABLE topic_partitions SET (
    fillfactor = 70,
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 5,
    autovacuum_analyze_scale_factor = 0.0,
    autovacuum_analyze_threshold = 5,
    autovacuum_vacuum_cost_delay = 0,
    autovacuum_vacuum_cost_limit = 1000
);
