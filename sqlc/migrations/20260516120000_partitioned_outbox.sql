-- +goose Up
-- +goose StatementBegin
ALTER TABLE messages RENAME TO messages_legacy;

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

DO $$
DECLARE
    rec RECORD;
    slug TEXT;
    list_rel TEXT;
    seq_name TEXT;
    max_id BIGINT;
    z BIGINT := 100000;
    n INT := 2;
    part_idx INT;
    part_from BIGINT;
    part_to BIGINT;
    range_rel TEXT;
    max_part_idx INT;
BEGIN
    FOR rec IN
        SELECT
            topic,
            COALESCE(MAX(id), 0) AS max_id
        FROM messages_legacy
        GROUP BY topic
    LOOP
        slug := substr(md5(rec.topic), 1, 16);
        list_rel := 'messages_t_' || slug;
        seq_name := 'fill_seq_' || slug;

        EXECUTE format(
            'CREATE SEQUENCE %I CACHE 100',
            seq_name
        );

        IF rec.max_id > 0 THEN
            EXECUTE format(
                'SELECT setval(%L, %s)',
                seq_name,
                rec.max_id
            );
        END IF;

        INSERT INTO topic_meta (
            topic,
            next_id,
            acked_id,
            partition_size,
            partition_count,
            fill_seq_name
        ) VALUES (
            rec.topic,
            rec.max_id + 1,
            0,
            z,
            n,
            seq_name
        );

        EXECUTE format(
            'CREATE TABLE %I PARTITION OF messages FOR VALUES IN (%L) PARTITION BY RANGE (id)',
            list_rel,
            rec.topic
        );

        IF rec.max_id = 0 THEN
            max_part_idx := n;
        ELSE
            max_part_idx := ((rec.max_id - 1) / z)::INT + n;
        END IF;

        FOR part_idx IN 0..max_part_idx LOOP
            part_from := part_idx::BIGINT * z + 1;
            part_to := (part_idx::BIGINT + 1) * z + 1;
            range_rel := list_rel || '_p' || part_idx::TEXT;

            EXECUTE format(
                'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%s) TO (%s)',
                range_rel,
                list_rel,
                part_from,
                part_to
            );

            INSERT INTO topic_partitions (
                topic,
                partition_index,
                relname,
                id_from,
                id_to,
                partition_size,
                high_water_id,
                status
            ) VALUES (
                rec.topic,
                part_idx,
                range_rel,
                part_from,
                part_to,
                z,
                CASE
                    WHEN part_idx = ((rec.max_id - 1) / z)::INT AND rec.max_id > 0 THEN rec.max_id
                    ELSE 0
                END,
                CASE
                    WHEN part_idx = ((rec.max_id - 1) / z)::INT AND rec.max_id > 0 THEN 'active'
                    WHEN part_idx > ((rec.max_id - 1) / z)::INT THEN 'future'
                    ELSE 'sealed'
                END
            );
        END LOOP;
    END LOOP;
END $$;

INSERT INTO messages (
    id,
    inserted_at,
    topic,
    payload
)
SELECT
    id,
    inserted_at,
    topic,
    payload
FROM messages_legacy;

DROP TABLE messages_legacy;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
CREATE TABLE messages_legacy (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    topic TEXT NOT NULL,
    payload JSONB NOT NULL,
    CONSTRAINT v1_message_pkey PRIMARY KEY (topic, id)
);

INSERT INTO messages_legacy (
    id,
    inserted_at,
    topic,
    payload
)
OVERRIDING SYSTEM VALUE
SELECT
    id,
    inserted_at,
    topic,
    payload
FROM messages;

DROP TABLE messages CASCADE;

DO $$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN SELECT fill_seq_name FROM topic_meta LOOP
        EXECUTE format('DROP SEQUENCE IF EXISTS %I', rec.fill_seq_name);
    END LOOP;
END $$;

DROP TABLE topic_partitions;
DROP TABLE topic_meta;

ALTER TABLE messages_legacy RENAME TO messages;
-- +goose StatementEnd
