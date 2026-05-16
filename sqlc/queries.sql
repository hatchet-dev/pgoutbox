-- name: InsertMessage :copyfrom
INSERT INTO /*tmpl*/ messages /*tmpl*/ (
    id,
    topic,
    payload
) VALUES ($1, $2, $3);

-- name: EnsureTopicMeta :exec
INSERT INTO /*tmpl*/ topic_meta /*tmpl*/ (
    topic,
    partition_size,
    partition_count,
    fill_seq_name
) VALUES ($1, $2, $3, $4)
ON CONFLICT (topic) DO NOTHING;

-- name: GetTopicMetaForUpdate :one
SELECT
    *
FROM /*tmpl*/ topic_meta /*tmpl*/
WHERE
    topic = $1
FOR UPDATE;

-- name: AllocateTopicIds :one
UPDATE /*tmpl*/ topic_meta /*tmpl*/
SET
    next_id = next_id + sqlc.arg('count')::bigint,
    writes_since_resize = writes_since_resize + sqlc.arg('count')::bigint,
    last_write_at = CURRENT_TIMESTAMP
WHERE
    topic = sqlc.arg('topic')
RETURNING
    (next_id - sqlc.arg('count')::bigint)::bigint AS start_id,
    (next_id - 1)::bigint AS end_id,
    topic,
    next_id,
    acked_id,
    partition_size,
    partition_count,
    fill_seq_name,
    lease_holder,
    lease_expires_at,
    lease_high_id,
    writes_since_resize,
    acks_since_resize,
    last_write_at,
    last_process_at,
    resized_at;

-- name: UpdateTopicSizing :exec
UPDATE /*tmpl*/ topic_meta /*tmpl*/
SET
    partition_size = $2,
    partition_count = $3,
    writes_since_resize = 0,
    acks_since_resize = 0,
    resized_at = CURRENT_TIMESTAMP
WHERE
    topic = $1;

-- name: TryAcquireTopicLease :one
UPDATE /*tmpl*/ topic_meta /*tmpl*/
SET
    lease_holder = sqlc.arg('holder'),
    lease_expires_at = CURRENT_TIMESTAMP + INTERVAL '5 minutes',
    lease_high_id = NULL
WHERE
    topic = sqlc.arg('topic')
    AND (
        lease_holder IS NULL
        OR lease_expires_at < CURRENT_TIMESTAMP
        OR lease_holder = sqlc.arg('holder')
    )
RETURNING
    *;

-- name: SetTopicLeaseHighID :exec
UPDATE /*tmpl*/ topic_meta /*tmpl*/
SET
    lease_high_id = $2
WHERE
    topic = $1
    AND lease_holder = $3;

-- name: ReleaseTopicLease :exec
UPDATE /*tmpl*/ topic_meta /*tmpl*/
SET
    lease_holder = NULL,
    lease_expires_at = NULL,
    lease_high_id = NULL
WHERE
    topic = $1
    AND lease_holder = $2;

-- name: AckTopicMessages :exec
UPDATE /*tmpl*/ topic_meta /*tmpl*/
SET
    acked_id = $2,
    acks_since_resize = acks_since_resize + $3,
    last_process_at = CURRENT_TIMESTAMP,
    lease_holder = NULL,
    lease_expires_at = NULL,
    lease_high_id = NULL
WHERE
    topic = $1
    AND lease_holder = $4;

-- name: ListMessagesAfterAcked :many
SELECT
    *
FROM /*tmpl*/ messages /*tmpl*/
WHERE
    topic = $1
    AND id > $2
ORDER BY
    id ASC
LIMIT
    COALESCE(sqlc.narg('limit')::integer, 100);

-- name: UpsertTopicPartition :exec
INSERT INTO /*tmpl*/ topic_partitions /*tmpl*/ (
    topic,
    partition_index,
    relname,
    id_from,
    id_to,
    partition_size,
    status
) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (topic, partition_index) DO NOTHING;

-- name: UpdateTopicPartitionHighWater :exec
UPDATE /*tmpl*/ topic_partitions /*tmpl*/
SET
    high_water_id = GREATEST(high_water_id, $3),
    status = CASE
        WHEN status = 'future' THEN 'active'
        ELSE status
    END
WHERE
    topic = $1
    AND partition_index = $2;

-- name: SealTopicPartitionsUpTo :exec
UPDATE /*tmpl*/ topic_partitions /*tmpl*/
SET
    status = 'sealed'
WHERE
    topic = $1
    AND partition_index < $2
    AND status IN ('active', 'future');

-- name: SealFullyAckedActivePartitions :exec
UPDATE /*tmpl*/ topic_partitions /*tmpl*/
SET
    status = 'sealed'
WHERE
    topic = $1
    AND status = 'active'
    AND high_water_id > 0
    AND high_water_id <= $2;

-- name: ListDroppablePartitions :many
SELECT
    *
FROM /*tmpl*/ topic_partitions /*tmpl*/
WHERE
    topic = $1
    AND status = 'sealed'
    AND high_water_id > 0
    AND high_water_id <= $2
ORDER BY
    partition_index ASC;

-- name: MarkTopicPartitionDropped :exec
UPDATE /*tmpl*/ topic_partitions /*tmpl*/
SET
    status = 'dropped'
WHERE
    topic = $1
    AND partition_index = $2;
