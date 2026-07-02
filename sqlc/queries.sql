-- name: InsertMessage :copyfrom
INSERT INTO /*tmpl*/ messages /*tmpl*/ (
    topic,
    payload
) VALUES ($1, $2);

-- name: AcquireMessagesByTopic :many
SELECT
    *
FROM /*tmpl*/ messages /*tmpl*/
WHERE
    topic = $1
LIMIT
    COALESCE(sqlc.narg('limit')::integer, 100)
FOR UPDATE SKIP LOCKED;

-- name: DeleteMessagesByIds :exec
DELETE FROM /*tmpl*/ messages /*tmpl*/
WHERE
    topic = @topic
    AND id = ANY(@ids::bigint[]);

-- name: GetOldestMessageInsertedAt :one
SELECT inserted_at FROM /*tmpl*/ messages /*tmpl*/
WHERE topic = $1
ORDER BY inserted_at ASC
LIMIT 1;

-- name: DeleteExpiredMessages :exec
DELETE FROM /*tmpl*/ messages /*tmpl*/
WHERE topic = @topic AND inserted_at < @cutoff;

-- name: InsertMaintenanceLeaseIfAbsent :exec
INSERT INTO /*tmpl*/ maintenance_leases /*tmpl*/ (topic, holder_id)
VALUES ($1, $2)
ON CONFLICT (topic) DO NOTHING;

-- name: SelectMaintenanceLeaseForUpdate :one
SELECT holder_id, acquired_at
FROM /*tmpl*/ maintenance_leases /*tmpl*/
WHERE topic = $1
FOR UPDATE;

-- name: UpdateMaintenanceLease :exec
UPDATE /*tmpl*/ maintenance_leases /*tmpl*/
SET holder_id = $2, acquired_at = NOW()
WHERE topic = $1;

-- name: InsertTopicIfAbsent :exec
INSERT INTO /*tmpl*/ topics /*tmpl*/ (topic, expiration_nanos)
VALUES ($1, $2)
ON CONFLICT (topic) DO UPDATE
SET expiration_nanos = COALESCE(topics.expiration_nanos, EXCLUDED.expiration_nanos);

-- name: GetTopicsWithExpiration :many
SELECT topic, expiration_nanos, exclusive_consumer_id, exclusive_consumer_expires_at
FROM /*tmpl*/ topics /*tmpl*/
WHERE expiration_nanos IS NOT NULL;

-- name: GetAllTopics :many
SELECT topic, expiration_nanos, exclusive_consumer_id, exclusive_consumer_expires_at
FROM /*tmpl*/ topics /*tmpl*/;

-- name: GetTopicForUpdate :one
SELECT exclusive_consumer_id, exclusive_consumer_expires_at
FROM /*tmpl*/ topics /*tmpl*/
WHERE topic = $1
FOR UPDATE;

-- name: SetTopicExclusiveConsumer :exec
UPDATE /*tmpl*/ topics /*tmpl*/
SET exclusive_consumer_id = $2,
    exclusive_consumer_expires_at = $3
WHERE topic = $1;

-- name: RenewTopicExclusiveConsumer :exec
UPDATE /*tmpl*/ topics /*tmpl*/
SET exclusive_consumer_expires_at = $3
WHERE topic = $1 AND exclusive_consumer_id = $2;