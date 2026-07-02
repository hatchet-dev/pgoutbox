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
-- A NULL exclusive_consumer_expires_at on the topics row means "defer to the
-- holder's consumer session for liveness"; a non-NULL value is a per-topic
-- override (release, grace-period expiry, or a lease written by a pre-session
-- version of pgoutbox) and wins the COALESCE. FOR UPDATE OF t locks only the
-- topics row, not the joined session row shared by every topic the holder owns.
SELECT
    t.exclusive_consumer_id,
    COALESCE(t.exclusive_consumer_expires_at, s.expires_at) AS exclusive_consumer_expires_at
FROM /*tmpl*/ topics /*tmpl*/ t
LEFT JOIN /*tmpl*/ consumer_sessions /*tmpl*/ s ON s.consumer_id = t.exclusive_consumer_id
WHERE t.topic = $1
FOR UPDATE OF t;

-- name: SetTopicExclusiveConsumer :exec
UPDATE /*tmpl*/ topics /*tmpl*/
SET exclusive_consumer_id = $2,
    exclusive_consumer_expires_at = $3
WHERE topic = $1;

-- name: RenewTopicExclusiveConsumer :exec
UPDATE /*tmpl*/ topics /*tmpl*/
SET exclusive_consumer_expires_at = $3
WHERE topic = $1 AND exclusive_consumer_id = $2;

-- name: UpsertConsumerSession :exec
INSERT INTO /*tmpl*/ consumer_sessions /*tmpl*/ (consumer_id, expires_at)
VALUES ($1, $2)
ON CONFLICT (consumer_id) DO UPDATE
SET expires_at = EXCLUDED.expires_at;