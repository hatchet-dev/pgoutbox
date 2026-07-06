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
-- Lease liveness defers to the holder's consumer session; a holder with no
-- session row (written by a pre-session version of pgoutbox) is judged by
-- acquired_at against the caller-computed staleness cutoff, exactly the rule
-- that version applies to itself. FOR UPDATE OF l leaves the session row
-- unlocked, since every other lease held by the same instance reads it too.
SELECT
    l.holder_id,
    COALESCE(s.expires_at < now(), l.acquired_at < @stale_cutoff::timestamptz, true)::boolean AS lease_expired
FROM /*tmpl*/ maintenance_leases /*tmpl*/ l
LEFT JOIN /*tmpl*/ consumer_sessions /*tmpl*/ s ON s.consumer_id = l.holder_id
WHERE l.topic = @topic
FOR UPDATE OF l;

-- name: UpdateMaintenanceLease :exec
UPDATE /*tmpl*/ maintenance_leases /*tmpl*/
SET holder_id = $2, acquired_at = NOW()
WHERE topic = $1;

-- name: InsertMaintenanceLeases :many
-- Claim path for topics that have no lease row yet. RETURNING reports the
-- subset this instance actually won; a concurrent instance may have inserted
-- a row first, in which case the conflict arm drops that topic silently.
INSERT INTO /*tmpl*/ maintenance_leases /*tmpl*/ (topic, holder_id)
SELECT unnest(@topics::text[]), @holder_id::uuid
ON CONFLICT (topic) DO NOTHING
RETURNING topic;

-- name: ClaimMaintenanceLeases :many
-- Takeover path for lease rows whose holder is gone: its session lapsed, or
-- (pre-session holder) its acquired_at went stale. SKIP LOCKED lets racing
-- claimants skip contested rows instead of queueing; the loser claims other
-- topics on its next scan. consumer_sessions (the small side, kept small by
-- the retention sweep) is narrowed once into a MATERIALIZED CTE so the
-- planner hashes it instead of choosing its own join shape against
-- maintenance_leases.
WITH sessions AS MATERIALIZED (
    SELECT consumer_id, (expires_at < now())::boolean AS expired
    FROM /*tmpl*/ consumer_sessions /*tmpl*/
),
claimable AS (
    SELECT l.topic
    FROM /*tmpl*/ maintenance_leases /*tmpl*/ l
    LEFT JOIN sessions s ON s.consumer_id = l.holder_id
    WHERE l.topic = ANY(@topics::text[])
      AND COALESCE(s.expired, l.acquired_at < @stale_cutoff::timestamptz)
    FOR UPDATE OF l SKIP LOCKED
)
UPDATE /*tmpl*/ maintenance_leases /*tmpl*/ l
SET holder_id = @holder_id::uuid, acquired_at = now()
FROM claimable c
WHERE l.topic = c.topic
RETURNING l.topic;

-- name: ReleaseMaintenanceLease :exec
-- Dormancy release: dormant topics carry no lease row at all. Guarded by
-- holder so a lease that was concurrently taken over is never deleted.
DELETE FROM /*tmpl*/ maintenance_leases /*tmpl*/
WHERE topic = @topic AND holder_id = @holder_id;

-- name: BumpTopicActivityIfStale :exec
-- Flush-path counterpart of the insert trigger's activity bump. The trigger
-- bumps with FOR UPDATE SKIP LOCKED, and ProcessMessages holds the topic row
-- lock for its whole transaction — so on a continuously-processed topic the
-- trigger can be starved out of every bump. The flush transaction already
-- holds that lock, so it compensates by bumping here after processing
-- messages, gated to the same 30-second granularity as the trigger.
UPDATE /*tmpl*/ topics /*tmpl*/
SET last_inserted_at = now()
WHERE topic = @topic
  AND (last_inserted_at IS NULL OR last_inserted_at < now() - interval '30 seconds');

-- name: InsertTopicIfAbsent :exec
INSERT INTO /*tmpl*/ topics /*tmpl*/ (topic, expiration_nanos, last_inserted_at)
VALUES ($1, $2, now())
ON CONFLICT (topic) DO UPDATE
SET expiration_nanos = COALESCE(topics.expiration_nanos, EXCLUDED.expiration_nanos);

-- name: GetTopicsWithExpiration :many
SELECT topic, expiration_nanos, exclusive_consumer_id, exclusive_consumer_expires_at
FROM /*tmpl*/ topics /*tmpl*/
WHERE expiration_nanos IS NOT NULL;

-- name: GetMaintenanceCandidateTopics :many
-- Topics that may still hold unexpired messages (last insert newer than
-- TTL + slack; NULL activity is treated as active, belt-and-braces for rows
-- that predate the column), plus any topics force-included by the catch-up
-- sweep, together with their maintenance-lease state. Lease liveness defers
-- to the holder's consumer session, falling back to the staleness cutoff for
-- pre-session holders (see SelectMaintenanceLeaseForUpdate). The plan is
-- pinned to a deliberate shape: one seq scan of topics (the activity
-- predicate is unindexable by design), maintenance_leases joined on its
-- primary key, and consumer_sessions — the small side, kept small by the
-- retention sweep — narrowed once into a MATERIALIZED CTE and hashed.
WITH sessions AS MATERIALIZED (
    SELECT consumer_id, (expires_at < now())::boolean AS expired
    FROM /*tmpl*/ consumer_sessions /*tmpl*/
)
SELECT
    t.topic,
    t.expiration_nanos,
    COALESCE(l.holder_id = @holder_id::uuid, false)::boolean AS held_by_me,
    (l.topic IS NULL)::boolean AS unleased,
    COALESCE(s.expired, l.acquired_at < @stale_cutoff::timestamptz, true)::boolean AS lease_expired
FROM /*tmpl*/ topics /*tmpl*/ t
LEFT JOIN /*tmpl*/ maintenance_leases /*tmpl*/ l ON l.topic = t.topic
LEFT JOIN sessions s ON s.consumer_id = l.holder_id
WHERE COALESCE(t.expiration_nanos, sqlc.narg('default_expiration_nanos')::bigint, 0) > 0
  AND (
    t.topic = ANY(@include_topics::text[])
    OR t.last_inserted_at IS NULL
    OR extract(epoch FROM (now() - t.last_inserted_at)) < COALESCE(t.expiration_nanos, sqlc.narg('default_expiration_nanos')::bigint)::float8 / 1e9 + @slack_seconds::float8
  );

-- name: GetTopicsWithMessages :many
-- Loose index scan emulated over the (topic, id) PK: O(distinct topics)
-- index probes the planner cannot degrade to a seq scan of messages. Used by
-- the catch-up sweep to find topics holding messages regardless of activity.
WITH RECURSIVE tops AS (
    (SELECT topic FROM /*tmpl*/ messages /*tmpl*/ ORDER BY topic LIMIT 1)
    UNION ALL
    SELECT (
        SELECT m.topic FROM /*tmpl*/ messages /*tmpl*/ m
        WHERE m.topic > tops.topic
        ORDER BY m.topic
        LIMIT 1
    )
    FROM tops
    WHERE tops.topic IS NOT NULL
)
SELECT topic::text AS topic FROM tops WHERE topic IS NOT NULL;

-- name: CountLiveConsumerSessions :one
SELECT COUNT(*)
FROM /*tmpl*/ consumer_sessions /*tmpl*/
WHERE expires_at > now();

-- name: DeleteExpiredConsumerSessions :exec
-- GC for session rows left behind by dead instances (one per process
-- lifetime, since instance ids are per-NewOutbox). Bounding the table keeps
-- CountLiveConsumerSessions and the lease-liveness joins on a small seq scan.
-- Deleting a row is equivalent to it being expired, so the cutoff buffer is
-- caution, not correctness; a live instance wrongly swept just re-inserts
-- itself on its next heartbeat.
DELETE FROM /*tmpl*/ consumer_sessions /*tmpl*/
WHERE expires_at < @cutoff::timestamptz;

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