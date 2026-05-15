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