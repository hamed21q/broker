-- name: BatchPublish :copyfrom
INSERT INTO messages ("id", "subject", "body", "expiration") VALUES ($1, $2, $3, $4);

-- name: Publish :exec
INSERT INTO messages ("id", "subject", "body", "expiration") VALUES ($1, $2, $3, $4);


-- name: Fetch :one
select * from messages where id = $1;

-- name: LastId :many
select subject, max(id) from messages group by subject;