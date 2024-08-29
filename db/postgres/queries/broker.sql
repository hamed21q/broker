-- name: Publish :copyfrom
INSERT INTO messages ("id", "subject", "body", "expiration") VALUES ($1, $2, $3, $4);


-- name: Fetch :one
select * from messages where id = $1;