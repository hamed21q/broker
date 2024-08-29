// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.26.0
// source: broker.sql

package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"
)

const fetch = `-- name: Fetch :one
select id, subject, body, expiration, create_at from messages where id = $1
`

func (q *Queries) Fetch(ctx context.Context, id int32) (Message, error) {
	row := q.db.QueryRow(ctx, fetch, id)
	var i Message
	err := row.Scan(
		&i.ID,
		&i.Subject,
		&i.Body,
		&i.Expiration,
		&i.CreateAt,
	)
	return i, err
}

type PublishParams struct {
	ID         int32
	Subject    string
	Body       string
	Expiration pgtype.Int4
}
