// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.26.0
// source: copyfrom.go

package db

import (
	"context"
)

// iteratorForPublish implements pgx.CopyFromSource.
type iteratorForPublish struct {
	rows                 []PublishParams
	skippedFirstNextCall bool
}

func (r *iteratorForPublish) Next() bool {
	if len(r.rows) == 0 {
		return false
	}
	if !r.skippedFirstNextCall {
		r.skippedFirstNextCall = true
		return true
	}
	r.rows = r.rows[1:]
	return len(r.rows) > 0
}

func (r iteratorForPublish) Values() ([]interface{}, error) {
	return []interface{}{
		r.rows[0].ID,
		r.rows[0].Subject,
		r.rows[0].Body,
		r.rows[0].Expiration,
	}, nil
}

func (r iteratorForPublish) Err() error {
	return nil
}

func (q *Queries) Publish(ctx context.Context, arg []PublishParams) (int64, error) {
	return q.db.CopyFrom(ctx, []string{"messages"}, []string{"id", "subject", "body", "expiration"}, &iteratorForPublish{rows: arg})
}