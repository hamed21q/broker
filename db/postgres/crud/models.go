// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.26.0

package db

import (
	"github.com/jackc/pgx/v5/pgtype"
)

type Message struct {
	ID         string
	Subject    string
	Body       string
	Expiration pgtype.Int4
	CreateAt   pgtype.Timestamptz
}
