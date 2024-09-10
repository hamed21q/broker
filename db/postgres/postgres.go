package db

import (
	db "BaleBroker/db/postgres/crud"
	"BaleBroker/pkg"
	"context"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"time"
)

type PostgresDb struct {
	queries *db.Queries
	pool    *pgxpool.Pool
}

func NewPostgresDb(pool *pgxpool.Pool, queries *db.Queries) *PostgresDb {
	return &PostgresDb{pool: pool, queries: queries}
}

func (pd *PostgresDb) Fetch(ctx context.Context, id int, subject string) (pkg.Message, error) {
	conn, err := pd.pool.Acquire(ctx)
	if err != nil {
		return pkg.Message{}, err
	}
	defer conn.Release()
	fetch, err := pd.queries.Fetch(ctx, conn, int32(id))
	if err != nil {
		return pkg.Message{}, err
	}
	return pkg.Message{
		Id:         id,
		Body:       fetch.Body,
		Expiration: time.Duration(fetch.Expiration.Int32) * time.Second,
		CreatedAt:  fetch.CreateAt.Time,
	}, nil
}

func (pd *PostgresDb) BatchSave(ctx context.Context, messages []pkg.PublishParams) error {
	conn, err := pd.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	var arg []db.BatchPublishParams
	for _, message := range messages {
		arg = append(arg, db.BatchPublishParams{
			ID:         int32(message.ID),
			Body:       message.Body,
			Subject:    message.Subject,
			Expiration: pgtype.Int4{Int32: int32(message.Expiration), Valid: true},
		})
	}
	_, err = pd.queries.BatchPublish(ctx, conn, arg)
	if err != nil {
		log.Println("batch save failed:", err)
		return err
	}

	return nil
}

func (pd *PostgresDb) ConcurrentSave(ctx context.Context, arg pkg.PublishParams) error {
	conn, err := pd.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	err = pd.queries.Publish(ctx, conn, db.PublishParams{
		ID:         int32(arg.ID),
		Body:       arg.Body,
		Subject:    arg.Subject,
		Expiration: pgtype.Int4{Int32: int32(arg.Expiration), Valid: true},
	})
	if err != nil {
		return err
	}
	return nil
}
