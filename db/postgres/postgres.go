package db

import (
	db "BaleBroker/db/postgres/crud"
	"BaleBroker/pkg"
	"context"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"sync"
	"time"
)

type PostgresDb struct {
	queries *db.Queries
	pool    *pgxpool.Pool
}

func (pd *PostgresDb) Fetch(ctx context.Context, subject string, id int) (*pkg.Message, error) {
	conn, err := pd.pool.Acquire(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	fetch, err := pd.queries.Fetch(ctx, conn, int32(id))
	if err != nil {
		return nil, err
	}
	return &pkg.Message{
		Id:         id,
		Body:       fetch.Body,
		Expiration: time.Duration(fetch.Expiration.Int32) * time.Second,
		CreatedAt:  fetch.CreateAt.Time,
	}, nil
}

type BatchPostgresDb struct {
	PostgresDb
	messages []db.BatchPublishParams
	ticker   *time.Ticker
	mu       sync.Mutex
	err      []chan error
	capacity chan bool
}

const BatchSize = 1000

func NewBatchPostgresDb(pool *pgxpool.Pool, queries *db.Queries) *BatchPostgresDb {
	pd := &BatchPostgresDb{
		PostgresDb: PostgresDb{queries: queries, pool: pool},
		ticker:     time.NewTicker(time.Second),
		capacity:   make(chan bool, 1),
	}
	go pd.flush()
	return pd
}

func (pd *BatchPostgresDb) Save(ctx context.Context, msg pkg.Message, subject string) error {
	d := make(chan error)
	arg := db.BatchPublishParams{
		ID:         int32(msg.Id),
		Expiration: pgtype.Int4{Int32: int32(msg.Expiration.Seconds()), Valid: true},
		Body:       msg.Body,
		Subject:    subject,
	}
	pd.mu.Lock()
	pd.err = append(pd.err, d)
	pd.messages = append(pd.messages, arg)
	cp := len(pd.messages)
	pd.mu.Unlock()
	if cp >= BatchSize {
		pd.capacity <- true
	}
	err := <-d
	if err != nil {
		return err
	}
	return nil
}

func (pd *BatchPostgresDb) notifyError(err error, errs []chan error) {
	for _, res := range errs {
		res <- err
		close(res)
	}
}

func (pd *BatchPostgresDb) flush() {
	persist := func() {
		pd.mu.Lock()
		msgs := pd.messages
		errs := pd.err
		pd.messages = pd.messages[:0:0]
		pd.err = pd.err[:0:0]
		pd.mu.Unlock()

		conn, err := pd.pool.Acquire(context.Background())
		if err != nil {
			pd.notifyError(err, errs)
			return
		}
		defer conn.Release()
		_, err = pd.queries.BatchPublish(context.Background(), conn, msgs)
		pd.notifyError(err, errs)
	}
	for {
		select {
		case <-pd.capacity:
			persist()
		case <-pd.ticker.C:
			persist()
		}
	}
}

type ParallelPostgresDb struct {
	PostgresDb
}

func (pd *ParallelPostgresDb) Save(ctx context.Context, msg pkg.Message, subject string) error {
	arg := db.PublishParams{
		ID:         int32(msg.Id),
		Expiration: pgtype.Int4{Int32: int32(msg.Expiration.Seconds()), Valid: true},
		Body:       msg.Body,
		Subject:    subject,
	}
	conn, err := pd.pool.Acquire(context.Background())
	if err != nil {
		return err
	}
	defer conn.Release()
	err = pd.queries.Publish(ctx, conn, arg)
	if err != nil {
		return err
	}
	return nil
}

func NewParallelPostgresDb(pool *pgxpool.Pool, queries *db.Queries) *ParallelPostgresDb {
	return &ParallelPostgresDb{
		PostgresDb: PostgresDb{queries: queries, pool: pool},
	}
}
