package db

import (
	db "BaleBroker/db/postgres/crud"
	"BaleBroker/pkg"
	"context"
	"errors"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"sync"
	"time"
)

type PostgresDb struct {
	queries *db.Queries
	pool    *pgxpool.Pool
}

func (pd *PostgresDb) Fetch(ctx context.Context, subject string, id int) (*pkg.Message, error) {
	conn, err := pd.pool.Acquire(ctx)
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
	mu       sync.Mutex
	counter  int
	err      []chan error
	ticker   *time.Ticker
}

const BatchSize = 1000

func NewBatchPostgresDb(pool *pgxpool.Pool, queries *db.Queries) *BatchPostgresDb {
	pd := &BatchPostgresDb{
		PostgresDb: PostgresDb{queries: queries, pool: pool},
		ticker:     time.NewTicker(time.Second),
	}
	go pd.TimeFlush()
	return pd
}

func (pd *BatchPostgresDb) TimeFlush() {
	for {
		select {
		case <-pd.ticker.C:
			pd.mu.Lock()
			messages, errs := pd.getMessages()
			pd.mu.Unlock()
			if len(messages) > 0 {
				pd.persist(context.Background(), messages, errs)
			}
		}
	}
}

func (pd *BatchPostgresDb) getMessages() ([]db.BatchPublishParams, []chan error) {
	pd.counter = 0
	messages := make([]db.BatchPublishParams, len(pd.messages))
	copy(messages, pd.messages)
	errs := make([]chan error, len(pd.messages))
	copy(errs, pd.err)
	pd.messages = pd.messages[:0:0]
	pd.err = pd.err[:0:0]
	return messages, errs
}

func (pd *BatchPostgresDb) Save(ctx context.Context, msg pkg.Message, subject string) error {
	ctx, span := pkg.Tracer.Start(ctx, "pgx.BatchPostgresDb.Save")
	defer span.End()
	result := make(chan error)
	arg := db.BatchPublishParams{
		ID:         int32(msg.Id),
		Expiration: pgtype.Int4{Int32: int32(msg.Expiration.Seconds()), Valid: true},
		Body:       msg.Body,
		Subject:    subject,
	}
	pd.mu.Lock()
	pd.err = append(pd.err, result)
	pd.messages = append(pd.messages, arg)
	pd.counter++
	if pd.counter == BatchSize {
		messages, errs := pd.getMessages()
		go pd.persist(ctx, messages, errs)
		log.Printf("%d messages published", len(messages))
	}
	pd.mu.Unlock()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second * 6):
		return errors.New("timeout")
	}
}

func (pd *BatchPostgresDb) persist(ctx context.Context, messages []db.BatchPublishParams, errs []chan error) {
	conn, err := pd.pool.Acquire(ctx)
	if err != nil {
		pd.notifyError(err, errs)
		return
	}
	defer conn.Release()
	_, err = pd.queries.BatchPublish(ctx, conn, messages)
	pd.notifyError(err, errs)
}

func (pd *BatchPostgresDb) notifyError(err error, errs []chan error) {
	for _, res := range errs {
		res <- err
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
	ctx, span := pkg.Tracer.Start(ctx, "pgx.ParallelPostgresDb.Save.Acquire")
	conn, err := pd.pool.Acquire(ctx)
	span.End()
	if err != nil {
		return err
	}
	defer conn.Release()
	ctx, span = pkg.Tracer.Start(ctx, "pgx.ParallelPostgresDb.Save.Write")
	err = pd.queries.Publish(ctx, conn, arg)
	span.End()
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
