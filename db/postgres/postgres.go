package db

import (
	db "BaleBroker/db/postgres/crud"
	"BaleBroker/pkg"
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"sync"
	"time"
)

type PostgresDb struct {
	conn *db.Queries
	pool *pgxpool.Pool
}

func (pd *PostgresDb) Fetch(ctx context.Context, subject string, id int) (*pkg.Message, error) {
	conn, err := pd.pool.Acquire(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	fetch, err := pd.conn.Fetch(ctx, conn, fmt.Sprintf("%v:%v", subject, id))
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

func NewBatchPostgresDb(pool *pgxpool.Pool) *BatchPostgresDb {
	pd := &BatchPostgresDb{
		PostgresDb: PostgresDb{conn: db.New(), pool: pool},
		ticker:     time.NewTicker(100 * time.Millisecond),
	}
	go pd.flush()
	return pd
}

func (pd *BatchPostgresDb) Save(ctx context.Context, msg pkg.Message, subject string) error {
	d := make(chan error)
	arg := db.BatchPublishParams{
		ID:         fmt.Sprintf("%v:%v", subject, msg.Id),
		Expiration: pgtype.Int4{Int32: int32(msg.Expiration.Seconds()), Valid: true},
		Body:       msg.Body,
		Subject:    subject,
	}
	pd.mu.Lock()
	pd.err = append(pd.err, d)
	pd.messages = append(pd.messages, arg)
	pd.mu.Unlock()
	if err := <-d; err != nil {
		return err
	}
	return nil
}

func (pd *BatchPostgresDb) flush() {
	for {
		select {
		case <-pd.ticker.C:
			conn, err := pd.pool.Acquire(context.Background())
			if err != nil {
				continue
			}
			defer conn.Release()
			pd.mu.Lock()
			_, err = pd.conn.BatchPublish(context.Background(), conn, pd.messages)
			if err != nil {
				log.Printf("error on writing to db %v\n", err.Error())
			}
			for _, res := range pd.err {
				res <- err
				close(res)
			}
			pd.messages = pd.messages[:0:0]
			pd.err = pd.err[:0:0]
			pd.mu.Unlock()
		default:
		}
	}
}

type ParallelPostgresDb struct {
	PostgresDb
}

func (pd *ParallelPostgresDb) Save(ctx context.Context, msg pkg.Message, subject string) error {
	arg := db.PublishParams{
		ID:         fmt.Sprintf("%v:%v", subject, msg.Id),
		Expiration: pgtype.Int4{Int32: int32(msg.Expiration.Seconds()), Valid: true},
		Body:       msg.Body,
		Subject:    subject,
	}
	conn, err := pd.pool.Acquire(context.Background())
	if err != nil {
		return err
	}
	defer conn.Release()
	err = pd.conn.Publish(ctx, conn, arg)
	if err != nil {
		return err
	}
	return nil
}

func NewParallelPostgresDb(pool *pgxpool.Pool) *ParallelPostgresDb {
	return &ParallelPostgresDb{
		PostgresDb: PostgresDb{conn: db.New(), pool: pool},
	}
}
