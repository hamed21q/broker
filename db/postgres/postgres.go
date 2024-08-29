package db

import (
	db "BaleBroker/db/postgres/crud"
	"BaleBroker/pkg"
	"context"
	"github.com/jackc/pgx/v5/pgtype"
	"log"
	"sync"
	"time"
)

type PostgresDb struct {
	conn     *db.Queries
	messages []db.PublishParams
	ticker   *time.Ticker
	mu       sync.Mutex
	err      []chan error
}

func NewPostgresDb(conn db.DBTX) *PostgresDb {
	pd := &PostgresDb{
		conn:   db.New(conn),
		ticker: time.NewTicker(1 * time.Second),
	}
	go pd.flush()
	return pd
}

func (pd *PostgresDb) Save(ctx context.Context, msg pkg.Message, subject string) error {
	d := make(chan error)
	arg := db.PublishParams{
		ID:         int32(msg.Id),
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

func (pd *PostgresDb) flush() {
	for {
		select {
		case <-pd.ticker.C:
			pd.mu.Lock()
			_, err := pd.conn.Publish(context.Background(), pd.messages)
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

func (pd *PostgresDb) Fetch(ctx context.Context, subject string, id int) (*pkg.Message, error) {
	fetch, err := pd.conn.Fetch(ctx, int32(id))
	if err != nil {
		return nil, err
	}
	return &pkg.Message{
		Id:         int(fetch.ID),
		Body:       fetch.Body,
		Expiration: time.Duration(fetch.Expiration.Int32) * time.Second,
		CreatedAt:  fetch.CreateAt.Time,
	}, nil
}
