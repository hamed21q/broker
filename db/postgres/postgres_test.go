package db

import (
	db "BaleBroker/db/postgres/crud"
	"BaleBroker/pkg"
	"BaleBroker/utils"
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"log"
	"sync"
	"testing"
	"time"
)

var (
	mainCtx = context.Background()
	pd      *BatchPostgresDb
	id      pkg.Identifier
)

func TestMain(m *testing.M) {
	id = pkg.NewSequentialIdentifier()
	conn, err := pgxpool.New(context.Background(), "postgresql://root:1qaz@localhost:5433/broker?sslmode=disable")
	if err != nil {
		log.Fatalf("database unreachable %v\n", err.Error())
	}
	queries := db.New()
	pd = NewBatchPostgresDb(conn, queries)
	m.Run()
}

func TestFlush(t *testing.T) {
	var wg sync.WaitGroup

	for i := 0; i < 50008; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := pd.Save(mainCtx, createMessageWithExpire(60), "bale")
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
}

func createMessageWithExpire(duration time.Duration) pkg.Message {
	body := utils.RandomString(7)

	return pkg.Message{
		Id:         id.GetID(mainCtx, "bale"),
		Body:       body,
		Expiration: duration,
	}
}
