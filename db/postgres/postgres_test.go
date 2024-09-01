package db

import (
	"BaleBroker/pkg"
	"BaleBroker/utils"
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
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
	conn, err := pgxpool.New(context.Background(), "postgresql://root:1qaz@localhost:5432/broker?sslmode=disable")
	if err != nil {
		log.Fatalf("database unreachable %v\n", err.Error())
	}
	pd = NewBatchPostgresDb(conn)
	m.Run()
}

func TestFlush(t *testing.T) {
	ch := make(chan error)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			require.NoError(t, <-ch)
		}
	}()
	for i := 0; i < 100; i++ {
		go func() {
			ch <- pd.Save(mainCtx, createMessageWithExpire(60), "bale")
		}()
	}
	wg.Wait()
}

func createMessageWithExpire(duration time.Duration) pkg.Message {
	body := utils.RandomString(7)

	return pkg.Message{
		Id:         id.GetID("bale"),
		Body:       body,
		Expiration: duration,
	}
}
