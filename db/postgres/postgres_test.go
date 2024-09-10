package db

import (
	db "BaleBroker/db/postgres/crud"
	"BaleBroker/pkg"
	"BaleBroker/utils"
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"log"
	"testing"
)

var (
	mainCtx = context.Background()
	pd      *PostgresDb
	idGen   pkg.Identifier
)

func TestMain(m *testing.M) {
	idGen = pkg.NewSequentialIdentifier()
	conn, err := pgxpool.New(context.Background(), "postgresql://root:1qaz@localhost:9095/broker?sslmode=disable")
	if err != nil {
		log.Fatalf("database unreachable %v\n", err.Error())
	}
	queries := db.New()
	pd = NewPostgresDb(conn, queries)
	m.Run()
}

func TestPostgresDb_BatchSave(t *testing.T) {
	count := 10
	var args []pkg.PublishParams
	var ids []int
	for i := 0; i < count; i++ {
		id := idGen.GetID(mainCtx, "ali")
		ids = append(ids, id)
		arg := pkg.PublishParams{
			ID:         id,
			Subject:    utils.RandomString(8),
			Body:       utils.RandomString(50),
			Expiration: int(utils.RandomInt(0, 100)),
		}
		args = append(args, arg)
	}

	err := pd.BatchSave(mainCtx, args)
	require.NoError(t, err)
	for _, id := range ids {
		msg, err := pd.Fetch(mainCtx, id, "")
		require.NoError(t, err, id)
		require.NotNil(t, msg)
	}
}
