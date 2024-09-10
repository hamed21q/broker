package cql

import (
	"BaleBroker/pkg"
	"BaleBroker/utils"
	"context"
	"errors"
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	"log"
	"testing"
	"time"
)

var qu *Queries
var idGen pkg.Identifier
var session *gocql.Session
var mainCtx = context.Background()

func TestMain(m *testing.M) {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "bale"
	cluster.Consistency = gocql.Quorum
	cluster.Port = 9097

	var err error
	session, err = cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	idGen = pkg.NewSequentialIdentifier()

	qu = NewCqlDb(session)

	m.Run()
}

func setupSuite() func(t *testing.T) {
	log.Println("setup suite")
	idGen = pkg.NewSequentialIdentifier()

	return func(t *testing.T) {
		err := session.Query("TRUNCATE bale.messages").Exec()
		if err != nil {
			log.Fatal(err)
		}
	}
}

func TestQueries_ConcurrentSave(t *testing.T) {
	teardown := setupSuite()
	defer teardown(t)
	id := idGen.GetID(mainCtx, "ali")
	err := qu.ConcurrentSave(mainCtx, pkg.PublishParams{
		ID:         id,
		Subject:    "ali",
		Body:       "hello world",
		Expiration: 12,
	})
	require.NoError(t, err)

	msg, err := qu.Fetch(mainCtx, id, "")
	require.NoError(t, err)
	require.Equal(t, "hello world", msg.Body)
	require.Equal(t, 12*time.Nanosecond, msg.Expiration)
	require.Equal(t, id, msg.Id)
}

func TestQueries_Fetch(t *testing.T) {
	teardown := setupSuite()
	defer teardown(t)

	msg, err := qu.Fetch(mainCtx, 85858585, "")
	require.Equal(t, msg, pkg.Message{})
	require.Error(t, err)
	require.True(t, errors.Is(err, gocql.ErrNotFound))
}

func TestQueries_BatchSave(t *testing.T) {
	teardown := setupSuite()
	defer teardown(t)

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

	err := qu.BatchSave(mainCtx, args)
	require.NoError(t, err)
	for _, id := range ids {
		msg, err := qu.Fetch(mainCtx, id, "")
		require.NoError(t, err, id)
		require.NotNil(t, msg)
	}
}

func TestQueries_GetMaxIds(t *testing.T) {
	teardown := setupSuite()
	defer teardown(t)

	var args []pkg.PublishParams
	for i := 0; i < 10; i++ {
		id := idGen.GetID(mainCtx, "ali")
		arg := pkg.PublishParams{
			ID:         id,
			Subject:    utils.RandomString(8),
			Body:       utils.RandomString(50),
			Expiration: int(utils.RandomInt(0, 100)),
		}
		args = append(args, arg)
	}

	err := qu.BatchSave(mainCtx, args)
	require.NoError(t, err)

	ids, err := qu.GetMaxIds(mainCtx)
	require.NoError(t, err)
	require.Equal(t, 10, len(ids))
}
