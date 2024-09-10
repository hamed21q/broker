package cql

import (
	"BaleBroker/pkg"
	"context"
	"github.com/gocql/gocql"
	"log"
	"time"
)

type PublishRequestParams struct {
	ID         int
	Subject    string
	Body       string
	Expiration int
}

const concurrentPublishQuery = `INSERT INTO messages ("id", "subject", "body", "expiration") VALUES (?, ?, ?, ?);`

func (queries *Queries) ConcurrentSave(ctx context.Context, arg pkg.PublishParams) error {
	err := queries.conn.Query(concurrentPublishQuery, arg.ID, arg.Subject, arg.Body, arg.Expiration).WithContext(ctx).Exec()
	if err != nil {
		log.Println(err, arg.Body)
		return err
	}
	return nil
}

type FetchResponse struct {
	ID         int
	Subject    string
	Body       string
	Expiration int
	CreatedAt  time.Time
}

const fetchQuery = `SELECT id, subject, body, expiration, created_at FROM messages WHERE id = ?;`

func (queries *Queries) Fetch(ctx context.Context, id int, subject string) (pkg.Message, error) {
	var response FetchResponse
	query := queries.conn.Query(fetchQuery, id).WithContext(ctx)
	err := query.Scan(&response.ID, &response.Subject, &response.Body, &response.Expiration, &response.CreatedAt)
	return pkg.Message{
		Id:         response.ID,
		Body:       response.Body,
		CreatedAt:  response.CreatedAt,
		Expiration: time.Duration(response.Expiration),
	}, err
}

const batchPublishQuery = `INSERT INTO messages ("id", "subject", "body", "expiration") VALUES (?, ?, ?, ?);`

func (queries *Queries) BatchSave(ctx context.Context, args []pkg.PublishParams) error {
	batch := queries.conn.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	for _, arg := range args {
		batch.Query(batchPublishQuery, arg.ID, arg.Subject, arg.Body, arg.Expiration)
	}
	err := queries.conn.ExecuteBatch(batch)
	if err != nil {
		log.Printf("err occured on inserting batch to cassandra: %v", err)
		return err
	}
	return nil
}

const maxIDQuery = `select subject, max(id) from messages group by subject;`

type MaxIdResponse struct {
	Subject string
	MaxId   int
}

func (queries *Queries) GetMaxIds(ctx context.Context) ([]MaxIdResponse, error) {
	var responses []MaxIdResponse

	iter := queries.conn.Query(maxIDQuery).WithContext(ctx).Iter()

	var response MaxIdResponse
	for iter.Scan(&response.Subject, &response.MaxId) {
		responses = append(responses, response)
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	return responses, nil
}
