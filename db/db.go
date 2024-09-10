package db

import (
	"BaleBroker/pkg"
	"context"
)

type DB interface {
	BatchSave(context.Context, []pkg.PublishParams) error
	ConcurrentSave(context.Context, pkg.PublishParams) error
	Fetch(context.Context, int, string) (pkg.Message, error)
}

type Store struct {
	writer Writer
	db     DB
}

func NewStore(writer Writer, db DB) *Store {
	return &Store{writer: writer, db: db}
}

func (store *Store) Publish(ctx context.Context, arg pkg.Message, subject string) error {
	err := store.writer.Write(ctx, pkg.PublishParams{
		ID:         arg.Id,
		Body:       arg.Body,
		Expiration: int(arg.Expiration.Seconds()),
		Subject:    subject,
	})
	if err != nil {
		return err
	}
	return nil
}

func (store *Store) Fetch(ctx context.Context, id int, subject string) (pkg.Message, error) {
	fetch, err := store.db.Fetch(ctx, id, subject)
	if err != nil {
		return pkg.Message{}, err
	}
	return fetch, nil
}
