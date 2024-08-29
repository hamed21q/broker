package db

import (
	"BaleBroker/pkg"
	"context"
)

type DB interface {
	Save(context.Context, pkg.Message, string) error
	Fetch(context.Context, string, int) (*pkg.Message, error)
}
