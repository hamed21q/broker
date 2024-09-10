package cql

import (
	gocql "github.com/gocql/gocql"
)

func NewCqlDb(conn *gocql.Session) *Queries {
	return &Queries{conn: conn}
}

type Queries struct {
	conn *gocql.Session
}
