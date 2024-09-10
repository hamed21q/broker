package pkg

import (
	"time"
)

type Message struct {
	Id         int
	Body       string
	Expiration time.Duration
	CreatedAt  time.Time
}

type PublishParams struct {
	ID         int
	Subject    string
	Body       string
	Expiration int
}
