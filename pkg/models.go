package pkg

import "time"

type Message struct {
	Id         int
	Body       string
	Expiration time.Duration
	CreatedAt  time.Time
}
