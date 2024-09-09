package broker

import (
	"BaleBroker/db"
	"BaleBroker/pkg"
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

type BaleBroker struct {
	mu         sync.RWMutex
	ctx        context.Context
	cancel     func()
	subs       map[string][]chan pkg.Message
	db         db.DB
	identifier pkg.Identifier
}

func NewBaleBroker(db db.DB, id pkg.Identifier) *BaleBroker {
	ctx, cancel := context.WithCancel(context.Background())
	return &BaleBroker{
		ctx:        ctx,
		cancel:     cancel,
		subs:       make(map[string][]chan pkg.Message),
		db:         db,
		identifier: id,
	}
}

func (broker *BaleBroker) Close() error {
	broker.cancel()
	return nil
}

func (broker *BaleBroker) Publish(ctx context.Context, subject string, msg pkg.Message) (int, error) {
	ctx, span := pkg.Tracer.Start(ctx, "BaleBroker.Publish")
	defer span.End()
	if broker.Closed(ctx) {
		return 0, ErrUnavailable
	}
	msg.Id = broker.identifier.GetID(ctx, subject)
	msg.CreatedAt = time.Now()
	err := broker.db.Save(ctx, msg, subject)
	if err != nil {
		log.Printf(err.Error())
		return 0, ErrStoreFailed
	}
	broker.mu.RLock()
	listeners, ok := broker.subs[subject]
	broker.mu.RUnlock()
	if ok {
		for _, sub := range listeners {
			sub <- msg
		}
	}
	return msg.Id, nil
}

func (broker *BaleBroker) Subscribe(ctx context.Context, subject string) (<-chan pkg.Message, error) {
	if broker.Closed(ctx) {
		return nil, ErrUnavailable
	}

	broker.mu.Lock()
	defer broker.mu.Unlock()
	c := make(chan pkg.Message)
	broker.subs[subject] = append(broker.subs[subject], c)
	return c, nil
}

func (broker *BaleBroker) Fetch(ctx context.Context, subject string, id int) (*pkg.Message, error) {
	if broker.Closed(ctx) {
		return nil, ErrUnavailable
	}

	msg, err := broker.db.Fetch(ctx, subject, id)
	if err != nil {
		return nil, ErrInvalidID
	}
	if msg.CreatedAt.Add(msg.Expiration).Before(time.Now()) {
		fmt.Println(msg.CreatedAt.Add(msg.Expiration), time.Now())
		return nil, ErrExpiredID
	}
	return msg, err
}

func (broker *BaleBroker) Closed(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	case <-broker.ctx.Done():
		return true
	default:
		return false
	}
}
