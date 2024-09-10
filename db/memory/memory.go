package db

import (
	"BaleBroker/pkg"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type MemoryDB struct {
	mutex    sync.RWMutex
	messages map[string]pkg.Message
}

func NewMemoryDB() *MemoryDB {
	return &MemoryDB{
		messages: make(map[string]pkg.Message),
	}
}

func (memory *MemoryDB) Fetch(ctx context.Context, id int, subject string) (pkg.Message, error) {
	memory.mutex.RLock()
	defer memory.mutex.RUnlock()
	if msg, ok := memory.messages[fmt.Sprintf("%v:%v", subject, id)]; ok {
		return msg, nil
	} else {
		return pkg.Message{}, errors.New("expired id")
	}
}

func (memory *MemoryDB) BatchSave(ctx context.Context, messages []pkg.PublishParams) error {
	panic("not applicable")
}

func (memory *MemoryDB) ConcurrentSave(ctx context.Context, arg pkg.PublishParams) error {
	ctx, span := pkg.Tracer.Start(ctx, "db.MemoryDB.Save")
	defer span.End()
	memory.mutex.Lock()
	defer memory.mutex.Unlock()
	memory.messages[fmt.Sprintf("%v:%v", arg.Subject, arg.ID)] = pkg.Message{
		Id:         arg.ID,
		Body:       arg.Body,
		Expiration: time.Duration(arg.Expiration),
		CreatedAt:  time.Now(),
	}
	return nil
}
