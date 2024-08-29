package db

import (
	"BaleBroker/pkg"
	"context"
	"errors"
	"sync"
)

type MemoryDB struct {
	mutex    sync.RWMutex
	messages map[int]pkg.Message
}

func (memory *MemoryDB) Save(ctx context.Context, msg pkg.Message, subject string) error {
	memory.mutex.Lock()
	defer memory.mutex.Unlock()
	memory.messages[msg.Id] = msg
	return nil
}

func (memory *MemoryDB) Fetch(ctx context.Context, subject string, id int) (*pkg.Message, error) {
	memory.mutex.RLock()
	defer memory.mutex.RUnlock()
	if msg, ok := memory.messages[id]; ok {
		return &msg, nil
	} else {
		return nil, errors.New("expired id")
	}
}

func NewMemoryDB() *MemoryDB {
	return &MemoryDB{
		messages: make(map[int]pkg.Message),
	}
}
