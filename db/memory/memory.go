package db

import (
	"BaleBroker/pkg"
	"context"
	"errors"
	"fmt"
	"sync"
)

type MemoryDB struct {
	mutex    sync.RWMutex
	messages map[string]pkg.Message
}

func (memory *MemoryDB) Save(ctx context.Context, msg pkg.Message, subject string) error {
	memory.mutex.Lock()
	defer memory.mutex.Unlock()
	memory.messages[fmt.Sprintf("%v:%v", subject, msg.Id)] = msg
	return nil
}

func (memory *MemoryDB) Fetch(ctx context.Context, subject string, id int) (*pkg.Message, error) {
	memory.mutex.RLock()
	defer memory.mutex.RUnlock()
	if msg, ok := memory.messages[fmt.Sprintf("%v:%v", subject, id)]; ok {
		return &msg, nil
	} else {
		return nil, errors.New("expired id")
	}
}

func NewMemoryDB() *MemoryDB {
	return &MemoryDB{
		messages: make(map[string]pkg.Message),
	}
}
