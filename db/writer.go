package db

import (
	"BaleBroker/pkg"
	"context"
	"log"
	"sync"
	"time"
)

type Writer interface {
	Write(context.Context, pkg.PublishParams) error
}

type BatchWriter struct {
	DB       DB
	messages []pkg.PublishParams
	mu       sync.Mutex
	counter  int
	err      []chan error
	ticker   *time.Ticker
}

func NewBatchWriter(db DB) *BatchWriter {
	bw := &BatchWriter{DB: db, ticker: time.NewTicker(200 * time.Millisecond)}
	go bw.timeFlush()
	return bw
}

const BatchSize = 10000
const Timeout = 25

func (bw *BatchWriter) timeFlush() {
	for {
		select {
		case <-bw.ticker.C:
			bw.mu.Lock()
			messages, clients := bw.copyMessages()
			bw.mu.Unlock()
			if len(messages) > 0 {
				err := bw.DB.BatchSave(context.Background(), messages)
				bw.notifyError(err, clients)
			}
		}
	}
}

func (bw *BatchWriter) copyMessages() ([]pkg.PublishParams, []chan error) {
	bw.counter = 0
	messages := make([]pkg.PublishParams, len(bw.messages))
	copy(messages, bw.messages)
	errs := make([]chan error, len(bw.messages))
	copy(errs, bw.err)
	bw.messages = bw.messages[:0:0]
	bw.err = bw.err[:0:0]
	return messages, errs
}

func (bw *BatchWriter) Write(ctx context.Context, arg pkg.PublishParams) error {
	ctx, span := pkg.Tracer.Start(ctx, "pgx.BatchPostgresDb.Save")
	defer span.End()
	result := make(chan error)
	bw.mu.Lock()
	bw.err = append(bw.err, result)
	bw.messages = append(bw.messages, arg)
	bw.counter++
	if bw.counter == BatchSize {
		messages, clients := bw.copyMessages()
		err := bw.DB.BatchSave(ctx, messages)
		if err != nil {
			log.Println("batch save error:", err)
		}
		bw.notifyError(err, clients)
	}
	bw.mu.Unlock()
	return <-result
}

func (bw *BatchWriter) notifyError(err error, clients []chan error) {
	var wg sync.WaitGroup
	wg.Add(len(clients))
	for i, client := range clients {
		go func(client chan error, i int) {
			defer wg.Done()
			select {
			case client <- err:
			case <-time.After(time.Duration(Timeout) * time.Millisecond):
			}
		}(client, i)
	}
	wg.Wait()
}

type ConcurrentWriter struct {
	DB DB
}

func NewConcurrentWriter(db DB) *ConcurrentWriter {
	return &ConcurrentWriter{DB: db}
}

func (cw *ConcurrentWriter) Write(ctx context.Context, arg pkg.PublishParams) error {
	ctx, span := pkg.Tracer.Start(ctx, "ConcurrentWriter.Write")
	defer span.End()
	return cw.DB.ConcurrentSave(ctx, arg)
}
