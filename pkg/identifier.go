package pkg

import (
	db "BaleBroker/db/postgres/crud"
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"sync"
)

type Identifier interface {
	GetID(context.Context, string) int
}

type SequentialIdentifier struct {
	mutex     *sync.Mutex
	sequences map[string]*SubjectSequence
}

func NewSequentialIdentifier() *SequentialIdentifier {
	return &SequentialIdentifier{
		sequences: make(map[string]*SubjectSequence),
		mutex:     &sync.Mutex{},
	}
}

func (si *SequentialIdentifier) AddSequence(subject string, init int) error {
	si.mutex.Lock()
	defer si.mutex.Unlock()

	if _, exists := si.sequences[subject]; exists {
		return errors.New(fmt.Sprintf("%v already exists", subject))
	}

	newSequence := &SubjectSequence{
		mu:     &sync.Mutex{},
		nextID: init,
	}
	si.sequences[subject] = newSequence
	return nil
}

func (si *SequentialIdentifier) GetID(ctx context.Context, subject string) int {
	ctx, span := Tracer.Start(ctx, "pkg.SequentialIdentifier.GetID")
	defer span.End()
	seq := si.getSubjectMutex(subject)
	return seq.GetID()
}

func (si *SequentialIdentifier) getSubjectMutex(subject string) *SubjectSequence {
	si.mutex.Lock()
	defer si.mutex.Unlock()
	if seq, exists := si.sequences[subject]; exists {
		return seq
	}

	newSequence := &SubjectSequence{
		mu:     &sync.Mutex{},
		nextID: 1,
	}
	si.sequences[subject] = newSequence
	return newSequence
}

type SubjectSequence struct {
	mu     *sync.Mutex
	nextID int
}

func (sq *SubjectSequence) GetID() int {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	id := sq.nextID
	sq.nextID++
	return id
}

type Sync interface {
	Sync(ctx context.Context) error
}

type PGSync struct {
	queries    *db.Queries
	pool       *pgxpool.Pool
	identifier *SequentialIdentifier
}

func NewPGSync(queries *db.Queries, pool *pgxpool.Pool, identifier *SequentialIdentifier) *PGSync {
	return &PGSync{queries: queries, pool: pool, identifier: identifier}
}

func (pgs *PGSync) Sync(ctx context.Context) error {
	log.Println("syncing pg with Sequential Identifier")
	conn, err := pgs.pool.Acquire(context.Background())
	if err != nil {
		return err
	}
	defer conn.Release()
	ids, err := pgs.queries.LastId(ctx, conn)
	if err != nil {
		return err
	}

	for _, id := range ids {
		err := pgs.identifier.AddSequence(id.Subject, int(id.Max.(int32))+1)
		if err != nil {
			log.Println(err.Error())
		}
	}
	return nil
}
