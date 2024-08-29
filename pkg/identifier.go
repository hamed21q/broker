package pkg

import "sync"

type Identifier interface {
	GetID(string) int
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

func (si *SequentialIdentifier) GetID(subject string) int {
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
