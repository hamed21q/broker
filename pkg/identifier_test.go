package pkg

import (
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestSequentialIDs(t *testing.T) {
	identifier := NewSequentialIdentifier()

	id1 := identifier.GetID("subject1")
	require.Equal(t, 1, id1)

	id2 := identifier.GetID("subject1")
	require.Equal(t, 2, id2)

	id3 := identifier.GetID("subject2")
	require.Equal(t, 1, id3)

	id4 := identifier.GetID("subject2")
	require.Equal(t, 2, id4)

	id5 := identifier.GetID("subject1")
	require.Equal(t, 3, id5)

}

func TestConcurrentAccess(t *testing.T) {
	identifier := NewSequentialIdentifier()
	var wg sync.WaitGroup

	const numGoroutines = 100
	const numIDs = 1000
	var mu sync.Mutex
	ids := make(map[string][]int)

	generateIDs := func(subject string) {
		defer wg.Done()
		for i := 0; i < numIDs; i++ {
			id := identifier.GetID(subject)

			mu.Lock()
			ids[subject] = append(ids[subject], id)
			mu.Unlock()
		}
	}

	wg.Add(numGoroutines * 2)
	for i := 0; i < numGoroutines; i++ {
		go generateIDs("subject1")
		go generateIDs("subject2")
	}
	wg.Wait()

	verifySequential(t, ids["subject1"])

	verifySequential(t, ids["subject2"])
}

func verifySequential(t *testing.T, ids []int) {
	require.Len(t, ids, 100000)
	seen := make(map[int]bool)
	for _, id := range ids {
		if seen[id] {
			t.Errorf("duplicate ID found: %d", id)
		}
		seen[id] = true
	}

	for i := 1; i <= len(ids); i++ {
		if !seen[i] {
			t.Errorf("missing ID: %d", i)
		}
	}
}
