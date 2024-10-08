package internal

import (
	"errors"
	"log"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
)

// InMemoryObjectStore ...
type InMemoryObjectStore struct {
	d     sync.Map
	count atomic.Int64

	mu   sync.Mutex
	keys []string
}

func (i *InMemoryObjectStore) List(s string) []string {
	var out []string
	for _, k := range i.keys {
		if strings.HasPrefix(k, s) {
			out = append(out, k)
		}
	}
	return out
}

func NewInMemoryObjectStore() *InMemoryObjectStore {
	return &InMemoryObjectStore{d: sync.Map{}}
}

func (i *InMemoryObjectStore) Put(s string, bytes []byte) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	log.Println("PUT:", s, len(bytes))

	// discard duplicate keys.
	if idx := slices.Index(i.keys, s); idx == -1 {
		i.keys = append(i.keys, s)
	}

	i.count.Add(1)
	i.d.Store(s, bytes)
	return nil
}

func (i *InMemoryObjectStore) Get(s string) ([]byte, error) {
	value, ok := i.d.Load(s)
	if !ok {
		return []byte{}, errors.New("not found")
	}
	return value.([]byte), nil
}

func (i *InMemoryObjectStore) Delete(s string) error {
	i.d.Delete(s)
	i.count.Add(-1)
	return nil
}

func (i *InMemoryObjectStore) Count() int {
	return int(i.count.Load())
}
