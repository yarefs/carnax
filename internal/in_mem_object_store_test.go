package internal

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInMemoryObjectStore_List(t *testing.T) {
	store := NewInMemoryObjectStore()

	for partIdx := 0; partIdx < 10; partIdx += 1 {
		for i := 0; i < 100; i += 1 {
			key := fmt.Sprintf("topic-%d/%020d", partIdx, uint64(i)*100)
			store.Put(key, []byte("hello, world"))
		}
	}

	paths := store.List("topic-5/")
	assert.Len(t, paths, 100)
}
