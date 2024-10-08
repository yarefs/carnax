package api

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTopicPartitionHash_String(t *testing.T) {
	res := newTopicHash("foo", 123)
	assert.Equal(t, "foo-123", res.String())
}
