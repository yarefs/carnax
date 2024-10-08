package api

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_findExactMatch(t *testing.T) {
	// given some mocked segment paths
	options := []string{
		"orders-0/00000000000000000000.index",
		"orders-0/00000000000000000000.log",
		"orders-0/00000000000000001010.index",
		"orders-0/00000000000000001010.log",
		"orders-0/00000000000000002010.index",
		"orders-0/00000000000000002010.log",
		"orders-0/00000000000000003010.index",
		"orders-0/00000000000000003010.log",
		"orders-0/00000000000000004010.index",
		"orders-0/00000000000000004010.log",
		"orders-0/00000000000000005010.index",
		"orders-0/00000000000000005010.log",
		"orders-0/00000000000000006010.index",
		"orders-0/00000000000000006010.log",
		"orders-0/00000000000000007010.index",
		"orders-0/00000000000000007010.log",
		"orders-0/00000000000000008010.index",
		"orders-0/00000000000000008010.log",
	}

	// and an offset to find the segment for
	offset := uint64(3010)

	// when we look for it in the paths
	res := findLowestSegmentFile(options, offset)

	// we find the segment that contains the offset.
	assert.Equal(t, "00000000000000003010", res)
}

func Test_findNearestIndex(t *testing.T) {
	// given some mocked segment paths
	options := []string{
		"orders-0/00000000000000000000.index",
		"orders-0/00000000000000000000.log",
		"orders-0/00000000000000001010.index",
		"orders-0/00000000000000001010.log",
		"orders-0/00000000000000002010.index",
		"orders-0/00000000000000002010.log",
		"orders-0/00000000000000003010.index",
		"orders-0/00000000000000003010.log",
		"orders-0/00000000000000004010.index",
		"orders-0/00000000000000004010.log",
		"orders-0/00000000000000005010.index",
		"orders-0/00000000000000005010.log",
		"orders-0/00000000000000006010.index",
		"orders-0/00000000000000006010.log",
		"orders-0/00000000000000007010.index",
		"orders-0/00000000000000007010.log",
		"orders-0/00000000000000008010.index",
		"orders-0/00000000000000008010.log",
	}

	// and an offset to find the segment for
	offset := uint64(4500)

	// when we look for it in the paths
	res := findLowestSegmentFile(options, offset)

	// we find the segment that contains the offset.
	assert.Equal(t, "00000000000000004010", res)
}

func Test_findNearestCachedSegment(t *testing.T) {
	sc := newSegmentCache()

	sc.cacheSeg(100, []byte("seg 1"))
	sc.cacheSeg(250, []byte("seg 2"))
	sc.cacheSeg(450, []byte("seg 3"))

	res := sc.findNearest(249)
	assert.NotNil(t, res)

	assert.Equal(t, "seg 1", string(res.data))
}

func Test_findNearestCachedSegment_ClearLowerSegs(t *testing.T) {
	sc := newSegmentCache()

	sc.cacheSeg(100, []byte("seg 1"))
	sc.cacheSeg(250, []byte("seg 2"))
	sc.cacheSeg(450, []byte("seg 3"))

	sc.cacheSeg(100, []byte("seg 4"))
	sc.cacheSeg(250, []byte("seg 5"))

	res := sc.findNearest(150)
	assert.NotNil(t, res)

	assert.Equal(t, "seg 4", string(res.data))
}
