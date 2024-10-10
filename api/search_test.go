package api

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	apiv1 "github.com/yarefs/carnax/gen/api/v1"
	"github.com/yarefs/carnax/internal"
	"google.golang.org/protobuf/encoding/protodelim"
	"testing"
)

// nit: timeIndexToBytes
// this is technically a reimplimentation of the topic segment work
func timeIndexToBytes(index []*apiv1.TimeIndex) []byte {
	buf := new(bytes.Buffer)
	for _, i := range index {
		if _, err := protodelim.MarshalTo(buf, i); err != nil {
			panic(err)
		}
	}
	return buf.Bytes()
}

func Test_findOffsetByTimestamp(t *testing.T) {
	segmentsList := []string{
		"orders-0/00000000000000000000.timeindex",
		"orders-0/00000000000000006010.timeindex",
		"orders-0/00000000000000008010.timeindex",
		"orders-0/00000000000000009010.timeindex",
	}

	// given some timeindex
	store := internal.NewInMemoryObjectStore()
	err := store.Put("orders-0/00000000000000000000.timeindex", timeIndexToBytes([]*apiv1.TimeIndex{
		{
			Timestamp: 0x1,
			Offset:    0,
		},
	}))
	assert.NoError(t, err)

	err = store.Put("orders-0/00000000000000006010.timeindex", timeIndexToBytes([]*apiv1.TimeIndex{
		{
			Timestamp: 0x10,
			Offset:    6010,
		},
	}))
	assert.NoError(t, err)

	pred := func(u uint64, ts int64) bool {
		key := SegmentName("orders", 0, u).Format(SegmentTimeIndex)
		data, err := store.Get(key)
		if err != nil {
			return false
		}

		index := TimeIndexFromBytes(data)
		if len(index) == 0 {
			panic("empty index")
		}

		return ts >= index[0].Timestamp
	}

	timestamp := int64(0x10)

	best := findLowestSegmentWithNearbyTimestamp(segmentsList, timestamp, pred)

	assert.Equal(t, 1, best)
}

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
