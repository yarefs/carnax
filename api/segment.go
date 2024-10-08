package api

import (
	"bytes"
	"encoding/binary"
	"github.com/hashicorp/go-uuid"
	apiv1 "github.com/yarefs/carnax/gen/api/v1"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/proto"
	"hash/crc32"
	"log"
)

// offset -> byte position
type Index struct {
	Offset   uint64
	Position uint64
}

type TopicPartitionSegment struct {
	datas *bytes.Buffer // .log
	index []Index       // .index
	// .timeindex TBD

	len   uint64
	start uint64

	// watermark of this segment
	low, high uint64

	bytesSinceLastIndexWrite uint64
	config                   CarnaxConfig
}

func (s *TopicPartitionSegment) Append(payload *apiv1.Record, offset uint64) {
	// specify watermark for this topic
	if offset < s.low {
		s.low = offset
	}

	if s.shouldWriteIndex() {
		s.index = append(s.index, Index{
			Offset:   offset,
			Position: s.len,
		})
		s.bytesSinceLastIndexWrite = 0
	}

	recordWithOffs := &apiv1.RecordWithOffset{
		Record: payload,
		Offset: offset,
	}
	recordWithOffsBytes, err := proto.Marshal(recordWithOffs)
	if err != nil {
		panic("Failed to marshal record")
	}
	checksum := crc32.ChecksumIEEE(recordWithOffsBytes)

	amt, err := protodelim.MarshalTo(s.datas, &apiv1.CommittedRecord{
		Record:   recordWithOffs,
		Checksum: checksum,
	})
	if err != nil {
		panic(err)
	}

	nextOffs := offset + uint64(amt)
	if nextOffs > s.high {
		s.high = nextOffs
	}

	s.len += uint64(amt)
	s.bytesSinceLastIndexWrite += uint64(amt)
}

type IndexFile []Index

func (i IndexFile) Search(offs uint64) Index {
	if len(i) == 0 {
		return Index{offs, 0}
	}

	log.Println("IndexFile Search:", offs, "Index Dump:")
	for _, idx := range i {
		log.Println(idx.Offset, "->", idx.Position)
	}

	left, right := 0, len(i)-1
	best := 0

	for left <= right {
		mid := left + ((right - left) / 2)
		if i[mid].Offset == offs {
			return i[mid]
		}

		if i[mid].Offset < offs {
			left = mid + 1
			best = mid
		} else {
			right = mid - 1
		}
	}

	return i[best]
}

func IndexFromBytes(data []byte) []Index {
	buf := bytes.NewBuffer(data)

	var res []Index

	for buf.Len() > 0 {
		offset := uint64(0)
		err := binary.Read(buf, binary.BigEndian, &offset)
		if err != nil {
			panic(err)
		}

		position := uint64(0)
		err = binary.Read(buf, binary.BigEndian, &position)
		if err != nil {
			panic(err)
		}

		res = append(res, Index{
			Offset:   offset,
			Position: position,
		})
	}

	return res
}

// segment.index.bytes
// This configuration controls the size of the index that maps offsets to file positions.
// We preallocate this index file and shrink it only after log rolls. You generally should not need to change this setting
func (s *TopicPartitionSegment) Index() []byte {
	// NOTE: in the future we can opt for an unsafe
	// packing into a []byte array.
	// we could also seprate them into different arrays for better
	// caching

	buf := new(bytes.Buffer)

	for _, index := range s.index {
		// NOTE: Kafka is binary.BigEndian

		err := binary.Write(buf, binary.BigEndian, index.Offset)
		if err != nil {
			panic(err)
		}

		err = binary.Write(buf, binary.BigEndian, index.Position)
		if err != nil {
			panic(err)
		}
	}

	return buf.Bytes()
}

func (s *TopicPartitionSegment) Data() []byte {
	return s.datas.Bytes()
}

func (s *TopicPartitionSegment) shouldWriteIndex() bool {
	return s.bytesSinceLastIndexWrite > uint64(s.config.LogIndexIntervalBytes)
}

func NewTopicPartitionSegment(config CarnaxConfig, start ...uint64) *TopicPartitionSegment {
	/*
		Experiment idea: pre-allocating segments of X size, e.g. 8mb or 4mb
		right now this buffer is re-sized as we go.
	*/

	ts := &TopicPartitionSegment{
		datas: new(bytes.Buffer),

		// specify watermarks at peak values
		// so writes after nullify to the real range.
		low:  ^uint64(0),
		high: 0,

		// FIXME(FELIX): we should only link to the topic partition related
		// config here...
		config: config,
	}

	if len(start) == 1 {
		ts.start = start[0]
	}

	return ts
}

type TopicSegment struct {
	activeSegments map[uint32]*TopicPartitionSegment

	// this is primarily used for tracing
	id string
}

// SegmentIncrement is the additional padding to enforce
// new segments to always be incremented by an offset of 1.
// this introduces an idea of 'time' in segments rather as, if we
// write a message to the current segment and do not publish anything to it,
// by the time we have to roll the next segment (every 250ms by default)
// we will over-write the segment due to the starting offset being identical.
const SegmentIncrement = 1

func NewTopicSegment(prev ...*TopicSegment) *TopicSegment {
	id, err := uuid.GenerateUUID()
	if err != nil {
		log.Fatalln(err)
	}

	res := &TopicSegment{
		activeSegments: map[uint32]*TopicPartitionSegment{},
		id:             id,
	}

	if len(prev) == 1 {
		previous := prev[0]
		for i, p := range previous.activeSegments {
			res.activeSegments[i] = NewTopicPartitionSegment(p.config, p.high+SegmentIncrement)
		}
	}

	return res
}

type SegmentLog struct {
	topicSegments map[string]*TopicSegment
}

func NewSegmentLog() *SegmentLog {
	return &SegmentLog{
		topicSegments: map[string]*TopicSegment{},
	}
}

func (s *SegmentLog) Size() uint64 {
	var total uint64
	for _, t := range s.topicSegments {
		for _, s := range t.activeSegments {
			total += s.len
		}
	}
	return total
}

var (
	KiB = 1024
	MiB = 1 * KiB
)

type cachedSegment struct {
	data   []byte
	offset uint64
}

type SegmentCache struct {
	cache []*cachedSegment
}

func (s *SegmentCache) findNearest(offs uint64) *cachedSegment {
	left, right := 0, len(s.cache)-1
	var best *cachedSegment

	for left <= right {
		mid := left + ((right - left) / 2)
		if s.cache[mid].offset == offs {
			return s.cache[mid]
		}

		if s.cache[mid].offset < offs {
			left = mid + 1
			best = s.cache[mid]
		} else {
			right = mid - 1
		}
	}

	return best
}

func (s *SegmentCache) cacheSeg(offs uint64, reader []byte) {
	// TODO(FELIX): measure this. and document it.
	if len(s.cache) > 0 {
		if offs < s.cache[len(s.cache)-1].offset {
			s.cache = []*cachedSegment{}
		}
	}

	s.cache = append(s.cache, &cachedSegment{
		data:   reader,
		offset: offs,
	})
}

func newSegmentCache() *SegmentCache {
	return &SegmentCache{
		cache: []*cachedSegment{},
	}
}
