package api

import (
	"bytes"
	"github.com/hashicorp/go-uuid"
	apiv1 "github.com/yarefs/carnax/gen/api/v1"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/proto"
	"hash/crc32"
	"log"
	"time"
)

type TopicPartitionSegment struct {
	dataLog   *bytes.Buffer // .log
	offsIndex *bytes.Buffer // .index
	timeIndex *bytes.Buffer

	len   uint64
	start uint64

	// watermark of this segment
	low, high     uint64
	lowTs, highTs int64

	bytesSinceLastIndexWrite uint64
	config                   CarnaxConfig
}

func (s *TopicPartitionSegment) CommitRecord(rec *apiv1.Record, offset uint64) {
	/**
	Here be dragons. There are some very crucially ordered
	steps here to ensure particular guarantees.
	*/

	// specify watermark for this topic
	if offset < s.low {
		s.low = offset
	}

	// the initial length of the segment
	// which is the position this message will be written at.
	initialWritePosition := s.len

	writeIndex := s.shouldWriteIndex()

	if writeIndex {
		_, err := protodelim.MarshalTo(s.offsIndex, &apiv1.Index{
			Offset:   offset,
			Position: s.len,
		})
		if err != nil {
			panic(err)
		}
		s.bytesSinceLastIndexWrite = 0
	}

	// In most cases the metadata will not be set
	// however, it is permissible, though unsafe, for
	// producing clients to mangle this data if absolutely necessary.
	if rec.Metadata != nil {
		panic("invalid state: metadata is immutable state that is generated when committed to the log")
	} else {
		rec.Metadata = &apiv1.Metadata{
			Timestamp:               time.Now().UnixMilli(),
			Offset:                  offset,
			KeyUncompressedLength:   int32(len(rec.Key)),
			ValueUncompressedLength: int32(len(rec.Payload)),
			RelativeOffset:          initialWritePosition,
		}
	}

	// Commit the timestamp watermark
	timestamp := rec.Metadata.Timestamp
	if rec.Headers.Timestamp != 0 {
		// the header timestamp takes precedence
		timestamp = rec.Headers.Timestamp
	}
	if timestamp < s.lowTs {
		s.lowTs = timestamp
	}
	if timestamp > s.highTs {
		s.highTs = timestamp
	}

	// Write a timestamp index log entry if necessary
	if writeIndex {
		_, err := protodelim.MarshalTo(s.timeIndex, &apiv1.TimeIndex{
			Offset:    offset,
			Timestamp: timestamp,
		})
		if err != nil {
			panic(err)
		}
	}

	// Defensive measure to ensure checksums are only set once.
	if rec.Checksum != 0 {
		panic("invalid state: checksum must not be set")
	}

	// Marhsal the record ONCE and generate a checksum
	// the ordering here is CRUCIAL! as we have now committed
	// metadata + so the checksum would include this.
	recordBytes, err := proto.Marshal(rec)
	if err != nil {
		panic("Failed to marshal record")
	}

	// Write the final checksum just before we commit to the log.
	rec.Checksum = crc32.ChecksumIEEE(recordBytes)

	// Commit the record to log
	numBytesWritten, err := protodelim.MarshalTo(s.dataLog, rec)
	if err != nil {
		panic(err)
	}

	// Vaguely calculates the next offset (it's not gauranteed with "SegmentIncrement")
	nextOffs := offset + uint64(numBytesWritten)
	if nextOffs > s.high {
		s.high = nextOffs
	}

	s.len += uint64(numBytesWritten)
	s.bytesSinceLastIndexWrite += uint64(numBytesWritten)
}

type IndexFile []*apiv1.Index

type TimeIndexFile []*apiv1.TimeIndex

func (i IndexFile) Search(offs uint64) *apiv1.Index {
	if len(i) == 0 {
		return nil
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

func TimeIndexFromBytes(data []byte) TimeIndexFile {
	buf := bytes.NewReader(data)

	var res TimeIndexFile
	for buf.Len() > 0 {
		timeIndexEntry := new(apiv1.TimeIndex)
		if err := protodelim.UnmarshalFrom(buf, timeIndexEntry); err != nil {
			panic(err)
		}
		res = append(res, timeIndexEntry)
	}
	return res
}

func IndexFromBytes(data []byte) IndexFile {
	buf := bytes.NewReader(data)

	var res IndexFile

	for buf.Len() > 0 {
		indexEntry := new(apiv1.Index)
		if err := protodelim.UnmarshalFrom(buf, indexEntry); err != nil {
			panic(err)
		}

		res = append(res, indexEntry)
	}

	return res
}

func (s *TopicPartitionSegment) Index() []byte {
	return s.offsIndex.Bytes()
}

func (s *TopicPartitionSegment) Data() []byte {
	return s.dataLog.Bytes()
}

func (s *TopicPartitionSegment) TimeIndex() []byte {
	return s.timeIndex.Bytes()
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
		dataLog:   new(bytes.Buffer),
		offsIndex: new(bytes.Buffer),
		timeIndex: new(bytes.Buffer),

		// specify watermarks at peak values
		// so writes after nullify to the real range.
		low:  ^uint64(0),
		high: 0,

		// NOTE: Ideally we should only link to the topic partition related
		// config here but for now we take the whole thing
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
	// NOTE: this is an attempt at a strategy to keep the cached
	// segments low and should be properly verified.
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
