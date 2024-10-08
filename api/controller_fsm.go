package api

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/hashicorp/raft"
	apiv1 "github.com/yarefs/carnax/gen/api/v1"
	commandv1 "github.com/yarefs/carnax/gen/command/v1"
	"github.com/yarefs/carnax/internal"
	"github.com/yarefs/murmur2-go/murmur"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/proto"
	"io"
	"log"
	"sync"
)

type fsmSnapshot struct {
	data []byte
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		if _, err := sink.Write(f.data); err != nil {
			return err
		}

		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		log.Println("Failed to persist", err)
		sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {}

// CarnaxControllerFSM implements the finite-state-machine
// mechanism used in hashicorp/raft
type CarnaxControllerFSM CarnaxController

func (f *CarnaxControllerFSM) Apply(l *raft.Log) interface{} {
	var cmd commandv1.Command
	if err := proto.Unmarshal(l.Data, &cmd); err != nil {
		log.Fatal(err)
	}

	switch cmd.Type {
	case commandv1.CommandType_COMMAND_TYPE_CREATE_TOPIC:
		ct := cmd.GetCreateTopic()
		return f.applyCreateTopic(ct.Config)
	case commandv1.CommandType_COMMAND_TYPE_RESERVE_ADDRESS:
		ra := cmd.GetReserveAddress()
		return f.applyReserveAddress(ra.Topic, ra.Key)
	case commandv1.CommandType_COMMAND_TYPE_WRITE_MESSAGE:
		wm := cmd.GetWriteMessage()
		return f.applyWrite(wm.Topic, wm.Record, wm.Address)
	case commandv1.CommandType_COMMAND_TYPE_FLUSH_SEGMENT:
		return f.applyFlushSegment()
	case commandv1.CommandType_COMMAND_TYPE_READ_MESSAGE:
		read := cmd.GetReadMessage()
		return f.applyReadMessage(read.Topic, read.Address, read.ResetPoint)
	case commandv1.CommandType_COMMAND_TYPE_SUBSCRIBE_TOPIC:
		sc := cmd.GetSubscribeTopic()
		return f.applySubscribeTopic(sc.Id, sc.Topic, sc.ClientId)
	case commandv1.CommandType_COMMAND_TYPE_REBALANCE_PARTITIONS:
		rb := cmd.GetRebalancePartitions()
		return f.applyRebalancePartitions(rb.ConsumerGroupId)
	case commandv1.CommandType_COMMAND_TYPE_POLL_MESSAGES:
		pc := cmd.GetPollMessages()
		return f.applyPollMessages(pc.ClientId, pc.ConsumerGroupId)
	case commandv1.CommandType_COMMAND_TYPE_COMMIT_SYNC:
		sc := cmd.GetCommitSync()
		return f.applyCommitSync(sc.ConsumerGroupId)
	case commandv1.CommandType_COMMAND_TYPE_SOFT_COMMIT:
		sc := cmd.GetSoftCommit()
		return f.applySoftCommit(sc.ConsumerGroupId, sc.ClientId, sc.PartitionIndex, sc.Offset)
	case commandv1.CommandType_COMMAND_TYPE_SEEK_OFFSET:
		so := cmd.GetSeekOffset()
		return f.applySeekOffset(so.ConsumerGroupId, so.ClientId, so.TopicPartition, so.SeekIndex)
	default:
		panic("Command is not handled " + cmd.Type.String())
	}
}

func (f *CarnaxControllerFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.lsMu.Lock()
	defer f.lsMu.Unlock()

	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(f.state)
	if err != nil {
		panic(err)
	}

	return &fsmSnapshot{
		data: buf.Bytes(),
	}, nil
}

func (f *CarnaxControllerFSM) Restore(snapshot io.ReadCloser) error {
	newState := &sharedMessageLogState{}
	if err := gob.NewDecoder(snapshot).Decode(&newState); err != nil {
		panic(err)
	}

	// no locking necessary according to the docs.
	f.state = newState

	return nil
}

func (f *CarnaxControllerFSM) applyReserveAddress(topic string, key []byte) interface{} {
	f.lsMu.Lock()
	defer f.lsMu.Unlock()

	if _, ok := f.state.topicConfig[topic]; !ok {
		panic(ErrTopicNotFound)
	}

	tps, ok := f.state.segment.topicSegments[topic]
	if !ok {
		panic(ErrTopicNotFound)
	}

	if _, ok := f.state.topicConfig[topic]; !ok {
		panic("No such topic config for " + topic)
	}

	numParts := f.state.topicConfig[topic].PartitionCount

	partIndex := f.assignPartition(topic, key, numParts)

	log.Printf("KEY '%s'; TOPIC '%s'; PIDX %d", string(key), string(topic), partIndex)

	assignedPartitionLog, ok := tps.activeSegments[partIndex]
	if !ok {
		assignedPartitionLog = NewTopicPartitionSegment(f.config)
		tps.activeSegments[partIndex] = assignedPartitionLog
	}

	// get a partition for the key

	reservedAddr := &commandv1.Address{
		Offset:         assignedPartitionLog.start + assignedPartitionLog.len,
		PartitionIndex: partIndex,
	}

	log.Println("RESERVED:", internal.FormatAddr(reservedAddr))

	resp := &commandv1.ReserveAddressCommand_Response{
		Address: reservedAddr,
	}

	return resp
}

func (f *CarnaxControllerFSM) applyWrite(topic string, rec *apiv1.Record, address *commandv1.Address) interface{} {
	f.lsMu.Lock()
	defer f.lsMu.Unlock()

	log.Println("WRITE:", internal.FormatAddr(address))

	if _, ok := f.state.topicConfig[topic]; !ok {
		return errors.New("no topic " + topic)
	}

	segment := f.state.segment.topicSegments[topic]

	// first initialisation of this segment
	if _, ok := segment.activeSegments[address.PartitionIndex]; !ok {
		segment.activeSegments[address.PartitionIndex] = NewTopicPartitionSegment(f.config)
	}

	offset := address.Offset
	segment.activeSegments[address.PartitionIndex].CommitRecord(rec, offset)

	return &commandv1.WriteMessageCommand_Response{
		Address: address,
	}
}

func (f *CarnaxControllerFSM) applyFlushSegment() interface{} {
	f.lsMu.Lock()
	defer f.lsMu.Unlock()

	seg := f.state.segment
	if seg == nil {
		return errors.New("no segment available")
	}

	var wg sync.WaitGroup

	for topicName, curr := range seg.topicSegments {
		wg.Add(1)

		curr := curr

		// cut a new topic segment for new writes.
		// we need to pass in the previous segment so that we can extend it with the correct
		// beginning offset.
		seg.topicSegments[topicName] = NewTopicSegment(curr)

		topicName := topicName

		log.Println(curr.id, "Topic:", topicName)

		go func() {
			defer wg.Done()

			// topic mapped by part -> partition-segment
			for partition, seg := range curr.activeSegments {
				seg := seg
				partition := partition

				logKey := SegmentName(topicName, partition, seg.start).Format(SegmentLogFile)
				err := f.storeBackedLog.Put(logKey, seg.Data())
				if err != nil {
					panic(err)
				}

				indexKey := SegmentName(topicName, partition, seg.start).Format(SegmentIndex)
				err = f.storeBackedLog.Put(indexKey, seg.Index())
				if err != nil {
					panic(err)
				}

				timeIndexKey := SegmentName(topicName, partition, seg.start).Format(SegmentTimeIndex)
				err = f.storeBackedLog.Put(timeIndexKey, seg.TimeIndex())
				if err != nil {
					panic(err)
				}
			}
		}()
	}

	wg.Wait()

	return nil
}

func (f *CarnaxControllerFSM) applyCreateTopic(config *apiv1.TopicConfig) interface{} {
	f.lsMu.Lock()
	defer f.lsMu.Unlock()

	if errs, ok := ValidateTopicConfig(config); !ok {
		for _, e := range errs {
			log.Println("invalid topic config:", e)
		}
		log.Fatal("Topic configuration is invalid")
	}

	f.state.segment.topicSegments[config.Name] = NewTopicSegment()
	f.state.topicConfig[config.Name] = config

	return nil
}

func (f *CarnaxControllerFSM) assignPartition(topic string, key []byte, numPartitions uint32) uint32 {
	// most common case: key is present.
	if key != nil && len(key) != 0 {
		kafkaSeed := 0x9747b28c
		hash := murmur.MurmurHash2(key, uint32(kafkaSeed))
		return hash % numPartitions
	}

	// We have no key specified
	// so we use round-robin assignment

	currentPartition, exists := f.state.rrPartitionAssignerState[topic]
	if !exists {
		currentPartition = 0
	}

	partition := currentPartition
	nextPartitionIdx := (currentPartition + 1) % numPartitions
	f.state.rrPartitionAssignerState[topic] = nextPartitionIdx

	return partition
}

func (f *CarnaxControllerFSM) applyReadMessage(topic string, address *commandv1.Address, point commandv1.ResetPoint) interface{} {
	f.lsMu.Lock()
	defer f.lsMu.Unlock()

	// NOTE: This is temporary until reads are re-designed
	return f.tryReadWithSegmentCacheHistory(topic, address, point)
}

func (f *CarnaxControllerFSM) tryReadWithSegmentCacheHistory(topic string, address *commandv1.Address, point commandv1.ResetPoint) interface{} {
	hash := newTopicHash(topic, address.PartitionIndex)

	log.Println("READ:", internal.FormatAddr(address), "RP:", point, hash.String())

	allSegmentPaths := f.storeBackedLog.List(hash.String())
	segmentFolderPath := findLowestSegmentFile(allSegmentPaths, address.Offset)

	// 1. find the offsIndex file.
	// .index file format is offset:byte_offset
	indexFilePath, err := func() (string, error) {
		indexSearchKey := fmt.Sprintf("%s/%s.index", hash.String(), segmentFolderPath)
		res := f.storeBackedLog.List(indexSearchKey)
		log.Println("INDEX_LU", indexSearchKey)
		if len(res) == 1 {
			return res[0], nil
		}
		return "", ErrNoIndexFound
	}()
	if err == ErrNoIndexFound {
		log.Println("no offsIndex found  for " + hash.String())
		return nil
	}

	indexData, err := f.storeBackedLog.Get(indexFilePath)
	if err != nil {
		panic(err)
	}

	// 2. find entry in offsIndex file
	// binary search it.
	// offsIndex file maps offset -> bytes position in file to return
	indexFile := IndexFromBytes(indexData)
	log.Println("IndexFile:", indexFilePath, ";", len(indexFile), "indices.")

	// BUG(FELIX): We don't handle misses in the offsIndex.
	pos := indexFile.Search(address.Offset)
	if pos == nil {
		// MISS! We mock this address offset to position 0
		// but really we should handle this some other way...
		// FIXME!
		pos = &apiv1.Index{
			Offset:   address.Offset,
			Position: 0,
		}
	}

	log.Println("Addr", address.Offset, "is offs:", pos.Offset, "byte pos:", pos.Position, "point", point)

	logSearchKey := fmt.Sprintf("%s/%s.log", hash.String(), segmentFolderPath)
	log.Println("DataFile:", logSearchKey)
	logSegmentFileData, err := f.storeBackedLog.Get(logSearchKey)
	if err != nil {
		panic(err)
	}

	logSegmentDataReader := bytes.NewReader(logSegmentFileData[pos.Position:])

	rec := &apiv1.Record{}
	err = protodelim.UnmarshalFrom(logSegmentDataReader, rec)
	if err != nil {
		panic(err)
	}

	// max.poll.records is how many to cache
	// we should fetch as many subsequent segments as possible and cache
	// then we need to figure out what the low and hi offsets are and
	// return the offset->bytepos in that segment
	// and that is what is a cache hit vs miss.

	f.cacheSegment(topic, address, logSegmentFileData) // SLICE?

	return &commandv1.ReadMessageCommand_Response{
		Record: rec,
	}
}

// NOTE(FELIX): This is probably too complex and needs to be refactored.
func (f *CarnaxControllerFSM) applySubscribeTopic(consumerGroupId string, topics []string, clientId string) interface{} {
	f.cgMu.Lock()
	defer f.cgMu.Unlock()

	log.Println("Subscribe", consumerGroupId, topics, clientId)

	cgd, ok := f.state.consumerGroupDescriptors[consumerGroupId]
	if !ok {
		log.Println("Initialising", consumerGroupId)

		// this is the first subscription
		cgd = &apiv1.ConsumerGroupDescriptor{
			Id:                 consumerGroupId,
			Topics:             topics,
			ActiveGenerationId: 0,
			RegisteredClients: map[string]*apiv1.ConsumerGroupNode{
				clientId: {
					ConsumerGroupId:    consumerGroupId,
					AssignedPartitions: []*apiv1.TopicPartition{},
					ActiveGenerationId: 0,
					State:              apiv1.ConsumerGroupState_CONSUMER_GROUP_STATE_PREPARING_REBALANCE,
					ClientId:           clientId,
					CommittedOffset:    map[uint32]uint64{},
					CurrentOffset:      map[uint32]uint64{},
				},
			},
		}
		f.state.consumerGroupDescriptors[consumerGroupId] = cgd
	} else {
		// NOTE: We are triggering a subscription therefore the active generation consumerGroupId increments
		cgd.ActiveGenerationId += 1
		newGenerationId := cgd.ActiveGenerationId

		// add the new client with the new id.
		cgd.RegisteredClients[clientId] = &apiv1.ConsumerGroupNode{
			ConsumerGroupId: consumerGroupId,
			State:           apiv1.ConsumerGroupState_CONSUMER_GROUP_STATE_PREPARING_REBALANCE,
			ClientId:        clientId,

			// we have no assignments yet. this happens later.
			AssignedPartitions: []*apiv1.TopicPartition{},

			// take the new active gen consumerGroupId we just bumped.
			ActiveGenerationId: newGenerationId,

			CommittedOffset: map[uint32]uint64{},
			CurrentOffset:   map[uint32]uint64{},
		}
	}

	return f.state.consumerGroupDescriptors[consumerGroupId].RegisteredClients[clientId]
}

func (f *CarnaxControllerFSM) applyRebalancePartitions(id string) interface{} {
	f.cgMu.Lock()
	defer f.cgMu.Unlock()

	// given a state of the world, i.e. what consumers are available
	// we should be able to see all available partitions and re-assign evenly.

	cgNode, ok := f.state.consumerGroupDescriptors[id]
	if !ok {
		panic("invalid state")
	}

	// for each topic
	// for some topic t, get all partition counts
	// get how many consumers there are in the group
	// assign each consumer a partition (round robin for instance)

	log.Println("rebalance", id, "ag_id", cgNode.ActiveGenerationId, "client id", cgNode.Id)

	// we need to clear all assigned partitions
	for clientId := range cgNode.RegisteredClients {
		cgNode.RegisteredClients[clientId].AssignedPartitions = []*apiv1.TopicPartition{}

		// we've bumped the primary descriptor
		// we need to update clients to match the generation id.

		// NOTE(FELIX): Should we swap to a model where this is atomically done instead?
		cgNode.RegisteredClients[clientId].ActiveGenerationId = cgNode.ActiveGenerationId
	}

	// acquire lock for topic related config.
	f.lsMu.Lock()

	for _, topic := range cgNode.Topics {
		// for this topic we want to assign partitions evenly amongst
		// all available client ids.

		numPartitions := f.state.topicConfig[topic].PartitionCount
		currPartitionIndex := uint32(0)

		for numPartitions > 0 {
			for clientId, clientMetadata := range cgNode.RegisteredClients {
				cgNode.RegisteredClients[clientId].AssignedPartitions = append(clientMetadata.AssignedPartitions, &apiv1.TopicPartition{
					Topic:          topic,
					PartitionIndex: currPartitionIndex,
				})
				cgNode.RegisteredClients[clientId].State = apiv1.ConsumerGroupState_CONSUMER_GROUP_STATE_COMPLETING_REBALANCE

				currPartitionIndex += 1
				numPartitions -= 1
			}
		}
	}

	// topic config lock goes.
	f.lsMu.Unlock()

	for clientId := range cgNode.RegisteredClients {
		cgNode.RegisteredClients[clientId].State = apiv1.ConsumerGroupState_CONSUMER_GROUP_STATE_STABLE
	}

	return nil
}

func (f *CarnaxControllerFSM) applyPollMessages(clientId string, consumerGroupId string) interface{} {
	f.cgMu.Lock()
	defer f.cgMu.Unlock()

	cgd, ok := f.state.consumerGroupDescriptors[consumerGroupId]
	if !ok {
		panic("invalid state")
	}

	node, ok := cgd.RegisteredClients[clientId]
	if !ok {
		panic("invalid state: no such client registered")
	}

	// this is the beginning offset that we
	// will fetch
	offsToFetch := map[string]*commandv1.OffsetResetPoint{}

	for _, tp := range node.AssignedPartitions {
		resetPoint := commandv1.ResetPoint_RESET_POINT_EXACT

		// NOTE: current is a misnomer. It is the last seen offset
		lastCommittedOffset, ok := node.CurrentOffset[tp.PartitionIndex]
		if !ok {
			// we haven't read the stream yet.
			// NIT(FELIX): If we don't have an offset committed here we
			// should rever to the offset reset strategy. for now it's earliest.
			lastCommittedOffset = 0
			resetPoint = commandv1.ResetPoint_RESET_POINT_EARLIEST
		}

		// poll returns the offsets to consume from?
		offsToFetch[tp.Topic] = &commandv1.OffsetResetPoint{
			Address: &commandv1.Address{
				Offset:         lastCommittedOffset,
				PartitionIndex: tp.PartitionIndex,
			},
			Point: resetPoint,
		}
	}

	return &commandv1.PollMessagesCommand_Response{
		OffsetsToFetch: offsToFetch,
		Metadata:       node,
	}
}

func (f *CarnaxControllerFSM) applyCommitSync(id string) interface{} {
	f.cgMu.Lock()
	defer f.cgMu.Unlock()

	descriptor, ok := f.state.consumerGroupDescriptors[id]
	if !ok {
		panic("invalid state")
	}

	for _, cgn := range descriptor.RegisteredClients {
		for p, _ := range cgn.CurrentOffset {
			log.Println("Commit:", cgn.CommittedOffset[p], ":=", cgn.CurrentOffset[p])
			cgn.CommittedOffset[p] = cgn.CurrentOffset[p]
		}
	}

	return nil
}

func (f *CarnaxControllerFSM) tryFindInCache(topic string, address *commandv1.Address) (*apiv1.Record, bool) {
	sc, ok := f.segmentCache[newTopicHash(topic, address.PartitionIndex)]
	if !ok {
		return nil, false
	}

	seg := sc.findNearest(address.Offset)
	if seg == nil {
		return nil, false
	}

	reader := bytes.NewReader(seg.data[address.Offset:])

	log.Println("CACHED_SEG_LEN", len(seg.data))

	res := &apiv1.Record{}

	if err := protodelim.UnmarshalFrom(reader, res); err != nil {
		// cache miss.
		if err == io.EOF {
			return nil, false
		}
		log.Println("ProtoDelim Unmarshal Failure:", err)

		// mark this segment as bad to try find in the next.

		return nil, false
	}

	result := new(apiv1.Record)
	if err := proto.Unmarshal(res.Payload, res); err != nil {
		panic(err)
	}

	return result, true
}

func (f *CarnaxControllerFSM) cacheSegment(topic string, address *commandv1.Address, reader []byte) {
	sc, ok := f.segmentCache[newTopicHash(topic, address.PartitionIndex)]
	if !ok {
		sc = newSegmentCache()
		f.segmentCache[newTopicHash(topic, address.PartitionIndex)] = sc
	}

	sc.cacheSeg(address.Offset, reader)
}

func (f *CarnaxControllerFSM) applySoftCommit(consumerGroupId string, clientId string, index uint32, offset uint64) interface{} {
	f.cgMu.Lock()
	defer f.cgMu.Unlock()

	log.Println("SOFT_COMMIT:", offset)

	cgd, ok := f.state.consumerGroupDescriptors[consumerGroupId]
	if !ok {
		panic("invalid state " + consumerGroupId)
	}

	cgn, ok := cgd.RegisteredClients[clientId]
	if !ok {
		panic("invalid state " + clientId)
	}

	cgn.CurrentOffset[index] = offset

	return nil
}

/**
Segment metadata exists that covers the range of offsets
this is IN MEMORY, which is the low and high watermark of
offset and timestamp for each segment.
Kafka is able to rebuild internal metadata from index files quickly. Disk writes are cheap here
However when backed by an object-store we should think about an alternative strategy, e.g.
checkpointing.
*/

func (f *CarnaxControllerFSM) applySeekOffset(id string, clientId string, topicPartition *apiv1.TopicPartition, seekIndex *apiv1.SeekIndex) interface{} {
	return nil
}
