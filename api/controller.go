package api

import (
	"bytes"
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	apiv1 "github.com/yarefs/carnax/gen/api/v1"
	commandv1 "github.com/yarefs/carnax/gen/command/v1"
	controllerv1 "github.com/yarefs/carnax/gen/controller/v1"
	"google.golang.org/protobuf/encoding/protodelim"
	"hash/crc32"
	"log"
	"strings"
	"sync"
	"time"
)

type ExecutionMode int

const (
	SingleNode ExecutionMode = iota
	MultiNode
)

var (
	ErrNoIndexFound  = errors.New("no index found")
	ErrNotALeader    = errors.New("not a leader")
	ErrTopicNotFound = errors.New("topic not found")
)

type sharedMessageLogState struct {
	segment                  *SegmentLog
	topicConfig              map[string]*apiv1.TopicConfig
	rrPartitionAssignerState map[string]uint32
	consumerGroupDescriptors map[string]*apiv1.ConsumerGroupDescriptor
}

type CarnaxConfig struct {
	LogSegmentBytes int

	raftTimeout time.Duration
	raftMaxPool int

	ExecutionMode ExecutionMode

	// max.poll.records
	MaxPollRecords int

	//log.index.interval.bytes

	SegmentIndexBytes  int
	SegmentJitterMs    int
	SegmentMs          int
	MaxMessageBytes    int
	IndexIntervalBytes int
	BootstrapServers   []string
}

type Properties map[string]any

func (p Properties) int(prop string, fallback int) int {
	v, ok := p[prop].(int)
	if !ok {
		return fallback
	}
	return v
}

func (p Properties) string(s string) string {
	v, ok := p[s].(string)
	if !ok {
		panic(s + " not found")
	}
	return v
}

func ParseFromProperties(props Properties) CarnaxConfig {
	return CarnaxConfig{
		BootstrapServers: strings.Split(props.string("bootstrap.servers"), ","),

		// ADJUST!
		LogSegmentBytes: props.int("log.segment.bytes", 0),

		IndexIntervalBytes: props.int("index.interval.bytes", 4*KiB),

		MaxPollRecords:  props.int("max.poll.records", 500),
		MaxMessageBytes: props.int("max.message.bytes", 1*MiB),

		SegmentMs: props.int("segment.ms", int((150 * time.Millisecond).Milliseconds())),

		// ADJUST!
		SegmentIndexBytes: props.int("segment.index.bytes", 0),

		SegmentJitterMs: props.int("segment.jitter.ms", 0),

		ExecutionMode: 0,

		// for now these are not public/configurable
		raftTimeout: 10 * time.Second,
		raftMaxPool: 3,
	}
}

var DefaultCarnaxConfig = ParseFromProperties(Properties{
	"log.segment.bytes": 8 * MiB,
	"segment.ms":        int(250 * time.Millisecond.Milliseconds()),
	"bootstrap.servers": "",

	// raft related config
	//raftTimeout: 10 * time.Second,
	//raftMaxPool: 3,
})

// CarnaxController
type CarnaxController struct {
	// raft consensus related bits
	RaftBind string
	RaftDir  string
	raft     *raft.Raft

	lsMu   sync.RWMutex // log state stuff
	cgMu   sync.RWMutex // consumer group stuff
	state  *sharedMessageLogState
	config CarnaxConfig

	// @local
	segmentCache map[TopicPartitionHash]*SegmentCache

	// output
	storeBackedLog ObjectStore // @leader only!

	shutdownSignal chan struct{}
}

func NewCarnaxControllerWithConfig(store ObjectStore, config CarnaxConfig) *CarnaxController {
	return &CarnaxController{
		storeBackedLog: store,
		segmentCache:   map[TopicPartitionHash]*SegmentCache{},
		state: &sharedMessageLogState{
			segment:                  NewSegmentLog(),
			topicConfig:              make(map[string]*apiv1.TopicConfig),
			consumerGroupDescriptors: map[string]*apiv1.ConsumerGroupDescriptor{},
			rrPartitionAssignerState: make(map[string]uint32),
		},
		shutdownSignal: make(chan struct{}),
		config:         config,
	}
}

func (m *CarnaxController) Start(raftInst *raft.Raft) error {
	m.raft = raftInst

	// NOTE: we are _expecting_ election here
	// so we wait until we are marked as a leader.
	if m.config.ExecutionMode == SingleNode {
		start := time.Now()
		for {
			if time.Now().Sub(start) > time.Second*15 {
				log.Println("expected leader election took too long")
				break
			}

			if m.getRaft().State() == raft.Leader {
				break
			}
		}
	} else {
		panic("not yet supported")
	}

	// we must start flushing _after_ raft has been set up
	if m.raft.State() != raft.Leader {
		return ErrNotALeader
	}

	go m.beginFlushCycle()

	return nil
}

func (m *CarnaxController) CreateTopic(config *apiv1.TopicConfig) error {
	if config.PartitionCount <= 0 {
		return errors.New("partition count must be more than zero for topic " + config.Name)
	}

	command := &commandv1.Command{
		Type: commandv1.CommandType_COMMAND_TYPE_CREATE_TOPIC,
		Command: &commandv1.Command_CreateTopic{
			CreateTopic: &commandv1.CreateTopicCommand{
				Config: config,
			},
		},
	}
	commandBytes, err := proto.Marshal(command)
	if err != nil {
		log.Fatal(err)
	}

	result := m.getRaft().Apply(commandBytes, m.config.raftTimeout)
	if e := result.Error(); e != nil {
		log.Fatal(e)
	}

	resp := result.Response()

	if e, ok := resp.(error); ok {
		return e
	}

	return nil
}

func (m *CarnaxController) getRaft() *raft.Raft {
	if m.raft == nil {
		panic(errors.New("raft has not been opened. perhaps try running Start()"))
	}

	return m.raft
}

func (m *CarnaxController) beginFlushCycle() {
	flushDurationThreshold := m.config.SegmentMs
	maxSegSize := m.config.LogSegmentBytes

	flushingEnabled := flushDurationThreshold != -1
	if !flushingEnabled {
		return
	}

	last := time.Now()
	for {
		select {
		case <-m.shutdownSignal:
			return
		default:
			elapsed := time.Now().Sub(last)

			m.lsMu.Lock()
			exceedsSegmentSize := m.state.segment.Size() > uint64(maxSegSize)
			m.lsMu.Unlock()

			if elapsed.Milliseconds() >= int64(flushDurationThreshold) || exceedsSegmentSize {
				go func() {
					err := m.Flush()
					if err != nil {
						log.Fatal(err)
					}
				}()
				last = time.Now()
			}
		}

	}
}

func (m *CarnaxController) Shutdown() error {
	log.Println("Shutting down ...")

	close(m.shutdownSignal)

	err := m.Flush()
	if err != nil {
		return err
	}

	m.getRaft().Shutdown()

	return nil
}

func (m *CarnaxController) CommitSync(consumerGroupId string) error {
	if m.getRaft().State() != raft.Leader {
		return ErrNotALeader
	}

	commitSyncBytes, err := proto.Marshal(&commandv1.Command{
		Type: commandv1.CommandType_COMMAND_TYPE_COMMIT_SYNC,
		Command: &commandv1.Command_CommitSync{
			CommitSync: &commandv1.CommitSyncCommand{
				ConsumerGroupId: consumerGroupId,
			},
		},
	})
	if err != nil {
		panic(err)
	}

	res := m.getRaft().Apply(commitSyncBytes, m.config.raftTimeout)
	if e := res.Error(); e != nil {
		panic(e)
	}

	// TOOD: Configure this timeout.
	// we use the Barrier here to force a sync across the
	// raft log so that we prioritise committing an offset.
	m.getRaft().Barrier(m.config.raftTimeout)

	res.Response()
	return nil
}

/*
 */
func (m *CarnaxController) read(topic string, partitionIndex uint32, offset uint64, point commandv1.ResetPoint) (*apiv1.Record, error) {
	if m.getRaft().State() != raft.Leader {
		return nil, ErrNotALeader
	}

	readMsgBytes, err := proto.Marshal(&commandv1.Command{
		Type: commandv1.CommandType_COMMAND_TYPE_READ_MESSAGE,
		Command: &commandv1.Command_ReadMessage{
			ReadMessage: &commandv1.ReadMessageCommand{
				Topic: topic,
				Address: &commandv1.Address{
					Offset:         offset,
					PartitionIndex: partitionIndex,
				},
				ResetPoint: point,
			},
		},
	})
	if err != nil {
		panic(err)
	}

	result := m.getRaft().Apply(readMsgBytes, m.config.raftTimeout)
	if e := result.Error(); e != nil {
		log.Fatal(e)
	}

	resp := result.Response()
	v, ok := resp.(*commandv1.ReadMessageCommand_Response)
	if ok {
		return v.Record, nil
	}

	return nil, errors.New("read failed")
}

// Poll ...
// FIXME(FELIX): This signature is somewhat inconsistent in how it returns information
// as opposed to the rest of the API
//
// nit: we should respect the state of the consumer group node here
// e.g. if it's in a rebalance or not.
func (m *CarnaxController) Poll(consumerGroupId string, clientId string, duration time.Duration) (*controllerv1.Poll_Response, error) {
	if m.getRaft().State() != raft.Leader {
		return nil, ErrNotALeader
	}

	cmdBytes, err := proto.Marshal(&commandv1.Command{
		Type: commandv1.CommandType_COMMAND_TYPE_POLL_MESSAGES,
		Command: &commandv1.Command_PollMessages{
			PollMessages: &commandv1.PollMessagesCommand{
				ConsumerGroupId: consumerGroupId,
				ClientId:        clientId,
			},
		},
	})
	if err != nil {
		panic(err)
	}

	res := m.getRaft().Apply(cmdBytes, m.config.raftTimeout)
	if e := res.Error(); e != nil {
		panic(e)
	}

	v, ok := res.Response().(*commandv1.PollMessagesCommand_Response)
	if !ok {
		panic("invalid state")
	}

	// NIT(FELIX): Should we return this keyed?
	var out []*apiv1.Record

	// TODO(FELIX): We can parallelize this.
	for topic, offset := range v.OffsetsToFetch {
		rec, err := m.read(topic, offset.Address.PartitionIndex, offset.Address.Offset, offset.Point)
		if err != nil {
			log.Println(err)
			continue
		}

		writtenBytes := calcRecordLen(rec, offset.Address.Offset)

		// NOTE: we can't necessarily foresee when we are in another segment at this point
		nextOffset := offset.Address.Offset + writtenBytes + SegmentIncrement

		err = m.softCommit(consumerGroupId,
			clientId,
			offset.Address.PartitionIndex,
			nextOffset)

		if err != nil {
			panic(err)
		}

		out = append(out, rec)
	}

	return &controllerv1.Poll_Response{
		Records:  out,
		Metadata: v.Metadata,
	}, nil
}

/*
*
FIXME(FELIX): This is a big symptom of something going a bit wrong with the design.
*/
func calcRecordLen(rec *apiv1.Record, offs uint64) uint64 {
	recordWithOffs := &apiv1.RecordWithOffset{
		Record: rec,
		Offset: offs,
	}
	recordWithOffsBytes, err := proto.Marshal(recordWithOffs)
	if err != nil {
		panic("Failed to marshal record")
	}
	checksum := crc32.ChecksumIEEE(recordWithOffsBytes)

	writtenRec := &apiv1.CommittedRecord{
		Record:   recordWithOffs,
		Checksum: checksum,
	}

	buf := new(bytes.Buffer)
	amt, err := protodelim.MarshalTo(buf, writtenRec)
	if err != nil {
		panic(err)
	}
	return uint64(amt)
}

// Subscribe ...
//
// NOTE: Ideally the client.id could just be generated
// as an uuid from the API wrapper.
func (m *CarnaxController) Subscribe(id string, clientId string, topics ...string) (*apiv1.ConsumerGroupNode, error) {
	if m.raft.State() != raft.Leader {
		return nil, ErrNotALeader
	}

	subCmdBytes, err := proto.Marshal(&commandv1.Command{
		Type: commandv1.CommandType_COMMAND_TYPE_SUBSCRIBE_TOPIC,
		Command: &commandv1.Command_SubscribeTopic{
			SubscribeTopic: &commandv1.SubscribeTopicCommand{
				Id:       id,
				Topic:    topics,
				ClientId: clientId,
			},
		},
	})
	if err != nil {
		panic(err)
	}

	res := m.getRaft().Apply(subCmdBytes, m.config.raftTimeout)
	if e := res.Error(); e != nil {
		return nil, e
	}

	// at this point, we can verify state of the CGN
	// if it's PENDING go ahead and re-assign all partitions.

	consumerGroupNode, ok := res.Response().(*apiv1.ConsumerGroupNode)
	if !ok {
		panic("invalid state: failed to subscribe to topic")
	}

	if consumerGroupNode.State != apiv1.ConsumerGroupState_CONSUMER_GROUP_STATE_PREPARING_REBALANCE {
		panic("invalid state: must be preparing for rebalance")
	}

	rebalanceCmdBytes, err := proto.Marshal(&commandv1.Command{
		Type: commandv1.CommandType_COMMAND_TYPE_REBALANCE_PARTITIONS,
		Command: &commandv1.Command_RebalancePartitions{
			RebalancePartitions: &commandv1.RebalancePartitionsCommand{
				ConsumerGroupId: id,
			},
		},
	})

	if err != nil {
		panic(err)
	}

	m.getRaft().Apply(rebalanceCmdBytes, m.config.raftTimeout)

	return consumerGroupNode, nil
}

// Flush will flush the open segment into the object-storeBackedLog.
func (m *CarnaxController) Flush() error {
	if m.getRaft().State() != raft.Leader {
		return ErrNotALeader
	}

	flushBytes, err := proto.Marshal(&commandv1.Command{
		Type: commandv1.CommandType_COMMAND_TYPE_FLUSH_SEGMENT,
		Command: &commandv1.Command_FlushSegment{
			FlushSegment: &commandv1.FlushSegmentCommand{},
		},
	})
	if err != nil {
		panic(err)
	}

	res := m.getRaft().Apply(flushBytes, m.config.raftTimeout)
	if e := res.Error(); e != nil {
		panic(e)
	}

	m.getRaft().Barrier(m.config.raftTimeout)

	return nil
}

// Write is a synchronous write to the log
// it is a fire-and-forget write and does not respond with any acknowledgement
func (m *CarnaxController) Write(topic string, record *apiv1.Record) (*commandv1.Address, error) {
	if m.getRaft().State() != raft.Leader {
		return nil, ErrNotALeader
	}

	roBytes, err := proto.Marshal(&commandv1.Command{
		Type: commandv1.CommandType_COMMAND_TYPE_RESERVE_ADDRESS,
		Command: &commandv1.Command_ReserveAddress{
			ReserveAddress: &commandv1.ReserveAddressCommand{
				Topic: topic,
				Key:   record.Key,
			},
		},
	})
	if err != nil {
		return nil, err
	}

	res := m.getRaft().Apply(roBytes, m.config.raftTimeout)
	if err := res.Error(); err != nil {
		panic(err)
	}

	m.getRaft().Barrier(m.config.raftTimeout)

	result := res.Response().(*commandv1.ReserveAddressCommand_Response)

	// send off a msg to write
	writeCmdBytes, err := proto.Marshal(&commandv1.Command{
		Type: commandv1.CommandType_COMMAND_TYPE_WRITE_MESSAGE,
		Command: &commandv1.Command_WriteMessage{
			WriteMessage: &commandv1.WriteMessageCommand{
				Topic:  topic,
				Record: record,
				Address: &commandv1.Address{
					Offset:         result.Address.Offset,
					PartitionIndex: result.Address.PartitionIndex,
				},
			},
		},
	})
	if err != nil {
		return nil, err
	}

	res = m.getRaft().Apply(writeCmdBytes, m.config.raftTimeout)
	if err := res.Error(); err != nil {
		return nil, err
	}

	// must be created response
	writeResult := res.Response()
	if e, ok := writeResult.(error); ok {
		return nil, e
	}

	// NOTE: FELIX if we fail to write
	// we should deal with this accordingly. for example,
	// we proably want to either purge the offset somehow or
	// allow it to be re-allocated.

	if v, ok := writeResult.(*commandv1.WriteMessageCommand_Response); ok {
		return v.Address, nil
	}

	panic("failed to get write result")
}

func (m *CarnaxController) softCommit(id string, clientId string, index uint32, delta uint64) error {
	if m.getRaft().State() != raft.Leader {
		return ErrNotALeader
	}

	softCommitBytes, err := proto.Marshal(&commandv1.Command{
		Type: commandv1.CommandType_COMMAND_TYPE_SOFT_COMMIT,
		Command: &commandv1.Command_SoftCommit{
			SoftCommit: &commandv1.SoftCommitCommand{
				ConsumerGroupId: id,
				ClientId:        clientId,
				PartitionIndex:  index,
				NextOffsetDelta: delta,
			},
		},
	})
	if err != nil {
		panic(err)
	}

	res := m.getRaft().Apply(softCommitBytes, m.config.raftTimeout)
	if err := res.Error(); err != nil {
		panic(err)
	}

	res.Response()
	return nil
}
