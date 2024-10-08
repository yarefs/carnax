package api

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	apiv1 "github.com/yarefs/carnax/gen/api/v1"
	commandv1 "github.com/yarefs/carnax/gen/command/v1"
	"github.com/yarefs/carnax/internal"
	"google.golang.org/protobuf/encoding/protodelim"
	"log"
	"sync"
	"testing"
	"time"
)

var carnaxConfigWithNoFlushInterval = CarnaxConfig{
	BootstrapServers: []string{},
	SegmentMs:        -1,
	LogSegmentBytes:  10 * KiB,

	raftTimeout: 10 * time.Second,
	raftMaxPool: 3,
}

func minRaftPropagationSleep() {
	// randomly tweaked sleep time to
	// allow raft commands to apply.
	time.Sleep(25 * time.Millisecond)
}

type carnaxTestSuite struct {
	suite.Suite
	store      ObjectStore
	controller *CarnaxController

	mu       sync.Mutex
	lastPort int
}

func (c *carnaxTestSuite) AfterTest(suiteName, testName string) {
	c.controller.Shutdown()
}

func TestCarnaxTestSuite(t *testing.T) {
	testingSuite := new(carnaxTestSuite)
	testingSuite.lastPort = 50050
	suite.Run(t, testingSuite)
}

func (c *carnaxTestSuite) SetupTest() {
	c.store = internal.NewInMemoryObjectStore()
	c.controller = NewCarnaxControllerWithConfig(c.store, carnaxConfigWithNoFlushInterval)

	id := "raft_node_" + uuid.NewString()

	c.mu.Lock()
	defer c.mu.Unlock()

	raft, _ := NewCarnaxRaft(
		"/tmp/carnax-test/"+id,
		1,
		id,
		fmt.Sprintf(":%d", c.lastPort),
		c.controller,
		true)

	c.controller.Start(raft)

	c.lastPort += 1
}

func (c *carnaxTestSuite) TestSharedMessageLog_Write_FailsIfTopicDoesntExist() {
	c.T().Skipf("returning error codes has not yet been solidifed so this is hard to test nicely.")

	assert.Panics(c.T(), func() {
		c.controller.Write("some_topic", &apiv1.Record{
			Key:     nil,
			Payload: []byte("hello, world"),
		})
	}, "no topic found: some_topic")
}

func (c *carnaxTestSuite) TestSharedMessageLog_CreateTopic_FailsOnZeroPartitions() {
	err := c.controller.CreateTopic(&apiv1.TopicConfig{
		Name:           "some_topic",
		PartitionCount: 0,
	})

	assert.EqualError(c.T(), err, "partition count must be more than zero for topic some_topic")
}

/*
This test ensures that writes that are sent to a _single_
partition topic are flushed in order
*/
func (c *carnaxTestSuite) TestSharedMessageLog_Write_SinglePartition_OrderedFlush() {
	err := c.controller.CreateTopic(&apiv1.TopicConfig{
		Name:           "some_topic",
		PartitionCount: 1,
	})
	assert.NoError(c.T(), err)

	_, err = c.controller.Write("some_topic", &apiv1.Record{
		Key:     nil,
		Payload: []byte("felix"),
	})
	assert.NoError(c.T(), err)

	_, err = c.controller.Write("some_topic", &apiv1.Record{
		Key:     nil,
		Payload: []byte("angell"),
	})
	assert.NoError(c.T(), err)

	err = c.controller.Flush()
	assert.NoError(c.T(), err)

	data, _ := c.store.Get("some_topic-0/00000000000000000000.log")
	buf := bytes.NewBuffer(data)

	var rec apiv1.CommittedRecord
	err = protodelim.UnmarshalFrom(buf, &rec)
	assert.NoError(c.T(), err)

	assert.Equal(c.T(), "felix", string(rec.Record.Record.Payload))

	var nextRec apiv1.CommittedRecord
	err = protodelim.UnmarshalFrom(buf, &nextRec)
	assert.NoError(c.T(), err)

	assert.Equal(c.T(), "angell", string(nextRec.Record.Record.Payload))

}

/*
We must publish round-robin if there are multiple partitions and a null or empty key.
*/
func (c *carnaxTestSuite) TestSharedMessageLog_Write_SinglePartition_MultiplePartitions_RoundRobinNullKey() {
	err := c.controller.CreateTopic(&apiv1.TopicConfig{
		Name:           "some_topic",
		PartitionCount: 4,
	})
	assert.NoError(c.T(), err)

	_, err = c.controller.Write("some_topic", &apiv1.Record{
		Key:     nil,
		Payload: []byte("felix"),
	})
	assert.NoError(c.T(), err)

	_, err = c.controller.Write("some_topic", &apiv1.Record{
		Key:     nil,
		Payload: []byte("angell"),
	})
	assert.NoError(c.T(), err)

	minRaftPropagationSleep()

	err = c.controller.Flush()
	assert.NoError(c.T(), err)

	// leaves time for log applies.
	minRaftPropagationSleep()

	firstPartData, _ := c.store.Get("some_topic-0/00000000000000000000.log")
	firstPartBuf := bytes.NewBuffer(firstPartData)

	var first apiv1.CommittedRecord
	err = protodelim.UnmarshalFrom(firstPartBuf, &first)
	assert.NoError(c.T(), err)
	assert.Equal(c.T(), "felix", string(first.Record.Record.Payload))

	secondPartData, _ := c.store.Get("some_topic-1/00000000000000000000.log")
	secondPartBuf := bytes.NewBuffer(secondPartData)

	var second apiv1.CommittedRecord
	err = protodelim.UnmarshalFrom(secondPartBuf, &second)
	assert.NoError(c.T(), err)
	assert.Equal(c.T(), "angell", string(second.Record.Record.Payload))
}

// ensure that we only have one active segment at a time.

// Test: write returns an offset(?)
// read this offset amongst a sea of the same messages

func (c *carnaxTestSuite) TestSharedMessageLog_Read_SpecificOffset() {
	err := c.controller.CreateTopic(&apiv1.TopicConfig{
		Name:           "orders",
		PartitionCount: 1,
	})
	assert.NoError(c.T(), err)

	var messageAddrToFind *commandv1.Address

	// 1k writes into one partition
	for i := 0; i < 100; i += 1 {
		res, err := c.controller.Write("orders", &apiv1.Record{
			Key:     nil,
			Payload: []byte(fmt.Sprintf("some_order %d", i)),
		})
		assert.NoError(c.T(), err)

		// grab the middle messages offset
		if i == 50 {
			log.Println("PICKED OUT", res.Offset, ":", res.PartitionIndex, "which is", i)
			messageAddrToFind = res
		}

		// flush every ~100 msgs.
		if i%10 == 0 && i > 0 {
			err = c.controller.Flush()
			log.Println("Flushing at", i)
			assert.NoError(c.T(), err)
		}
	}

	// we need to give time for the raft
	// writes to happen.
	minRaftPropagationSleep()

	res, err := c.controller.read("orders", messageAddrToFind.PartitionIndex, messageAddrToFind.Offset, commandv1.ResetPoint_RESET_POINT_EXACT)
	assert.NoError(c.T(), err)

	assert.Equal(c.T(), "some_order 50", string(res.Payload))
}

func (c *carnaxTestSuite) TestSharedMessageLog_Read() {
	c.controller.CreateTopic(&apiv1.TopicConfig{
		Name:           "orders",
		PartitionCount: 1,
	})

	// 1k writes into one partition
	for i := 0; i < 1_000; i += 1 {
		c.controller.Write("orders", &apiv1.Record{
			Key:     nil,
			Payload: []byte("some_order"),
		})

		// flush every ~100 msgs.
		if i%100 == 0 && i > 0 {
			c.controller.Flush()
		}
	}

	minRaftPropagationSleep()

	res, err := c.controller.read("orders", 0, 4500, commandv1.ResetPoint_RESET_POINT_EXACT)
	assert.NoError(c.T(), err)

	assert.Equal(c.T(), "some_order", string(res.Payload))
}

func (c *carnaxTestSuite) TestMessageLog_Subscribe_TriggersPartitionAssignment_MultipleConsumers_ManyPartitions() {
	NumPartitions := 4

	c.controller.CreateTopic(&apiv1.TopicConfig{
		Name:           "orders",
		PartitionCount: uint32(NumPartitions),
	})
	c.controller.Write("orders", &apiv1.Record{
		Key:     nil,
		Payload: []byte("woah!"),
	})

	// make a new consumer group called "my_consumer_group"
	_, err := c.controller.Subscribe("my_consumer_group", "client-1", "orders")
	assert.NoError(c.T(), err)

	_, err = c.controller.Subscribe("my_consumer_group", "client-2", "orders")
	assert.NoError(c.T(), err)

	minRaftPropagationSleep()

	c.controller.Poll("my_consumer_group", "client-1", time.Second*10)

	// TODO(FELIX): Describe the consumer group

	minRaftPropagationSleep()
}

func (c *carnaxTestSuite) TestMessageLog_Subscribe_TriggersPartitionAssignment_SingleConsumer_ManyPartitions() {
	NumPartitions := 4

	c.controller.CreateTopic(&apiv1.TopicConfig{
		Name:           "orders",
		PartitionCount: uint32(NumPartitions),
	})
	c.controller.Write("orders", &apiv1.Record{
		Key:     nil,
		Payload: []byte("woah!"),
	})

	// make a new consumer group called "my_consumer_group"
	res, err := c.controller.Subscribe("my_consumer_group", "client-1", "orders")
	assert.NoError(c.T(), err)

	assert.Equal(c.T(), apiv1.ConsumerGroupState_CONSUMER_GROUP_STATE_PREPARING_REBALANCE, res.State)
	assert.Equal(c.T(), "my_consumer_group", res.ConsumerGroupId)
	assert.Equal(c.T(), uint64(0), res.ActiveGenerationId, 0)
	assert.Equal(c.T(), "client-1", res.ClientId)

	// some time to allow for the rebalance
	minRaftPropagationSleep()

	// now, we must poll for some additional updates/state
	// ideally we have finalised a rebalance and a partition assignment has been
	// completed and propagated to all nodes in the group by the leader.

	pollResult, _ := c.controller.Poll("my_consumer_group", "client-1", time.Second*10)

	// some time to allow for the poll
	minRaftPropagationSleep()

	assert.Equal(c.T(), "client-1", pollResult.Metadata.ClientId)
	assert.Equal(c.T(), apiv1.ConsumerGroupState_CONSUMER_GROUP_STATE_STABLE, pollResult.Metadata.State)
	assert.Len(c.T(), pollResult.Metadata.AssignedPartitions, NumPartitions)

	// we should have at least one message in there.
	assert.Empty(c.T(), pollResult.Records)
}

func (c *carnaxTestSuite) TestMessageLog_Subscribe() {
	c.controller.CreateTopic(&apiv1.TopicConfig{
		Name:           "orders",
		PartitionCount: 1,
	})
	c.controller.Write("orders", &apiv1.Record{
		Key:     nil,
		Payload: []byte("woah!"),
	})

	// make a new consumer group called "my_consumer_group"
	res, err := c.controller.Subscribe("my_consumer_group", "client-1", "orders")
	assert.NoError(c.T(), err)

	// some time to allow for the rebalance
	minRaftPropagationSleep()

	// after rebalancing we should be stable, i.e. all partitions are assigned
	// that said, this is not from a Poll so this might be a flaky state/test.
	assert.Equal(c.T(), apiv1.ConsumerGroupState_CONSUMER_GROUP_STATE_STABLE, res.State)
	assert.Equal(c.T(), "my_consumer_group", res.ConsumerGroupId)
	assert.Equal(c.T(), uint64(0), res.ActiveGenerationId, 0)
	assert.Equal(c.T(), "client-1", res.ClientId)
}

// -----------------------------------
// POLLING & CONSUMER GROUP SEMANTICS
// -----------------------------------

func (c *carnaxTestSuite) TestMessageLog_Poll_SinglePartitionAndConsumer() {
	c.controller.CreateTopic(&apiv1.TopicConfig{
		Name:           "orders",
		PartitionCount: 1,
	})

	addr, err := c.controller.Write("orders", &apiv1.Record{
		Key:     nil,
		Payload: []byte("woah!"),
	})
	assert.NoError(c.T(), err)
	log.Println("Write", addr.PartitionIndex, addr.Offset)

	// enforce a flush as we don't have this enabled.
	c.controller.Flush()

	// allow write to sink.
	minRaftPropagationSleep()

	// make a new consumer group called "my_consumer_group"
	res, err := c.controller.Subscribe("my_consumer_group", "client-1", "orders")
	assert.NoError(c.T(), err)

	// some time to allow for the rebalance
	minRaftPropagationSleep()
	assert.Equal(c.T(), apiv1.ConsumerGroupState_CONSUMER_GROUP_STATE_STABLE, res.State)
	assert.Equal(c.T(), "my_consumer_group", res.ConsumerGroupId)
	assert.Equal(c.T(), uint64(0), res.ActiveGenerationId, 0)
	assert.Equal(c.T(), "client-1", res.ClientId)

	// we are now in a place to poll and receive our message
	// a poll command should return a single message
	pollResult, _ := c.controller.Poll("my_consumer_group", "client-1", time.Second*10)
	assert.NotEmpty(c.T(), pollResult.Records)

	first := pollResult.Records[0]
	assert.Equal(c.T(), "woah!", string(first.Payload))
}

/*
We can poll for _no messages_ on an empty topic.
*/
func (c *carnaxTestSuite) TestMessageLog_Poll_SinglePartitionAndConsumer_EmptyPartitionPoll() {
	c.controller.CreateTopic(&apiv1.TopicConfig{
		Name:           "orders",
		PartitionCount: 4,
	})

	// make a new consumer group called "my_consumer_group"
	res, err := c.controller.Subscribe("my_consumer_group", "client-1", "orders")
	assert.NoError(c.T(), err)

	// some time to allow for the rebalance
	minRaftPropagationSleep()
	assert.Equal(c.T(), apiv1.ConsumerGroupState_CONSUMER_GROUP_STATE_STABLE, res.State)
	assert.Equal(c.T(), "my_consumer_group", res.ConsumerGroupId)
	assert.Equal(c.T(), uint64(0), res.ActiveGenerationId, 0)
	assert.Equal(c.T(), "client-1", res.ClientId)

	// we are now in a place to poll and receive no messages
	pollResult, _ := c.controller.Poll("my_consumer_group", "client-1", time.Second*10)
	assert.Empty(c.T(), pollResult.Records)
}

func (c *carnaxTestSuite) TestSharedMessageLog_SinglePartition_MultipleSegment_SequentialReads() {
	err := c.controller.CreateTopic(&apiv1.TopicConfig{
		Name:           "some_topic",
		PartitionCount: 1,
	})
	assert.NoError(c.T(), err)

	// FIRST SEGMENT/WRITE
	_, err = c.controller.Write("some_topic", &apiv1.Record{
		Key:     nil,
		Payload: []byte("felix"),
	})
	assert.NoError(c.T(), err)

	err = c.controller.Flush()
	assert.NoError(c.T(), err)

	// SECOND SEGMENT/WRITE
	_, err = c.controller.Write("some_topic", &apiv1.Record{
		Key:     nil,
		Payload: []byte("angell"),
	})
	assert.NoError(c.T(), err)

	err = c.controller.Flush()
	assert.NoError(c.T(), err)

	// leaves time for log applies.
	minRaftPropagationSleep()

	cgn, err := c.controller.Subscribe("some_id", "my-client", "some_topic")
	assert.NoError(c.T(), err)

	// read (discard)
	firstPoll, _ := c.controller.Poll(cgn.ConsumerGroupId, cgn.ClientId, 15*time.Second)
	assert.Len(c.T(), firstPoll.Records, 1)

	// commit the offset we read
	err = c.controller.CommitSync(cgn.ConsumerGroupId)
	assert.NoError(c.T(), err)

	// read and verify. TODO(FELIX): Adjust config to only poll 1 record at a time.
	poll, _ := c.controller.Poll(cgn.ConsumerGroupId, cgn.ClientId, 15*time.Second)
	assert.Len(c.T(), poll.Records, 1)
	assert.Equal(c.T(), "angell", string(poll.Records[0].Payload))
}

func (c *carnaxTestSuite) TestSharedMessageLog_SinglePartition_MultipleSegment_SequentialReads_IrregularSegmentStrides() {
	err := c.controller.CreateTopic(&apiv1.TopicConfig{
		Name:           "some_topic",
		PartitionCount: 1,
	})
	assert.NoError(c.T(), err)

	// FIRST SEGMENT/WRITE
	_, err = c.controller.Write("some_topic", &apiv1.Record{
		Key:     nil,
		Payload: []byte("felix"),
	})
	assert.NoError(c.T(), err)

	// FIRST SEGMENT/SECOND WRITE.
	_, err = c.controller.Write("some_topic", &apiv1.Record{
		Key:     nil,
		Payload: []byte("loves"),
	})
	assert.NoError(c.T(), err)

	err = c.controller.Flush()
	assert.NoError(c.T(), err)

	// SECOND SEGMENT/WRITE
	_, err = c.controller.Write("some_topic", &apiv1.Record{
		Key:     nil,
		Payload: []byte("carnax"),
	})
	assert.NoError(c.T(), err)

	err = c.controller.Flush()
	assert.NoError(c.T(), err)

	// leaves time for log applies.
	minRaftPropagationSleep()

	cgn, err := c.controller.Subscribe("some_id", "my-client", "some_topic")
	assert.NoError(c.T(), err)

	// read (discard)
	firstPoll, _ := c.controller.Poll(cgn.ConsumerGroupId, cgn.ClientId, 15*time.Second)
	assert.Len(c.T(), firstPoll.Records, 1)

	// commit the offset we read
	err = c.controller.CommitSync(cgn.ConsumerGroupId)
	assert.NoError(c.T(), err)

	// read and verify
	poll, _ := c.controller.Poll(cgn.ConsumerGroupId, cgn.ClientId, 15*time.Second)
	assert.Len(c.T(), poll.Records, 1)
	assert.Equal(c.T(), "loves", string(poll.Records[0].Payload))

	// read and verify
	poll, _ = c.controller.Poll(cgn.ConsumerGroupId, cgn.ClientId, 15*time.Second)
	assert.Len(c.T(), poll.Records, 1)
	assert.Equal(c.T(), "carnax", string(poll.Records[0].Payload))
}
