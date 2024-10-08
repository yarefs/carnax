package api

import (
	"fmt"
	apiv1 "github.com/yarefs/carnax/gen/api/v1"
)

type TopicPartitionHash apiv1.TopicPartition

func (t *TopicPartitionHash) String() string {
	return fmt.Sprintf("%s-%d", t.Topic, t.PartitionIndex)
}

func newTopicHash(topic string, partitionIndex uint32) *TopicPartitionHash {
	tp := &apiv1.TopicPartition{
		Topic:          topic,
		PartitionIndex: partitionIndex,
	}
	return (*TopicPartitionHash)(tp)
}
