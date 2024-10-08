package api

import "fmt"

type TopicPartitionHash string

func tpHash(topic string, partitionIndex uint32) TopicPartitionHash {
	rawHash := fmt.Sprintf("%s-%d", topic, partitionIndex)
	return TopicPartitionHash(rawHash)
}
