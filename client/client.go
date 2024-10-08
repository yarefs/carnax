package client

import (
	"context"
	controllerv1 "github.com/yarefs/carnax/gen/controller/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"sync"
	"time"
)

type CarnaxPublisherConfigProperties map[string]string

type CarnaxPublishConfig struct {
	BatchSizeBytes int
	LingerMS       int64
}

var DefaultCarnaxPublisherConfig = CarnaxPublishConfig{
	BatchSizeBytes: 0,
	LingerMS:       10,
}

type CarnaxPublisher struct {
	client                  controllerv1.CarnaxServiceClient
	mu                      sync.Mutex
	publishBatch            []*controllerv1.PublishRequest
	bytesAccumulatedInBatch int
	config                  CarnaxPublishConfig
	shutdownSignal          chan struct{}
}

func (c *CarnaxPublisher) Start() {
	go c.batchProcessor()
}
func (c *CarnaxPublisher) Shutdown() {
	close(c.shutdownSignal)
}

func (c *CarnaxPublisher) batchProcessor() {
	lastTime := time.Now()
	for {
		select {
		case <-c.shutdownSignal:
			return
		default:
			elapsedMs := time.Now().Sub(lastTime).Milliseconds()
			if c.hasFullBatch() || elapsedMs > c.config.LingerMS {
				c.flush()
			}
		}
	}
}

func (c *CarnaxPublisher) Publish(topic string, key []byte, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	request := &controllerv1.PublishRequest{
		Topic: topic,
		Key:   key,
		Value: value,
	}

	// NIT: This isn't equivalent to a record per se which is the actual
	// size going over the wire (or into the log).
	approxBytesInPayload := len(request.Key) + len(request.Value)

	c.bytesAccumulatedInBatch += approxBytesInPayload

	c.publishBatch = append(c.publishBatch, request)
}

func (c *CarnaxPublisher) flush() {
	c.mu.Lock()
	defer c.mu.Unlock()

	toSend := make([]*controllerv1.PublishRequest, len(c.publishBatch))
	copy(toSend, c.publishBatch)

	c.publishBatch = []*controllerv1.PublishRequest{}
	c.bytesAccumulatedInBatch = 0

	go c.sendBatch(toSend)
}

func (c *CarnaxPublisher) hasFullBatch() bool {
	return c.bytesAccumulatedInBatch > c.config.BatchSizeBytes
}

func (c *CarnaxPublisher) sendBatch(send []*controllerv1.PublishRequest) {
	// create a batch request and send it to the client.
	if len(send) != 0 {
		log.Println("Publish the batch of", len(send), "messages.")
	}

	_, err := c.client.BatchPublish(context.Background(), &controllerv1.BatchPublishRequest{
		Requests: send,
	})
	if err != nil {
		panic(err)
	}
}

func NewCarnaxPublisher(addr string) *CarnaxPublisher {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			panic(err)
		}
	}(conn)

	client := controllerv1.NewCarnaxServiceClient(conn)
	return &CarnaxPublisher{
		client:                  client,
		publishBatch:            []*controllerv1.PublishRequest{},
		bytesAccumulatedInBatch: 0,
		shutdownSignal:          make(chan struct{}),
		config:                  DefaultCarnaxPublisherConfig,
	}
}
