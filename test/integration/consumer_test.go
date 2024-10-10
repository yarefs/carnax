package integration

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/yarefs/carnax/client"
	"github.com/yarefs/carnax/cmd/carnax-broker/server"
	"sync"
	"testing"
	"time"
)

/*
Spins up a single node leader + and a minio instance to write to disk
*/
func TestConsumerRestart(t *testing.T) {
	t.Skip()

	ctx := context.Background()

	signal := make(chan struct{})

	// leader
	go func() {
		m, s, sock := server.ListenAndServe(":1234", "/tmp/leader", 1, "leaderNode", true)

		go func() {
			if err := s.Serve(sock); err != nil {
				panic(err)
			}
		}()
		<-signal

		if err := m.Shutdown(); err != nil {
			panic(err)
		}
	}()

	_, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "minio/minio",
			ExposedPorts: []string{"9000/tcp"},

			// the final log when starting minio.
			WaitingFor: wait.ForLog("Docs: https://docs.min.io"),
		},
	})
	assert.NoError(t, err)

	// we should wait for the broker to be ready.
	time.Sleep(5 * time.Second)

	publisherClient := client.NewCarnaxPublisher("localhost:1234")
	publisherClient.Start()

	var wg sync.WaitGroup
	for i := 0; i < 10_000; i += 1 {
		wg.Add(1)
		go func() {
			publisherClient.Publish("some_topic", nil, []byte("hello, world!"))
			wg.Done()
		}()
	}
	wg.Wait()

	time.Sleep(10 * time.Second)

	publisherClient.Shutdown()

	close(signal)
}
