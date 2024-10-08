package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	apiv1 "github.com/yarefs/carnax/gen/api/v1"
	controllerv1 "github.com/yarefs/carnax/gen/controller/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"os"
	"strings"
)

var (
	serverAddr = flag.String("bootstrap.servers", "", "address of any or all broker nodes")
)

func main() {
	flag.Parse()

	conn, err := grpc.NewClient(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	log.Println("Connected to", *serverAddr)

	client := controllerv1.NewCarnaxServiceClient(conn)
	_, err = client.CreateTopic(context.Background(), &controllerv1.CreateTopicRequest{
		Config: &apiv1.TopicConfig{
			Name:           "orders",
			PartitionCount: 40,
		},
	})
	if err != nil {
		panic(err)
	}

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Printf("%s> ", *serverAddr)

		// exits on EOF, etc.
		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())
		parts := strings.Split(input, " ")

		// Handle special cases like exit or quit
		switch parts[0] {
		case "exit", "quit":
			fmt.Println("Exiting REPL.")
			return
		case "publish", "pub":
			handlePublish(client, parts)
		default:
			// Evaluate and respond
			log.Println("unhandled command:", input)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "Error reading input:", err)
	}
}

func handlePublish(ctrl controllerv1.CarnaxServiceClient, parts []string) {
	topic := parts[1]

	keyVal := strings.Split(parts[2], ":")
	key, val := keyVal[0], keyVal[1]
	log.Println("Publish", key, val, "to", topic)

	_, err := ctrl.Publish(context.Background(), &controllerv1.PublishRequest{
		Topic: topic,
		Key:   []byte(key),
		Value: []byte(val),
	})
	if err != nil {
		panic(err)
	}
}
