package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/Jille/raft-grpc-leader-rpc/leaderhealth"
	"github.com/Jille/raftadmin"
	"github.com/yarefs/carnax/api"
	apiv1 "github.com/yarefs/carnax/gen/api/v1"
	controllerv1 "github.com/yarefs/carnax/gen/controller/v1"
	"github.com/yarefs/carnax/internal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"log"
	"net"
)

var (
	myAddr = flag.String("address", "localhost:50051", "TCP host+port for this node")
	raftId = flag.String("raft_id", "", "Node id used by Raft")

	raftDir           = flag.String("raft_data_dir", "data/", "Raft data dir")
	raftBootstrap     = flag.Bool("raft_bootstrap", false, "Whether to bootstrap the Raft cluster")
	raftSnapshotCount = flag.Int("raft_snapshot_count", 3, "How many snapshots to retain")
)

type CarnaxControllerServer struct {
	controllerv1.UnimplementedCarnaxServiceServer
	controller *api.CarnaxController
}

func (c CarnaxControllerServer) Publish(ctx context.Context, request *controllerv1.PublishRequest) (*controllerv1.PublishResponse, error) {
	_, err := c.controller.Write(request.Topic, &apiv1.Record{
		Key:     nil,
		Payload: nil,
	})
	if err != nil {
		log.Println(err)
		return &controllerv1.PublishResponse{
			Ok: false,
		}, nil
	}

	return &controllerv1.PublishResponse{
		Ok: true,
	}, nil
}

func (c CarnaxControllerServer) CreateTopic(ctx context.Context, request *controllerv1.CreateTopicRequest) (*controllerv1.CreateTopicResponse, error) {
	err := c.controller.CreateTopic(request.Config)
	if err != nil {
		return nil, err
	}

	return &controllerv1.CreateTopicResponse{}, nil
}

func main() {
	flag.Parse()

	_, port, err := net.SplitHostPort(*myAddr)
	if err != nil {
		log.Fatal(err)
	}

	sock, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatal(err)
	}

	store := internal.NewInMemoryObjectStore()
	m := api.NewCarnaxControllerWithConfig(store, api.DefaultCarnaxConfig)

	raftInstance, ts := api.NewCarnaxRaft(*raftDir, *raftSnapshotCount, *raftId, *myAddr, m, *raftBootstrap)

	err = m.Start(raftInstance)
	if err != nil {
		panic(err)
	}

	s := grpc.NewServer()
	controllerv1.RegisterCarnaxServiceServer(s, &CarnaxControllerServer{
		controller: m,
	})

	ts.Register(s)

	raftadmin.Register(s, raftInstance)
	leaderhealth.Setup(raftInstance, s, []string{"CarnaxService"})
	reflection.Register(s)

	if e := s.Serve(sock); e != nil {
		log.Fatal(e)
	}
}
