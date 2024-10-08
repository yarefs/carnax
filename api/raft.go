package api

import (
	transport "github.com/Jille/raft-grpc-transport"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"os"
)

// NewCarnaxRaft ... this is a bit messy
func NewCarnaxRaft(raftDir string, retainSnapshotCount int, raftId string, myAddr string, msgLog *CarnaxController, raftBootstrap bool) (*raft.Raft, *transport.Manager) {
	snapshots, err := raft.NewFileSnapshotStore(raftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		log.Fatal(err)
	}

	var (
		logStore    raft.LogStore
		stableStore raft.StableStore
	)
	if /* in mem*/ true {
		logStore = raft.NewInmemStore()
		stableStore = raft.NewInmemStore()
	} else {
		// we can use raft boltdb.
	}

	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(raftId)

	ts := transport.New(raft.ServerAddress(myAddr), []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	})

	r, err := raft.NewRaft(c, (*CarnaxControllerFSM)(msgLog), logStore, stableStore, snapshots, ts.Transport())
	if err != nil {
		log.Fatal(err)
	}

	if raftBootstrap {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(raftId),
					Address:  raft.ServerAddress(myAddr),
				},
			},
		}
		f := r.BootstrapCluster(cfg)
		if err := f.Error(); err != nil {
			log.Fatal(err)
		}
	}

	return r, ts
}
