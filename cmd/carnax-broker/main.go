package main

import (
	"flag"
	"github.com/yarefs/carnax/cmd/carnax-broker/server"
	"log"
)

var (
	myAddr = flag.String("address", "localhost:50051", "TCP host+port for this node")
	raftId = flag.String("raft_id", "", "Node id used by Raft")

	raftDir           = flag.String("raft_data_dir", "data/", "Raft data dir")
	raftBootstrap     = flag.Bool("raft_bootstrap", false, "Whether to bootstrap the Raft cluster")
	raftSnapshotCount = flag.Int("raft_snapshot_count", 3, "How many snapshots to retain")
)

func main() {
	flag.Parse()

	_, s, sock := server.ListenAndServe(*myAddr, *raftDir, *raftSnapshotCount, *raftId, *raftBootstrap)
	if e := s.Serve(sock); e != nil {
		log.Fatal(e)
	}
}
