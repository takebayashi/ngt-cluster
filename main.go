package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

func main() {
	joinAddr := flag.String("join-addrs", "", "Remote HTTP addresses to join (comma-separated)")
	httpAddr := flag.String("http-addr", "0.0.0.0:8000", "HTTP address")
	httpAdvAddr := flag.String("http-adv-addr", "", "HTTP advertising address")
	rpcAddr := flag.String("rpc-addr", "0.0.0.0:8001", "RPC address")
	rpcAdvAddr := flag.String("rpc-adv-addr", "", "RPC advertising address")
	dataDir := flag.String("data-dir", "", "data directory")
	nodeID := flag.String("id", "", "node id")
	dimension := flag.Int("dimension", 0, "dimension")
	flag.Parse()

	if *httpAdvAddr == "" {
		*httpAdvAddr = *httpAddr
	}
	if *rpcAdvAddr == "" {
		*rpcAdvAddr = *rpcAddr
	}

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(*nodeID)
	config.SnapshotInterval = 20 * time.Second
	config.SnapshotThreshold = 2

	addr, err := net.ResolveTCPAddr("tcp", *rpcAddr)
	if err != nil {
		panic(err)
	}
	transport, err := raft.NewTCPTransport(*rpcAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		panic(err)
	}
	snapshots, err := raft.NewFileSnapshotStore(*dataDir, 3, os.Stderr)
	if err != nil {
		panic(err)
	}
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(*dataDir, "logstore.db"))
	if err != nil {
		panic(err)
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(*dataDir, "stablestore.db"))
	if err != nil {
		panic(err)
	}
	fsm, err := NewNGTState(filepath.Join(*dataDir, "index"), *dimension)
	if err != nil {
		panic(err)
	}
	ra, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		panic(err)
	}

	if *joinAddr != "" && *joinAddr != *httpAddr {
		for i := 0; i < 3; i++ {
			if err := join(*joinAddr, *rpcAdvAddr, *httpAdvAddr, *nodeID); err != nil {
				log.Printf("failed to join: %+v", err)
				time.Sleep(time.Duration(1+i) * time.Second)
				continue
			}
			break
		}
	} else {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(configuration)
	}
	handler := &HTTPHandler{Raft: ra, State: fsm}
	http.ListenAndServe(*httpAddr, handler)
}

func join(joinAddr, raftAddr, httpAddr, nodeID string) error {
	b, err := json.Marshal(MemberJoinRequest{RPCAddr: raftAddr, HTTPAddr: httpAddr, ID: nodeID})
	if err != nil {
		return err
	}
	for _, j := range strings.Split(joinAddr, ",") {
		resp, err := http.Post(fmt.Sprintf("http://%s/members", j), "application/json", bytes.NewReader(b))
		if err != nil || resp.StatusCode != http.StatusOK {
			continue
		}
	}
	return nil
}
