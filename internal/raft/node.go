package raft

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// Node represents a Raft node in the cluster
type Node struct {
	raft      *raft.Raft
	fsm       *FSM
	config    *Config
	transport *raft.NetworkTransport
}

// Config holds Raft node configuration
type Config struct {
	NodeID    string
	RaftAddr  string
	RaftDir   string
	Bootstrap bool
	JoinAddr  string
	Peers     []string
}

// NewNode creates a new Raft node
func NewNode(config *Config, fsm *FSM) (*Node, error) {
	// Create raft directory if it doesn't exist
	if err := os.MkdirAll(config.RaftDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create raft directory: %w", err)
	}

	// Setup Raft configuration
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.NodeID)

	// Optimize for sub-2-second leader election
	raftConfig.HeartbeatTimeout = 500 * time.Millisecond
	raftConfig.ElectionTimeout = 500 * time.Millisecond
	raftConfig.LeaderLeaseTimeout = 500 * time.Millisecond
	raftConfig.CommitTimeout = 50 * time.Millisecond

	// Setup Raft communication
	addr, err := net.ResolveTCPAddr("tcp", config.RaftAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve raft address: %w", err)
	}

	transport, err := raft.NewTCPTransport(config.RaftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft transport: %w", err)
	}

	// Create the snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(config.RaftDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}

	// Create the log store and stable store
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(config.RaftDir, "raft-log.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to create log store: %w", err)
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(config.RaftDir, "raft-stable.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to create stable store: %w", err)
	}

	// Instantiate the Raft system
	r, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft instance: %w", err)
	}

	node := &Node{
		raft:      r,
		fsm:       fsm,
		config:    config,
		transport: transport,
	}

	// Bootstrap cluster if needed
	if config.Bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(config.NodeID),
					Address: transport.LocalAddr(),
				},
			},
		}
		r.BootstrapCluster(configuration)
	}

	return node, nil
}

// IsLeader returns true if this node is the leader
func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

// NodeID returns the node ID
func (n *Node) NodeID() string {
	return n.config.NodeID
}

// LeaderAddr returns the address of the current leader
func (n *Node) LeaderAddr() string {
	addr, _ := n.raft.LeaderWithID()
	return string(addr)
}

// Apply applies a command to the Raft log
func (n *Node) Apply(cmd []byte, timeout time.Duration) error {
	future := n.raft.Apply(cmd, timeout)
	return future.Error()
}

// Join adds a new node to the cluster
func (n *Node) Join(nodeID, addr string) error {
	if !n.IsLeader() {
		return fmt.Errorf("not the leader")
	}

	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("failed to get raft configuration: %w", err)
	}

	// Check if node already exists
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(nodeID) {
			return nil // Already a member
		}
	}

	future := n.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	return future.Error()
}

// Leave removes a node from the cluster
func (n *Node) Leave(nodeID string) error {
	if !n.IsLeader() {
		return fmt.Errorf("not the leader")
	}

	future := n.raft.RemoveServer(raft.ServerID(nodeID), 0, 0)
	return future.Error()
}

// Shutdown gracefully shuts down the Raft node
func (n *Node) Shutdown() error {
	return n.raft.Shutdown().Error()
}

// Stats returns Raft statistics
func (n *Node) Stats() map[string]string {
	return n.raft.Stats()
}
