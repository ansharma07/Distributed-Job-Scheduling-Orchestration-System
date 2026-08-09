package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/chandan0804/distributed-task-scheduler/internal/metrics"
	"github.com/chandan0804/distributed-task-scheduler/internal/raft"
	"github.com/chandan0804/distributed-task-scheduler/internal/scheduler"
	"github.com/chandan0804/distributed-task-scheduler/internal/storage"
	pb "github.com/chandan0804/distributed-task-scheduler/proto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
)

var (
	nodeID    = flag.String("node-id", "node1", "Unique node identifier")
	raftAddr  = flag.String("raft-addr", "127.0.0.1:7000", "Raft bind address")
	grpcAddr  = flag.String("grpc-addr", "127.0.0.1:8000", "gRPC bind address")
	httpAddr  = flag.String("http-addr", "127.0.0.1:9000", "HTTP metrics address")
	raftDir   = flag.String("raft-dir", "./data/raft", "Raft data directory")
	bootstrap = flag.Bool("bootstrap", false, "Bootstrap a new cluster")
	joinAddr  = flag.String("join", "", "Address of node to join")
	etcdAddr  = flag.String("etcd", "127.0.0.1:2379", "etcd endpoints (comma-separated)")
)

func main() {
	flag.Parse()

	// Setup logging
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Printf("Starting scheduler node: %s", *nodeID)

	// Create Raft FSM
	fsm := raft.NewFSM()

	// Create Raft node
	raftConfig := &raft.Config{
		NodeID:    *nodeID,
		RaftAddr:  *raftAddr,
		RaftDir:   fmt.Sprintf("%s/%s", *raftDir, *nodeID),
		Bootstrap: *bootstrap,
		JoinAddr:  *joinAddr,
	}

	raftNode, err := raft.NewNode(raftConfig, fsm)
	if err != nil {
		log.Fatalf("Failed to create Raft node: %v", err)
	}
	defer raftNode.Shutdown()

	log.Printf("Raft node created successfully")

	// Connect to etcd
	etcdEndpoints := strings.Split(*etcdAddr, ",")
	etcdStorage, err := storage.NewEtcdStorage(etcdEndpoints)
	if err != nil {
		log.Fatalf("Failed to connect to etcd: %v", err)
	}
	defer etcdStorage.Close()

	log.Printf("Connected to etcd at %v", etcdEndpoints)

	// Initialize Datadog metrics client
	datadogClient, err := metrics.NewDatadogClient("datadog-agent:8125")
	if err != nil {
		log.Printf("Warning: Failed to initialize Datadog client: %v", err)
		datadogClient = nil
	} else {
		defer datadogClient.Close()
		log.Printf("Datadog metrics enabled")
	}

	// Create scheduler server
	schedulerServer := scheduler.NewServer(raftNode, etcdStorage)
	if datadogClient != nil {
		schedulerServer.SetDatadogClient(datadogClient)
	}

	// Start gRPC server
	grpcListener, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", *grpcAddr, err)
	}

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(10*1024*1024), // 10MB
		grpc.MaxSendMsgSize(10*1024*1024), // 10MB
	)
	pb.RegisterTaskSchedulerServer(grpcServer, schedulerServer)

	go func() {
		log.Printf("gRPC server listening on %s", *grpcAddr)
		if err := grpcServer.Serve(grpcListener); err != nil {
			log.Fatalf("Failed to serve gRPC: %v", err)
		}
	}()

	// Start HTTP server for metrics
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if raftNode.IsLeader() {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "OK - Leader")
		} else {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "OK - Follower (Leader: %s)", raftNode.LeaderAddr())
		}
	})

	go func() {
		log.Printf("HTTP metrics server listening on %s", *httpAddr)
		if err := http.ListenAndServe(*httpAddr, nil); err != nil {
			log.Fatalf("Failed to serve HTTP: %v", err)
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	log.Printf("Scheduler node %s is running", *nodeID)
	log.Printf("  Raft address: %s", *raftAddr)
	log.Printf("  gRPC address: %s", *grpcAddr)
	log.Printf("  HTTP address: %s", *httpAddr)
	log.Printf("  Is Leader: %v", raftNode.IsLeader())

	<-sigChan
	log.Println("Shutting down gracefully...")

	grpcServer.GracefulStop()
	log.Println("Scheduler stopped")
}
