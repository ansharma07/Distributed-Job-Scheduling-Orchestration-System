#!/bin/bash

# Script to run a 5-node distributed task scheduler cluster

set -e

echo "Starting 5-node Distributed Task Scheduler Cluster"
echo "=================================================="

# Clean up previous runs
echo "Cleaning up previous data..."
rm -rf data/ logs/
mkdir -p data logs

# Check if etcd is running
if ! nc -z localhost 2379 2>/dev/null; then
    echo "Error: etcd is not running on localhost:2379"
    echo "Please start etcd first:"
    echo "  docker run -d --name etcd -p 2379:2379 -p 2380:2380 \\"
    echo "    gcr.io/etcd-development/etcd:v3.5.11 \\"
    echo "    /usr/local/bin/etcd \\"
    echo "    --advertise-client-urls http://0.0.0.0:2379 \\"
    echo "    --listen-client-urls http://0.0.0.0:2379"
    exit 1
fi

echo "etcd is running ✓"

# Build the project
echo "Building project..."
make build

# Start node 1 (bootstrap)
echo "Starting node1 (bootstrap)..."
./bin/scheduler \
    -node-id=node1 \
    -raft-addr=127.0.0.1:7001 \
    -grpc-addr=127.0.0.1:8001 \
    -http-addr=127.0.0.1:9001 \
    -raft-dir=./data/raft \
    -bootstrap=true \
    -etcd=127.0.0.1:2379 \
    > logs/node1.log 2>&1 &
NODE1_PID=$!
echo "Node1 started (PID: $NODE1_PID)"

# Wait for node1 to be ready
sleep 3

# Start node 2
echo "Starting node2..."
./bin/scheduler \
    -node-id=node2 \
    -raft-addr=127.0.0.1:7002 \
    -grpc-addr=127.0.0.1:8002 \
    -http-addr=127.0.0.1:9002 \
    -raft-dir=./data/raft \
    -bootstrap=false \
    -join=127.0.0.1:7001 \
    -etcd=127.0.0.1:2379 \
    > logs/node2.log 2>&1 &
NODE2_PID=$!
echo "Node2 started (PID: $NODE2_PID)"

sleep 2

# Start node 3
echo "Starting node3..."
./bin/scheduler \
    -node-id=node3 \
    -raft-addr=127.0.0.1:7003 \
    -grpc-addr=127.0.0.1:8003 \
    -http-addr=127.0.0.1:9003 \
    -raft-dir=./data/raft \
    -bootstrap=false \
    -join=127.0.0.1:7001 \
    -etcd=127.0.0.1:2379 \
    > logs/node3.log 2>&1 &
NODE3_PID=$!
echo "Node3 started (PID: $NODE3_PID)"

sleep 2

# Start node 4
echo "Starting node4..."
./bin/scheduler \
    -node-id=node4 \
    -raft-addr=127.0.0.1:7004 \
    -grpc-addr=127.0.0.1:8004 \
    -http-addr=127.0.0.1:9004 \
    -raft-dir=./data/raft \
    -bootstrap=false \
    -join=127.0.0.1:7001 \
    -etcd=127.0.0.1:2379 \
    > logs/node4.log 2>&1 &
NODE4_PID=$!
echo "Node4 started (PID: $NODE4_PID)"

sleep 2

# Start node 5
echo "Starting node5..."
./bin/scheduler \
    -node-id=node5 \
    -raft-addr=127.0.0.1:7005 \
    -grpc-addr=127.0.0.1:8005 \
    -http-addr=127.0.0.1:9005 \
    -raft-dir=./data/raft \
    -bootstrap=false \
    -join=127.0.0.1:7001 \
    -etcd=127.0.0.1:2379 \
    > logs/node5.log 2>&1 &
NODE5_PID=$!
echo "Node5 started (PID: $NODE5_PID)"

sleep 3

echo ""
echo "Cluster started successfully!"
echo "=============================="
echo "Node PIDs: $NODE1_PID $NODE2_PID $NODE3_PID $NODE4_PID $NODE5_PID"
echo ""
echo "gRPC endpoints:"
echo "  Node1: 127.0.0.1:8001"
echo "  Node2: 127.0.0.1:8002"
echo "  Node3: 127.0.0.1:8003"
echo "  Node4: 127.0.0.1:8004"
echo "  Node5: 127.0.0.1:8005"
echo ""
echo "Metrics endpoints:"
echo "  Node1: http://127.0.0.1:9001/metrics"
echo "  Node2: http://127.0.0.1:9002/metrics"
echo "  Node3: http://127.0.0.1:9003/metrics"
echo "  Node4: http://127.0.0.1:9004/metrics"
echo "  Node5: http://127.0.0.1:9005/metrics"
echo ""
echo "Health check:"
echo "  curl http://127.0.0.1:9001/health"
echo ""
echo "Logs are in ./logs/"
echo ""
echo "To stop the cluster, run: ./scripts/stop-cluster.sh"
echo ""

# Save PIDs for cleanup
echo "$NODE1_PID $NODE2_PID $NODE3_PID $NODE4_PID $NODE5_PID" > .cluster.pids

