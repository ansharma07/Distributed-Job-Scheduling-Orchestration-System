#!/bin/bash

# Script to run multiple workers

set -e

NUM_WORKERS=${1:-5}
SCHEDULER_ADDR=${2:-127.0.0.1:8001}

echo "Starting $NUM_WORKERS workers..."
echo "Connecting to scheduler at $SCHEDULER_ADDR"

mkdir -p logs

for i in $(seq 1 $NUM_WORKERS); do
    ./bin/worker \
        -scheduler=$SCHEDULER_ADDR \
        -worker-id=worker-$i \
        -capacity=10 \
        > logs/worker-$i.log 2>&1 &
    
    WORKER_PID=$!
    echo "Worker $i started (PID: $WORKER_PID)"
    echo $WORKER_PID >> .workers.pids
    
    sleep 0.5
done

echo ""
echo "$NUM_WORKERS workers started successfully!"
echo "Logs are in ./logs/worker-*.log"
echo ""
echo "To stop workers, run: ./scripts/stop-workers.sh"

