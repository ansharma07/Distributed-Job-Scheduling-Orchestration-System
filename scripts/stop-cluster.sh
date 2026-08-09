#!/bin/bash

# Script to stop the distributed task scheduler cluster

echo "Stopping Distributed Task Scheduler Cluster..."

if [ -f .cluster.pids ]; then
    PIDS=$(cat .cluster.pids)
    for PID in $PIDS; do
        if kill -0 $PID 2>/dev/null; then
            echo "Stopping process $PID..."
            kill $PID
        fi
    done
    rm .cluster.pids
    echo "Cluster stopped successfully!"
else
    echo "No cluster PIDs found. Trying to kill by process name..."
    pkill -f "bin/scheduler" || true
    echo "Done!"
fi

