#!/bin/bash

# Script to stop all workers

echo "Stopping workers..."

if [ -f .workers.pids ]; then
    while read PID; do
        if kill -0 $PID 2>/dev/null; then
            echo "Stopping worker $PID..."
            kill $PID
        fi
    done < .workers.pids
    rm .workers.pids
    echo "Workers stopped successfully!"
else
    echo "No worker PIDs found. Trying to kill by process name..."
    pkill -f "bin/worker" || true
    echo "Done!"
fi

