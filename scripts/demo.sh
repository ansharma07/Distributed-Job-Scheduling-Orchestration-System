#!/bin/bash

set -e

# Get script directory and project root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

cd "$PROJECT_ROOT"

echo "Demo: Distributed Task Scheduler"
echo "================================="
echo ""

# check if etcd is up
if ! nc -z localhost 2379 2>/dev/null; then
    echo "etcd not found, starting it..."
    docker run -d --name etcd-demo \
        -p 2379:2379 -p 2380:2380 \
        gcr.io/etcd-development/etcd:v3.5.11 \
        /usr/local/bin/etcd \
        --advertise-client-urls http://0.0.0.0:2379 \
        --listen-client-urls http://0.0.0.0:2379

    sleep 5
fi

echo "starting cluster..."
./scripts/run-cluster.sh

sleep 5

echo ""
echo "starting workers..."
./scripts/run-workers.sh 5 127.0.0.1:8001

sleep 3

echo ""
echo "submitting 10k tasks..."
./bin/client -scheduler=127.0.0.1:8001 -tasks=10000 -concurrency=100

echo ""
echo "done!"
echo ""
echo "metrics: curl http://127.0.0.1:9001/metrics"
echo "health:  curl http://127.0.0.1:9001/health"
echo ""
echo "cleanup:"
echo "  ./scripts/stop-workers.sh"
echo "  ./scripts/stop-cluster.sh"
echo "  docker stop etcd-demo && docker rm etcd-demo"
echo ""

