#!/bin/bash

# Interview Demo Script
# This script demonstrates the distributed task scheduler in action

set -e

echo "🚀 Distributed Task Scheduler - Live Demo"
echo "=========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}📦 Step 1: Starting the cluster...${NC}"
echo "   - 5 Scheduler nodes (Raft cluster)"
echo "   - 5 Worker nodes"
echo "   - 1 etcd instance"
echo "   - 1 Datadog agent"
echo ""

docker-compose up -d

echo ""
echo -e "${YELLOW}⏳ Waiting for cluster to be ready (10 seconds)...${NC}"
sleep 10

echo ""
echo -e "${GREEN}✅ Step 2: Cluster Status${NC}"
docker-compose ps

echo ""
echo -e "${BLUE}📊 Step 3: Submitting 10,000 tasks...${NC}"
echo "   Watch the throughput!"
echo ""

# Run client and capture output
./docker-run-client.sh

echo ""
echo -e "${GREEN}✅ Step 4: Checking Metrics${NC}"
echo ""
echo "Task Metrics:"
curl -s http://localhost:9001/metrics | grep -E "task_submit_total|task_complete_total|task_fail_total" | head -5

echo ""
echo ""
echo "Active Workers:"
curl -s http://localhost:9001/metrics | grep "active_workers" | head -1

echo ""
echo ""
echo -e "${BLUE}📋 Step 5: Recent Scheduler Logs${NC}"
docker-compose logs --tail=15 scheduler-1 | grep -E "Task|Worker|Raft"

echo ""
echo ""
echo -e "${GREEN}✅ Demo Complete!${NC}"
echo ""
echo "The cluster is now running. You can:"
echo "  • View metrics:  curl http://localhost:9001/metrics"
echo "  • Health check:  curl http://localhost:9001/health"
echo "  • View logs:     docker-compose logs -f scheduler-1"
echo "  • Stop cluster:  docker-compose down"
echo ""
echo -e "${YELLOW}💡 Try killing a worker to see fault tolerance:${NC}"
echo "   docker stop worker-1"
echo "   docker-compose logs -f scheduler-1  # Watch task reassignment"
echo ""

