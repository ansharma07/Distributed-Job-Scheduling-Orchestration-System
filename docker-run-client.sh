#!/bin/bash

# Script to run the client in Docker

echo "Running task scheduler client..."
echo ""

docker run --rm --network project_scheduler-network \
  project-scheduler-1:latest \
  ./client -scheduler=scheduler-1:8001 -tasks=10000 -concurrency=100

