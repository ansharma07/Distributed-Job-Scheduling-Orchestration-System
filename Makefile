.PHONY: proto build run-cluster clean test docker-build docker-up docker-down docker-logs demo help docker-build-images docker-push-images docker-publish

# Generate protobuf files
proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/scheduler.proto

# Build the project
build: proto
	go build -o bin/scheduler cmd/scheduler/main.go
	go build -o bin/worker cmd/worker/main.go
	go build -o bin/client cmd/client/main.go

# Run a 5-node cluster locally
run-cluster:
	./scripts/run-cluster.sh

# Run single node
run-node:
	go run cmd/scheduler/main.go

# Run worker
run-worker:
	go run cmd/worker/main.go

# Run client demo
run-client:
	go run cmd/client/main.go

# Clean build artifacts
clean:
	rm -rf bin/
	rm -rf data/
	rm -rf logs/
	rm -f proto/*.pb.go

# Run tests
test:
	go test -v -race ./...

# Run benchmarks
bench:
	go test -bench=. -benchmem ./...

# Install dependencies
deps:
	go mod download
	go mod tidy

# Docker build
docker-build:
	docker build -t distributed-task-scheduler:latest .

# Format code
fmt:
	go fmt ./...

# Lint code
lint:
	@echo "Running go fmt check..."
	@if [ "$$(gofmt -s -l . | wc -l)" -gt 0 ]; then \
		echo "Code is not formatted. Run 'make fmt'"; \
		gofmt -s -l .; \
		exit 1; \
	fi
	@echo "Running go vet..."
	go vet ./...

# Docker Compose targets
docker-up:
	docker-compose up -d
	@echo "Waiting for cluster to start..."
	@sleep 10
	docker-compose ps

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

# Run the demo with Docker
demo: docker-up
	@echo "Waiting for cluster to be ready..."
	@sleep 15
	./docker-run-client.sh

# Help target
help:
	@echo "Available targets:"
	@echo "  make proto        - Generate protobuf code"
	@echo "  make build        - Build all binaries"
	@echo "  make test         - Run tests"
	@echo "  make bench              - Run benchmarks"
	@echo "  make lint               - Run linters"
	@echo "  make fmt                - Format code"
	@echo "  make clean              - Clean build artifacts"
	@echo "  make docker-build       - Build Docker images"
	@echo "  make docker-up          - Start Docker Compose cluster"
	@echo "  make docker-down        - Stop Docker Compose cluster"
	@echo "  make docker-logs        - View Docker logs"
	@echo "  make demo               - Run full demo with Docker"
	@echo "  make run-cluster        - Run 5-node cluster locally"
	@echo "  make deps               - Install dependencies"
	@echo "  make docker-build-images - Build production Docker images"
	@echo "  make docker-push-images  - Push images to Docker Hub"
	@echo "  make docker-publish      - Build and push images"

# Docker Hub configuration
DOCKER_USERNAME ?= chandan0804
IMAGE_TAG ?= latest
SCHEDULER_IMAGE = $(DOCKER_USERNAME)/distributed-task-scheduler
WORKER_IMAGE = $(DOCKER_USERNAME)/distributed-task-worker

# Build production Docker images
docker-build-images:
	@echo "🔨 Building production Docker images..."
	docker build -f Dockerfile.scheduler -t $(SCHEDULER_IMAGE):$(IMAGE_TAG) .
	docker build -f Dockerfile.worker -t $(WORKER_IMAGE):$(IMAGE_TAG) .
	@echo "✅ Images built successfully!"
	@echo "   - $(SCHEDULER_IMAGE):$(IMAGE_TAG)"
	@echo "   - $(WORKER_IMAGE):$(IMAGE_TAG)"

# Push images to Docker Hub
docker-push-images:
	@echo "📤 Pushing images to Docker Hub..."
	docker push $(SCHEDULER_IMAGE):$(IMAGE_TAG)
	docker push $(WORKER_IMAGE):$(IMAGE_TAG)
	@echo "✅ Images pushed successfully!"

# Build and push images
docker-publish: docker-build-images docker-push-images
	@echo "🎉 Images published to Docker Hub!"
	@echo ""
	@echo "Try it:"
	@echo "  docker run -p 8001:8001 $(SCHEDULER_IMAGE):$(IMAGE_TAG)"
	@echo "  docker run $(WORKER_IMAGE):$(IMAGE_TAG)"
