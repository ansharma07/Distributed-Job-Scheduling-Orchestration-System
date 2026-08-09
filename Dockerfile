# Build stage
FROM golang:1.21-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make protobuf-dev

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Install protoc plugins (use compatible versions for Go 1.21)
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.1 && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0

# Copy source code
COPY . .

# Generate protobuf files
RUN protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    proto/scheduler.proto

# Build binaries
RUN go build -mod=mod -o /app/bin/scheduler cmd/scheduler/main.go && \
    go build -mod=mod -o /app/bin/worker cmd/worker/main.go && \
    go build -mod=mod -o /app/bin/client cmd/client/main.go

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy binaries from builder
COPY --from=builder /app/bin/scheduler .
COPY --from=builder /app/bin/worker .
COPY --from=builder /app/bin/client .

# Default command (can be overridden)
CMD ["./scheduler"]

