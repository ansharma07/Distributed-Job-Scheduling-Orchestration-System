package test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/chandan0804/distributed-task-scheduler/internal/raft"
	"github.com/chandan0804/distributed-task-scheduler/internal/scheduler"
	"github.com/chandan0804/distributed-task-scheduler/internal/storage"
	pb "github.com/chandan0804/distributed-task-scheduler/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_SingleNode tests a single-node cluster
func TestIntegration_SingleNode(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Skip if etcd is not available
	if os.Getenv("ETCD_ENDPOINTS") == "" {
		t.Skip("Skipping integration test: ETCD_ENDPOINTS not set")
	}

	// Create temporary directory for Raft data
	raftDir := t.TempDir()

	// Create Raft FSM
	fsm := raft.NewFSM()

	// Create Raft node
	raftConfig := &raft.Config{
		NodeID:    "test-node-1",
		RaftAddr:  "127.0.0.1:9001",
		RaftDir:   raftDir,
		Bootstrap: true,
	}

	raftNode, err := raft.NewNode(raftConfig, fsm)
	require.NoError(t, err)
	defer raftNode.Shutdown()

	// Wait for leader election
	time.Sleep(2 * time.Second)
	assert.True(t, raftNode.IsLeader(), "Node should become leader")

	// Connect to etcd
	etcdEndpoints := []string{os.Getenv("ETCD_ENDPOINTS")}
	if etcdEndpoints[0] == "" {
		etcdEndpoints = []string{"localhost:2379"}
	}

	etcdStorage, err := storage.NewEtcdStorage(etcdEndpoints)
	require.NoError(t, err)
	defer etcdStorage.Close()

	// Create scheduler server
	server := scheduler.NewServer(raftNode, etcdStorage)

	// Test task submission
	ctx := context.Background()
	req := &pb.SubmitTaskRequest{
		Name:        "integration-test-task",
		Payload:     []byte("test-payload"),
		ScheduledAt: time.Now().Unix(),
		MaxRetries:  3,
	}

	resp, err := server.SubmitTask(ctx, req)
	require.NoError(t, err)
	assert.True(t, resp.Success)
	assert.NotEmpty(t, resp.TaskId)

	// Verify task was stored
	task, err := etcdStorage.GetTask(ctx, resp.TaskId)
	require.NoError(t, err)
	assert.Equal(t, "integration-test-task", task.Name)
	assert.Equal(t, "PENDING", task.Status)

	// Test health check
	healthReq := &pb.HealthCheckRequest{}
	healthResp, err := server.HealthCheck(ctx, healthReq)
	require.NoError(t, err)
	assert.True(t, healthResp.IsLeader)
	assert.Equal(t, int64(1), healthResp.TasksPending)
}

// TestIntegration_TaskLifecycle tests the complete lifecycle of a task
func TestIntegration_TaskLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if os.Getenv("ETCD_ENDPOINTS") == "" {
		t.Skip("Skipping integration test: ETCD_ENDPOINTS not set")
	}

	ctx := context.Background()

	// Connect to etcd
	etcdEndpoints := []string{os.Getenv("ETCD_ENDPOINTS")}
	if etcdEndpoints[0] == "" {
		etcdEndpoints = []string{"localhost:2379"}
	}

	etcdStorage, err := storage.NewEtcdStorage(etcdEndpoints)
	require.NoError(t, err)
	defer etcdStorage.Close()

	// Create a task
	task := &storage.Task{
		ID:          "lifecycle-test-task",
		Name:        "Test Task",
		Payload:     []byte("test-payload"),
		CreatedAt:   time.Now(),
		ScheduledAt: time.Now(),
		Status:      "PENDING",
		RetryCount:  0,
		MaxRetries:  3,
	}

	// Enqueue task
	err = etcdStorage.EnqueueTask(ctx, task)
	require.NoError(t, err)

	// Dequeue task
	dequeuedTask, err := etcdStorage.DequeueTask(ctx, "test-worker")
	require.NoError(t, err)
	assert.NotNil(t, dequeuedTask)
	assert.Equal(t, "lifecycle-test-task", dequeuedTask.ID)
	assert.Equal(t, "RUNNING", dequeuedTask.Status)

	// Complete task
	err = etcdStorage.CompleteTask(ctx, task.ID)
	require.NoError(t, err)

	// Verify task is completed
	completedTask, err := etcdStorage.GetTask(ctx, task.ID)
	require.NoError(t, err)
	assert.Equal(t, "COMPLETED", completedTask.Status)
}

// TestIntegration_ConcurrentTaskSubmission tests concurrent task submissions
func TestIntegration_ConcurrentTaskSubmission(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if os.Getenv("ETCD_ENDPOINTS") == "" {
		t.Skip("Skipping integration test: ETCD_ENDPOINTS not set")
	}

	// This test would require a full cluster setup
	// For now, we'll keep it as a placeholder for future implementation
	t.Skip("Full cluster integration test not yet implemented")
}
