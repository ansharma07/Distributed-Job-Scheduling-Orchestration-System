package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/chandan0804/distributed-task-scheduler/internal/storage"
	pb "github.com/chandan0804/distributed-task-scheduler/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockRaftNode is a mock implementation of Raft node for testing
type MockRaftNode struct {
	isLeader   bool
	leaderAddr string
	nodeID     string
}

func (m *MockRaftNode) IsLeader() bool {
	return m.isLeader
}

func (m *MockRaftNode) LeaderAddr() string {
	return m.leaderAddr
}

func (m *MockRaftNode) NodeID() string {
	return m.nodeID
}

func (m *MockRaftNode) Stats() map[string]string {
	return map[string]string{
		"state": "Leader",
	}
}

func (m *MockRaftNode) Apply(cmd []byte, timeout time.Duration) error {
	return nil
}

func (m *MockRaftNode) Shutdown() error {
	return nil
}

// MockEtcdStorage is a mock implementation of etcd storage for testing
type MockEtcdStorage struct {
	tasks map[string]*storage.Task
}

func NewMockEtcdStorage() *MockEtcdStorage {
	return &MockEtcdStorage{
		tasks: make(map[string]*storage.Task),
	}
}

func (m *MockEtcdStorage) EnqueueTask(ctx context.Context, task *storage.Task) error {
	m.tasks[task.ID] = task
	return nil
}

func (m *MockEtcdStorage) DequeueTask(ctx context.Context, workerID string) (*storage.Task, error) {
	for _, task := range m.tasks {
		if task.Status == "PENDING" {
			task.Status = "RUNNING"
			task.AssignedWorker = workerID
			return task, nil
		}
	}
	return nil, nil
}

func (m *MockEtcdStorage) UpdateTask(ctx context.Context, task *storage.Task) error {
	m.tasks[task.ID] = task
	return nil
}

func (m *MockEtcdStorage) CompleteTask(ctx context.Context, taskID string) error {
	if task, exists := m.tasks[taskID]; exists {
		task.Status = "COMPLETED"
	}
	return nil
}

func (m *MockEtcdStorage) GetTask(ctx context.Context, taskID string) (*storage.Task, error) {
	if task, exists := m.tasks[taskID]; exists {
		return task, nil
	}
	return nil, nil
}

func (m *MockEtcdStorage) ListTasks(ctx context.Context, status string) ([]*storage.Task, error) {
	var result []*storage.Task
	for _, task := range m.tasks {
		if status == "" || task.Status == status {
			result = append(result, task)
		}
	}
	return result, nil
}

func (m *MockEtcdStorage) Close() error {
	return nil
}

func TestNewServer(t *testing.T) {
	mockRaft := &MockRaftNode{
		isLeader:   true,
		leaderAddr: "localhost:8001",
		nodeID:     "node1",
	}
	mockStorage := NewMockEtcdStorage()

	// Use nil registry for tests to avoid Prometheus registration conflicts
	metrics := NewMetricsWithRegistry(nil)
	server := NewServerWithMetrics(mockRaft, mockStorage, metrics)

	assert.NotNil(t, server)
	assert.NotNil(t, server.metrics)
	assert.NotNil(t, server.workers)
	assert.NotNil(t, server.taskChannels)
}

func TestSubmitTask_AsLeader(t *testing.T) {
	mockRaft := &MockRaftNode{
		isLeader:   true,
		leaderAddr: "localhost:8001",
		nodeID:     "node1",
	}
	mockStorage := NewMockEtcdStorage()
	metrics := NewMetricsWithRegistry(nil)
	server := NewServerWithMetrics(mockRaft, mockStorage, metrics)

	ctx := context.Background()
	req := &pb.SubmitTaskRequest{
		Name:        "test-task",
		Payload:     []byte("test-payload"),
		ScheduledAt: time.Now().Unix(),
		MaxRetries:  3,
	}

	resp, err := server.SubmitTask(ctx, req)

	require.NoError(t, err)
	assert.True(t, resp.Success)
	assert.NotEmpty(t, resp.TaskId)
	assert.Equal(t, "task submitted successfully", resp.Message)
}

func TestSubmitTask_AsFollower(t *testing.T) {
	mockRaft := &MockRaftNode{
		isLeader:   false,
		leaderAddr: "localhost:8002",
		nodeID:     "node1",
	}
	mockStorage := NewMockEtcdStorage()
	metrics := NewMetricsWithRegistry(nil)
	server := NewServerWithMetrics(mockRaft, mockStorage, metrics)

	ctx := context.Background()
	req := &pb.SubmitTaskRequest{
		Name:        "test-task",
		Payload:     []byte("test-payload"),
		ScheduledAt: time.Now().Unix(),
		MaxRetries:  3,
	}

	resp, err := server.SubmitTask(ctx, req)

	require.NoError(t, err)
	assert.False(t, resp.Success)
	assert.Contains(t, resp.Message, "not the leader")
}

func TestHealthCheck(t *testing.T) {
	mockRaft := &MockRaftNode{
		isLeader:   true,
		leaderAddr: "localhost:8001",
		nodeID:     "node1",
	}
	mockStorage := NewMockEtcdStorage()
	metrics := NewMetricsWithRegistry(nil)
	server := NewServerWithMetrics(mockRaft, mockStorage, metrics)

	// Add some test tasks
	ctx := context.Background()
	mockStorage.EnqueueTask(ctx, &storage.Task{
		ID:     "task1",
		Status: "PENDING",
	})
	mockStorage.EnqueueTask(ctx, &storage.Task{
		ID:     "task2",
		Status: "RUNNING",
	})

	req := &pb.HealthCheckRequest{}
	resp, err := server.HealthCheck(ctx, req)

	require.NoError(t, err)
	assert.True(t, resp.IsLeader)
	assert.Equal(t, "localhost:8001", resp.LeaderId)
	assert.Equal(t, int64(1), resp.TasksPending)
	assert.Equal(t, int64(1), resp.TasksRunning)
}

func TestRegisterWorker(t *testing.T) {
	mockRaft := &MockRaftNode{
		isLeader:   true,
		leaderAddr: "localhost:8001",
		nodeID:     "node1",
	}
	mockStorage := NewMockEtcdStorage()
	metrics := NewMetricsWithRegistry(nil)
	server := NewServerWithMetrics(mockRaft, mockStorage, metrics)

	workerID := "worker-1"
	capacity := int32(10)

	server.registerWorker(workerID, capacity)

	server.workersMu.RLock()
	worker, exists := server.workers[workerID]
	server.workersMu.RUnlock()

	assert.True(t, exists)
	assert.Equal(t, workerID, worker.ID)
	assert.Equal(t, capacity, worker.Capacity)
	assert.Equal(t, int32(0), worker.ActiveTasks)
}

func TestMetrics(t *testing.T) {
	metrics := NewMetricsWithRegistry(nil)

	// Test task submission
	metrics.RecordTaskSubmitted()
	assert.Equal(t, int64(1), metrics.GetSubmittedTasks())

	// Test task distribution
	metrics.RecordTasksDistributed(5)
	assert.Equal(t, int64(5), metrics.GetDistributedTasks())

	// Test task completion
	metrics.RecordTaskCompleted()
	assert.Equal(t, int64(1), metrics.GetCompletedTasks())

	// Test task failure
	metrics.RecordTaskFailed()
	assert.Equal(t, int64(1), metrics.GetFailedTasks())
}

func TestWorkerFailureDetection(t *testing.T) {
	mockRaft := &MockRaftNode{
		isLeader:   true,
		leaderAddr: "localhost:8001",
		nodeID:     "node1",
	}
	mockStorage := NewMockEtcdStorage()
	metrics := NewMetricsWithRegistry(nil)
	server := NewServerWithMetrics(mockRaft, mockStorage, metrics)

	// Register a worker
	server.workersMu.Lock()
	server.workers["worker-1"] = &WorkerInfo{
		ID:            "worker-1",
		Capacity:      10,
		ActiveTasks:   2,
		LastHeartbeat: time.Now().Add(-60 * time.Second), // 60 seconds ago (dead)
		AssignedTasks: []string{"task-1", "task-2"},
	}
	server.workersMu.Unlock()

	// Add tasks to storage
	ctx := context.Background()
	mockStorage.EnqueueTask(ctx, &storage.Task{
		ID:             "task-1",
		Name:           "test-task-1",
		Status:         "RUNNING",
		AssignedWorker: "worker-1",
	})
	mockStorage.EnqueueTask(ctx, &storage.Task{
		ID:             "task-2",
		Name:           "test-task-2",
		Status:         "RUNNING",
		AssignedWorker: "worker-1",
	})

	// Run health check
	server.checkAndReassignDeadWorkers()

	// Verify worker was removed
	server.workersMu.RLock()
	_, exists := server.workers["worker-1"]
	server.workersMu.RUnlock()
	assert.False(t, exists, "Dead worker should be removed")

	// Verify tasks were reassigned (status changed to PENDING)
	task1, _ := mockStorage.GetTask(ctx, "task-1")
	task2, _ := mockStorage.GetTask(ctx, "task-2")

	assert.Equal(t, "PENDING", task1.Status, "Task should be reassigned")
	assert.Equal(t, "", task1.AssignedWorker, "Task should have no assigned worker")
	assert.Equal(t, "PENDING", task2.Status, "Task should be reassigned")
	assert.Equal(t, "", task2.AssignedWorker, "Task should have no assigned worker")
}

func TestRemoveTaskFromList(t *testing.T) {
	tests := []struct {
		name     string
		tasks    []string
		taskID   string
		expected []string
	}{
		{
			name:     "Remove from middle",
			tasks:    []string{"task-1", "task-2", "task-3"},
			taskID:   "task-2",
			expected: []string{"task-1", "task-3"},
		},
		{
			name:     "Remove from beginning",
			tasks:    []string{"task-1", "task-2", "task-3"},
			taskID:   "task-1",
			expected: []string{"task-2", "task-3"},
		},
		{
			name:     "Remove from end",
			tasks:    []string{"task-1", "task-2", "task-3"},
			taskID:   "task-3",
			expected: []string{"task-1", "task-2"},
		},
		{
			name:     "Task not found",
			tasks:    []string{"task-1", "task-2"},
			taskID:   "task-99",
			expected: []string{"task-1", "task-2"},
		},
		{
			name:     "Empty list",
			tasks:    []string{},
			taskID:   "task-1",
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := removeTaskFromList(tt.tasks, tt.taskID)
			assert.Equal(t, tt.expected, result)
		})
	}
}
