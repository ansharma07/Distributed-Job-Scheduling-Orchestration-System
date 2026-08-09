package test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/chandan0804/distributed-task-scheduler/internal/scheduler"
	"github.com/chandan0804/distributed-task-scheduler/internal/storage"
	pb "github.com/chandan0804/distributed-task-scheduler/proto"
)

// MockRaftNode for benchmarking
type MockRaftNode struct {
	isLeader   bool
	leaderAddr string
	nodeID     string
}

func (m *MockRaftNode) IsLeader() bool                                { return m.isLeader }
func (m *MockRaftNode) LeaderAddr() string                            { return m.leaderAddr }
func (m *MockRaftNode) NodeID() string                                { return m.nodeID }
func (m *MockRaftNode) Stats() map[string]string                      { return map[string]string{"state": "Leader"} }
func (m *MockRaftNode) Apply(cmd []byte, timeout time.Duration) error { return nil }
func (m *MockRaftNode) Shutdown() error                               { return nil }

// MockEtcdStorage for benchmarking
type MockEtcdStorage struct {
	mu    sync.RWMutex
	tasks map[string]*storage.Task
}

func NewMockEtcdStorage() *MockEtcdStorage {
	return &MockEtcdStorage{tasks: make(map[string]*storage.Task)}
}

func (m *MockEtcdStorage) EnqueueTask(ctx context.Context, task *storage.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks[task.ID] = task
	return nil
}

func (m *MockEtcdStorage) DequeueTask(ctx context.Context, workerID string) (*storage.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, task := range m.tasks {
		if task.Status == "PENDING" {
			task.Status = "RUNNING"
			return task, nil
		}
	}
	return nil, nil
}

func (m *MockEtcdStorage) UpdateTask(ctx context.Context, task *storage.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks[task.ID] = task
	return nil
}

func (m *MockEtcdStorage) CompleteTask(ctx context.Context, taskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tasks, taskID)
	return nil
}

func (m *MockEtcdStorage) GetTask(ctx context.Context, taskID string) (*storage.Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if task, exists := m.tasks[taskID]; exists {
		return task, nil
	}
	return nil, nil
}

func (m *MockEtcdStorage) ListTasks(ctx context.Context, status string) ([]*storage.Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*storage.Task
	for _, task := range m.tasks {
		if status == "" || task.Status == status {
			result = append(result, task)
		}
	}
	return result, nil
}

func (m *MockEtcdStorage) Close() error { return nil }

// BenchmarkTaskSubmission benchmarks task submission performance
func BenchmarkTaskSubmission(b *testing.B) {
	mockRaft := &MockRaftNode{
		isLeader:   true,
		leaderAddr: "localhost:8001",
		nodeID:     "bench-node",
	}
	mockStorage := NewMockEtcdStorage()
	metrics := scheduler.NewMetricsWithRegistry(nil)
	server := scheduler.NewServerWithMetrics(mockRaft, mockStorage, metrics)

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := &pb.SubmitTaskRequest{
			Name:        fmt.Sprintf("bench-task-%d", i),
			Payload:     []byte("benchmark-payload"),
			ScheduledAt: time.Now().Unix(),
			MaxRetries:  3,
		}
		_, _ = server.SubmitTask(ctx, req)
	}
}

// BenchmarkTaskSubmission_Parallel benchmarks parallel task submission
func BenchmarkTaskSubmission_Parallel(b *testing.B) {
	mockRaft := &MockRaftNode{
		isLeader:   true,
		leaderAddr: "localhost:8001",
		nodeID:     "bench-node",
	}
	mockStorage := NewMockEtcdStorage()
	metrics := scheduler.NewMetricsWithRegistry(nil)
	server := scheduler.NewServerWithMetrics(mockRaft, mockStorage, metrics)

	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(p *testing.PB) {
		i := 0
		for p.Next() {
			req := &pb.SubmitTaskRequest{
				Name:        fmt.Sprintf("bench-task-%d", i),
				Payload:     []byte("benchmark-payload"),
				ScheduledAt: time.Now().Unix(),
				MaxRetries:  3,
			}
			_, _ = server.SubmitTask(ctx, req)
			i++
		}
	})
}

// BenchmarkHealthCheck benchmarks health check performance
func BenchmarkHealthCheck(b *testing.B) {
	mockRaft := &MockRaftNode{
		isLeader:   true,
		leaderAddr: "localhost:8001",
		nodeID:     "bench-node",
	}
	mockStorage := NewMockEtcdStorage()
	metrics := scheduler.NewMetricsWithRegistry(nil)
	server := scheduler.NewServerWithMetrics(mockRaft, mockStorage, metrics)

	ctx := context.Background()
	req := &pb.HealthCheckRequest{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = server.HealthCheck(ctx, req)
	}
}

// BenchmarkMetricsRecording benchmarks metrics recording
func BenchmarkMetricsRecording(b *testing.B) {
	metrics := scheduler.NewMetricsWithRegistry(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		metrics.RecordTaskSubmitted()
		metrics.RecordTaskCompleted()
	}
}
