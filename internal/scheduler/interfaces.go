package scheduler

import (
	"context"
	"time"

	"github.com/chandan0804/distributed-task-scheduler/internal/storage"
)

// RaftNode interface for testing
type RaftNode interface {
	IsLeader() bool
	LeaderAddr() string
	NodeID() string
	Stats() map[string]string
	Apply(cmd []byte, timeout time.Duration) error
	Shutdown() error
}

// Storage interface for testing
type Storage interface {
	EnqueueTask(ctx context.Context, task *storage.Task) error
	DequeueTask(ctx context.Context, workerID string) (*storage.Task, error)
	UpdateTask(ctx context.Context, task *storage.Task) error
	CompleteTask(ctx context.Context, taskID string) error
	GetTask(ctx context.Context, taskID string) (*storage.Task, error)
	ListTasks(ctx context.Context, status string) ([]*storage.Task, error)
	Close() error
}
