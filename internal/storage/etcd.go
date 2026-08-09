package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const (
	taskQueuePrefix = "/tasks/queue/"
	taskLockPrefix  = "/tasks/locks/"
	taskStatePrefix = "/tasks/state/"
)

// EtcdStorage provides distributed storage using etcd
type EtcdStorage struct {
	client  *clientv3.Client
	session *concurrency.Session
}

// Task represents a task in storage
type Task struct {
	ID             string    `json:"id"`
	Name           string    `json:"name"`
	Payload        []byte    `json:"payload"`
	CreatedAt      time.Time `json:"created_at"`
	ScheduledAt    time.Time `json:"scheduled_at"`
	Status         string    `json:"status"`
	RetryCount     int32     `json:"retry_count"`
	MaxRetries     int32     `json:"max_retries"`
	AssignedWorker string    `json:"assigned_worker,omitempty"`
	NextRetryAt    time.Time `json:"next_retry_at,omitempty"` // For exponential backoff
}

// NewEtcdStorage creates a new etcd storage instance
func NewEtcdStorage(endpoints []string) (*EtcdStorage, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd: %w", err)
	}

	session, err := concurrency.NewSession(client, concurrency.WithTTL(10))
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to create etcd session: %w", err)
	}

	return &EtcdStorage{
		client:  client,
		session: session,
	}, nil
}

// EnqueueTask adds a task to the queue
func (s *EtcdStorage) EnqueueTask(ctx context.Context, task *Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	key := fmt.Sprintf("%s%s", taskQueuePrefix, task.ID)
	_, err = s.client.Put(ctx, key, string(data))
	if err != nil {
		return fmt.Errorf("failed to enqueue task: %w", err)
	}

	return nil
}

// DequeueTask retrieves and locks a task from the queue
func (s *EtcdStorage) DequeueTask(ctx context.Context, workerID string) (*Task, error) {
	// Get all pending tasks
	resp, err := s.client.Get(ctx, taskQueuePrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to get tasks: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, nil // No tasks available
	}

	// Try to acquire lock on first available task
	for _, kv := range resp.Kvs {
		var task Task
		if err := json.Unmarshal(kv.Value, &task); err != nil {
			continue
		}

		// Skip if task is not pending or scheduled time hasn't arrived
		if task.Status != "PENDING" || time.Now().Before(task.ScheduledAt) {
			continue
		}

		// Skip if task is waiting for retry backoff (exponential backoff)
		if !task.NextRetryAt.IsZero() && time.Now().Before(task.NextRetryAt) {
			continue
		}

		// Try to acquire lock
		lockKey := fmt.Sprintf("%s%s", taskLockPrefix, task.ID)
		mutex := concurrency.NewMutex(s.session, lockKey)

		if err := mutex.TryLock(ctx); err != nil {
			continue // Lock failed, try next task
		}

		// Successfully locked, assign to worker
		task.AssignedWorker = workerID
		task.Status = "RUNNING"

		if err := s.UpdateTask(ctx, &task); err != nil {
			mutex.Unlock(ctx)
			continue
		}

		return &task, nil
	}

	return nil, nil // No available tasks
}

// UpdateTask updates a task's state
func (s *EtcdStorage) UpdateTask(ctx context.Context, task *Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	key := fmt.Sprintf("%s%s", taskQueuePrefix, task.ID)
	_, err = s.client.Put(ctx, key, string(data))
	if err != nil {
		return fmt.Errorf("failed to update task: %w", err)
	}

	return nil
}

// CompleteTask marks a task as completed and removes it from queue
func (s *EtcdStorage) CompleteTask(ctx context.Context, taskID string) error {
	key := fmt.Sprintf("%s%s", taskQueuePrefix, taskID)
	lockKey := fmt.Sprintf("%s%s", taskLockPrefix, taskID)

	// Delete task from queue
	_, err := s.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to delete task: %w", err)
	}

	// Release lock
	_, err = s.client.Delete(ctx, lockKey)
	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}

	return nil
}

// GetTask retrieves a task by ID
func (s *EtcdStorage) GetTask(ctx context.Context, taskID string) (*Task, error) {
	key := fmt.Sprintf("%s%s", taskQueuePrefix, taskID)
	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get task: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("task not found")
	}

	var task Task
	if err := json.Unmarshal(resp.Kvs[0].Value, &task); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task: %w", err)
	}

	return &task, nil
}

// ListTasks returns all tasks with optional status filter
func (s *EtcdStorage) ListTasks(ctx context.Context, status string) ([]*Task, error) {
	resp, err := s.client.Get(ctx, taskQueuePrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to list tasks: %w", err)
	}

	tasks := make([]*Task, 0)
	for _, kv := range resp.Kvs {
		var task Task
		if err := json.Unmarshal(kv.Value, &task); err != nil {
			continue
		}

		if status == "" || task.Status == status {
			tasks = append(tasks, &task)
		}
	}

	return tasks, nil
}

// Close closes the etcd connection
func (s *EtcdStorage) Close() error {
	s.session.Close()
	return s.client.Close()
}
