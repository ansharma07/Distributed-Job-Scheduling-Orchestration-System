package scheduler

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/chandan0804/distributed-task-scheduler/internal/metrics"
	"github.com/chandan0804/distributed-task-scheduler/internal/storage"
	pb "github.com/chandan0804/distributed-task-scheduler/proto"
	"github.com/google/uuid"
)

const (
	// WorkerHealthCheckInterval is how often we check worker health
	WorkerHealthCheckInterval = 5 * time.Second

	// WorkerTimeout is how long before a worker is considered dead
	WorkerTimeout = 30 * time.Second

	// TaskReassignmentDelay is how long to wait before reassigning tasks from a dead worker
	TaskReassignmentDelay = 2 * time.Second
)

// Server implements the TaskScheduler gRPC service
type Server struct {
	pb.UnimplementedTaskSchedulerServer
	raftNode      RaftNode
	storage       Storage
	metrics       *Metrics
	datadogClient *metrics.DatadogClient

	// Worker management
	workers      map[string]*WorkerInfo
	workersMu    sync.RWMutex
	taskChannels map[string]chan *pb.Task
	channelsMu   sync.RWMutex
}

// SetDatadogClient sets the Datadog metrics client
func (s *Server) SetDatadogClient(client *metrics.DatadogClient) {
	s.datadogClient = client
}

// WorkerInfo holds information about a connected worker
type WorkerInfo struct {
	ID            string
	Capacity      int32
	ActiveTasks   int32
	LastHeartbeat time.Time
	AssignedTasks []string // Track tasks assigned to this worker
}

// NewServer creates a new scheduler server
func NewServer(raftNode RaftNode, storage Storage) *Server {
	return NewServerWithMetrics(raftNode, storage, NewMetrics())
}

// NewServerWithMetrics creates a new scheduler server with custom metrics
func NewServerWithMetrics(raftNode RaftNode, storage Storage, metrics *Metrics) *Server {
	s := &Server{
		raftNode:     raftNode,
		storage:      storage,
		metrics:      metrics,
		workers:      make(map[string]*WorkerInfo),
		taskChannels: make(map[string]chan *pb.Task),
	}

	// Start background task distributor
	go s.distributeTasksLoop()

	// Start worker health checker
	go s.healthCheckLoop()

	return s
}

// StreamTasks handles bidirectional streaming for task distribution
func (s *Server) StreamTasks(stream pb.TaskScheduler_StreamTasksServer) error {
	ctx := stream.Context()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Receive task request from worker
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		// Register or update worker
		s.registerWorker(req.WorkerId, req.Capacity)

		// Get tasks for this worker
		tasks, err := s.getTasksForWorker(ctx, req.WorkerId, req.Capacity)
		if err != nil {
			log.Printf("Error getting tasks for worker %s: %v", req.WorkerId, err)
			continue
		}

		// Send tasks to worker
		resp := &pb.TaskResponse{
			Tasks:   tasks,
			HasMore: len(tasks) > 0,
		}

		if err := stream.Send(resp); err != nil {
			return err
		}

		s.metrics.RecordTasksDistributed(int64(len(tasks)))
	}
}

// StreamResults handles bidirectional streaming for task results
func (s *Server) StreamResults(stream pb.TaskScheduler_StreamResultsServer) error {
	ctx := stream.Context()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Receive task result from worker
		result, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		// Process task result
		success := s.processTaskResult(ctx, result)

		// Send acknowledgment
		ack := &pb.TaskAck{
			TaskId:  result.TaskId,
			Success: success,
		}

		if err := stream.Send(ack); err != nil {
			return err
		}
	}
}

// SubmitTask handles task submission
func (s *Server) SubmitTask(ctx context.Context, req *pb.SubmitTaskRequest) (*pb.SubmitTaskResponse, error) {
	// Only leader can accept new tasks
	if !s.raftNode.IsLeader() {
		return &pb.SubmitTaskResponse{
			Success: false,
			Message: fmt.Sprintf("not the leader, redirect to: %s", s.raftNode.LeaderAddr()),
		}, nil
	}

	// Create task
	taskID := uuid.New().String()
	task := &storage.Task{
		ID:          taskID,
		Name:        req.Name,
		Payload:     req.Payload,
		CreatedAt:   time.Now(),
		ScheduledAt: time.Unix(req.ScheduledAt, 0),
		Status:      "PENDING",
		RetryCount:  0,
		MaxRetries:  req.MaxRetries,
	}

	// Enqueue task in etcd
	if err := s.storage.EnqueueTask(ctx, task); err != nil {
		if s.datadogClient != nil {
			s.datadogClient.TaskFailed("reason:enqueue_failed")
		}
		return &pb.SubmitTaskResponse{
			Success: false,
			Message: fmt.Sprintf("failed to enqueue task: %v", err),
		}, nil
	}

	s.metrics.RecordTaskSubmitted()
	if s.datadogClient != nil {
		s.datadogClient.TaskSubmitted(fmt.Sprintf("node:%s", s.raftNode.NodeID()))
	}

	return &pb.SubmitTaskResponse{
		TaskId:  taskID,
		Success: true,
		Message: "task submitted successfully",
	}, nil
}

// HealthCheck returns cluster health status
func (s *Server) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	stats := s.raftNode.Stats()

	pendingTasks, _ := s.storage.ListTasks(ctx, "PENDING")
	runningTasks, _ := s.storage.ListTasks(ctx, "RUNNING")

	// Send metrics to Datadog
	if s.datadogClient != nil {
		s.datadogClient.QueuePending(int64(len(pendingTasks)))
		s.datadogClient.QueueRunning(int64(len(runningTasks)))
		s.datadogClient.QueueCompleted(s.metrics.GetCompletedTasks())
		s.datadogClient.HealthCheck(s.raftNode.IsLeader(), fmt.Sprintf("node:%s", s.raftNode.NodeID()))
	}

	return &pb.HealthCheckResponse{
		IsLeader:       s.raftNode.IsLeader(),
		LeaderId:       s.raftNode.LeaderAddr(),
		ClusterSize:    int32(len(stats)),
		TasksPending:   int64(len(pendingTasks)),
		TasksRunning:   int64(len(runningTasks)),
		TasksCompleted: s.metrics.GetCompletedTasks(),
	}, nil
}

// Helper methods

func (s *Server) registerWorker(workerID string, capacity int32) {
	s.workersMu.Lock()
	defer s.workersMu.Unlock()

	if worker, exists := s.workers[workerID]; exists {
		worker.LastHeartbeat = time.Now()
		worker.Capacity = capacity
	} else {
		s.workers[workerID] = &WorkerInfo{
			ID:            workerID,
			Capacity:      capacity,
			ActiveTasks:   0,
			LastHeartbeat: time.Now(),
		}
		log.Printf("Registered new worker: %s with capacity %d", workerID, capacity)
		if s.datadogClient != nil {
			s.datadogClient.WorkerConnected(fmt.Sprintf("worker:%s", workerID))
			s.datadogClient.WorkerCount(len(s.workers))
		}
	}
}

func (s *Server) getTasksForWorker(ctx context.Context, workerID string, capacity int32) ([]*pb.Task, error) {
	tasks := make([]*pb.Task, 0, capacity)
	taskIDs := make([]string, 0, capacity)

	for i := int32(0); i < capacity; i++ {
		task, err := s.storage.DequeueTask(ctx, workerID)
		if err != nil {
			log.Printf("Error dequeuing task: %v", err)
			break
		}

		if task == nil {
			break // No more tasks available
		}

		pbTask := &pb.Task{
			Id:             task.ID,
			Name:           task.Name,
			Payload:        task.Payload,
			CreatedAt:      task.CreatedAt.Unix(),
			ScheduledAt:    task.ScheduledAt.Unix(),
			Status:         pb.TaskStatus(pb.TaskStatus_value[task.Status]),
			RetryCount:     task.RetryCount,
			MaxRetries:     task.MaxRetries,
			AssignedWorker: task.AssignedWorker,
		}

		tasks = append(tasks, pbTask)
		taskIDs = append(taskIDs, task.ID)
	}

	// Track assigned tasks for this worker
	if len(taskIDs) > 0 {
		s.workersMu.Lock()
		if worker, exists := s.workers[workerID]; exists {
			worker.AssignedTasks = append(worker.AssignedTasks, taskIDs...)
		}
		s.workersMu.Unlock()
	}

	return tasks, nil
}

func (s *Server) processTaskResult(ctx context.Context, result *pb.TaskResult) bool {
	task, err := s.storage.GetTask(ctx, result.TaskId)
	if err != nil {
		log.Printf("Error getting task %s: %v", result.TaskId, err)
		return false
	}

	switch result.Status {
	case pb.TaskStatus_COMPLETED:
		// Task completed successfully
		if err := s.storage.CompleteTask(ctx, result.TaskId); err != nil {
			log.Printf("Error completing task %s: %v", result.TaskId, err)
			return false
		}
		s.metrics.RecordTaskCompleted()
		log.Printf("Task %s completed successfully by worker %s", result.TaskId, result.WorkerId)

	case pb.TaskStatus_FAILED:
		// Task failed, check if we should retry
		if task.RetryCount < task.MaxRetries {
			task.RetryCount++
			task.Status = "PENDING"
			task.AssignedWorker = ""

			// Calculate exponential backoff with jitter
			task.NextRetryAt = CalculateNextRetryTime(task.RetryCount)
			retryDelay := task.NextRetryAt.Sub(time.Now())

			if err := s.storage.UpdateTask(ctx, task); err != nil {
				log.Printf("Error updating task %s for retry: %v", result.TaskId, err)
				return false
			}
			s.metrics.RecordTaskRetried()
			log.Printf("Task %s failed, retrying (%d/%d) after %v (exponential backoff)",
				result.TaskId, task.RetryCount, task.MaxRetries, retryDelay.Round(time.Second))
		} else {
			// Max retries exceeded, mark as permanently failed
			if err := s.storage.CompleteTask(ctx, result.TaskId); err != nil {
				log.Printf("Error completing failed task %s: %v", result.TaskId, err)
				return false
			}
			s.metrics.RecordTaskFailed()
			log.Printf("Task %s failed permanently after %d retries", result.TaskId, task.RetryCount)
		}
	}

	// Update worker stats and remove task from assigned list
	s.workersMu.Lock()
	if worker, exists := s.workers[result.WorkerId]; exists {
		worker.ActiveTasks--
		// Remove task from assigned tasks list
		worker.AssignedTasks = removeTaskFromList(worker.AssignedTasks, result.TaskId)
	}
	s.workersMu.Unlock()

	return true
}

// removeTaskFromList removes a task ID from a slice
func removeTaskFromList(tasks []string, taskID string) []string {
	for i, id := range tasks {
		if id == taskID {
			return append(tasks[:i], tasks[i+1:]...)
		}
	}
	return tasks
}

func (s *Server) distributeTasksLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		// Only leader distributes tasks
		if !s.raftNode.IsLeader() {
			continue
		}

		// This is handled by StreamTasks, but we could add
		// additional background distribution logic here if needed
	}
}

func (s *Server) healthCheckLoop() {
	ticker := time.NewTicker(WorkerHealthCheckInterval)
	defer ticker.Stop()

	for range ticker.C {
		s.checkAndReassignDeadWorkers()
	}
}

// checkAndReassignDeadWorkers detects dead workers and reassigns their tasks
func (s *Server) checkAndReassignDeadWorkers() {
	s.workersMu.Lock()
	now := time.Now()
	deadWorkers := make([]string, 0)

	// Find dead workers
	for workerID, worker := range s.workers {
		if now.Sub(worker.LastHeartbeat) > WorkerTimeout {
			log.Printf("⚠️  Worker %s is dead (last heartbeat: %v ago)",
				workerID, now.Sub(worker.LastHeartbeat).Round(time.Second))
			deadWorkers = append(deadWorkers, workerID)
		}
	}
	s.workersMu.Unlock()

	// Reassign tasks from dead workers
	for _, workerID := range deadWorkers {
		s.reassignTasksFromDeadWorker(workerID)
	}
}

// reassignTasksFromDeadWorker reassigns all tasks from a dead worker
func (s *Server) reassignTasksFromDeadWorker(workerID string) {
	ctx := context.Background()

	s.workersMu.Lock()
	worker, exists := s.workers[workerID]
	if !exists {
		s.workersMu.Unlock()
		return
	}

	assignedTasks := worker.AssignedTasks
	delete(s.workers, workerID)
	s.workersMu.Unlock()

	// Update metrics
	if s.datadogClient != nil {
		s.datadogClient.WorkerDisconnected(fmt.Sprintf("worker:%s", workerID))
		s.datadogClient.WorkerCount(len(s.workers))
	}

	log.Printf("🔄 Reassigning %d tasks from dead worker %s", len(assignedTasks), workerID)

	// Reassign each task
	tasksReassigned := 0
	for _, taskID := range assignedTasks {
		if err := s.reassignTask(ctx, taskID); err != nil {
			log.Printf("❌ Failed to reassign task %s: %v", taskID, err)
		} else {
			tasksReassigned++
		}
	}

	log.Printf("✅ Successfully reassigned %d/%d tasks from dead worker %s",
		tasksReassigned, len(assignedTasks), workerID)
}

// reassignTask marks a task as pending so it can be picked up by another worker
func (s *Server) reassignTask(ctx context.Context, taskID string) error {
	task, err := s.storage.GetTask(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}

	if task == nil {
		// Task already completed or doesn't exist
		return nil
	}

	// Only reassign if task is still running
	if task.Status != "RUNNING" {
		return nil
	}

	// Mark task as pending and clear assigned worker
	task.Status = "PENDING"
	task.AssignedWorker = ""

	// Add a small delay before retry to avoid immediate reassignment to another failing worker
	task.NextRetryAt = time.Now().Add(TaskReassignmentDelay)

	if err := s.storage.UpdateTask(ctx, task); err != nil {
		return fmt.Errorf("failed to update task: %w", err)
	}

	log.Printf("📋 Task %s reassigned (was on dead worker)", taskID)
	return nil
}
