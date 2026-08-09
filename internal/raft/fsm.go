package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

// CommandType represents the type of command
type CommandType string

const (
	CommandAddTask    CommandType = "ADD_TASK"
	CommandUpdateTask CommandType = "UPDATE_TASK"
	CommandDeleteTask CommandType = "DELETE_TASK"
)

// Command represents a Raft command
type Command struct {
	Type    CommandType     `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// TaskState represents the state of a task in the FSM
type TaskState struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	Status         string `json:"status"`
	AssignedWorker string `json:"assigned_worker,omitempty"`
	RetryCount     int32  `json:"retry_count"`
}

// FSM implements the Raft Finite State Machine
type FSM struct {
	mu    sync.RWMutex
	tasks map[string]*TaskState
}

// NewFSM creates a new FSM
func NewFSM() *FSM {
	return &FSM{
		tasks: make(map[string]*TaskState),
	}
}

// Apply applies a Raft log entry to the FSM
func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("failed to unmarshal command: %w", err)
	}

	switch cmd.Type {
	case CommandAddTask:
		return f.applyAddTask(cmd.Payload)
	case CommandUpdateTask:
		return f.applyUpdateTask(cmd.Payload)
	case CommandDeleteTask:
		return f.applyDeleteTask(cmd.Payload)
	default:
		return fmt.Errorf("unknown command type: %s", cmd.Type)
	}
}

func (f *FSM) applyAddTask(payload json.RawMessage) interface{} {
	var task TaskState
	if err := json.Unmarshal(payload, &task); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.tasks[task.ID] = &task
	return nil
}

func (f *FSM) applyUpdateTask(payload json.RawMessage) interface{} {
	var task TaskState
	if err := json.Unmarshal(payload, &task); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if existing, ok := f.tasks[task.ID]; ok {
		if task.Status != "" {
			existing.Status = task.Status
		}
		if task.AssignedWorker != "" {
			existing.AssignedWorker = task.AssignedWorker
		}
		existing.RetryCount = task.RetryCount
	}

	return nil
}

func (f *FSM) applyDeleteTask(payload json.RawMessage) interface{} {
	var data struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(payload, &data); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.tasks, data.ID)
	return nil
}

// Snapshot returns a snapshot of the FSM state
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Clone the tasks map
	tasks := make(map[string]*TaskState)
	for k, v := range f.tasks {
		taskCopy := *v
		tasks[k] = &taskCopy
	}

	return &fsmSnapshot{tasks: tasks}, nil
}

// Restore restores the FSM state from a snapshot
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var tasks map[string]*TaskState
	if err := json.NewDecoder(rc).Decode(&tasks); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.tasks = tasks
	return nil
}

// GetTask retrieves a task by ID
func (f *FSM) GetTask(id string) (*TaskState, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	task, ok := f.tasks[id]
	return task, ok
}

// GetAllTasks returns all tasks
func (f *FSM) GetAllTasks() []*TaskState {
	f.mu.RLock()
	defer f.mu.RUnlock()

	tasks := make([]*TaskState, 0, len(f.tasks))
	for _, task := range f.tasks {
		taskCopy := *task
		tasks = append(tasks, &taskCopy)
	}
	return tasks
}

// fsmSnapshot implements raft.FSMSnapshot
type fsmSnapshot struct {
	tasks map[string]*TaskState
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := json.NewEncoder(sink).Encode(s.tasks)
	if err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
