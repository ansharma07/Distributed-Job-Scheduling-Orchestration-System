package raft

import (
	"encoding/json"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFSM(t *testing.T) {
	fsm := NewFSM()
	assert.NotNil(t, fsm)
	assert.NotNil(t, fsm.tasks)
}

func TestFSM_Apply_SubmitTask(t *testing.T) {
	fsm := NewFSM()

	taskState := &TaskState{
		ID:     "task-123",
		Name:   "test-task",
		Status: "PENDING",
	}

	payload, err := json.Marshal(taskState)
	require.NoError(t, err)

	cmd := Command{
		Type:    CommandAddTask,
		Payload: payload,
	}

	data, err := json.Marshal(cmd)
	require.NoError(t, err)

	log := &raft.Log{
		Data: data,
	}

	result := fsm.Apply(log)
	assert.Nil(t, result)

	// Verify task was added
	fsm.mu.RLock()
	task, exists := fsm.tasks["task-123"]
	fsm.mu.RUnlock()

	assert.True(t, exists)
	assert.Equal(t, "task-123", task.ID)
	assert.Equal(t, "test-task", task.Name)
	assert.Equal(t, "PENDING", task.Status)
}

func TestFSM_Apply_UpdateTask(t *testing.T) {
	fsm := NewFSM()

	// First, submit a task
	taskState := &TaskState{
		ID:     "task-123",
		Name:   "test-task",
		Status: "PENDING",
	}
	payload, _ := json.Marshal(taskState)
	submitCmd := Command{
		Type:    CommandAddTask,
		Payload: payload,
	}
	submitData, _ := json.Marshal(submitCmd)
	fsm.Apply(&raft.Log{Data: submitData})

	// Now update it
	updatedTask := &TaskState{
		ID:     "task-123",
		Name:   "test-task",
		Status: "RUNNING",
	}
	updatePayload, _ := json.Marshal(updatedTask)
	updateCmd := Command{
		Type:    CommandUpdateTask,
		Payload: updatePayload,
	}
	updateData, err := json.Marshal(updateCmd)
	require.NoError(t, err)

	result := fsm.Apply(&raft.Log{Data: updateData})
	assert.Nil(t, result)

	// Verify task was updated
	fsm.mu.RLock()
	task, exists := fsm.tasks["task-123"]
	fsm.mu.RUnlock()

	assert.True(t, exists)
	assert.Equal(t, "RUNNING", task.Status)
}

func TestFSM_Apply_CompleteTask(t *testing.T) {
	fsm := NewFSM()

	// Submit a task first
	taskState := &TaskState{
		ID:     "task-123",
		Name:   "test-task",
		Status: "PENDING",
	}
	payload, _ := json.Marshal(taskState)
	submitCmd := Command{
		Type:    CommandAddTask,
		Payload: payload,
	}
	submitData, _ := json.Marshal(submitCmd)
	fsm.Apply(&raft.Log{Data: submitData})

	// Complete the task
	deletePayload, _ := json.Marshal(map[string]string{"id": "task-123"})
	completeCmd := Command{
		Type:    CommandDeleteTask,
		Payload: deletePayload,
	}
	completeData, err := json.Marshal(completeCmd)
	require.NoError(t, err)

	result := fsm.Apply(&raft.Log{Data: completeData})
	assert.Nil(t, result)

	// Verify task was removed
	fsm.mu.RLock()
	_, exists := fsm.tasks["task-123"]
	fsm.mu.RUnlock()

	assert.False(t, exists)
}

func TestFSM_Apply_InvalidJSON(t *testing.T) {
	fsm := NewFSM()

	log := &raft.Log{
		Data: []byte("invalid json"),
	}

	result := fsm.Apply(log)
	assert.NotNil(t, result)
}

func TestFSM_Snapshot(t *testing.T) {
	fsm := NewFSM()

	// Add some tasks
	fsm.tasks["task-1"] = &TaskState{ID: "task-1", Name: "Task 1", Status: "PENDING"}
	fsm.tasks["task-2"] = &TaskState{ID: "task-2", Name: "Task 2", Status: "RUNNING"}

	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)
	assert.NotNil(t, snapshot)
}

func TestFSM_Restore(t *testing.T) {
	fsm := NewFSM()

	// Create snapshot data
	tasks := map[string]*TaskState{
		"task-1": {ID: "task-1", Name: "Task 1", Status: "PENDING"},
		"task-2": {ID: "task-2", Name: "Task 2", Status: "RUNNING"},
	}

	data, err := json.Marshal(tasks)
	require.NoError(t, err)

	// Restore from snapshot
	err = fsm.Restore(&mockSnapshotReader{data: data})
	require.NoError(t, err)

	// Verify tasks were restored
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	assert.Len(t, fsm.tasks, 2)
	assert.Equal(t, "Task 1", fsm.tasks["task-1"].Name)
	assert.Equal(t, "Task 2", fsm.tasks["task-2"].Name)
}

// mockSnapshotReader is a mock implementation of io.ReadCloser for testing
type mockSnapshotReader struct {
	data []byte
	pos  int
}

func (m *mockSnapshotReader) Read(p []byte) (n int, err error) {
	if m.pos >= len(m.data) {
		return 0, nil
	}
	n = copy(p, m.data[m.pos:])
	m.pos += n
	return n, nil
}

func (m *mockSnapshotReader) Close() error {
	return nil
}
