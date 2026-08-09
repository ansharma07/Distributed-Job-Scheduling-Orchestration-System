package metrics

import (
	"fmt"
	"log"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
)

// DatadogClient wraps the Datadog StatsD client
type DatadogClient struct {
	client *statsd.Client
}

// NewDatadogClient creates a new Datadog metrics client
func NewDatadogClient(addr string) (*DatadogClient, error) {
	client, err := statsd.New(addr,
		statsd.WithNamespace("task_scheduler."),
		statsd.WithTags([]string{"env:production", "service:distributed-scheduler"}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Datadog client: %w", err)
	}

	log.Printf("Datadog metrics client connected to %s", addr)
	return &DatadogClient{client: client}, nil
}

// Close closes the Datadog client
func (d *DatadogClient) Close() error {
	return d.client.Close()
}

// Task Metrics
func (d *DatadogClient) TaskSubmitted(tags ...string) {
	d.client.Incr("task.submitted", tags, 1)
}

func (d *DatadogClient) TaskStarted(tags ...string) {
	d.client.Incr("task.started", tags, 1)
}

func (d *DatadogClient) TaskCompleted(duration time.Duration, tags ...string) {
	d.client.Incr("task.completed", tags, 1)
	d.client.Timing("task.duration", duration, tags, 1)
}

func (d *DatadogClient) TaskFailed(tags ...string) {
	d.client.Incr("task.failed", tags, 1)
}

// Queue Metrics
func (d *DatadogClient) QueueSize(size int64, tags ...string) {
	d.client.Gauge("queue.size", float64(size), tags, 1)
}

func (d *DatadogClient) QueuePending(count int64, tags ...string) {
	d.client.Gauge("queue.pending", float64(count), tags, 1)
}

func (d *DatadogClient) QueueRunning(count int64, tags ...string) {
	d.client.Gauge("queue.running", float64(count), tags, 1)
}

func (d *DatadogClient) QueueCompleted(count int64, tags ...string) {
	d.client.Gauge("queue.completed", float64(count), tags, 1)
}

// Worker Metrics
func (d *DatadogClient) WorkerConnected(tags ...string) {
	d.client.Incr("worker.connected", tags, 1)
}

func (d *DatadogClient) WorkerDisconnected(tags ...string) {
	d.client.Incr("worker.disconnected", tags, 1)
}

func (d *DatadogClient) WorkerCount(count int, tags ...string) {
	d.client.Gauge("worker.count", float64(count), tags, 1)
}

// Raft Metrics
func (d *DatadogClient) RaftLeaderElection(tags ...string) {
	d.client.Incr("raft.leader_election", tags, 1)
}

func (d *DatadogClient) RaftStateChange(state string, tags ...string) {
	allTags := append(tags, fmt.Sprintf("state:%s", state))
	d.client.Incr("raft.state_change", allTags, 1)
}

func (d *DatadogClient) RaftApplyLog(duration time.Duration, tags ...string) {
	d.client.Timing("raft.apply_log", duration, tags, 1)
}

// Throughput Metrics
func (d *DatadogClient) Throughput(tasksPerSecond float64, tags ...string) {
	d.client.Gauge("throughput.tasks_per_second", tasksPerSecond, tags, 1)
}

// Latency Metrics
func (d *DatadogClient) TaskLatency(latency time.Duration, tags ...string) {
	d.client.Distribution("latency.task", float64(latency.Milliseconds()), tags, 1)
}

func (d *DatadogClient) APILatency(endpoint string, latency time.Duration, tags ...string) {
	allTags := append(tags, fmt.Sprintf("endpoint:%s", endpoint))
	d.client.Distribution("latency.api", float64(latency.Milliseconds()), allTags, 1)
}

// Health Metrics
func (d *DatadogClient) HealthCheck(healthy bool, tags ...string) {
	value := 0.0
	if healthy {
		value = 1.0
	}
	d.client.Gauge("health.status", value, tags, 1)
}

// Custom Event
func (d *DatadogClient) Event(title, text string, tags ...string) {
	event := statsd.NewEvent(title, text)
	event.Tags = tags
	d.client.Event(event)
}
