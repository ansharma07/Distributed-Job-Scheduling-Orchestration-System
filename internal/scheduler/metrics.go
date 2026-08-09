package scheduler

import (
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all scheduler metrics
type Metrics struct {
	tasksSubmitted   int64
	tasksCompleted   int64
	tasksFailed      int64
	tasksRetried     int64
	tasksDistributed int64

	// Prometheus metrics
	taskSubmitCounter     prometheus.Counter
	taskCompleteCounter   prometheus.Counter
	taskFailCounter       prometheus.Counter
	taskRetryCounter      prometheus.Counter
	taskDistributeCounter prometheus.Counter
	taskLatencyHistogram  prometheus.Histogram
	activeWorkersGauge    prometheus.Gauge
}

// NewMetrics creates a new metrics instance
func NewMetrics() *Metrics {
	return NewMetricsWithRegistry(prometheus.DefaultRegisterer)
}

// NewMetricsWithRegistry creates a new metrics instance with a custom registry
func NewMetricsWithRegistry(registerer prometheus.Registerer) *Metrics {
	if registerer == nil {
		// For testing: use a no-op metrics instance
		return &Metrics{}
	}

	factory := promauto.With(registerer)

	return &Metrics{
		taskSubmitCounter: factory.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_tasks_submitted_total",
			Help: "Total number of tasks submitted",
		}),
		taskCompleteCounter: factory.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_tasks_completed_total",
			Help: "Total number of tasks completed",
		}),
		taskFailCounter: factory.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_tasks_failed_total",
			Help: "Total number of tasks failed",
		}),
		taskRetryCounter: factory.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_tasks_retried_total",
			Help: "Total number of tasks retried",
		}),
		taskDistributeCounter: factory.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_tasks_distributed_total",
			Help: "Total number of tasks distributed to workers",
		}),
		taskLatencyHistogram: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "scheduler_task_latency_milliseconds",
			Help:    "Task processing latency in milliseconds",
			Buckets: prometheus.ExponentialBuckets(1, 2, 10), // 1ms to ~1s
		}),
		activeWorkersGauge: factory.NewGauge(prometheus.GaugeOpts{
			Name: "scheduler_active_workers",
			Help: "Number of active workers",
		}),
	}
}

// RecordTaskSubmitted increments the submitted tasks counter
func (m *Metrics) RecordTaskSubmitted() {
	atomic.AddInt64(&m.tasksSubmitted, 1)
	if m.taskSubmitCounter != nil {
		m.taskSubmitCounter.Inc()
	}
}

// RecordTaskCompleted increments the completed tasks counter
func (m *Metrics) RecordTaskCompleted() {
	atomic.AddInt64(&m.tasksCompleted, 1)
	if m.taskCompleteCounter != nil {
		m.taskCompleteCounter.Inc()
	}
}

// RecordTaskFailed increments the failed tasks counter
func (m *Metrics) RecordTaskFailed() {
	atomic.AddInt64(&m.tasksFailed, 1)
	if m.taskFailCounter != nil {
		m.taskFailCounter.Inc()
	}
}

// RecordTaskRetried increments the retried tasks counter
func (m *Metrics) RecordTaskRetried() {
	atomic.AddInt64(&m.tasksRetried, 1)
	if m.taskRetryCounter != nil {
		m.taskRetryCounter.Inc()
	}
}

// RecordTasksDistributed increments the distributed tasks counter
func (m *Metrics) RecordTasksDistributed(count int64) {
	atomic.AddInt64(&m.tasksDistributed, count)
	if m.taskDistributeCounter != nil {
		m.taskDistributeCounter.Add(float64(count))
	}
}

// RecordTaskLatency records task processing latency
func (m *Metrics) RecordTaskLatency(duration time.Duration) {
	if m.taskLatencyHistogram != nil {
		m.taskLatencyHistogram.Observe(float64(duration.Milliseconds()))
	}
}

// SetActiveWorkers sets the number of active workers
func (m *Metrics) SetActiveWorkers(count int) {
	if m.activeWorkersGauge != nil {
		m.activeWorkersGauge.Set(float64(count))
	}
}

// GetCompletedTasks returns the number of completed tasks
func (m *Metrics) GetCompletedTasks() int64 {
	return atomic.LoadInt64(&m.tasksCompleted)
}

// GetSubmittedTasks returns the number of submitted tasks
func (m *Metrics) GetSubmittedTasks() int64 {
	return atomic.LoadInt64(&m.tasksSubmitted)
}

// GetFailedTasks returns the number of failed tasks
func (m *Metrics) GetFailedTasks() int64 {
	return atomic.LoadInt64(&m.tasksFailed)
}

// GetRetriedTasks returns the number of retried tasks
func (m *Metrics) GetRetriedTasks() int64 {
	return atomic.LoadInt64(&m.tasksRetried)
}

// GetDistributedTasks returns the number of distributed tasks
func (m *Metrics) GetDistributedTasks() int64 {
	return atomic.LoadInt64(&m.tasksDistributed)
}
