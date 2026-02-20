package telemetry

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint:gochecknoglobals // Prometheus collectors are global for registration.
var (
	// Generic operation latencies (seconds) - works for any backend.
	// Prometheus requires these to be global for automatic registration with the global registry.
	operationLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "operation_duration_seconds",
			Help:    "Operation latency in seconds",
			Buckets: []float64{.0001, .0005, .001, .005, .01, .05, .1, .5, 1, 5},
		},
		[]string{"operation", "resource"},
	)

	// Operation counters.
	// Prometheus requires these to be global for automatic registration with the global registry.
	operationCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "operations_total",
			Help: "Total operations",
		},
		[]string{"operation", "resource", "status"},
	)

	// Lock hold times.
	// Prometheus requires these to be global for automatic registration with the global registry.
	lockDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "lock_hold_duration_seconds",
			Help:    "Lock hold duration in seconds",
			Buckets: []float64{.0001, .0005, .001, .005, .01, .05, .1},
		},
		[]string{"lock_type"},
	)

	// mu protects access to metrics data structures.
	mu sync.RWMutex
)

// RecordOperation records an operation latency and status
// operation: name of the operation (e.g., "GetItem", "PutObject")
// resource: name of the resource (e.g., table name, bucket name)
// durationSeconds: how long the operation took
// status: "success" or "error"
func RecordOperation(operation, resource string, durationSeconds float64, status string) {
	operationLatency.WithLabelValues(operation, resource).Observe(durationSeconds)
	operationCounter.WithLabelValues(operation, resource, status).Inc()
}

// RecordLockDuration records lock hold time.
func RecordLockDuration(lockType string, durationSeconds float64) {
	lockDuration.WithLabelValues(lockType).Observe(durationSeconds)
}

// GetMetrics returns a snapshot of metrics for dashboard consumption.
func GetMetrics() map[string]any {
	mu.RLock()
	defer mu.RUnlock()

	// This will be populated by gathering metrics from prometheus
	// For now, return an empty structure that will be filled in
	return map[string]any{
		"operations": map[string]any{},
	}
}
