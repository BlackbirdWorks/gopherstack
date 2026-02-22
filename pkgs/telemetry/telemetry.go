package telemetry

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	io_prometheus_client "github.com/prometheus/client_model/go"
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
		[]string{"operation"},
	)

	// Operation running average latencies (seconds).
	operationAvgLatency = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "operation_avg_duration_seconds",
			Help: "Running average operation latency in seconds",
		},
		[]string{"operation"},
	)

	// Operation counters.
	// Prometheus requires these to be global for automatic registration with the global registry.
	operationCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "operations_total",
			Help: "Total operations",
		},
		[]string{"operation", "status"},
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

	// deleteQueueDepth is a gauge for pending background deletions.
	// Prometheus requires these to be global for automatic registration with the global registry.
	deleteQueueDepth = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gopherstack_delete_queue_depth",
			Help: "Number of resources currently queued for async background deletion",
		},
		[]string{"service"},
	)

	// ttlEvictions is a counter for items deleted via TTL sweep.
	ttlEvictions = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gopherstack_ttl_evictions_total",
			Help: "Total items evicted via TTL background sweep",
		},
		[]string{"service"},
	)

	// streamEventsTotal is a counter for DynamoDB Streams records delivered via GetRecords.
	streamEventsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gopherstack_stream_events_total",
			Help: "Total stream records delivered via GetRecords",
		},
		[]string{"service"},
	)

	// mu protects access to metrics data structures.
	mu sync.RWMutex
)

// RecordOperation records an operation latency and status.
// operation: name of the operation (e.g., "GetItem", "PutObject")
// resource: name of the resource (ignored for Prometheus metrics to reduce cardinality)
// durationSeconds: how long the operation took
// status: "success" or "error"
func RecordOperation(operation, _ string, durationSeconds float64, status string) {
	operationLatency.WithLabelValues(operation).Observe(durationSeconds)
	operationCounter.WithLabelValues(operation, status).Inc()

	// Update running average (approximate EMA with alpha=0.1)
	const alpha = 0.1
	m, err := operationAvgLatency.GetMetricWithLabelValues(operation)
	if err == nil {
		// Get current value from the metric if possible
		var dto io_prometheus_client.Metric
		_ = m.Write(&dto)
		curr := dto.GetGauge().GetValue()
		if curr == 0 {
			m.Set(durationSeconds)
		} else {
			m.Set(curr*(1-alpha) + durationSeconds*alpha)
		}
	}
}

// RecordLockDuration records lock hold time.
func RecordLockDuration(lockType string, durationSeconds float64) {
	lockDuration.WithLabelValues(lockType).Observe(durationSeconds)
}

// RecordDeleteQueueDepth updates the live delete-queue depth gauge for a service.
// service should be "s3" or "dynamodb".
func RecordDeleteQueueDepth(service string, depth int) {
	deleteQueueDepth.WithLabelValues(service).Set(float64(depth))
}

// RecordTTLEvictions records that count items were evicted via background TTL sweep.
func RecordTTLEvictions(service string, count int) {
	ttlEvictions.WithLabelValues(service).Add(float64(count))
}

// RecordStreamEvents records the number of stream records delivered via GetRecords.
func RecordStreamEvents(service string, count int) {
	if count > 0 {
		streamEventsTotal.WithLabelValues(service).Add(float64(count))
	}
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
