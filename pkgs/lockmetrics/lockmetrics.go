// Package lockmetrics provides an instrumented sync.RWMutex that emits
// Prometheus metrics on every lock acquisition and release.
//
// Usage:
//
//	mu := lockmetrics.New("s3.global")
//	mu.Lock("DeleteBucket")
//	defer mu.Unlock()
//
//	mu.RLock("ListBuckets")
//	defer mu.RUnlock()
//
// Metrics emitted:
//   - gopherstack_lock_wait_seconds    – time waiting to acquire a lock
//   - gopherstack_lock_hold_seconds    – duration the lock was held
//   - gopherstack_lock_active_writers  – current number of write-lock holders (0 or 1)
//   - gopherstack_lock_active_readers  – current number of read-lock holders
//   - gopherstack_lock_write_held_seconds – gauge emitted by a custom Collector
//     showing the live hold duration for any currently-held write lock; useful
//     for detecting deadlocks in the metrics dashboard.
package lockmetrics

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// lockBuckets spans from 1µs to 10s to capture both the common sub-millisecond
// fast path and the deadlock-scale multi-second hold durations that the metrics
// are specifically designed to surface.
var lockBuckets = []float64{.000001, .00001, .0001, .001, .01, .1, 1, 10}

// Prometheus metrics shared across all RWMutex instances.
//
//nolint:gochecknoglobals // Prometheus vectors must be global
var (
	waitSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "gopherstack",
			Name:      "lock_wait_seconds",
			Help:      "Time spent waiting to acquire a lock, by lock name, operation, and type (read|write).",
			Buckets:   lockBuckets,
		},
		[]string{"lock", "operation", "type"},
	)

	holdSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "gopherstack",
			Name:      "lock_hold_seconds",
			Help:      "Duration a write lock was held, by lock name and operation.",
			Buckets:   lockBuckets,
		},
		[]string{"lock", "operation"},
	)

	activeWriters = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "gopherstack",
			Name:      "lock_active_writers",
			Help:      "Current number of goroutines holding the write lock (0 or 1 per lock).",
		},
		[]string{"lock", "operation"},
	)

	activeReaders = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "gopherstack",
			Name:      "lock_active_readers",
			Help:      "Current number of goroutines holding the read lock.",
		},
		[]string{"lock"},
	)

	// writeHeldDesc is the Prometheus descriptor for the live write-lock-hold gauge.
	writeHeldDesc = prometheus.NewDesc(
		"gopherstack_lock_write_held_seconds",
		"Live duration in seconds that the write lock is currently held (emitted only while held). "+
			"A consistently high value indicates a potential deadlock.",
		[]string{"lock", "operation"},
		nil,
	)
)

// allMutexes holds weak references to every live RWMutex so the custom
// Collector can enumerate them at scrape time without the mutexes needing
// to self-register/deregister.
var allMutexes sync.Map // map[*RWMutex]struct{}

func init() {
	prometheus.MustRegister(&liveStateCollector{})
}

// liveStateCollector implements prometheus.Collector and emits a gauge for
// every write lock that is currently held.
type liveStateCollector struct{}

func (liveStateCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- writeHeldDesc
}

func (liveStateCollector) Collect(ch chan<- prometheus.Metric) {
	allMutexes.Range(func(k, _ any) bool {
		m, ok := k.(*RWMutex)
		if !ok {
			return true
		}

		ts := m.writeStart.Load()
		if ts == 0 {
			return true // not held
		}

		op, _ := m.writeOp.Load().(string)
		held := time.Since(time.Unix(0, ts)).Seconds()
		ch <- prometheus.MustNewConstMetric(writeHeldDesc, prometheus.GaugeValue, held, m.name, op)

		return true
	})
}

// RWMutex is a drop-in replacement for sync.RWMutex that records Prometheus
// metrics on every Lock/RLock call.
//
// The zero value is not usable; always create via New.
type RWMutex struct {
	mu   sync.RWMutex
	name string

	// writeOp and writeStart track the current write-lock holder.
	// writeStart == 0 means the write lock is not currently held.
	writeOp    atomic.Value // string
	writeStart atomic.Int64 // unix nanoseconds; 0 when not held
}

// New creates a new RWMutex. The name appears as the "lock" label in all
// emitted metrics and should be a stable, human-readable identifier
// (e.g. "s3.global", "ddb.table.users").
func New(name string) *RWMutex {
	m := &RWMutex{name: name}
	m.writeOp.Store("")
	allMutexes.Store(m, struct{}{})

	return m
}

// Lock acquires the exclusive write lock. op is the name of the calling
// operation (e.g. "DeleteBucket") and is recorded in metrics so lock
// contention can be attributed to specific callers.
func (m *RWMutex) Lock(op string) {
	start := time.Now()
	m.mu.Lock()

	waited := time.Since(start).Seconds()
	waitSeconds.WithLabelValues(m.name, op, "write").Observe(waited)
	activeWriters.WithLabelValues(m.name, op).Inc()
	m.writeOp.Store(op)
	m.writeStart.Store(time.Now().UnixNano())
}

// Unlock releases the exclusive write lock. The operation name recorded
// during Lock is used to attribute the hold-duration histogram.
func (m *RWMutex) Unlock() {
	ts := m.writeStart.Load()
	op, _ := m.writeOp.Load().(string)

	var held float64
	if ts != 0 {
		held = time.Since(time.Unix(0, ts)).Seconds()
	}

	holdSeconds.WithLabelValues(m.name, op).Observe(held)
	activeWriters.WithLabelValues(m.name, op).Dec()
	m.writeStart.Store(0)
	m.writeOp.Store("")
	m.mu.Unlock()
}

// RLock acquires the shared read lock. op names the calling operation and
// is recorded in the wait-time histogram.
func (m *RWMutex) RLock(op string) {
	start := time.Now()
	m.mu.RLock()

	waitSeconds.WithLabelValues(m.name, op, "read").Observe(time.Since(start).Seconds())
	activeReaders.WithLabelValues(m.name).Inc()
}

// RUnlock releases the shared read lock.
func (m *RWMutex) RUnlock() {
	activeReaders.WithLabelValues(m.name).Dec()
	m.mu.RUnlock()
}
