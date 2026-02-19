// Package lockmetrics provides an instrumented [sync.RWMutex] that emits
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
//   - gopherstack_lock_wait_seconds      – histogram of time waiting to acquire
//   - gopherstack_lock_hold_seconds      – histogram of write-lock hold duration
//   - gopherstack_lock_active_writers    – gauge: current write-lock holders (0 or 1)
//   - gopherstack_lock_active_readers    – gauge: current read-lock holders
//   - gopherstack_lock_write_held_seconds – live gauge: seconds the write lock has been
//     held right now (emitted only while held)
//   - gopherstack_lock_write_waiters     – live gauge: goroutines currently blocked
//     waiting to acquire the write lock
//   - gopherstack_lock_read_waiters      – live gauge: goroutines currently blocked
//     waiting to acquire the read lock
//
// Deadlock detection: if gopherstack_lock_write_waiters (or read_waiters) is > 0
// and gopherstack_lock_write_held_seconds keeps climbing, a goroutine is stuck
// waiting for a lock that is never released.
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
//
//nolint:gochecknoglobals // shared histogram buckets used by all Prometheus metrics in this package
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

	// writeWaitersDesc is the Prometheus descriptor for the live write-waiter gauge.
	writeWaitersDesc = prometheus.NewDesc(
		"gopherstack_lock_write_waiters",
		"Number of goroutines currently blocked waiting to acquire the write lock. "+
			"A non-zero value combined with a large gopherstack_lock_write_held_seconds "+
			"indicates a deadlock: something holds the lock and cannot release it.",
		[]string{"lock"},
		nil,
	)

	// readWaitersDesc is the Prometheus descriptor for the live read-waiter gauge.
	readWaitersDesc = prometheus.NewDesc(
		"gopherstack_lock_read_waiters",
		"Number of goroutines currently blocked waiting to acquire the read lock. "+
			"Persistent non-zero values alongside a held write lock indicate lock starvation.",
		[]string{"lock"},
		nil,
	)
)

// allMutexes holds weak references to every live RWMutex so the custom
// Collector can enumerate them at scrape time without the mutexes needing
// to self-register/deregister.
//
//nolint:gochecknoglobals // global registry of all instrumented mutexes for the Prometheus Collector
var allMutexes sync.Map // map[*RWMutex]struct{}

//nolint:gochecknoinits // registers the Prometheus Collector for live lock-state metrics
func init() {
	prometheus.MustRegister(&liveStateCollector{})
}

// liveStateCollector implements prometheus.Collector and emits live gauges for
// the current write-lock hold duration and the number of goroutines waiting
// for each registered lock. These metrics are the primary deadlock indicators.
type liveStateCollector struct{}

func (liveStateCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- writeHeldDesc
	ch <- writeWaitersDesc
	ch <- readWaitersDesc
}

func (liveStateCollector) Collect(ch chan<- prometheus.Metric) {
	allMutexes.Range(func(k, _ any) bool {
		m, ok := k.(*RWMutex)
		if !ok {
			return true
		}

		// Write-lock hold duration (only emitted while lock is held).
		ts := m.writeStart.Load()
		if ts != 0 {
			op, _ := m.writeOp.Load().(string)
			held := time.Since(time.Unix(0, ts)).Seconds()
			ch <- prometheus.MustNewConstMetric(writeHeldDesc, prometheus.GaugeValue, held, m.name, op)
		}

		// Write waiters: goroutines currently blocked in Lock().
		writeW := float64(m.writeWaiters.Load())
		ch <- prometheus.MustNewConstMetric(writeWaitersDesc, prometheus.GaugeValue, writeW, m.name)

		// Read waiters: goroutines currently blocked in RLock().
		readW := float64(m.readWaiters.Load())
		ch <- prometheus.MustNewConstMetric(readWaitersDesc, prometheus.GaugeValue, readW, m.name)

		return true
	})
}

// RWMutex is a drop-in replacement for [sync.RWMutex] that records Prometheus
// metrics on every Lock/RLock call.
//
// The zero value is not usable; always create via New.
type RWMutex struct {
	// writeOp and writeStart track the current write-lock holder.
	// writeStart == 0 means the write lock is not currently held.
	// Placed first to minimise GC scan range (both contain pointers).
	writeOp atomic.Value // string
	name    string

	mu sync.RWMutex

	writeStart atomic.Int64 // unix nanoseconds; 0 when not held

	// writeWaiters and readWaiters count goroutines currently blocked
	// waiting to acquire the respective lock.  These are the primary
	// deadlock-detection metrics: a non-zero waiter count that stays
	// non-zero indefinitely means a goroutine is stuck waiting.
	writeWaiters atomic.Int32
	readWaiters  atomic.Int32
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

// WriteWaiters returns the current number of goroutines blocked waiting for
// the write lock. Exposed for testing; the Prometheus Collector is the primary
// consumer in production.
func (m *RWMutex) WriteWaiters() int32 {
	return m.writeWaiters.Load()
}

// ReadWaiters returns the current number of goroutines blocked waiting for
// the read lock. Exposed for testing; the Prometheus Collector is the primary
// consumer in production.
func (m *RWMutex) ReadWaiters() int32 {
	return m.readWaiters.Load()
}

// Lock acquires the exclusive write lock. op is the name of the calling
// operation (e.g. "DeleteBucket") and is recorded in metrics so lock
// contention can be attributed to specific callers.
func (m *RWMutex) Lock(op string) {
	start := time.Now()
	m.writeWaiters.Add(1) // goroutine is now waiting
	m.mu.Lock()
	m.writeWaiters.Add(-1) // acquired — no longer waiting

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
	m.readWaiters.Add(1) // goroutine is now waiting
	m.mu.RLock()
	m.readWaiters.Add(-1) // acquired — no longer waiting

	waitSeconds.WithLabelValues(m.name, op, "read").Observe(time.Since(start).Seconds())
	activeReaders.WithLabelValues(m.name).Inc()
}

// RUnlock releases the shared read lock.
func (m *RWMutex) RUnlock() {
	activeReaders.WithLabelValues(m.name).Dec()
	m.mu.RUnlock()
}
