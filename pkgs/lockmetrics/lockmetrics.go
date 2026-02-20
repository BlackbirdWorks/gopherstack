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
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// liveCollector implements [prometheus.Collector] and emits live gauges for
// the current write-lock hold duration and waiter counts for every registered
// [RWMutex]. It is shared across all instances via registerOrReuse.
type liveCollector struct {
	// *prometheus.Desc pointer fields come first to minimise the GC scan range.
	writeHeldDesc    *prometheus.Desc
	writeWaitersDesc *prometheus.Desc
	readWaitersDesc  *prometheus.Desc
	// allMutexes follows; sync.Map contains internal pointers so the GC scan
	// still extends into it, but placing it after the Desc pointers is optimal.
	allMutexes sync.Map // map[*RWMutex]struct{}
}

func newLiveCollector() *liveCollector {
	return &liveCollector{
		writeHeldDesc: prometheus.NewDesc(
			"gopherstack_lock_write_held_seconds",
			"Live duration in seconds that the write lock is currently held (emitted only while held). "+
				"A consistently high value indicates a potential deadlock.",
			[]string{"lock", "operation"},
			nil,
		),
		writeWaitersDesc: prometheus.NewDesc(
			"gopherstack_lock_write_waiters",
			"Number of goroutines currently blocked waiting to acquire the write lock. "+
				"A non-zero value combined with a large gopherstack_lock_write_held_seconds "+
				"indicates a deadlock: something holds the lock and cannot release it.",
			[]string{"lock"},
			nil,
		),
		readWaitersDesc: prometheus.NewDesc(
			"gopherstack_lock_read_waiters",
			"Number of goroutines currently blocked waiting to acquire the read lock. "+
				"Persistent non-zero values alongside a held write lock indicate lock starvation.",
			[]string{"lock"},
			nil,
		),
	}
}

// Describe implements [prometheus.Collector].
func (c *liveCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.writeHeldDesc
	ch <- c.writeWaitersDesc
	ch <- c.readWaitersDesc
}

// Collect implements [prometheus.Collector].
func (c *liveCollector) Collect(ch chan<- prometheus.Metric) {
	c.allMutexes.Range(func(k, _ any) bool {
		m, ok := k.(*RWMutex)
		if !ok {
			return true
		}

		// Write-lock hold duration (only emitted while lock is held).
		ts := m.writeStart.Load()
		if ts != 0 {
			op, _ := m.writeOp.Load().(string)
			held := time.Since(time.Unix(0, ts)).Seconds()
			ch <- prometheus.MustNewConstMetric(c.writeHeldDesc, prometheus.GaugeValue, held, m.name, op)
		}

		// Write waiters: goroutines currently blocked in Lock().
		ch <- prometheus.MustNewConstMetric(c.writeWaitersDesc, prometheus.GaugeValue,
			float64(m.writeWaiters.Load()), m.name)

		// Read waiters: goroutines currently blocked in RLock().
		ch <- prometheus.MustNewConstMetric(c.readWaitersDesc, prometheus.GaugeValue,
			float64(m.readWaiters.Load()), m.name)

		return true
	})
}

// registerOrReuse registers c with [prometheus.DefaultRegisterer].
// If c is already registered with the same descriptor IDs it returns the
// already-registered Collector of type T, allowing all [RWMutex] instances
// to share a single set of metric objects without package-level variables.
//
// the ireturn linter does not look through generic type constraints and incorrectly treats T as
// returning the prometheus.Collector interface.
//
//nolint:ireturn // Returns generic type T; linter misreads type constraint as interface return.
func registerOrReuse[T prometheus.Collector](c T) T {
	if err := prometheus.DefaultRegisterer.Register(c); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			existing, ok := are.ExistingCollector.(T)
			if !ok {
				panic("lockmetrics: registered collector has unexpected type")
			}

			return existing
		}

		panic(err)
	}

	return c
}

// RWMutex is a drop-in replacement for [sync.RWMutex] that records Prometheus
// metrics on every Lock/RLock call.
//
// The zero value is not usable; always create via New.
type RWMutex struct {
	// *prometheus pointer fields first; they are 8 bytes each (pure pointer).
	waitSeconds   *prometheus.HistogramVec
	holdSeconds   *prometheus.HistogramVec
	activeWriters *prometheus.GaugeVec
	activeReaders *prometheus.GaugeVec
	// writeOp and name follow; each contains a pointer so the GC scan extends
	// through them, but their trailing non-pointer word (len/cap) falls outside
	// the scan range, minimising pointer bytes to 56.
	writeOp atomic.Value // string — current write-lock operation name
	name    string

	// Non-pointer fields: GC scan stops above this line.
	mu sync.RWMutex

	writeStart atomic.Int64 // unix nanoseconds; 0 when write lock is not held

	// writeWaiters and readWaiters count goroutines currently blocked
	// waiting to acquire the respective lock. A non-zero count that stays
	// non-zero indefinitely indicates a deadlock or severe starvation.
	writeWaiters atomic.Int32
	readWaiters  atomic.Int32
}

// New creates a new [RWMutex]. The name appears as the "lock" label in all
// emitted metrics and should be a stable, human-readable identifier
// (e.g. "s3", "ddb.table.users").
func New(name string) *RWMutex {
	buckets := []float64{.000001, .00001, .0001, .001, .01, .1, 1, 10}

	m := &RWMutex{name: name}
	m.writeOp.Store("")

	m.waitSeconds = registerOrReuse(prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "gopherstack",
			Name:      "lock_wait_seconds",
			Help:      "Time spent waiting to acquire a lock, by lock name, operation, and type (read|write).",
			Buckets:   buckets,
		},
		[]string{"lock", "operation", "type"},
	))
	m.holdSeconds = registerOrReuse(prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "gopherstack",
			Name:      "lock_hold_seconds",
			Help:      "Duration a write lock was held, by lock name and operation.",
			Buckets:   buckets,
		},
		[]string{"lock", "operation"},
	))
	m.activeWriters = registerOrReuse(prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "gopherstack",
			Name:      "lock_active_writers",
			Help:      "Current number of goroutines holding the write lock (0 or 1 per lock).",
		},
		[]string{"lock", "operation"},
	))
	m.activeReaders = registerOrReuse(prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "gopherstack",
			Name:      "lock_active_readers",
			Help:      "Current number of goroutines holding the read lock.",
		},
		[]string{"lock"},
	))

	// Register the shared live-state Collector. All instances share one Collector
	// (retrieved via registerOrReuse on subsequent calls) so the Prometheus registry
	// sees a single Collector per metric family.
	coll := registerOrReuse(newLiveCollector())
	coll.allMutexes.Store(m, struct{}{})

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
	m.waitSeconds.WithLabelValues(m.name, op, "write").Observe(waited)
	m.activeWriters.WithLabelValues(m.name, op).Inc()
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

	m.holdSeconds.WithLabelValues(m.name, op).Observe(held)
	m.activeWriters.WithLabelValues(m.name, op).Dec()
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

	m.waitSeconds.WithLabelValues(m.name, op, "read").Observe(time.Since(start).Seconds())
	m.activeReaders.WithLabelValues(m.name).Inc()
}

// RUnlock releases the shared read lock.
func (m *RWMutex) RUnlock() {
	m.activeReaders.WithLabelValues(m.name).Dec()
	m.mu.RUnlock()
}
