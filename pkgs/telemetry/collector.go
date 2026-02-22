package telemetry

import (
	"math"
	"runtime"

	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

const (
	msPerSecond = 1000.0
	p50Divisor  = 2
	p95Factor   = 95
	p99Factor   = 99
	pctBase     = 100
)

// Summary holds aggregated metrics for a single operation.
type Summary struct {
	Operation  string  `json:"operation"`
	Count      int64   `json:"count"`
	ErrorCount int64   `json:"error_count"`
	P50Ms      float64 `json:"p50_ms"`
	P95Ms      float64 `json:"p95_ms"`
	P99Ms      float64 `json:"p99_ms"`
	AvgMs      float64 `json:"avg_ms"`
	MaxMs      float64 `json:"max_ms"`
}

// DeadlockInfo holds information about a potential deadlock.
type DeadlockInfo struct {
	Lock      string  `json:"lock"`
	Operation string  `json:"operation"`
	HeldSec   float64 `json:"held_sec"`
	Waiters   int     `json:"waiters"`
}

// WorkerStats holds aggregated metrics for a background worker.
type WorkerStats struct {
	Service        string `json:"service"`
	Worker         string `json:"worker"`
	QueueDepth     int    `json:"queue_depth"`
	TasksTotal     int64  `json:"tasks_total"`
	ErrorsTotal    int64  `json:"errors_total"`
	ItemsProcessed int64  `json:"items_processed_total"`
}

// RuntimeMetrics holds Go runtime statistics.
type RuntimeMetrics struct {
	Goroutines   int     `json:"goroutines"`
	HeapAllocMB  float64 `json:"heap_alloc_mb"`
	HeapSysMB    float64 `json:"heap_sys_mb"`
	NumGC        uint32  `json:"num_gc"`
	LastGCPause  float64 `json:"last_gc_pause_ms"`
	TotalAllocMB float64 `json:"total_alloc_mb"`
}

// Dashboard holds all metrics for dashboard display.
type Dashboard struct {
	Runtime    *RuntimeMetrics `json:"runtime"`
	Operations []Summary       `json:"operations"`
	Deadlocks  []DeadlockInfo  `json:"deadlocks"`
	Workers    []WorkerStats   `json:"workers"`
}

// CollectMetrics gathers current metrics from Prometheus registry.
func CollectMetrics() *Dashboard {
	gatherer := prometheus.DefaultGatherer

	metrics, err := gatherer.Gather()
	if err != nil && len(metrics) == 0 {
		return &Dashboard{
			Runtime:    collectRuntimeMetrics(),
			Operations: []Summary{},
		}
	}

	result := &Dashboard{
		Runtime:    collectRuntimeMetrics(),
		Operations: []Summary{},
		Deadlocks:  []DeadlockInfo{},
		Workers:    []WorkerStats{},
	}

	processCollectedMetrics(metrics, result)

	return result
}

// processCollectedMetrics parses and processes metrics from Prometheus.
func processCollectedMetrics(metrics []*io_prometheus_client.MetricFamily, result *Dashboard) {
	deadlockCandidates := make(map[string]*DeadlockInfo)

	for _, mf := range metrics {
		name := mf.GetName()
		switch name {
		case "operation_duration_seconds":
			result.Operations = append(result.Operations, parseHistogram(mf)...)
		case "operation_avg_duration_seconds":
			// We consolidate this into the Summary during parseHistogram or similar
			// but Prometheus handles them separately. For now, let's update Summaries with it.
			updateAverages(mf, result.Operations)
		case "gopherstack_lock_write_held_seconds":
			processLockHeldMetrics(mf, deadlockCandidates)
		case "gopherstack_lock_write_waiters":
			processLockWaitersMetrics(mf, deadlockCandidates, result)
		case "gopherstack_delete_queue_depth":
			processWorkerMetrics(mf, result)
		case "gopherstack_worker_tasks_total":
			processWorkerTasks(mf, result)
		case "gopherstack_worker_items_total":
			processWorkerItems(mf, result)
		case "gopherstack_worker_queue_depth":
			processWorkerMetrics(mf, result)
		}
	}
}

// processWorkerMetrics extracts worker queue depth metrics.
func processWorkerMetrics(mf *io_prometheus_client.MetricFamily, result *Dashboard) {
	for _, m := range mf.GetMetric() {
		svc := getLabelValue(m, "service")
		name := getLabelValue(m, "worker")
		if name == "" {
			name = "Janitor" // fallback for legacy gauge
		}
		depth := int(m.GetGauge().GetValue())

		found := false
		for i := range result.Workers {
			if result.Workers[i].Service == svc && result.Workers[i].Worker == name {
				result.Workers[i].QueueDepth = depth
				found = true

				break
			}
		}
		if !found {
			result.Workers = append(result.Workers, WorkerStats{
				Service:    svc,
				Worker:     name,
				QueueDepth: depth,
			})
		}
	}
}

// processWorkerTasks extracts worker task completion metrics.
func processWorkerTasks(mf *io_prometheus_client.MetricFamily, result *Dashboard) {
	for _, m := range mf.GetMetric() {
		svc := getLabelValue(m, "service")
		name := getLabelValue(m, "worker")
		status := getLabelValue(m, "status")
		val := int64(m.GetCounter().GetValue())

		idx := findOrCreateWorker(result, svc, name)
		if status == "error" {
			result.Workers[idx].ErrorsTotal += val
		}
		result.Workers[idx].TasksTotal += val
	}
}

// processWorkerItems extracts worker item processing metrics.
func processWorkerItems(mf *io_prometheus_client.MetricFamily, result *Dashboard) {
	for _, m := range mf.GetMetric() {
		svc := getLabelValue(m, "service")
		name := getLabelValue(m, "worker")
		val := int64(m.GetCounter().GetValue())

		idx := findOrCreateWorker(result, svc, name)
		result.Workers[idx].ItemsProcessed += val
	}
}

func findOrCreateWorker(result *Dashboard, service, worker string) int {
	for i := range result.Workers {
		if result.Workers[i].Service == service && result.Workers[i].Worker == worker {
			return i
		}
	}
	result.Workers = append(result.Workers, WorkerStats{
		Service: service,
		Worker:  worker,
	})

	return len(result.Workers) - 1
}

// processLockHeldMetrics processes lock held time metrics.
func processLockHeldMetrics(mf *io_prometheus_client.MetricFamily, candidates map[string]*DeadlockInfo) {
	const heldThreshold = 1.0 // Held for more than 1 second is suspicious
	for _, m := range mf.GetMetric() {
		info := extractLockInfo(m)
		if info.HeldSec > heldThreshold {
			candidates[info.Lock] = info
		}
	}
}

// processLockWaitersMetrics processes lock waiter metrics.
func processLockWaitersMetrics(
	mf *io_prometheus_client.MetricFamily,
	candidates map[string]*DeadlockInfo,
	result *Dashboard,
) {
	for _, m := range mf.GetMetric() {
		lockName := getLabelValue(m, "lock")
		waiters := int(m.GetGauge().GetValue())
		if waiters > 0 {
			if info, ok := candidates[lockName]; ok {
				info.Waiters = waiters
				result.Deadlocks = append(result.Deadlocks, *info)
			}
		}
	}
}

func getLabelValue(m *io_prometheus_client.Metric, name string) string {
	for _, l := range m.GetLabel() {
		if l.GetName() == name {
			return l.GetValue()
		}
	}

	return ""
}

func extractLockInfo(m *io_prometheus_client.Metric) *DeadlockInfo {
	return &DeadlockInfo{
		Lock:      getLabelValue(m, "lock"),
		Operation: getLabelValue(m, "operation"),
		HeldSec:   m.GetGauge().GetValue(),
	}
}

// collectRuntimeMetrics gathers Go runtime statistics.
func collectRuntimeMetrics() *RuntimeMetrics {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	const bytesToMB = 1024.0 * 1024.0
	const nsToMs = 1e6
	const pauseHistorySize = 256
	const pauseOffset = 255

	var lastGCPause float64
	if m.NumGC > 0 {
		lastGCPause = float64(m.PauseNs[(m.NumGC+pauseOffset)%pauseHistorySize]) / nsToMs
	}

	return &RuntimeMetrics{
		Goroutines:   runtime.NumGoroutine(),
		HeapAllocMB:  float64(m.HeapAlloc) / bytesToMB,
		HeapSysMB:    float64(m.HeapSys) / bytesToMB,
		NumGC:        m.NumGC,
		LastGCPause:  lastGCPause,
		TotalAllocMB: float64(m.TotalAlloc) / bytesToMB,
	}
}

// parseHistogram extracts summary statistics from a Prometheus histogram metric.
func parseHistogram(mf *io_prometheus_client.MetricFamily) []Summary {
	var summaries []Summary

	for _, metric := range mf.GetMetric() {
		if metric.GetHistogram() == nil {
			continue
		}

		var operation string
		for _, label := range metric.GetLabel() {
			if label.Name != nil && label.GetName() == "operation" {
				operation = label.GetValue()
			}
		}

		// Get total count from histogram
		rawCount := metric.GetHistogram().SampleCount
		if rawCount == nil {
			rawCount = new(uint64)
		}

		// G115: integer overflow conversion uint64 -> int64
		var countVal int64
		if *rawCount > math.MaxInt64 {
			countVal = math.MaxInt64
		} else {
			// #nosec G115 -- Guarded by check above
			countVal = int64(*rawCount)
		}

		// Estimate percentiles from histogram buckets
		p50, p95, p99, avg, maxVal := estimatePercentiles(metric.GetHistogram())

		summaries = append(summaries, Summary{
			Operation: operation,
			Count:     countVal,
			P50Ms:     p50 * msPerSecond,
			P95Ms:     p95 * msPerSecond,
			P99Ms:     p99 * msPerSecond,
			AvgMs:     avg * msPerSecond,
			MaxMs:     maxVal * msPerSecond,
		})
	}

	return summaries
}

// estimatePercentiles estimates p50, p95, p99, average, and max from histogram buckets.
func estimatePercentiles(
	h *io_prometheus_client.Histogram,
) (float64, float64, float64, float64, float64) {
	if h == nil || h.SampleCount == nil || h.GetSampleCount() == 0 {
		return 0, 0, 0, 0, 0
	}

	totalCount := h.GetSampleCount()
	if h.SampleSum == nil {
		return 0, 0, 0, 0, 0
	}
	sum := h.GetSampleSum()

	// Average
	avg := sum / float64(totalCount)

	// Find percentiles by walking buckets
	p50, p95, p99, maxVal := calculatePercentilesFromBuckets(h, totalCount)

	return p50, p95, p99, avg, maxVal
}

func calculatePercentilesFromBuckets(
	h *io_prometheus_client.Histogram,
	totalCount uint64,
) (float64, float64, float64, float64) {
	var p50, p95, p99, maxVal float64
	p50Found, p95Found, p99Found := false, false, false

	for _, bucket := range h.GetBucket() {
		if bucket.CumulativeCount == nil {
			continue
		}

		currCount := bucket.GetCumulativeCount()
		bound := bucket.UpperBound
		if bound == nil {
			if maxVal == 0 {
				maxVal = 5.0 // Default max if not found
			}

			continue
		}

		if !p50Found && currCount >= (totalCount/p50Divisor) {
			p50 = *bound
			p50Found = true
		}
		if !p95Found && currCount >= (totalCount*p95Factor/pctBase) {
			p95 = *bound
			p95Found = true
		}
		if !p99Found && currCount >= (totalCount*p99Factor/pctBase) {
			p99 = *bound
			p99Found = true
		}
		maxVal = *bound
	}

	p50, p95, p99 = fillMissingPercentiles(p50Found, p95Found, p99Found, p50, p95, p99, maxVal)

	return p50, p95, p99, maxVal
}

func fillMissingPercentiles(
	p50Found, p95Found, p99Found bool,
	p50, p95, p99, maxVal float64,
) (float64, float64, float64) {
	newP50, newP95, newP99 := p50, p95, p99
	if !p50Found {
		newP50 = maxVal
	}
	if !p95Found {
		newP95 = maxVal
	}
	if !p99Found {
		newP99 = maxVal
	}

	return newP50, newP95, newP99
}

// updateAverages updates the AvgMs in Summaries using the explicit average gauge.
func updateAverages(mf *io_prometheus_client.MetricFamily, summaries []Summary) {
	avgMap := make(map[string]float64)
	for _, m := range mf.GetMetric() {
		op := getLabelValue(m, "operation")
		avgMap[op] = m.GetGauge().GetValue()
	}

	for i := range summaries {
		if val, ok := avgMap[summaries[i].Operation]; ok {
			summaries[i].AvgMs = val * msPerSecond
		}
	}
}
