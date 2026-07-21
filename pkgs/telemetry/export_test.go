package telemetry

import (
	io_prometheus_client "github.com/prometheus/client_model/go"
)

// Export internal functions for testing.
func ProcessLockHeldMetrics(mf *io_prometheus_client.MetricFamily, candidates map[string]*DeadlockInfo) {
	processLockHeldMetrics(mf, candidates)
}

func ProcessLockWaitersMetrics(
	mf *io_prometheus_client.MetricFamily,
	candidates map[string]*DeadlockInfo,
	result *Dashboard,
) {
	processLockWaitersMetrics(mf, candidates, result)
}

func FillMissingPercentiles(
	p50Found, p95Found, p99Found bool,
	p50, p95, p99, maxVal float64,
) (float64, float64, float64) {
	return fillMissingPercentiles(p50Found, p95Found, p99Found, p50, p95, p99, maxVal)
}

func CalculatePercentilesFromBuckets(
	h *io_prometheus_client.Histogram,
	totalCount uint64,
) (float64, float64, float64, float64) {
	return calculatePercentilesFromBuckets(h, totalCount)
}

func EstimatePercentiles(
	h *io_prometheus_client.Histogram,
) (float64, float64, float64, float64, float64) {
	return estimatePercentiles(h)
}

// Export internal types for testing if needed.
// DeadlockInfo and Dashboard are exported already? No, DeadlockInfo is exported, Dashboard is exported.
// The functions themselves just take them as arguments.

// Type aliases for testing.
type MetricFamily = io_prometheus_client.MetricFamily
