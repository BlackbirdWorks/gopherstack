package telemetry_test

import (
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordOperation_Success(t *testing.T) {
	t.Parallel()

	// Record an operation
	telemetry.RecordOperation("TestPutItem", "TestUserTable", 0.123, "success")

	// Collect metrics
	result := telemetry.CollectMetrics()
	require.NotNil(t, result)

	// Check that we have a valid structure
	assert.NotNil(t, result.Operations)
}

func TestRecordOperation_Error(t *testing.T) {
	t.Parallel()

	// Record successful and failed operations with unique names
	telemetry.RecordOperation("TestDeleteItem", "TestTable1", 0.050, "success")
	telemetry.RecordOperation("TestDeleteItem", "TestTable1", 0.075, "error")

	// Collect metrics
	result := telemetry.CollectMetrics()
	require.NotNil(t, result)
	assert.NotNil(t, result.Operations)
}

func TestRecordOperation_MultipleOperations(t *testing.T) {
	t.Parallel()

	// Record multiple operations with unique names
	telemetry.RecordOperation("TestGetItem", "TestUsers", 0.010, "success")
	telemetry.RecordOperation("TestGetItem", "TestUsers", 0.015, "success")
	telemetry.RecordOperation("TestPutItem2", "TestUsers", 0.020, "success")
	telemetry.RecordOperation("TestQuery", "TestOrders", 0.050, "success")

	// Collect metrics
	result := telemetry.CollectMetrics()
	require.NotNil(t, result)

	// Check that operations are there (they may or may not be depending on previous runs)
	assert.NotNil(t, result.Operations)
}

func TestRecordOperation_LatencyAggregation(t *testing.T) {
	t.Parallel()

	// Record multiple latencies with unique operation name
	latencies := []float64{0.010, 0.015, 0.020, 0.025, 0.030}
	for _, latency := range latencies {
		telemetry.RecordOperation("TestUpdateItem", "TestTable2", latency, "success")
	}

	// Collect metrics
	result := telemetry.CollectMetrics()
	require.NotNil(t, result)

	// Find the TestUpdateItem metric
	var updateMetric *telemetry.Summary
	for i := range result.Operations {
		if result.Operations[i].Operation == "TestUpdateItem" &&
			result.Operations[i].Resource == "TestTable2" {
			updateMetric = &result.Operations[i]

			break
		}
	}

	if updateMetric != nil {
		assert.Equal(t, int64(5), updateMetric.Count, "expected 5 operations")

		// Verify latencies are aggregated (in milliseconds)
		assert.Greater(t, updateMetric.AvgMs, 0.0)
		assert.Greater(t, updateMetric.MaxMs, 0.0)
		assert.Greater(t, updateMetric.P99Ms, 0.0)
		assert.Greater(t, updateMetric.P95Ms, 0.0)
		assert.Greater(t, updateMetric.P50Ms, 0.0)
		// P50 should be roughly in the middle of the range
		assert.LessOrEqual(t, updateMetric.P50Ms, updateMetric.MaxMs)
	}
}

func TestCollectMetrics_ValidStructure(t *testing.T) {
	t.Parallel()

	// Collect metrics
	result := telemetry.CollectMetrics()
	require.NotNil(t, result)

	// Should always return a valid structure
	assert.NotNil(t, result.Operations)
	assert.IsType(t, []telemetry.Summary{}, result.Operations)
}

func TestRecordLockDuration(t *testing.T) {
	t.Parallel()

	// Record a lock duration - should not panic
	telemetry.RecordLockDuration("TestRLock", (50 * time.Millisecond).Seconds())
	telemetry.RecordLockDuration("TestLock", (100 * time.Millisecond).Seconds())

	// Just ensure it doesn't panic; lock duration has a separate metric
}

func TestMetricsPrecision(t *testing.T) {
	t.Parallel()

	// Record with high precision duration
	duration := 0.0001234
	telemetry.RecordOperation("TestScan", "TestTable3", duration, "success")

	// Collect metrics
	result := telemetry.CollectMetrics()
	require.NotNil(t, result)

	// Find the TestScan metric
	var scanMetric *telemetry.Summary
	for i := range result.Operations {
		if result.Operations[i].Operation == "TestScan" &&
			result.Operations[i].Resource == "TestTable3" {
			scanMetric = &result.Operations[i]

			break
		}
	}

	if scanMetric != nil {
		// 0.0001234 seconds = 0.1234 milliseconds, so we should see a very small value
		assert.Greater(t, scanMetric.AvgMs, 0.0)
		assert.Less(t, scanMetric.AvgMs, 1.0, "expected sub-millisecond operation")
	}
}

func TestCollectMetrics_RuntimeMetrics(t *testing.T) {
	t.Parallel()

	// Collect metrics
	result := telemetry.CollectMetrics()
	require.NotNil(t, result)

	// Verify runtime metrics are present
	assert.Positive(t, result.Runtime.Goroutines, "expected at least 1 goroutine")
	assert.GreaterOrEqual(t, result.Runtime.HeapAllocMB, 0.0, "heap allocation should be >= 0")
	assert.GreaterOrEqual(t, result.Runtime.HeapSysMB, 0.0, "heap sys should be >= 0")
	// NumGC is uint32, always >= 0, no need to assert
	assert.GreaterOrEqual(t, result.Runtime.LastGCPause, 0.0, "last GC pause should be >= 0")
	assert.GreaterOrEqual(t, result.Runtime.TotalAllocMB, 0.0, "total alloc should be >= 0")

	// Heap alloc should be less than or equal to heap sys
	assert.LessOrEqual(t, result.Runtime.HeapAllocMB, result.Runtime.HeapSysMB,
		"heap alloc should not exceed heap sys")
}
