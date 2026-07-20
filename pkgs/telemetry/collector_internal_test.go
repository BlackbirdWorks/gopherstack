package telemetry

import (
	"testing"

	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetServiceCount(t *testing.T) {
	t.Parallel()

	SetServiceCount(5)

	result := CollectMetrics()
	require.NotNil(t, result)
	assert.Equal(t, 5, result.Runtime.NumServices)
}

func TestProcessLockHeldMetrics(t *testing.T) {
	t.Parallel()

	// Create a dummy metric for gopherstack_lock_write_held_seconds
	labelName1 := "lock"
	labelValue1 := "TestLock"
	labelName2 := "operation"
	labelValue2 := "TestOp"

	val := 2.5 // greater than heldThreshold 1.0

	metric := &io_prometheus_client.Metric{
		Label: []*io_prometheus_client.LabelPair{
			{Name: &labelName1, Value: &labelValue1},
			{Name: &labelName2, Value: &labelValue2},
		},
		Gauge: &io_prometheus_client.Gauge{
			Value: &val,
		},
	}

	name := "gopherstack_lock_write_held_seconds"
	mf := &io_prometheus_client.MetricFamily{
		Name:   &name,
		Metric: []*io_prometheus_client.Metric{metric},
	}

	candidates := make(map[string]*DeadlockInfo)
	processLockHeldMetrics(mf, candidates)

	require.Len(t, candidates, 1)
	assert.Equal(t, "TestLock", candidates["TestLock"].Lock)
	assert.Equal(t, "TestOp", candidates["TestLock"].Operation)
	assert.InDelta(t, 2.5, candidates["TestLock"].HeldSec, 0.0001)
}

func TestProcessLockWaitersMetrics(t *testing.T) {
	t.Parallel()

	// Create a dummy metric for gopherstack_lock_write_waiters
	labelName1 := "lock"
	labelValue1 := "TestLock"

	val := float64(3)

	metric := &io_prometheus_client.Metric{
		Label: []*io_prometheus_client.LabelPair{
			{Name: &labelName1, Value: &labelValue1},
		},
		Gauge: &io_prometheus_client.Gauge{
			Value: &val,
		},
	}

	name := "gopherstack_lock_write_waiters"
	mf := &io_prometheus_client.MetricFamily{
		Name:   &name,
		Metric: []*io_prometheus_client.Metric{metric},
	}

	candidates := map[string]*DeadlockInfo{
		"TestLock": {
			Lock:      "TestLock",
			Operation: "TestOp",
			HeldSec:   2.5,
		},
	}
	result := &Dashboard{}

	processLockWaitersMetrics(mf, candidates, result)

	require.Len(t, result.Deadlocks, 1)
	assert.Equal(t, 3, result.Deadlocks[0].Waiters)
	assert.Equal(t, "TestLock", result.Deadlocks[0].Lock)
}

func TestFillMissingPercentiles(t *testing.T) {
	t.Parallel()

	p50, p95, p99 := fillMissingPercentiles(false, false, false, 0, 0, 0, 100)
	assert.InDelta(t, 100.0, p50, 0.0001)
	assert.InDelta(t, 100.0, p95, 0.0001)
	assert.InDelta(t, 100.0, p99, 0.0001)

	p50, p95, p99 = fillMissingPercentiles(true, true, true, 50, 95, 99, 100)
	assert.InDelta(t, 50.0, p50, 0.0001)
	assert.InDelta(t, 95.0, p95, 0.0001)
	assert.InDelta(t, 99.0, p99, 0.0001)
}

func TestCalculatePercentilesFromBuckets_Empty(t *testing.T) {
	t.Parallel()

	h := &io_prometheus_client.Histogram{}
	p50, p95, p99, maxVal := calculatePercentilesFromBuckets(h, 10)
	assert.InDelta(t, 0.0, p50, 0.0001)
	assert.InDelta(t, 0.0, p95, 0.0001)
	assert.InDelta(t, 0.0, p99, 0.0001)
	assert.InDelta(t, 0.0, maxVal, 0.0001)
}

func TestEstimatePercentiles_Empty(t *testing.T) {
	t.Parallel()

	p50, p95, p99, avg, maxVal := estimatePercentiles(nil)
	assert.InDelta(t, 0.0, p50, 0.0001)
	assert.InDelta(t, 0.0, p95, 0.0001)
	assert.InDelta(t, 0.0, p99, 0.0001)
	assert.InDelta(t, 0.0, avg, 0.0001)
	assert.InDelta(t, 0.0, maxVal, 0.0001)
}
