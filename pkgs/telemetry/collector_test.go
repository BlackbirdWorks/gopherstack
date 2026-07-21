package telemetry_test

import (
	"testing"

	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

func TestSetServiceCount(t *testing.T) {
	tests := []struct {
		name      string
		count     int
		wantCount int
	}{
		{
			name:      "set_to_5",
			count:     5,
			wantCount: 5,
		},
		{
			name:      "set_to_0",
			count:     0,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			telemetry.SetServiceCount(tt.count)

			result := telemetry.CollectMetrics()
			require.NotNil(t, result)
			assert.Equal(t, tt.wantCount, result.Runtime.NumServices)
		})
	}
}

func TestProcessLockHeldMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		labels    map[string]string
		name      string
		wantLock  string
		wantOp    string
		val       float64
		wantLen   int
		wantValue float64
	}{
		{
			name:      "above_threshold",
			labels:    map[string]string{"lock": "TestLock", "operation": "TestOp"},
			val:       2.5,
			wantLen:   1,
			wantLock:  "TestLock",
			wantOp:    "TestOp",
			wantValue: 2.5,
		},
		{
			name:      "below_threshold",
			labels:    map[string]string{"lock": "TestLock", "operation": "TestOp"},
			val:       0.5,
			wantLen:   0,
			wantLock:  "",
			wantOp:    "",
			wantValue: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var labelPairs []*io_prometheus_client.LabelPair
			for k, v := range tt.labels {
				kStr, vStr := k, v
				labelPairs = append(labelPairs, &io_prometheus_client.LabelPair{
					Name:  &kStr,
					Value: &vStr,
				})
			}

			metric := &io_prometheus_client.Metric{
				Label: labelPairs,
				Gauge: &io_prometheus_client.Gauge{
					Value: &tt.val,
				},
			}

			metricName := "gopherstack_lock_write_held_seconds"
			mf := &io_prometheus_client.MetricFamily{
				Name:   &metricName,
				Metric: []*io_prometheus_client.Metric{metric},
			}

			candidates := make(map[string]*telemetry.DeadlockInfo)
			telemetry.ProcessLockHeldMetrics(mf, candidates)

			require.Len(t, candidates, tt.wantLen)

			if tt.wantLen > 0 {
				assert.Equal(t, tt.wantLock, candidates[tt.wantLock].Lock)
				assert.Equal(t, tt.wantOp, candidates[tt.wantLock].Operation)
				assert.InDelta(t, tt.wantValue, candidates[tt.wantLock].HeldSec, 0.0001)
			}
		})
	}
}

func TestProcessLockWaitersMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		labels     map[string]string
		candidates map[string]*telemetry.DeadlockInfo
		name       string
		wantLock   string
		val        float64
		wantLen    int
		wantWaiter int
	}{
		{
			name:   "has_waiters",
			labels: map[string]string{"lock": "TestLock"},
			val:    3.0,
			candidates: map[string]*telemetry.DeadlockInfo{
				"TestLock": {Lock: "TestLock", Operation: "TestOp", HeldSec: 2.5},
			},
			wantLen:    1,
			wantLock:   "TestLock",
			wantWaiter: 3,
		},
		{
			name:   "no_waiters",
			labels: map[string]string{"lock": "TestLock"},
			val:    0.0,
			candidates: map[string]*telemetry.DeadlockInfo{
				"TestLock": {Lock: "TestLock", Operation: "TestOp", HeldSec: 2.5},
			},
			wantLen:    0,
			wantLock:   "",
			wantWaiter: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var labelPairs []*io_prometheus_client.LabelPair
			for k, v := range tt.labels {
				kStr, vStr := k, v
				labelPairs = append(labelPairs, &io_prometheus_client.LabelPair{
					Name:  &kStr,
					Value: &vStr,
				})
			}

			metric := &io_prometheus_client.Metric{
				Label: labelPairs,
				Gauge: &io_prometheus_client.Gauge{
					Value: &tt.val,
				},
			}

			metricName := "gopherstack_lock_write_waiters"
			mf := &io_prometheus_client.MetricFamily{
				Name:   &metricName,
				Metric: []*io_prometheus_client.Metric{metric},
			}

			result := &telemetry.Dashboard{}
			telemetry.ProcessLockWaitersMetrics(mf, tt.candidates, result)

			require.Len(t, result.Deadlocks, tt.wantLen)

			if tt.wantLen > 0 {
				assert.Equal(t, tt.wantWaiter, result.Deadlocks[0].Waiters)
				assert.Equal(t, tt.wantLock, result.Deadlocks[0].Lock)
			}
		})
	}
}

func TestFillMissingPercentiles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		has50   bool
		has95   bool
		has99   bool
		p50     float64
		p95     float64
		p99     float64
		max     float64
		wantP50 float64
		wantP95 float64
		wantP99 float64
	}{
		{
			name:  "all_missing",
			has50: false, has95: false, has99: false,
			p50: 0, p95: 0, p99: 0, max: 100,
			wantP50: 100, wantP95: 100, wantP99: 100,
		},
		{
			name:  "all_present",
			has50: true, has95: true, has99: true,
			p50: 50, p95: 95, p99: 99, max: 100,
			wantP50: 50, wantP95: 95, wantP99: 99,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got50, got95, got99 := telemetry.FillMissingPercentiles(
				tt.has50, tt.has95, tt.has99, tt.p50, tt.p95, tt.p99, tt.max,
			)

			assert.InDelta(t, tt.wantP50, got50, 0.0001)
			assert.InDelta(t, tt.wantP95, got95, 0.0001)
			assert.InDelta(t, tt.wantP99, got99, 0.0001)
		})
	}
}

func TestCalculatePercentilesFromBuckets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		hist    *io_prometheus_client.Histogram
		name    string
		count   uint64
		want50  float64
		want95  float64
		want99  float64
		wantMax float64
	}{
		{
			name:   "empty_histogram",
			hist:   &io_prometheus_client.Histogram{},
			count:  10,
			want50: 0, want95: 0, want99: 0, wantMax: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got50, got95, got99, gotMax := telemetry.CalculatePercentilesFromBuckets(tt.hist, tt.count)

			assert.InDelta(t, tt.want50, got50, 0.0001)
			assert.InDelta(t, tt.want95, got95, 0.0001)
			assert.InDelta(t, tt.want99, got99, 0.0001)
			assert.InDelta(t, tt.wantMax, gotMax, 0.0001)
		})
	}
}

func TestEstimatePercentiles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		hist    *io_prometheus_client.Histogram
		name    string
		want50  float64
		want95  float64
		want99  float64
		wantAvg float64
		wantMax float64
	}{
		{
			name:   "nil_metric",
			hist:   nil,
			want50: 0, want95: 0, want99: 0, wantAvg: 0, wantMax: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got50, got95, got99, gotAvg, gotMax := telemetry.EstimatePercentiles(tt.hist)

			assert.InDelta(t, tt.want50, got50, 0.0001)
			assert.InDelta(t, tt.want95, got95, 0.0001)
			assert.InDelta(t, tt.want99, got99, 0.0001)
			assert.InDelta(t, tt.wantAvg, gotAvg, 0.0001)
			assert.InDelta(t, tt.wantMax, gotMax, 0.0001)
		})
	}
}
