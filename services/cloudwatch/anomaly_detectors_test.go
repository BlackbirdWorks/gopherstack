package cloudwatch_test

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// ---------------------------------------------------------------------------
// rollingStats
// ---------------------------------------------------------------------------

func TestRollingStats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		verify func(t *testing.T, mean, stddev float64)
		name   string
		vals   []float64
	}{
		{
			name: "empty",
			vals: nil,
			verify: func(t *testing.T, mean, stddev float64) {
				t.Helper()
				assert.InDelta(t, 0.0, mean, 1e-9)
				assert.InDelta(t, 0.0, stddev, 1e-9)
			},
		},
		{
			name: "single value",
			vals: []float64{42.0},
			verify: func(t *testing.T, mean, stddev float64) {
				t.Helper()
				assert.InDelta(t, 42.0, mean, 1e-9)
				assert.InDelta(t, 0.0, stddev, 1e-9, "stddev of single value is 0")
			},
		},
		{
			name: "constant values",
			vals: []float64{5, 5, 5, 5, 5},
			verify: func(t *testing.T, mean, stddev float64) {
				t.Helper()
				assert.InDelta(t, 5.0, mean, 1e-9)
				assert.InDelta(t, 0.0, stddev, 1e-9)
			},
		},
		{
			// mean=3, variance=(1+0+1)/3 ≈ 0.666, stddev≈0.8165
			name: "known values",
			vals: []float64{2, 3, 4},
			verify: func(t *testing.T, mean, stddev float64) {
				t.Helper()
				assert.InDelta(t, 3.0, mean, 1e-9)
				assert.InDelta(t, math.Sqrt(2.0/3.0), stddev, 1e-6)
			},
		},
		{
			name: "large variance",
			vals: []float64{0, 100},
			verify: func(t *testing.T, mean, stddev float64) {
				t.Helper()
				assert.InDelta(t, 50.0, mean, 1e-9)
				assert.Greater(t, stddev, 0.0, "non-constant input should have positive stddev")
			},
		},
		{
			name: "negative values",
			vals: []float64{-10, 0, 10},
			verify: func(t *testing.T, mean, stddev float64) {
				t.Helper()
				assert.InDelta(t, 0.0, mean, 1e-9)
				assert.Greater(t, stddev, 0.0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mean, stddev := cloudwatch.RollingStatsForTest(tc.vals)
			tc.verify(t, mean, stddev)
		})
	}
}

// ---------------------------------------------------------------------------
// computeAnomalyBand
// ---------------------------------------------------------------------------

func TestComputeAnomalyBand(t *testing.T) {
	t.Parallel()

	tests := []struct {
		verify func(t *testing.T, vals, lo, hi []float64)
		name   string
		vals   []float64
	}{
		{
			name: "empty",
			vals: nil,
			verify: func(t *testing.T, _, lo, hi []float64) {
				t.Helper()
				assert.Nil(t, lo)
				assert.Nil(t, hi)
			},
		},
		{
			name: "band length matches input length",
			vals: []float64{1, 2, 3, 4, 5},
			verify: func(t *testing.T, vals, lo, hi []float64) {
				t.Helper()
				assert.Len(t, lo, len(vals))
				assert.Len(t, hi, len(vals))
			},
		},
		{
			name: "upper always at least lower",
			vals: []float64{10, 20, 30, 15, 25},
			verify: func(t *testing.T, _, lo, hi []float64) {
				t.Helper()
				require.NotEmpty(t, lo)

				for i := range lo {
					assert.LessOrEqual(t, lo[i], hi[i], "lower bound ≤ upper bound at index %d", i)
				}
			},
		},
		{
			name: "constant series collapses to point",
			vals: []float64{7, 7, 7, 7},
			verify: func(t *testing.T, _, lo, hi []float64) {
				t.Helper()
				require.NotEmpty(t, lo)

				// stddev=0 → lower == upper == mean
				for i := range lo {
					assert.InDelta(t, lo[i], hi[i], 1e-9, "constant series: band collapses to point")
				}
			},
		},
		{
			// All values equal → mean=5, stddev=0, band collapses.
			name: "symmetric around mean for zero variance",
			vals: []float64{5, 5, 5},
			verify: func(t *testing.T, _, lo, hi []float64) {
				t.Helper()
				for i := range lo {
					assert.InDelta(t, 5.0, lo[i], 1e-9, "lower should equal mean for zero-variance")
					assert.InDelta(t, 5.0, hi[i], 1e-9, "upper should equal mean for zero-variance")
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			lo, hi := cloudwatch.ComputeAnomalyBandForTest(tc.vals)
			tc.verify(t, tc.vals, lo, hi)
		})
	}
}

func TestComputeAnomalyBand_SameLengthAsInput(t *testing.T) {
	t.Parallel()

	for _, n := range []int{1, 5, 10, 100} {
		vals := make([]float64, n)
		for i := range vals {
			vals[i] = float64(i)
		}

		lo, hi := cloudwatch.ComputeAnomalyBandForTest(vals)
		assert.Len(t, lo, n, "lower band length mismatch for n=%d", n)
		assert.Len(t, hi, n, "upper band length mismatch for n=%d", n)
	}
}

// ---------------------------------------------------------------------------
// evalAnomalyDetectionBand (ANOMALY_DETECTION_BAND expression)
// ---------------------------------------------------------------------------

func makeTimestamps(n int) []time.Time {
	t0 := time.Now().UTC()
	ts := make([]time.Time, n)
	for i := range ts {
		ts[i] = t0.Add(time.Duration(i) * time.Minute)
	}

	return ts
}

func TestEvalAnomalyDetectionBand_Basic(t *testing.T) {
	t.Parallel()

	ts := makeTimestamps(5)
	resolved := map[string]cloudwatch.MetricDataResult{
		"m1": {
			Timestamps: ts,
			Values:     []float64{10, 20, 30, 20, 10},
			StatusCode: "Complete",
		},
	}

	upper, lower, ok := cloudwatch.EvalAnomalyDetectionBandForTest("ANOMALY_DETECTION_BAND(m1)", resolved)
	require.True(t, ok)
	assert.Len(t, upper.Values, 5)
	assert.Len(t, lower.Values, 5)

	for i := range upper.Values {
		assert.LessOrEqual(t, lower.Values[i], upper.Values[i])
	}
}

func TestEvalAnomalyDetectionBand_CustomStdDevs(t *testing.T) {
	t.Parallel()

	ts := makeTimestamps(3)
	resolved := map[string]cloudwatch.MetricDataResult{
		"base": {Timestamps: ts, Values: []float64{5, 10, 15}, StatusCode: "Complete"},
	}

	upper2, lower2, ok2 := cloudwatch.EvalAnomalyDetectionBandForTest("ANOMALY_DETECTION_BAND(base, 2)", resolved)
	require.True(t, ok2)

	upper4, lower4, ok4 := cloudwatch.EvalAnomalyDetectionBandForTest("ANOMALY_DETECTION_BAND(base, 4)", resolved)
	require.True(t, ok4)

	// Wider stddev factor → wider band.
	band2 := upper2.Values[0] - lower2.Values[0]
	band4 := upper4.Values[0] - lower4.Values[0]
	assert.Greater(t, band4, band2, "4-stddev band should be wider than 2-stddev")
}

func TestEvalAnomalyDetectionBand_UnknownRef(t *testing.T) {
	t.Parallel()

	resolved := map[string]cloudwatch.MetricDataResult{}
	upper, lower, ok := cloudwatch.EvalAnomalyDetectionBandForTest("ANOMALY_DETECTION_BAND(missing)", resolved)
	require.True(t, ok, "pattern matched but ref not found → still ok=true with empty result")
	assert.Empty(t, upper.Values)
	assert.Empty(t, lower.Values)
}

func TestEvalAnomalyDetectionBand_NoMatch(t *testing.T) {
	t.Parallel()

	resolved := map[string]cloudwatch.MetricDataResult{}
	_, _, ok := cloudwatch.EvalAnomalyDetectionBandForTest("m1 + m2", resolved)
	assert.False(t, ok, "non-band expression should return ok=false")
}

func TestEvalAnomalyDetectionBand_CaseInsensitive(t *testing.T) {
	t.Parallel()

	ts := makeTimestamps(2)
	resolved := map[string]cloudwatch.MetricDataResult{
		"x": {Timestamps: ts, Values: []float64{1, 2}, StatusCode: "Complete"},
	}

	_, _, ok := cloudwatch.EvalAnomalyDetectionBandForTest("anomaly_detection_band(x)", resolved)
	assert.True(t, ok, "lowercase form should also match")
}

// ---------------------------------------------------------------------------
// Backend integration: anomaly detection band in GetMetricStatistics (gap #11)
// ---------------------------------------------------------------------------

func TestBackend_GetMetricStatistics_AnomalyBand(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	now := time.Now().UTC()

	// Store metric data.
	err := b.PutMetricData("App", []cloudwatch.MetricDatum{
		{MetricName: "Latency", Value: 10, Count: 1, Sum: 10, Min: 10, Max: 10, Timestamp: now.Add(-2 * time.Minute)},
		{MetricName: "Latency", Value: 20, Count: 1, Sum: 20, Min: 20, Max: 20, Timestamp: now.Add(-time.Minute)},
		{MetricName: "Latency", Value: 30, Count: 1, Sum: 30, Min: 30, Max: 30, Timestamp: now.Add(-30 * time.Second)},
	})
	require.NoError(t, err)

	// Add anomaly detector.
	require.NoError(t, b.PutAnomalyDetector(&cloudwatch.AnomalyDetector{
		Namespace: "App", MetricName: "Latency", Stat: "Average",
	}))

	dps, err := b.GetMetricStatistics(
		"App", "Latency", nil,
		now.Add(-5*time.Minute), now,
		60, []string{"Average"}, nil,
	)
	require.NoError(t, err)
	require.NotEmpty(t, dps)

	// At least one datapoint should have band annotations.
	hasBand := false

	for _, dp := range dps {
		if dp.BandLower != nil && dp.BandUpper != nil {
			hasBand = true
			assert.LessOrEqual(t, *dp.BandLower, *dp.BandUpper)
		}
	}

	assert.True(t, hasBand, "anomaly detector should annotate datapoints with band")
}

func TestBackend_GetMetricStatistics_NoAnomalyDetector_NoBand(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	now := time.Now().UTC()

	err := b.PutMetricData("App", []cloudwatch.MetricDatum{
		{MetricName: "CPU", Value: 50, Count: 1, Sum: 50, Min: 50, Max: 50, Timestamp: now.Add(-time.Minute)},
	})
	require.NoError(t, err)

	dps, err := b.GetMetricStatistics(
		"App", "CPU", nil,
		now.Add(-5*time.Minute), now,
		60, []string{"Average"}, nil,
	)
	require.NoError(t, err)
	require.NotEmpty(t, dps)

	for _, dp := range dps {
		assert.Nil(t, dp.BandLower, "no detector → no band")
		assert.Nil(t, dp.BandUpper, "no detector → no band")
	}
}

// ---------------------------------------------------------------------------
// Additional anomaly detection band edge cases
// ---------------------------------------------------------------------------

func TestBackend_AnomalyDetector_DimensionMatch(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	now := time.Now().UTC()
	dimProd := []cloudwatch.Dimension{{Name: "Env", Value: "prod"}}
	dimStaging := []cloudwatch.Dimension{{Name: "Env", Value: "staging"}}

	err := b.PutMetricData("App", []cloudwatch.MetricDatum{
		{
			MetricName: "CPU", Value: 80, Count: 1, Sum: 80, Min: 80, Max: 80,
			Timestamp: now.Add(-time.Minute), Dimensions: dimProd,
		},
		{
			MetricName: "CPU", Value: 20, Count: 1, Sum: 20, Min: 20, Max: 20,
			Timestamp: now.Add(-time.Minute), Dimensions: dimStaging,
		},
	})
	require.NoError(t, err)

	// Detector for prod only.
	require.NoError(t, b.PutAnomalyDetector(&cloudwatch.AnomalyDetector{
		Namespace: "App", MetricName: "CPU", Stat: "Average",
		Dimensions: dimProd,
	}))

	dpsProd, err := b.GetMetricStatistics("App", "CPU", dimProd,
		now.Add(-5*time.Minute), now, 60, []string{"Average"}, nil)
	require.NoError(t, err)

	hasBandProd := false
	for _, dp := range dpsProd {
		if dp.BandLower != nil {
			hasBandProd = true
		}
	}

	dpsStaging, err := b.GetMetricStatistics("App", "CPU", dimStaging,
		now.Add(-5*time.Minute), now, 60, []string{"Average"}, nil)
	require.NoError(t, err)

	for _, dp := range dpsStaging {
		assert.Nil(t, dp.BandLower, "staging should not have band from prod detector")
	}

	assert.True(t, hasBandProd, "prod query should have anomaly band from matching detector")
}

func TestBackend_AnomalyDetector_CustomBandWidth(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	now := time.Now().UTC()

	err := b.PutMetricData("Srv", []cloudwatch.MetricDatum{
		{MetricName: "Req", Value: 10, Count: 1, Sum: 10, Min: 10, Max: 10, Timestamp: now.Add(-2 * time.Minute)},
		{MetricName: "Req", Value: 30, Count: 1, Sum: 30, Min: 30, Max: 30, Timestamp: now.Add(-time.Minute)},
		{MetricName: "Req", Value: 20, Count: 1, Sum: 20, Min: 20, Max: 20, Timestamp: now.Add(-30 * time.Second)},
	})
	require.NoError(t, err)

	// BandWidth 3 → 3 stddevs, BandWidth 1 → 1 stddev.
	for _, bw := range []float64{1.0, 3.0} {
		require.NoError(t, b.PutAnomalyDetector(&cloudwatch.AnomalyDetector{
			Namespace: "Srv", MetricName: "Req", Stat: "Sum",
			BandWidth: bw,
		}))

		dps, getErr := b.GetMetricStatistics("Srv", "Req", nil,
			now.Add(-5*time.Minute), now, 60, []string{"Sum"}, nil)
		require.NoError(t, getErr)

		for _, dp := range dps {
			if dp.BandLower != nil && dp.BandUpper != nil {
				assert.LessOrEqual(t, *dp.BandLower, *dp.BandUpper)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// AnomalyDetector: CRUD
// ---------------------------------------------------------------------------

func TestBackend_AnomalyDetector_CRUD(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()

	require.NoError(t, b.PutAnomalyDetector(&cloudwatch.AnomalyDetector{
		Namespace:  "NS",
		MetricName: "M",
		Stat:       "Average",
	}))

	p, err := b.DescribeAnomalyDetectors("NS", "M", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "NS", p.Data[0].Namespace)

	require.NoError(t, b.DeleteAnomalyDetector("NS", "M", "Average", nil))

	p, err = b.DescribeAnomalyDetectors("NS", "M", "", 0)
	require.NoError(t, err)
	assert.Empty(t, p.Data)
}

func TestBackend_AnomalyDetector_MissingNamespace(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	err := b.PutAnomalyDetector(&cloudwatch.AnomalyDetector{MetricName: "M", Stat: "Average"})
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// Anomaly detector key isolation by dimensions
// ---------------------------------------------------------------------------

func TestAnomalyDetectorDimensionIsolation(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()

	dimA := []cloudwatch.Dimension{{Name: "Host", Value: "a"}}
	dimB := []cloudwatch.Dimension{{Name: "Host", Value: "b"}}

	require.NoError(t, b.PutAnomalyDetector(&cloudwatch.AnomalyDetector{
		Namespace:  "NS",
		MetricName: "CPU",
		Stat:       "Average",
		Dimensions: dimA,
	}))

	require.NoError(t, b.PutAnomalyDetector(&cloudwatch.AnomalyDetector{
		Namespace:  "NS",
		MetricName: "CPU",
		Stat:       "Average",
		Dimensions: dimB,
	}))

	// Delete only dimA detector; dimB must remain.
	require.NoError(t, b.DeleteAnomalyDetector("NS", "CPU", "Average", dimA))

	page, err := b.DescribeAnomalyDetectors("NS", "CPU", "", 0)
	require.NoError(t, err)
	require.Len(t, page.Data, 1, "only dimB detector should remain")
	assert.Equal(t, dimB, page.Data[0].Dimensions)
}

func TestCloudWatchBackend_PutAnomalyDetector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		det     *cloudwatch.AnomalyDetector
		name    string
		wantErr bool
	}{
		{
			name: "valid",
			det: &cloudwatch.AnomalyDetector{
				Namespace:  "AWS/EC2",
				MetricName: "CPUUtilization",
				Stat:       "Average",
			},
		},
		{
			name:    "missing_namespace",
			det:     &cloudwatch.AnomalyDetector{MetricName: "CPUUtilization"},
			wantErr: true,
		},
		{
			name:    "missing_metric_name",
			det:     &cloudwatch.AnomalyDetector{Namespace: "AWS/EC2"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackend()
			err := b.PutAnomalyDetector(tt.det)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			p, err2 := b.DescribeAnomalyDetectors(tt.det.Namespace, tt.det.MetricName, "", 0)
			require.NoError(t, err2)
			require.Len(t, p.Data, 1)
			assert.Equal(t, tt.det.Namespace, p.Data[0].Namespace)
			assert.Equal(t, tt.det.MetricName, p.Data[0].MetricName)
		})
	}
}
