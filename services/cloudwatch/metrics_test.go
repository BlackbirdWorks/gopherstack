package cloudwatch_test

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// ---------------------------------------------------------------------------
// validateMetricDatum (gap #2 mutual-exclusion enforcement)
// ---------------------------------------------------------------------------

func TestValidateMetricDatum_ShapeCombinations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		datum      func(now time.Time) cloudwatch.MetricDatum
		name       string
		wantErrSub string
	}{
		{
			name: "value only",
			datum: func(now time.Time) cloudwatch.MetricDatum {
				return cloudwatch.MetricDatum{MetricName: "M", Value: 1.0, HasValue: true, Timestamp: now}
			},
		},
		{
			name: "statistic set only",
			datum: func(now time.Time) cloudwatch.MetricDatum {
				return cloudwatch.MetricDatum{
					MetricName:      "M",
					HasStatisticSet: true,
					Count:           5, Sum: 100, Min: 10, Max: 30,
					Timestamp: now,
				}
			},
		},
		{
			name: "both value and statistic set rejected",
			datum: func(now time.Time) cloudwatch.MetricDatum {
				return cloudwatch.MetricDatum{
					MetricName:      "M",
					Value:           1.0,
					HasValue:        true,
					HasStatisticSet: true,
					Count:           5, Sum: 100, Min: 10, Max: 30,
					Timestamp: now,
				}
			},
			wantErrSub: "mutually exclusive",
		},
		{
			name: "no statistic set with zero value",
			datum: func(now time.Time) cloudwatch.MetricDatum {
				return cloudwatch.MetricDatum{MetricName: "M", Value: 0, HasStatisticSet: false, Timestamp: now}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := cloudwatch.ValidateMetricDatumForTest(tc.datum(time.Now().UTC()))
			if tc.wantErrSub == "" {
				assert.NoError(t, err)

				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErrSub)
		})
	}
}

func TestValidateMetricDatum_TimestampWithinWindow(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	cases := []struct {
		ts   time.Time
		name string
		ok   bool
	}{
		{name: "now", ts: now, ok: true},
		{name: "13 days 23 hours in past", ts: now.Add(-13*24*time.Hour - 23*time.Hour), ok: true},
		{name: "just over 2 weeks in past", ts: now.Add(-14*24*time.Hour - time.Minute), ok: false},
		{name: "1 hour 59 minutes in future", ts: now.Add(time.Hour + 59*time.Minute), ok: true},
		{name: "just over 2 hours in future", ts: now.Add(2*time.Hour + time.Minute), ok: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d := cloudwatch.MetricDatum{MetricName: "M", Value: 1.0, HasValue: true, Timestamp: tc.ts}
			err := cloudwatch.ValidateMetricDatumAtForTest(d, now)

			if tc.ok {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "two weeks")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// validateStorageResolution (gap #3)
// ---------------------------------------------------------------------------

func TestValidateStorageResolution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		res     int32
		wantErr bool
	}{
		{name: "default", res: 0, wantErr: false},
		{name: "high resolution", res: 1, wantErr: false},
		{name: "standard resolution", res: 60, wantErr: false},
		{name: "2 is invalid", res: 2, wantErr: true},
		{name: "30 is invalid", res: 30, wantErr: true},
		{name: "120 is invalid", res: 120, wantErr: true},
		{name: "negative is invalid", res: -1, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := cloudwatch.ValidateStorageResolutionForTest(tc.res)
			if tc.wantErr {
				assert.Error(t, err, "res=%d should be invalid", tc.res)

				return
			}
			assert.NoError(t, err, "res=%d", tc.res)
		})
	}
}

// ---------------------------------------------------------------------------
// Backend integration: StatisticSet validation via PutMetricData (gap #2)
// ---------------------------------------------------------------------------

func TestBackend_PutMetricData_StatisticSet_Valid(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC()

	err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{
			MetricName:      "Reqs",
			HasStatisticSet: true,
			Count:           5, Sum: 250, Min: 40, Max: 60,
			Timestamp: ts,
		},
	})
	require.NoError(t, err)
}

// TestBackend_PutMetricData_ValueAndStatisticSet_Rejected verifies AWS's
// all-or-nothing PutMetricData contract: PutMetricDataOutput carries no
// per-datum result (confirmed against aws-sdk-go-v2 cloudwatch types), so a
// request containing an invalid datum must fail the entire call rather than
// silently dropping the bad entry into a fabricated "unprocessed" list.
func TestBackend_PutMetricData_ValueAndStatisticSet_Rejected(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC()

	err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{
			MetricName:      "BadEntry",
			Value:           1.0,
			HasValue:        true,
			HasStatisticSet: true,
			Count:           5, Sum: 100, Min: 10, Max: 30,
			Timestamp: ts,
		},
	})
	require.ErrorIs(t, err, cloudwatch.ErrValueAndStatisticSet)

	// The whole request must be rejected: nothing gets stored.
	p, lerr := b.ListMetrics("NS", "", nil, "", "", 0)
	require.NoError(t, lerr)
	assert.Empty(t, p.Data)
}

func TestBackend_PutMetricData_InvalidStorageResolution_Rejected(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC()

	err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{
			MetricName: "BadRes",
			Value:      1.0,
			HasValue:   true,
			Count:      1, Sum: 1, Min: 1, Max: 1,
			Timestamp:         ts,
			StorageResolution: 30,
		},
	})
	require.ErrorIs(t, err, cloudwatch.ErrValidation)
}

// TestBackend_PutMetricData_MixedValidAndInvalid verifies that when one datum
// in a batch is invalid, none of the batch is stored (no partial commit),
// matching AWS's atomic PutMetricData contract.
func TestBackend_PutMetricData_MixedValidAndInvalid(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC()

	err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "Good", Value: 1, HasValue: true, Count: 1, Sum: 1, Min: 1, Max: 1, Timestamp: ts},
		{
			MetricName: "Bad", Value: 1, HasValue: true, HasStatisticSet: true,
			Count: 2, Sum: 50, Min: 10, Max: 40, Timestamp: ts,
		},
		{MetricName: "AlsoGood", Value: 2, HasValue: true, Count: 1, Sum: 2, Min: 2, Max: 2, Timestamp: ts},
	})
	require.ErrorIs(t, err, cloudwatch.ErrValueAndStatisticSet)

	// Nothing from the batch should be stored, including the otherwise-valid entries.
	p, lerr := b.ListMetrics("NS", "", nil, "", "", 0)
	require.NoError(t, lerr)
	assert.Empty(t, p.Data)
}

func Test_PutMetricData_ValuesCountsArray(t *testing.T) {
	t.Parallel()

	ts := cloudwatch.RecentTestAnchor()

	cases := []struct {
		wantErr    error
		name       string
		datum      cloudwatch.MetricDatum
		wantSum    float64
		wantCount  float64
		wantMin    float64
		wantMax    float64
		wantStored bool
	}{
		{
			name: "counts default to 1 when omitted",
			datum: cloudwatch.MetricDatum{
				MetricName: "M", Timestamp: ts,
				HasValuesArray: true,
				Values:         []float64{4, 8},
				Counts:         []float64{1, 1},
			},
			wantStored: true, wantSum: 12, wantCount: 2, wantMin: 4, wantMax: 8,
		},
		{
			name: "weighted counts aggregate correctly",
			datum: cloudwatch.MetricDatum{
				MetricName: "M", Timestamp: ts,
				HasValuesArray: true,
				Values:         []float64{1, 2, 3},
				Counts:         []float64{10, 20, 30},
			},
			// sum = 1*10 + 2*20 + 3*30 = 140; count = 60.
			wantStored: true, wantSum: 140, wantCount: 60, wantMin: 1, wantMax: 3,
		},
		{
			name: "mismatched Values/Counts lengths rejected",
			datum: cloudwatch.MetricDatum{
				MetricName: "M", Timestamp: ts,
				HasValuesArray: true,
				Values:         []float64{1, 2, 3},
				Counts:         []float64{1, 1},
			},
			wantErr: cloudwatch.ErrValuesCountsLengthMismatch,
		},
		{
			name: "more than 150 values rejected",
			datum: cloudwatch.MetricDatum{
				MetricName: "M", Timestamp: ts,
				HasValuesArray: true,
				Values:         make([]float64, 151),
				Counts:         make([]float64, 151),
			},
			wantErr: cloudwatch.ErrTooManyValues,
		},
		{
			name: "NaN entry rejected",
			datum: cloudwatch.MetricDatum{
				MetricName: "M", Timestamp: ts,
				HasValuesArray: true,
				Values:         []float64{1, math.NaN()},
				Counts:         []float64{1, 1},
			},
			wantErr: cloudwatch.ErrInvalidMetricValue,
		},
		{
			name: "Values array combined with a plain Value is rejected",
			datum: cloudwatch.MetricDatum{
				MetricName: "M", Timestamp: ts,
				HasValuesArray: true,
				Values:         []float64{1},
				Counts:         []float64{1},
				HasValue:       true,
				Value:          5,
			},
			wantErr: cloudwatch.ErrValueAndStatisticSet,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackend()
			err := b.PutMetricData("NS", []cloudwatch.MetricDatum{tc.datum})

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)

				return
			}

			require.NoError(t, err)

			if !tc.wantStored {
				return
			}

			dps, gerr := b.GetMetricStatistics("NS", "M", nil,
				ts.Add(-time.Minute), ts.Add(time.Minute), 60,
				[]string{"Sum", "SampleCount", "Minimum", "Maximum"}, nil)
			require.NoError(t, gerr)
			require.Len(t, dps, 1)
			require.NotNil(t, dps[0].Sum)
			assert.InDelta(t, tc.wantSum, *dps[0].Sum, 1e-9, "Sum")
			require.NotNil(t, dps[0].SampleCount)
			assert.InDelta(t, tc.wantCount, *dps[0].SampleCount, 1e-9, "SampleCount")
			require.NotNil(t, dps[0].Minimum)
			assert.InDelta(t, tc.wantMin, *dps[0].Minimum, 1e-9, "Minimum")
			require.NotNil(t, dps[0].Maximum)
			assert.InDelta(t, tc.wantMax, *dps[0].Maximum, 1e-9, "Maximum")
		})
	}
}

func Test_ValidateMetricDatum_ValueRange(t *testing.T) {
	t.Parallel()

	cases := []struct {
		wantErr error
		name    string
		datum   cloudwatch.MetricDatum
	}{
		{
			name:  "finite value in range accepted",
			datum: cloudwatch.MetricDatum{MetricName: "M", HasValue: true, Value: 42.5, Timestamp: time.Now().UTC()},
		},
		{
			name: "NaN value rejected",
			datum: cloudwatch.MetricDatum{
				MetricName: "M", HasValue: true, Value: math.NaN(), Timestamp: time.Now().UTC(),
			},
			wantErr: cloudwatch.ErrInvalidMetricValue,
		},
		{
			name: "+Inf value rejected",
			datum: cloudwatch.MetricDatum{
				MetricName: "M", HasValue: true, Value: math.Inf(1), Timestamp: time.Now().UTC(),
			},
			wantErr: cloudwatch.ErrInvalidMetricValue,
		},
		{
			name: "-Inf value rejected",
			datum: cloudwatch.MetricDatum{
				MetricName: "M", HasValue: true, Value: math.Inf(-1), Timestamp: time.Now().UTC(),
			},
			wantErr: cloudwatch.ErrInvalidMetricValue,
		},
		{
			name: "StatisticSet with NaN Sum rejected",
			datum: cloudwatch.MetricDatum{
				MetricName: "M", HasStatisticSet: true,
				Count: 1, Sum: math.NaN(), Min: 1, Max: 1,
				Timestamp: time.Now().UTC(),
			},
			wantErr: cloudwatch.ErrInvalidMetricValue,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := cloudwatch.ValidateMetricDatumForTest(tc.datum)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

// ---------------------------------------------------------------------------
// PutMetricData: unit round-trip
// ---------------------------------------------------------------------------

func TestBackend_PutMetricData_UnitStored(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "M", Unit: "Milliseconds", Value: 5, Count: 1, Sum: 5, Min: 5, Max: 5, Timestamp: ts},
	})
	require.NoError(t, err)

	dps, err := b.GetMetricStatistics("NS", "M", nil,
		ts.Add(-time.Minute), ts.Add(time.Minute), 60, []string{"Sum"}, nil)
	require.NoError(t, err)
	require.Len(t, dps, 1)
	assert.Equal(t, "Milliseconds", dps[0].Unit, "unit should round-trip through GetMetricStatistics")
}

// ---------------------------------------------------------------------------
// Metric retention sweep
// ---------------------------------------------------------------------------

func TestBackend_SweepExpiredMetrics_RemovesOldPoints(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()

	// Put a point 20 days ago (beyond retention). This predates PutMetricData's
	// write-time Timestamp acceptance window, so seed it directly via the
	// StoreDatumForTest bypass (models a point that was valid when written and
	// has since aged past retention).
	old := time.Now().UTC().AddDate(0, 0, -(cloudwatch.CwMetricRetentionDays + 1))
	b.StoreDatumForTest("NS", cloudwatch.MetricDatum{
		MetricName: "Old", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1, Timestamp: old,
	})

	// Put a recent point.
	err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "Recent", Value: 2, Count: 1, Sum: 2, Min: 2, Max: 2, Timestamp: time.Now().UTC()},
	})
	require.NoError(t, err)

	b.SweepExpiredMetrics()

	// Old metric should be gone.
	p, err := b.ListMetrics("NS", "Old", nil, "", "", 0)
	require.NoError(t, err)
	assert.Empty(t, p.Data, "expired metric should be swept")

	// Recent metric should remain.
	p, err = b.ListMetrics("NS", "Recent", nil, "", "", 0)
	require.NoError(t, err)
	assert.Len(t, p.Data, 1, "recent metric should survive sweep")
}

// ---------------------------------------------------------------------------
// PutMetricData: limits
// ---------------------------------------------------------------------------

func TestBackend_PutMetricData_ExceedsPerRequestLimit(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	data := make([]cloudwatch.MetricDatum, cloudwatch.CwMaxMetricNamesPerNamespace+1)
	for i := range data {
		data[i] = cloudwatch.MetricDatum{
			MetricName: fmt.Sprintf("M%d", i),
			Value:      1, Count: 1, Sum: 1, Min: 1, Max: 1,
			Timestamp: time.Now().UTC(),
		}
	}

	// Exceeds per-request limit (1000) only if data > 1000.
	// CwMaxMetricNamesPerNamespace is 500. Build a 1001-entry slice explicitly.
	data1001 := make([]cloudwatch.MetricDatum, 1001)
	for i := range data1001 {
		data1001[i] = cloudwatch.MetricDatum{
			MetricName: fmt.Sprintf("M%d", i),
			Value:      1, Count: 1, Sum: 1, Min: 1, Max: 1,
			Timestamp: time.Now().UTC(),
		}
	}

	err := b.PutMetricData("NS", data1001)
	assert.Error(t, err, "PutMetricData should reject > 1000 entries per request")
}

// TestSweepExpiredMetrics_TwoPhase verifies that SweepExpiredMetrics removes
// expired points and leaves live points intact, without holding the write lock
// for the duration of the filter pass.
func TestSweepExpiredMetrics_TwoPhase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		putAge    time.Duration
		wantAlive bool
	}{
		{
			name:      "fresh datapoint survives sweep",
			putAge:    0,
			wantAlive: true,
		},
		{
			name:      "old datapoint removed by sweep",
			putAge:    time.Duration(cloudwatch.CwMetricRetentionDays+1) * 24 * time.Hour,
			wantAlive: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

			ts := time.Now().UTC().Add(-tc.putAge)
			// putAge may exceed PutMetricData's write-time Timestamp acceptance
			// window (two weeks); StoreDatumForTest seeds already-aged data
			// directly, modeling a point that was valid when written and has
			// since aged past retention.
			b.StoreDatumForTest("NS/Sweep", cloudwatch.MetricDatum{
				MetricName: "M", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1, Timestamp: ts,
			})

			b.SweepExpiredMetrics()

			metrics, err := b.ListMetrics("NS/Sweep", "M", nil, "", "", 0)
			require.NoError(t, err)
			if tc.wantAlive {
				assert.Len(t, metrics.Data, 1, "live metric must survive sweep")
			} else {
				assert.Empty(t, metrics.Data, "expired metric must be removed by sweep")
			}
		})
	}
}
func TestCloudWatchBackend_PutMetricData(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	data := []cloudwatch.MetricDatum{
		{
			MetricName: "Requests",
			Value:      42,
			Count:      1,
			Sum:        42,
			Min:        42,
			Max:        42,
			Timestamp:  time.Now(),
		},
	}
	err := b.PutMetricData("AWS/EC2", data)
	require.NoError(t, err)
}

func TestCloudWatchBackend_PutMetricData_Multiple(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	data := []cloudwatch.MetricDatum{
		{MetricName: "CPU", Value: 10, Count: 1, Sum: 10, Min: 10, Max: 10, Timestamp: time.Now()},
		{MetricName: "CPU", Value: 20, Count: 1, Sum: 20, Min: 20, Max: 20, Timestamp: time.Now()},
	}
	err := b.PutMetricData("AWS/EC2", data)
	require.NoError(t, err)
	metrics, err := b.ListMetrics("AWS/EC2", "CPU", nil, "", "", 0)
	require.NoError(t, err)
	assert.Len(t, metrics.Data, 1)
}

func TestCloudWatchBackend_MetricDataCap(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	// Insert more than cwMaxMetricDataPoints data points.
	const total = 1100
	for range total {
		err := b.PutMetricData("AWS/EC2", []cloudwatch.MetricDatum{
			{
				MetricName: "CPUUtilization",
				Value:      42.0,
				Unit:       "Percent",
				Timestamp:  time.Now(),
			},
		})
		require.NoError(t, err)
	}

	// At least one metric entry should still exist after capping.
	page, err := b.ListMetrics("AWS/EC2", "CPUUtilization", nil, "", "", 0)
	require.NoError(t, err)
	assert.NotEmpty(t, page.Data)
}

func TestCloudWatchBackend_PutMetricData_NamespaceCapEnforced(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()

	// Fill the namespace to the cap by putting one data point per unique metric name.
	for i := range cloudwatch.CwMaxMetricNamesPerNamespace {
		name := fmt.Sprintf("Metric%d", i)
		datum := cloudwatch.MetricDatum{
			MetricName: name,
			Value:      1,
			Count:      1,
			Sum:        1,
			Min:        1,
			Max:        1,
			Timestamp:  time.Now(),
		}
		err := b.PutMetricData("NS/Cap", []cloudwatch.MetricDatum{datum})
		require.NoError(t, err)
	}

	// Attempt to add one more unique metric; the whole request must fail since
	// PutMetricData has no partial-success shape in real CloudWatch.
	extra := cloudwatch.MetricDatum{
		MetricName: "ExtraMetric",
		Value:      1,
		Count:      1,
		Sum:        1,
		Min:        1,
		Max:        1,
		Timestamp:  time.Now(),
	}
	err := b.PutMetricData("NS/Cap", []cloudwatch.MetricDatum{extra})
	require.ErrorIs(t, err, cloudwatch.ErrMetricSeriesLimitExceeded)

	metrics, err2 := b.ListMetrics("NS/Cap", "", nil, "", "", 0)
	require.NoError(t, err2)
	assert.LessOrEqual(t, len(metrics.Data), cloudwatch.CwMaxMetricNamesPerNamespace,
		"namespace metric count should not exceed the cap")
	assert.Len(t, metrics.Data, cloudwatch.CwMaxMetricNamesPerNamespace,
		"exactly cap metrics should be present")
}

func TestCloudWatchBackend_SweepExpiredMetrics(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()

	// Use a timestamp outside the retention window by a safe margin.
	oldTimestamp := time.Now().UTC().AddDate(0, 0, -(cloudwatch.CwMetricRetentionDays + 5))
	recentTimestamp := time.Now().UTC()

	oldDatum := cloudwatch.MetricDatum{
		MetricName: "OldMetric",
		Value:      1, Count: 1, Sum: 1, Min: 1, Max: 1,
		Timestamp: oldTimestamp,
	}
	recentDatum := cloudwatch.MetricDatum{
		MetricName: "RecentMetric",
		Value:      2, Count: 1, Sum: 2, Min: 2, Max: 2,
		Timestamp: recentTimestamp,
	}

	// oldTimestamp predates PutMetricData's write-time Timestamp acceptance
	// window, so seed it directly (models a point that was valid when written
	// and has since aged past retention); recentDatum goes through the normal
	// validated path.
	b.StoreDatumForTest("NS/Sweep", oldDatum)
	err := b.PutMetricData("NS/Sweep", []cloudwatch.MetricDatum{recentDatum})
	require.NoError(t, err)

	b.SweepExpiredMetrics()

	// OldMetric should be evicted; RecentMetric should remain.
	all, err := b.ListMetrics("NS/Sweep", "", nil, "", "", 0)
	require.NoError(t, err)

	names := make(map[string]bool, len(all.Data))
	for _, m := range all.Data {
		names[m.MetricName] = true
	}

	assert.False(t, names["OldMetric"], "expired metric should have been swept")
	assert.True(t, names["RecentMetric"], "recent metric should remain after sweep")
}

func TestCloudWatchBackend_SweepExpiredMetrics_OutOfOrder(t *testing.T) {
	t.Parallel()

	// Verify that SweepExpiredMetrics correctly handles out-of-order data points
	// (i.e. it uses a linear filter, not binary search).
	b := cloudwatch.NewInMemoryBackend()

	old := time.Now().UTC().AddDate(0, 0, -(cloudwatch.CwMetricRetentionDays + 5))
	recent := time.Now().UTC()

	// Intentionally store points out of order: recent first, then old. old
	// predates PutMetricData's write-time Timestamp acceptance window, so seed
	// it directly via StoreDatumForTest.
	err := b.PutMetricData("NS/OutOfOrder", []cloudwatch.MetricDatum{
		{MetricName: "Mixed", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1, Timestamp: recent},
	})
	require.NoError(t, err)
	b.StoreDatumForTest("NS/OutOfOrder",
		cloudwatch.MetricDatum{MetricName: "Mixed", Value: 2, Count: 1, Sum: 2, Min: 2, Max: 2, Timestamp: old})

	b.SweepExpiredMetrics()

	// The metric still has the recent point; old point should be gone.
	stats, err := b.GetMetricStatistics(
		"NS/OutOfOrder", "Mixed",
		nil,
		recent.Add(-time.Minute), recent.Add(time.Minute),
		60, []string{"Sum"}, nil,
	)
	require.NoError(t, err)
	require.Len(t, stats, 1)
	assert.NotNil(t, stats[0].Sum)
	assert.InDelta(t, 1.0, *stats[0].Sum, 1e-9, "only the recent data point should remain")
}

// TestCloudWatchBackend_PutMetricData_NamespaceCap verifies that once a
// namespace has reached its distinct-time-series cap, PutMetricData rejects a
// request introducing one more new series with the whole call failing (real
// CloudWatch has no per-datum "unprocessed" result — PutMetricDataOutput only
// carries a request ID) rather than a partial/fabricated success.
func TestCloudWatchBackend_PutMetricData_NamespaceCap(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	// Fill the namespace to the cap.
	for i := range cloudwatch.CwMaxMetricNamesPerNamespace {
		err := b.PutMetricData("NS/Full", []cloudwatch.MetricDatum{
			{
				MetricName: fmt.Sprintf("M%d", i),
				Value:      float64(i),
				Count:      1, Sum: float64(i), Min: float64(i), Max: float64(i),
				Timestamp: time.Now(),
			},
		})
		require.NoError(t, err)
	}

	// One more new metric should fail the whole request.
	err := b.PutMetricData("NS/Full", []cloudwatch.MetricDatum{
		{MetricName: "OverflowMetric", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1, Timestamp: time.Now()},
	})
	require.ErrorIs(t, err, cloudwatch.ErrMetricSeriesLimitExceeded)

	// The overflow metric must not have been stored.
	p, lerr := b.ListMetrics("NS/Full", "", nil, "", "", 0)
	require.NoError(t, lerr)
	assert.NotContains(t, metricNames(p.Data), "OverflowMetric")
}

func TestCloudWatchBackend_StorageResolution_StoredOnDatum(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC()

	err := b.PutMetricData("App", []cloudwatch.MetricDatum{
		{
			MetricName: "Ticks", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1,
			Timestamp: ts, StorageResolution: 1,
		},
	})
	require.NoError(t, err)

	// Metric should be stored and queryable.
	p, err := b.ListMetrics("App", "Ticks", nil, "", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
}
