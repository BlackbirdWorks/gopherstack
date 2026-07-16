package cloudwatch_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// ---------------------------------------------------------------------------
// Backend integration: GetMetricStatistics dimensions (gap #9)
// ---------------------------------------------------------------------------

func TestBackend_GetMetricStatistics_WithDimensions(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	dimProd := []cloudwatch.Dimension{{Name: "Env", Value: "prod"}}
	dimStaging := []cloudwatch.Dimension{{Name: "Env", Value: "staging"}}

	err := b.PutMetricData("App", []cloudwatch.MetricDatum{
		{MetricName: "RPM", Value: 100, Count: 1, Sum: 100, Min: 100, Max: 100, Timestamp: ts, Dimensions: dimProd},
		{MetricName: "RPM", Value: 200, Count: 1, Sum: 200, Min: 200, Max: 200, Timestamp: ts, Dimensions: dimStaging},
	})
	require.NoError(t, err)

	dpsProd, err := b.GetMetricStatistics(
		"App", "RPM", dimProd,
		ts.Add(-time.Minute), ts.Add(time.Minute),
		60, []string{"Sum"}, nil,
	)
	require.NoError(t, err)
	require.Len(t, dpsProd, 1)
	assert.InDelta(t, 100.0, *dpsProd[0].Sum, 1e-9, "prod query should return prod value")

	dpsStaging, err := b.GetMetricStatistics(
		"App", "RPM", dimStaging,
		ts.Add(-time.Minute), ts.Add(time.Minute),
		60, []string{"Sum"}, nil,
	)
	require.NoError(t, err)
	require.Len(t, dpsStaging, 1)
	assert.InDelta(t, 200.0, *dpsStaging[0].Sum, 1e-9, "staging query should return staging value")
}

// ---------------------------------------------------------------------------
// DimensionSetKey ordering invariant
// ---------------------------------------------------------------------------

func TestDimensionSetKey_OrderIndependent(t *testing.T) {
	t.Parallel()

	d1 := []cloudwatch.Dimension{{Name: "Z", Value: "z"}, {Name: "A", Value: "a"}}
	d2 := []cloudwatch.Dimension{{Name: "A", Value: "a"}, {Name: "Z", Value: "z"}}

	assert.Equal(t, cloudwatch.DimensionSetKeyForTest(d1), cloudwatch.DimensionSetKeyForTest(d2),
		"dimension set key should be order-independent")
}

func TestDimensionSetKey_Empty(t *testing.T) {
	t.Parallel()

	key := cloudwatch.DimensionSetKeyForTest(nil)
	assert.Empty(t, key, "empty dimension set key should be empty string")
}

func TestDimensionSetKey_SingleDimension(t *testing.T) {
	t.Parallel()

	dims := []cloudwatch.Dimension{{Name: "Host", Value: "web1"}}
	key := cloudwatch.DimensionSetKeyForTest(dims)
	assert.NotEmpty(t, key)
	assert.Contains(t, key, "Host")
	assert.Contains(t, key, "web1")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func metricNames(metrics []cloudwatch.Metric) []string {
	names := make([]string, 0, len(metrics))
	for _, m := range metrics {
		names = append(names, m.MetricName)
	}

	return names
}

// ---------------------------------------------------------------------------
// dimsContainAll: partial dimension matching (filter subset of stored)
// ---------------------------------------------------------------------------

func TestListMetrics_PartialDimensionFilter(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-time.Minute)

	// Store a metric with 2 dimensions.
	err := b.PutMetricData("App", []cloudwatch.MetricDatum{
		{
			MetricName: "CPU",
			Value:      80, Count: 1, Sum: 80, Min: 80, Max: 80,
			Timestamp:  ts,
			Dimensions: []cloudwatch.Dimension{{Name: "Env", Value: "prod"}, {Name: "Host", Value: "web1"}},
		},
	})
	require.NoError(t, err)

	// Filter by only one dimension – should still match (partial filter).
	p, err := b.ListMetrics("App", "CPU", []cloudwatch.Dimension{{Name: "Env", Value: "prod"}}, "", "", 0)
	require.NoError(t, err)
	assert.Len(t, p.Data, 1, "partial dimension filter should match metric with superset of dims")
}

func TestListMetrics_PartialDimensionFilter_NoMatch(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-time.Minute)

	err := b.PutMetricData("App", []cloudwatch.MetricDatum{
		{
			MetricName: "CPU", Value: 80, Count: 1, Sum: 80, Min: 80, Max: 80,
			Timestamp:  ts,
			Dimensions: []cloudwatch.Dimension{{Name: "Env", Value: "prod"}},
		},
	})
	require.NoError(t, err)

	// Filter by a dimension that doesn't exist on the metric.
	p, err := b.ListMetrics("App", "CPU", []cloudwatch.Dimension{{Name: "Env", Value: "staging"}}, "", "", 0)
	require.NoError(t, err)
	assert.Empty(t, p.Data, "non-matching filter should return no metrics")
}

func TestListMetrics_MultiDimFilter_AllMustMatch(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-time.Minute)

	err := b.PutMetricData("Svc", []cloudwatch.MetricDatum{
		{
			MetricName: "Req", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1,
			Timestamp:  ts,
			Dimensions: []cloudwatch.Dimension{{Name: "A", Value: "1"}, {Name: "B", Value: "2"}},
		},
		{
			MetricName: "Req", Value: 2, Count: 1, Sum: 2, Min: 2, Max: 2,
			Timestamp:  ts,
			Dimensions: []cloudwatch.Dimension{{Name: "A", Value: "1"}, {Name: "B", Value: "9"}},
		},
	})
	require.NoError(t, err)

	p, err := b.ListMetrics(
		"Svc", "Req",
		[]cloudwatch.Dimension{{Name: "A", Value: "1"}, {Name: "B", Value: "2"}},
		"", "", 0,
	)
	require.NoError(t, err)
	assert.Len(t, p.Data, 1, "all filter dims must match")
}

func TestBackend_GetMetricStatistics_ExtendedStats_Computed(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	base := cloudwatch.RecentTestAnchor()

	err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "Lat", Value: 10, Count: 1, Sum: 10, Min: 10, Max: 10, Timestamp: base.Add(10 * time.Second)},
		{MetricName: "Lat", Value: 50, Count: 1, Sum: 50, Min: 50, Max: 50, Timestamp: base.Add(20 * time.Second)},
		{MetricName: "Lat", Value: 90, Count: 1, Sum: 90, Min: 90, Max: 90, Timestamp: base.Add(30 * time.Second)},
	})
	require.NoError(t, err)

	dps, err := b.GetMetricStatistics("NS", "Lat", nil,
		base, base.Add(time.Minute), 60, nil, []string{"p99", "p50"})
	require.NoError(t, err)
	require.Len(t, dps, 1)
	assert.NotNil(t, dps[0].ExtendedStatistics)
	assert.Contains(t, dps[0].ExtendedStatistics, "p99")
	assert.Contains(t, dps[0].ExtendedStatistics, "p50")
	assert.Greater(t, dps[0].ExtendedStatistics["p99"], dps[0].ExtendedStatistics["p50"])
}

func TestBackend_GetMetricStatistics_p99_HigherThanMedian(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	base := cloudwatch.RecentTestAnchor()

	vals := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 100}
	for i, v := range vals {
		err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
			{
				MetricName: "M",
				Value:      v,
				Count:      1,
				Sum:        v,
				Min:        v,
				Max:        v,
				Timestamp:  base.Add(time.Duration(i) * time.Second),
			},
		})
		require.NoError(t, err)
	}

	dps, err := b.GetMetricStatistics("NS", "M", nil,
		base, base.Add(time.Minute), 60, nil, []string{"p99", "p50"})
	require.NoError(t, err)
	require.NotEmpty(t, dps)
	require.NotNil(t, dps[0].ExtendedStatistics)
	p99 := dps[0].ExtendedStatistics["p99"]
	p50 := dps[0].ExtendedStatistics["p50"]
	assert.Greater(t, p99, p50, "p99 must be > p50 for skewed data")
	assert.Greater(t, p99, 50.0, "p99 should be near 100 for this dataset")
}

// ---------------------------------------------------------------------------
// Pagination: ListMetrics
// ---------------------------------------------------------------------------

func TestBackend_ListMetrics_Pagination(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-time.Minute)

	for i := range 10 {
		err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
			{
				MetricName: fmt.Sprintf("M%02d", i), Value: float64(i), Count: 1,
				Sum: float64(i), Min: float64(i), Max: float64(i), Timestamp: ts,
			},
		})
		require.NoError(t, err)
	}

	// First page of 4.
	p1, err := b.ListMetrics("NS", "", nil, "", "", 4)
	require.NoError(t, err)
	assert.Len(t, p1.Data, 4)
	assert.NotEmpty(t, p1.Next)

	// Second page.
	p2, err := b.ListMetrics("NS", "", nil, "", p1.Next, 4)
	require.NoError(t, err)
	assert.Len(t, p2.Data, 4)

	// Ensure no overlap.
	names1 := make(map[string]bool)
	for _, m := range p1.Data {
		names1[m.MetricName] = true
	}
	for _, m := range p2.Data {
		assert.False(t, names1[m.MetricName], "pages must not overlap")
	}
}

// ---------------------------------------------------------------------------
// GetMetricStatistics: period boundary alignment
// ---------------------------------------------------------------------------

func TestBackend_GetMetricStatistics_PeriodBuckets(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	base := cloudwatch.RecentTestAnchor()

	// Three points in different 60-second buckets.
	for i, v := range []float64{10, 20, 30} {
		err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
			{
				MetricName: "M",
				Value:      v, Count: 1, Sum: v, Min: v, Max: v,
				Timestamp: base.Add(time.Duration(i) * time.Minute),
			},
		})
		require.NoError(t, err)
	}

	dps, err := b.GetMetricStatistics("NS", "M", nil,
		base, base.Add(5*time.Minute), 60, []string{"Sum"}, nil)
	require.NoError(t, err)
	assert.Len(t, dps, 3, "three points in separate buckets should yield three datapoints")
}

func TestBackend_GetMetricStatistics_AggregatesWithinPeriod(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	base := cloudwatch.RecentTestAnchor()

	// Two points within the same 60-second bucket.
	err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "M", Value: 10, Count: 1, Sum: 10, Min: 10, Max: 10, Timestamp: base.Add(5 * time.Second)},
		{MetricName: "M", Value: 30, Count: 1, Sum: 30, Min: 30, Max: 30, Timestamp: base.Add(10 * time.Second)},
	})
	require.NoError(t, err)

	dps, err := b.GetMetricStatistics("NS", "M", nil,
		base, base.Add(time.Minute), 60, []string{"Sum", "SampleCount", "Minimum", "Maximum", "Average"}, nil)
	require.NoError(t, err)
	require.Len(t, dps, 1, "same-bucket points should be aggregated")

	dp := dps[0]
	require.NotNil(t, dp.Sum)
	assert.InDelta(t, 40.0, *dp.Sum, 1e-9)
	require.NotNil(t, dp.SampleCount)
	assert.InDelta(t, 2.0, *dp.SampleCount, 1e-9)
	require.NotNil(t, dp.Average)
	assert.InDelta(t, 20.0, *dp.Average, 1e-9)
	require.NotNil(t, dp.Minimum)
	assert.InDelta(t, 10.0, *dp.Minimum, 1e-9)
	require.NotNil(t, dp.Maximum)
	assert.InDelta(t, 30.0, *dp.Maximum, 1e-9)
}

// ---------------------------------------------------------------------------
// ListMetrics — name-only dimension filter (empty Value = match any)
// ---------------------------------------------------------------------------

func TestListMetrics_NameOnlyDimensionFilter(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	now := time.Now().UTC()

	for _, datum := range []cloudwatch.MetricDatum{
		{Namespace: "SvcNS", MetricName: "Req", Timestamp: now.Add(-10 * time.Second), Value: 1,
			Dimensions: []cloudwatch.Dimension{{Name: "Host", Value: "alpha"}}},
		{Namespace: "SvcNS", MetricName: "Req", Timestamp: now.Add(-10 * time.Second), Value: 1,
			Dimensions: []cloudwatch.Dimension{{Name: "Host", Value: "beta"}}},
		{Namespace: "SvcNS", MetricName: "Req", Timestamp: now.Add(-10 * time.Second), Value: 1,
			Dimensions: []cloudwatch.Dimension{{Name: "Region", Value: "us-east-1"}}},
	} {
		putMetric(t, b, datum)
	}

	tests := []struct {
		name      string
		dimFilter []cloudwatch.Dimension
		wantLen   int
	}{
		{
			name:      "name_only_filter_matches_all_host_values",
			dimFilter: []cloudwatch.Dimension{{Name: "Host", Value: ""}},
			wantLen:   2,
		},
		{
			name:      "name_value_filter_matches_exact",
			dimFilter: []cloudwatch.Dimension{{Name: "Host", Value: "alpha"}},
			wantLen:   1,
		},
		{
			name:      "name_filter_non_matching_dim_returns_empty",
			dimFilter: []cloudwatch.Dimension{{Name: "AZ", Value: ""}},
			wantLen:   0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			page, err := b.ListMetrics("SvcNS", "Req", tc.dimFilter, "", "", 0)
			require.NoError(t, err)
			assert.Len(t, page.Data, tc.wantLen, "filter=%v", tc.dimFilter)
		})
	}
}

// TestStatValue exercises all branches of the statValue function via GetMetricData.
// statValue is only called from GetMetricData in the backend.
func TestStatValue_AllBranches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		stat     string
		wantCode int
	}{
		{name: "Sum", stat: "Sum", wantCode: http.StatusOK},
		{name: "Average", stat: "Average", wantCode: http.StatusOK},
		{name: "Minimum", stat: "Minimum", wantCode: http.StatusOK},
		{name: "Min", stat: "Min", wantCode: http.StatusOK},
		{name: "Maximum", stat: "Maximum", wantCode: http.StatusOK},
		{name: "Max", stat: "Max", wantCode: http.StatusOK},
		{name: "SampleCount", stat: "SampleCount", wantCode: http.StatusOK},
		{name: "Unknown_default", stat: "UnknownStat", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := cwServer(t)

			cwPost(
				t, ts,
				"Action=PutMetricData&Namespace=StatTest"+
					"&MetricData.member.1.MetricName=Hits"+
					"&MetricData.member.1.Value=10",
			).Body.Close()

			statBody := "Action=GetMetricData" +
				"&StartTime=2000-01-01T00:00:00Z" +
				"&EndTime=2099-01-01T00:00:00Z" +
				"&MetricDataQueries.member.1.Id=q1" +
				"&MetricDataQueries.member.1.MetricStat.Metric.Namespace=StatTest" +
				"&MetricDataQueries.member.1.MetricStat.Metric.MetricName=Hits" +
				"&MetricDataQueries.member.1.MetricStat.Stat=" + tt.stat +
				"&MetricDataQueries.member.1.MetricStat.Period=60"

			resp := cwPost(t, ts, statBody)
			defer resp.Body.Close()
			assert.Equal(t, tt.wantCode, resp.StatusCode)
		})
	}
}

// TestStatValue_NilPointers tests statValue when datapoint fields are nil (nil pointer branches).
func TestStatValue_NilPointers(t *testing.T) {
	t.Parallel()

	// Use GetMetricData for a namespace/metric with NO data at all.
	// statValue is called with a datapoint that has only the requested stat set.
	// The *other* stats' pointers are nil, which hits the nil-pointer early-exits.
	tests := []struct {
		name     string
		stat     string
		wantCode int
	}{
		{name: "Sum_nil_pointer_path", stat: "Sum", wantCode: http.StatusOK},
		{name: "Average_nil_pointer_path", stat: "Average", wantCode: http.StatusOK},
		{name: "Minimum_nil_pointer_path", stat: "Minimum", wantCode: http.StatusOK},
		{name: "Maximum_nil_pointer_path", stat: "Maximum", wantCode: http.StatusOK},
		{name: "SampleCount_nil_pointer_path", stat: "SampleCount", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := cwServer(t)

			cwPost(
				t, ts,
				"Action=PutMetricData&Namespace=NilTest"+
					"&MetricData.member.1.MetricName=NilMetric"+
					"&MetricData.member.1.Value=5",
			).Body.Close()

			// Use a DIFFERENT stat in the query than the one put (so the pointer will be nil).
			// For example, put with Sum built, then request Average stat which won't be in the dp.
			statBody := "Action=GetMetricData" +
				"&StartTime=2000-01-01T00:00:00Z" +
				"&EndTime=2099-01-01T00:00:00Z" +
				"&MetricDataQueries.member.1.Id=q1" +
				"&MetricDataQueries.member.1.MetricStat.Metric.Namespace=NilTest" +
				"&MetricDataQueries.member.1.MetricStat.Metric.MetricName=NilMetric" +
				"&MetricDataQueries.member.1.MetricStat.Stat=" + tt.stat +
				"&MetricDataQueries.member.1.MetricStat.Period=60"

			resp := cwPost(t, ts, statBody)
			defer resp.Body.Close()
			assert.Equal(t, tt.wantCode, resp.StatusCode)
		})
	}
}

func TestCloudWatchBackend_ListMetrics(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	_ = b.PutMetricData("NS1", []cloudwatch.MetricDatum{
		{MetricName: "M1", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1, Timestamp: time.Now()},
	})
	_ = b.PutMetricData("NS2", []cloudwatch.MetricDatum{
		{MetricName: "M2", Value: 2, Count: 1, Sum: 2, Min: 2, Max: 2, Timestamp: time.Now()},
	})

	all, err := b.ListMetrics("", "", nil, "", "", 0)
	require.NoError(t, err)
	assert.Len(t, all.Data, 2)

	ns1, err := b.ListMetrics("NS1", "", nil, "", "", 0)
	require.NoError(t, err)
	assert.Len(t, ns1.Data, 1)
	assert.Equal(t, "M1", ns1.Data[0].MetricName)

	byName, err := b.ListMetrics("", "M2", nil, "", "", 0)
	require.NoError(t, err)
	assert.Len(t, byName.Data, 1)
}

func TestCloudWatchBackend_GetMetricStatistics(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Minute)

	tests := []struct {
		start           time.Time
		end             time.Time
		wantAverage     *float64
		setup           func(t *testing.T, b *cloudwatch.InMemoryBackend)
		wantSampleCount *float64
		wantMaximum     *float64
		wantMinimum     *float64
		wantSum         *float64
		metricName      string
		name            string
		namespace       string
		statistics      []string
		period          int32
		wantEmpty       bool
	}{
		{
			name: "average",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				data := []cloudwatch.MetricDatum{
					{
						MetricName: "CPU",
						Value:      10,
						Count:      1,
						Sum:        10,
						Min:        10,
						Max:        10,
						Timestamp:  now,
					},
					{
						MetricName: "CPU",
						Value:      20,
						Count:      1,
						Sum:        20,
						Min:        20,
						Max:        20,
						Timestamp:  now.Add(5 * time.Second),
					},
				}
				err := b.PutMetricData("AWS/EC2", data)
				require.NoError(t, err)
			},
			namespace:       "AWS/EC2",
			metricName:      "CPU",
			start:           now.Add(-time.Second),
			end:             now.Add(time.Minute),
			period:          60,
			statistics:      []string{"Average", "Sum", "Minimum", "Maximum", "SampleCount"},
			wantEmpty:       false,
			wantAverage:     new(15.0),
			wantSum:         new(30.0),
			wantMinimum:     new(10.0),
			wantMaximum:     new(20.0),
			wantSampleCount: new(2.0),
		},
		{
			name: "outside_range",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				old := time.Now().Add(-24 * time.Hour)
				data := []cloudwatch.MetricDatum{
					{
						MetricName: "CPU",
						Value:      10,
						Count:      1,
						Sum:        10,
						Min:        10,
						Max:        10,
						Timestamp:  old,
					},
				}
				err := b.PutMetricData("AWS/EC2", data)
				require.NoError(t, err)
			},
			namespace:  "AWS/EC2",
			metricName: "CPU",
			start:      time.Now().Add(-time.Hour),
			end:        time.Now(),
			period:     60,
			statistics: []string{"Sum"},
			wantEmpty:  true,
		},
		{
			name:       "no_data",
			namespace:  "NS",
			metricName: "Missing",
			start:      time.Now().Add(-time.Hour),
			end:        time.Now(),
			period:     60,
			statistics: []string{"Average"},
			wantEmpty:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			dps, err := b.GetMetricStatistics(
				tt.namespace,
				tt.metricName,
				nil,
				tt.start,
				tt.end,
				tt.period,
				tt.statistics,
				nil,
			)
			require.NoError(t, err)

			if tt.wantEmpty {
				assert.Empty(t, dps)

				return
			}

			require.NotEmpty(t, dps)

			if tt.wantAverage != nil {
				assert.NotNil(t, dps[0].Average)
				assert.InDelta(t, *tt.wantAverage, *dps[0].Average, 0.01)
			}

			if tt.wantSum != nil {
				assert.NotNil(t, dps[0].Sum)
				assert.InDelta(t, *tt.wantSum, *dps[0].Sum, 0.01)
			}

			if tt.wantMinimum != nil {
				assert.NotNil(t, dps[0].Minimum)
				assert.InDelta(t, *tt.wantMinimum, *dps[0].Minimum, 0.01)
			}

			if tt.wantMaximum != nil {
				assert.NotNil(t, dps[0].Maximum)
				assert.InDelta(t, *tt.wantMaximum, *dps[0].Maximum, 0.01)
			}

			if tt.wantSampleCount != nil {
				assert.NotNil(t, dps[0].SampleCount)
				assert.InDelta(t, *tt.wantSampleCount, *dps[0].SampleCount, 0.01)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Accuracy audit tests — gaps from issue #1686
// ---------------------------------------------------------------------------

func TestCloudWatchBackend_DimensionAwareStorage(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	now := time.Now().UTC()

	dims1 := []cloudwatch.Dimension{{Name: "Host", Value: "host-1"}}
	dims2 := []cloudwatch.Dimension{{Name: "Host", Value: "host-2"}}

	err := b.PutMetricData("AWS/EC2", []cloudwatch.MetricDatum{
		{
			MetricName: "CPUUtilization", Value: 10, Count: 1, Sum: 10, Min: 10, Max: 10,
			Timestamp: now, Dimensions: dims1,
		},
		{
			MetricName: "CPUUtilization", Value: 90, Count: 1, Sum: 90, Min: 90, Max: 90,
			Timestamp: now, Dimensions: dims2,
		},
	})
	require.NoError(t, err)

	// ListMetrics should return two separate series, not one.
	all, err := b.ListMetrics("AWS/EC2", "CPUUtilization", nil, "", "", 0)
	require.NoError(t, err)
	assert.Len(t, all.Data, 2, "each dimension set is a distinct metric series")

	// Filter by dims1 should return exactly one series.
	filtered, err := b.ListMetrics("AWS/EC2", "CPUUtilization", dims1, "", "", 0)
	require.NoError(t, err)
	require.Len(t, filtered.Data, 1)
	assert.Equal(t, "host-1", filtered.Data[0].Dimensions[0].Value)
}

func TestCloudWatchBackend_DimensionAwareGetMetricStatistics(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	start := time.Now().UTC().Add(-2 * time.Minute)
	mid := time.Now().UTC().Add(-time.Minute)

	dimsA := []cloudwatch.Dimension{{Name: "Service", Value: "A"}}
	dimsB := []cloudwatch.Dimension{{Name: "Service", Value: "B"}}

	err := b.PutMetricData("App/Metrics", []cloudwatch.MetricDatum{
		{MetricName: "Latency", Value: 100, Count: 1, Sum: 100, Min: 100, Max: 100, Timestamp: mid, Dimensions: dimsA},
		{MetricName: "Latency", Value: 200, Count: 1, Sum: 200, Min: 200, Max: 200, Timestamp: mid, Dimensions: dimsB},
	})
	require.NoError(t, err)

	dpA, err := b.GetMetricStatistics(
		"App/Metrics", "Latency", dimsA, start, time.Now().UTC(), 60, []string{"Average"}, nil,
	)
	require.NoError(t, err)
	require.Len(t, dpA, 1)
	assert.InDelta(t, 100.0, *dpA[0].Average, 1e-9, "should return series A data only")

	dpB, err := b.GetMetricStatistics(
		"App/Metrics", "Latency", dimsB, start, time.Now().UTC(), 60, []string{"Average"}, nil,
	)
	require.NoError(t, err)
	require.Len(t, dpB, 1)
	assert.InDelta(t, 200.0, *dpB[0].Average, 1e-9, "should return series B data only")
}

func TestCloudWatchBackend_StatisticSet(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	start := time.Now().UTC().Add(-2 * time.Minute)
	ts := time.Now().UTC().Add(-time.Minute)

	// Pre-aggregated StatisticSet datum.
	err := b.PutMetricData("App", []cloudwatch.MetricDatum{
		{
			MetricName: "RequestCount",
			Timestamp:  ts,
			Count:      10,
			Sum:        250,
			Min:        20,
			Max:        35,
		},
	})
	require.NoError(t, err)

	dps, err := b.GetMetricStatistics(
		"App", "RequestCount", nil,
		start, time.Now().UTC(), 60,
		[]string{"Sum", "SampleCount", "Minimum", "Maximum"}, nil,
	)
	require.NoError(t, err)
	require.Len(t, dps, 1)
	assert.InDelta(t, 250.0, *dps[0].Sum, 1e-9)
	assert.InDelta(t, 10.0, *dps[0].SampleCount, 1e-9)
	assert.InDelta(t, 20.0, *dps[0].Minimum, 1e-9)
	assert.InDelta(t, 35.0, *dps[0].Maximum, 1e-9)
}

func TestCloudWatchBackend_DimensionlessVsDimensioned(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	err := b.PutMetricData("MyNS", []cloudwatch.MetricDatum{
		{MetricName: "M", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1, Timestamp: ts},
		{
			MetricName: "M", Value: 2, Count: 1, Sum: 2, Min: 2, Max: 2, Timestamp: ts,
			Dimensions: []cloudwatch.Dimension{{Name: "D", Value: "v"}},
		},
	})
	require.NoError(t, err)

	// No-dim query should return only the dimensionless series.
	all, err := b.ListMetrics("MyNS", "M", nil, "", "", 0)
	require.NoError(t, err)
	assert.Len(t, all.Data, 2, "dimensionless and dimensioned are separate series")

	noDim, err := b.ListMetrics("MyNS", "M", []cloudwatch.Dimension{}, "", "", 0)
	require.NoError(t, err)
	assert.Len(t, noDim.Data, 2, "empty dimension filter matches all")
}

func TestCloudWatchBackend_ListMetrics_DimensionFilter(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC()

	dimsA := []cloudwatch.Dimension{{Name: "Env", Value: "prod"}}
	dimsB := []cloudwatch.Dimension{{Name: "Env", Value: "staging"}}

	err := b.PutMetricData("Custom", []cloudwatch.MetricDatum{
		{MetricName: "RPM", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1, Timestamp: ts, Dimensions: dimsA},
		{MetricName: "RPM", Value: 2, Count: 1, Sum: 2, Min: 2, Max: 2, Timestamp: ts, Dimensions: dimsB},
		{MetricName: "RPM", Value: 3, Count: 1, Sum: 3, Min: 3, Max: 3, Timestamp: ts},
	})
	require.NoError(t, err)

	// Filter to prod only.
	prod, err := b.ListMetrics("Custom", "RPM", dimsA, "", "", 0)
	require.NoError(t, err)
	require.Len(t, prod.Data, 1)
	assert.Equal(t, "prod", prod.Data[0].Dimensions[0].Value)
}

// Test_ListMetrics_RecentlyActive verifies the RecentlyActive=PT3H filter:
// AWS's only documented value, restricting results to metrics with a
// datapoint in the last 3 hours. Previously this parameter was not parsed or
// forwarded at all (silently ignored, never even reaching the backend), so a
// caller filtering for recently-active metrics got back everything instead.
func Test_ListMetrics_RecentlyActive(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name           string
		recentlyActive string
		wantNames      []string
		wantErr        bool
	}{
		{
			name:           "no filter returns everything",
			recentlyActive: "",
			wantNames:      []string{"Fresh", "Stale"},
		},
		{
			name:           "PT3H returns only recently-active metrics",
			recentlyActive: "PT3H",
			wantNames:      []string{"Fresh"},
		},
		{
			name:           "invalid value rejected",
			recentlyActive: "PT1H",
			wantErr:        true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackend()
			now := time.Now().UTC()

			err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
				{MetricName: "Fresh", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1, Timestamp: now.Add(-time.Minute)},
				{MetricName: "Stale", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1, Timestamp: now.Add(-4 * time.Hour)},
			})
			require.NoError(t, err)

			p, lerr := b.ListMetrics("NS", "", nil, tc.recentlyActive, "", 0)
			if tc.wantErr {
				require.ErrorIs(t, lerr, cloudwatch.ErrValidation)

				return
			}

			require.NoError(t, lerr)
			assert.ElementsMatch(t, tc.wantNames, metricNames(p.Data))
		})
	}
}

func TestCloudWatchBackend_ListMetrics_ReturnsDimensions(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	dims := []cloudwatch.Dimension{
		{Name: "Region", Value: "us-east-1"},
		{Name: "Service", Value: "web"},
	}

	err := b.PutMetricData("Infra", []cloudwatch.MetricDatum{
		{
			MetricName: "Errors", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1,
			Timestamp: time.Now(), Dimensions: dims,
		},
	})
	require.NoError(t, err)

	p, err := b.ListMetrics("Infra", "Errors", nil, "", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Len(t, p.Data[0].Dimensions, 2, "dimensions should be returned in ListMetrics")
}

func TestCloudWatchBackend_DimensionOrderNormalized(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	// Store with dims in one order.
	dims1 := []cloudwatch.Dimension{{Name: "B", Value: "2"}, {Name: "A", Value: "1"}}
	// Query with dims in different order.
	dims2 := []cloudwatch.Dimension{{Name: "A", Value: "1"}, {Name: "B", Value: "2"}}

	err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{
			MetricName: "M", Value: 42, Count: 1, Sum: 42, Min: 42, Max: 42,
			Timestamp: ts, Dimensions: dims1,
		},
	})
	require.NoError(t, err)

	// Should find with reordered dims.
	dps, err := b.GetMetricStatistics("NS", "M", dims2,
		time.Now().UTC().Add(-time.Minute), time.Now().UTC(),
		60, []string{"Sum"}, nil)
	require.NoError(t, err)
	require.Len(t, dps, 1)
	assert.InDelta(t, 42.0, *dps[0].Sum, 1e-9, "dimension order should be normalized")
}
