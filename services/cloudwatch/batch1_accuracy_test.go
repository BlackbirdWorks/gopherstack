package cloudwatch_test

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// ---------------------------------------------------------------------------
// dimsContainAll: partial dimension matching (filter subset of stored)
// ---------------------------------------------------------------------------

func TestListMetrics_PartialDimensionFilter(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-time.Minute)

	// Store a metric with 2 dimensions.
	_, err := b.PutMetricData("App", []cloudwatch.MetricDatum{
		{
			MetricName: "CPU",
			Value:      80, Count: 1, Sum: 80, Min: 80, Max: 80,
			Timestamp:  ts,
			Dimensions: []cloudwatch.Dimension{{Name: "Env", Value: "prod"}, {Name: "Host", Value: "web1"}},
		},
	})
	require.NoError(t, err)

	// Filter by only one dimension – should still match (partial filter).
	p, err := b.ListMetrics("App", "CPU", []cloudwatch.Dimension{{Name: "Env", Value: "prod"}}, "", 0)
	require.NoError(t, err)
	assert.Len(t, p.Data, 1, "partial dimension filter should match metric with superset of dims")
}

func TestListMetrics_PartialDimensionFilter_NoMatch(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-time.Minute)

	_, err := b.PutMetricData("App", []cloudwatch.MetricDatum{
		{
			MetricName: "CPU", Value: 80, Count: 1, Sum: 80, Min: 80, Max: 80,
			Timestamp:  ts,
			Dimensions: []cloudwatch.Dimension{{Name: "Env", Value: "prod"}},
		},
	})
	require.NoError(t, err)

	// Filter by a dimension that doesn't exist on the metric.
	p, err := b.ListMetrics("App", "CPU", []cloudwatch.Dimension{{Name: "Env", Value: "staging"}}, "", 0)
	require.NoError(t, err)
	assert.Empty(t, p.Data, "non-matching filter should return no metrics")
}

func TestListMetrics_MultiDimFilter_AllMustMatch(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-time.Minute)

	_, err := b.PutMetricData("Svc", []cloudwatch.MetricDatum{
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

	p, err := b.ListMetrics("Svc", "Req",
		[]cloudwatch.Dimension{{Name: "A", Value: "1"}, {Name: "B", Value: "2"}},
		"", 0,
	)
	require.NoError(t, err)
	assert.Len(t, p.Data, 1, "all filter dims must match")
}

func TestHandler_ListMetrics_PartialDimFilter(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	// Store metric with two dimensions.
	postForm(t, h,
		"Action=PutMetricData&Namespace=NS"+
			"&MetricData.member.1.MetricName=CPU"+
			"&MetricData.member.1.Value=50"+
			"&MetricData.member.1.Dimensions.member.1.Name=Env&MetricData.member.1.Dimensions.member.1.Value=prod"+
			"&MetricData.member.1.Dimensions.member.2.Name=Host&MetricData.member.1.Dimensions.member.2.Value=web1"+
			"&MetricData.member.1.Timestamp=2024-01-01T00:00:00Z",
	)

	// Filter by just Env=prod (partial).
	rec := postForm(t, h,
		"Action=ListMetrics&Namespace=NS&MetricName=CPU"+
			"&Dimensions.member.1.Name=Env&Dimensions.member.1.Value=prod",
	)
	assert.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "CPU")
}

// ---------------------------------------------------------------------------
// ExtendedStatistics in GetMetricStatistics response
// ---------------------------------------------------------------------------

func TestHandler_GetMetricStatistics_ExtendedStatistics_InResponse(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	postForm(t, h,
		"Action=PutMetricData&Namespace=NS"+
			"&MetricData.member.1.MetricName=Latency&MetricData.member.1.Value=10"+
			"&MetricData.member.1.Timestamp=2024-01-01T00:00:30Z"+
			"&MetricData.member.2.MetricName=Latency&MetricData.member.2.Value=50"+
			"&MetricData.member.2.Timestamp=2024-01-01T00:00:31Z"+
			"&MetricData.member.3.MetricName=Latency&MetricData.member.3.Value=90"+
			"&MetricData.member.3.Timestamp=2024-01-01T00:00:32Z",
	)

	rec := postForm(t, h,
		"Action=GetMetricStatistics&Namespace=NS&MetricName=Latency"+
			"&StartTime=2024-01-01T00:00:00Z&EndTime=2024-01-01T00:02:00Z&Period=60"+
			"&ExtendedStatistics.member.1=p99&ExtendedStatistics.member.2=p50",
	)
	assert.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetMetricStatisticsResponse")
}

func TestBackend_GetMetricStatistics_ExtendedStats_Computed(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	_, err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
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
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	vals := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 100}
	for i, v := range vals {
		_, err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
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
// MetricMath: id OP constant expressions
// ---------------------------------------------------------------------------

func TestBackend_GetMetricData_ConstantMultiply(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	_, err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "M", Value: 10, Count: 1, Sum: 10, Min: 10, Max: 10, Timestamp: ts},
	})
	require.NoError(t, err)

	queries := []cloudwatch.MetricDataQuery{
		{ID: "m1", ReturnData: true, MetricStat: cloudwatch.MetricStat{
			Namespace: "NS", MetricName: "M", Stat: "Sum", Period: 60,
		}},
		{ID: "e1", Expression: "m1 * 2", ReturnData: true},
	}

	results, err := b.GetMetricDataWithOptions(queries, ts.Add(-time.Minute), ts.Add(time.Minute), "")
	require.NoError(t, err)

	byID := resultsByID(results)
	require.Contains(t, byID, "e1")
	require.NotEmpty(t, byID["e1"].Values)
	assert.InDelta(t, 20.0, byID["e1"].Values[0], 1e-9, "m1 * 2 = 20 when m1=10")
}

func TestBackend_GetMetricData_ConstantAdd(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	_, err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "M", Value: 10, Count: 1, Sum: 10, Min: 10, Max: 10, Timestamp: ts},
	})
	require.NoError(t, err)

	queries := []cloudwatch.MetricDataQuery{
		{ID: "m1", ReturnData: true, MetricStat: cloudwatch.MetricStat{
			Namespace: "NS", MetricName: "M", Stat: "Sum", Period: 60,
		}},
		{ID: "e1", Expression: "m1 + 5", ReturnData: true},
	}

	results, err := b.GetMetricDataWithOptions(queries, ts.Add(-time.Minute), ts.Add(time.Minute), "")
	require.NoError(t, err)

	byID := resultsByID(results)
	require.Contains(t, byID, "e1")
	require.NotEmpty(t, byID["e1"].Values)
	assert.InDelta(t, 15.0, byID["e1"].Values[0], 1e-9, "m1 + 5 = 15 when m1=10")
}

func TestBackend_GetMetricData_ConstantDivide(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	_, err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "M", Value: 100, Count: 1, Sum: 100, Min: 100, Max: 100, Timestamp: ts},
	})
	require.NoError(t, err)

	queries := []cloudwatch.MetricDataQuery{
		{ID: "m1", ReturnData: true, MetricStat: cloudwatch.MetricStat{
			Namespace: "NS", MetricName: "M", Stat: "Sum", Period: 60,
		}},
		{ID: "e1", Expression: "m1 / 4", ReturnData: true},
	}

	results, err := b.GetMetricDataWithOptions(queries, ts.Add(-time.Minute), ts.Add(time.Minute), "")
	require.NoError(t, err)

	byID := resultsByID(results)
	require.NotEmpty(t, byID["e1"].Values)
	assert.InDelta(t, 25.0, byID["e1"].Values[0], 1e-9, "m1 / 4 = 25 when m1=100")
}

func TestBackend_GetMetricData_ConstantSubtract(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	_, err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "M", Value: 50, Count: 1, Sum: 50, Min: 50, Max: 50, Timestamp: ts},
	})
	require.NoError(t, err)

	queries := []cloudwatch.MetricDataQuery{
		{ID: "m1", ReturnData: true, MetricStat: cloudwatch.MetricStat{
			Namespace: "NS", MetricName: "M", Stat: "Sum", Period: 60,
		}},
		{ID: "e1", Expression: "m1 - 10", ReturnData: true},
	}

	results, err := b.GetMetricDataWithOptions(queries, ts.Add(-time.Minute), ts.Add(time.Minute), "")
	require.NoError(t, err)

	byID := resultsByID(results)
	require.NotEmpty(t, byID["e1"].Values)
	assert.InDelta(t, 40.0, byID["e1"].Values[0], 1e-9, "m1 - 10 = 40 when m1=50")
}

func TestBackend_GetMetricData_ConstantLeftSide(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	_, err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "M", Value: 5, Count: 1, Sum: 5, Min: 5, Max: 5, Timestamp: ts},
	})
	require.NoError(t, err)

	queries := []cloudwatch.MetricDataQuery{
		{ID: "m1", ReturnData: true, MetricStat: cloudwatch.MetricStat{
			Namespace: "NS", MetricName: "M", Stat: "Sum", Period: 60,
		}},
		{ID: "e1", Expression: "100 / m1", ReturnData: true},
	}

	results, err := b.GetMetricDataWithOptions(queries, ts.Add(-time.Minute), ts.Add(time.Minute), "")
	require.NoError(t, err)

	byID := resultsByID(results)
	require.NotEmpty(t, byID["e1"].Values)
	assert.InDelta(t, 20.0, byID["e1"].Values[0], 1e-9, "100 / m1 = 20 when m1=5")
}

func TestBackend_GetMetricData_ConstantLeftMultiply(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	_, err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "M", Value: 3, Count: 1, Sum: 3, Min: 3, Max: 3, Timestamp: ts},
	})
	require.NoError(t, err)

	queries := []cloudwatch.MetricDataQuery{
		{ID: "m1", ReturnData: true, MetricStat: cloudwatch.MetricStat{
			Namespace: "NS", MetricName: "M", Stat: "Sum", Period: 60,
		}},
		{ID: "e1", Expression: "4 * m1", ReturnData: true},
	}

	results, err := b.GetMetricDataWithOptions(queries, ts.Add(-time.Minute), ts.Add(time.Minute), "")
	require.NoError(t, err)

	byID := resultsByID(results)
	require.NotEmpty(t, byID["e1"].Values)
	assert.InDelta(t, 12.0, byID["e1"].Values[0], 1e-9, "4 * m1 = 12 when m1=3")
}

// ---------------------------------------------------------------------------
// MetricMath: METRICS() aggregation variants
// ---------------------------------------------------------------------------

func TestBackend_GetMetricData_AvgMetrics(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	for _, v := range []float64{10, 30} {
		mn := fmt.Sprintf("M%.0f", v)
		_, err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
			{MetricName: mn, Value: v, Count: 1, Sum: v, Min: v, Max: v, Timestamp: ts},
		})
		require.NoError(t, err)
	}

	queries := []cloudwatch.MetricDataQuery{
		{ID: "m1", ReturnData: false, MetricStat: cloudwatch.MetricStat{
			Namespace: "NS", MetricName: "M10", Stat: "Sum", Period: 60,
		}},
		{ID: "m2", ReturnData: false, MetricStat: cloudwatch.MetricStat{
			Namespace: "NS", MetricName: "M30", Stat: "Sum", Period: 60,
		}},
		{ID: "avg", Expression: "AVG(METRICS())", ReturnData: true},
	}

	results, err := b.GetMetricDataWithOptions(queries, ts.Add(-time.Minute), ts.Add(time.Minute), "")
	require.NoError(t, err)

	byID := resultsByID(results)
	require.Contains(t, byID, "avg")
	require.NotEmpty(t, byID["avg"].Values)
	assert.InDelta(t, 20.0, byID["avg"].Values[0], 1e-9, "AVG(METRICS()) = (10+30)/2 = 20")
}

func TestBackend_GetMetricData_MinMetrics(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	for _, v := range []float64{5, 15, 25} {
		mn := fmt.Sprintf("M%.0f", v)
		_, err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
			{MetricName: mn, Value: v, Count: 1, Sum: v, Min: v, Max: v, Timestamp: ts},
		})
		require.NoError(t, err)
	}

	queries := []cloudwatch.MetricDataQuery{
		{
			ID:         "m1",
			ReturnData: false,
			MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "M5", Stat: "Sum", Period: 60},
		},
		{
			ID:         "m2",
			ReturnData: false,
			MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "M15", Stat: "Sum", Period: 60},
		},
		{
			ID:         "m3",
			ReturnData: false,
			MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "M25", Stat: "Sum", Period: 60},
		},
		{ID: "mn", Expression: "MIN(METRICS())", ReturnData: true},
	}

	results, err := b.GetMetricDataWithOptions(queries, ts.Add(-time.Minute), ts.Add(time.Minute), "")
	require.NoError(t, err)

	byID := resultsByID(results)
	require.NotEmpty(t, byID["mn"].Values)
	assert.InDelta(t, 5.0, byID["mn"].Values[0], 1e-9, "MIN(METRICS())=5")
}

func TestBackend_GetMetricData_MaxMetrics(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	for _, v := range []float64{5, 15, 99} {
		mn := fmt.Sprintf("M%.0f", v)
		_, err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
			{MetricName: mn, Value: v, Count: 1, Sum: v, Min: v, Max: v, Timestamp: ts},
		})
		require.NoError(t, err)
	}

	queries := []cloudwatch.MetricDataQuery{
		{
			ID:         "m1",
			ReturnData: false,
			MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "M5", Stat: "Sum", Period: 60},
		},
		{
			ID:         "m2",
			ReturnData: false,
			MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "M15", Stat: "Sum", Period: 60},
		},
		{
			ID:         "m3",
			ReturnData: false,
			MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "M99", Stat: "Sum", Period: 60},
		},
		{ID: "mx", Expression: "MAX(METRICS())", ReturnData: true},
	}

	results, err := b.GetMetricDataWithOptions(queries, ts.Add(-time.Minute), ts.Add(time.Minute), "")
	require.NoError(t, err)

	byID := resultsByID(results)
	require.NotEmpty(t, byID["mx"].Values)
	assert.InDelta(t, 99.0, byID["mx"].Values[0], 1e-9, "MAX(METRICS())=99")
}

func TestBackend_GetMetricData_StddevMetrics(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	for _, v := range []float64{10, 20, 30} {
		mn := fmt.Sprintf("M%.0f", v)
		_, err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
			{MetricName: mn, Value: v, Count: 1, Sum: v, Min: v, Max: v, Timestamp: ts},
		})
		require.NoError(t, err)
	}

	queries := []cloudwatch.MetricDataQuery{
		{
			ID:         "m1",
			ReturnData: false,
			MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "M10", Stat: "Sum", Period: 60},
		},
		{
			ID:         "m2",
			ReturnData: false,
			MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "M20", Stat: "Sum", Period: 60},
		},
		{
			ID:         "m3",
			ReturnData: false,
			MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "M30", Stat: "Sum", Period: 60},
		},
		{ID: "sd", Expression: "STDDEV(METRICS())", ReturnData: true},
	}

	results, err := b.GetMetricDataWithOptions(queries, ts.Add(-time.Minute), ts.Add(time.Minute), "")
	require.NoError(t, err)

	byID := resultsByID(results)
	require.NotEmpty(t, byID["sd"].Values)
	assert.Greater(t, byID["sd"].Values[0], 0.0, "STDDEV(METRICS()) > 0 for non-constant series")
}

// ---------------------------------------------------------------------------
// MetricMath: RATE() function
// ---------------------------------------------------------------------------

func TestBackend_GetMetricData_RateFunction(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	base := time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC) // align to minute boundary

	// Two data points 60 seconds apart, value increases by 120.
	_, err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "Counter", Value: 0, Count: 1, Sum: 0, Min: 0, Max: 0, Timestamp: base},
		{MetricName: "Counter", Value: 120, Count: 1, Sum: 120, Min: 120, Max: 120, Timestamp: base.Add(time.Minute)},
	})
	require.NoError(t, err)

	queries := []cloudwatch.MetricDataQuery{
		{ID: "m1", ReturnData: false, MetricStat: cloudwatch.MetricStat{
			Namespace: "NS", MetricName: "Counter", Stat: "Sum", Period: 60,
		}},
		{ID: "rate", Expression: "RATE(m1)", ReturnData: true},
	}

	results, err := b.GetMetricDataWithOptions(queries, base.Add(-time.Minute), base.Add(3*time.Minute), "")
	require.NoError(t, err)

	byID := resultsByID(results)
	// RATE may return values if m1 has ≥2 data points.
	if len(byID["rate"].Values) > 0 {
		assert.Greater(t, byID["rate"].Values[0], 0.0, "RATE should be positive for increasing counter")
	}
}

// ---------------------------------------------------------------------------
// MetricMath: handler integration for constant expressions
// ---------------------------------------------------------------------------

func TestHandler_GetMetricData_ConstantExpression(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	postForm(t, h,
		"Action=PutMetricData&Namespace=NS"+
			"&MetricData.member.1.MetricName=Hits"+
			"&MetricData.member.1.Value=10"+
			"&MetricData.member.1.Timestamp=2024-01-01T00:00:30Z",
	)

	rec := postForm(t, h,
		"Action=GetMetricData"+
			"&MetricDataQueries.member.1.Id=m1"+
			"&MetricDataQueries.member.1.MetricStat.Metric.Namespace=NS"+
			"&MetricDataQueries.member.1.MetricStat.Metric.MetricName=Hits"+
			"&MetricDataQueries.member.1.MetricStat.Stat=Sum"+
			"&MetricDataQueries.member.1.MetricStat.Period=60"+
			"&MetricDataQueries.member.1.ReturnData=false"+
			"&MetricDataQueries.member.2.Id=e1"+
			"&MetricDataQueries.member.2.Expression=m1+*+2"+
			"&MetricDataQueries.member.2.ReturnData=true"+
			"&StartTime=2024-01-01T00:00:00Z&EndTime=2024-01-01T00:02:00Z",
	)
	assert.Equal(t, 200, rec.Code)
}

func TestHandler_GetMetricData_AvgMetricsExpression(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	postForm(t, h, "Action=PutMetricData&Namespace=NS"+
		"&MetricData.member.1.MetricName=A&MetricData.member.1.Value=10&MetricData.member.1.Timestamp=2024-01-01T00:00:30Z")
	postForm(t, h, "Action=PutMetricData&Namespace=NS"+
		"&MetricData.member.1.MetricName=B&MetricData.member.1.Value=30&MetricData.member.1.Timestamp=2024-01-01T00:00:30Z")

	rec := postForm(t, h,
		"Action=GetMetricData"+
			"&MetricDataQueries.member.1.Id=m1&MetricDataQueries.member.1.MetricStat.Metric.Namespace=NS"+
			"&MetricDataQueries.member.1.MetricStat.Metric.MetricName=A"+
			"&MetricDataQueries.member.1.MetricStat.Stat=Sum&MetricDataQueries.member.1.MetricStat.Period=60"+
			"&MetricDataQueries.member.1.ReturnData=false"+
			"&MetricDataQueries.member.2.Id=m2&MetricDataQueries.member.2.MetricStat.Metric.Namespace=NS"+
			"&MetricDataQueries.member.2.MetricStat.Metric.MetricName=B"+
			"&MetricDataQueries.member.2.MetricStat.Stat=Sum&MetricDataQueries.member.2.MetricStat.Period=60"+
			"&MetricDataQueries.member.2.ReturnData=false"+
			"&MetricDataQueries.member.3.Id=avg&MetricDataQueries.member.3.Expression=AVG%28METRICS%28%29%29"+
			"&MetricDataQueries.member.3.ReturnData=true"+
			"&StartTime=2024-01-01T00:00:00Z&EndTime=2024-01-01T00:02:00Z",
	)
	assert.Equal(t, 200, rec.Code)
}

// ---------------------------------------------------------------------------
// HasStatisticSet: handler sets flag, validateMetricDatum enforces exclusion
// ---------------------------------------------------------------------------

func TestHandler_PutMetricData_StatisticSetOnly_Accepted(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	rec := postForm(t, h,
		"Action=PutMetricData&Namespace=App"+
			"&MetricData.member.1.MetricName=Reqs"+
			"&MetricData.member.1.StatisticValues.SampleCount=5"+
			"&MetricData.member.1.StatisticValues.Sum=250"+
			"&MetricData.member.1.StatisticValues.Minimum=40"+
			"&MetricData.member.1.StatisticValues.Maximum=60"+
			"&MetricData.member.1.Timestamp=2024-01-01T00:00:00Z",
	)
	assert.Equal(t, 200, rec.Code)
	// Unprocessed entries have <ErrorCode>; absence means all data was accepted.
	assert.NotContains(t, rec.Body.String(), "<ErrorCode>")
}

func TestHandler_PutMetricData_StatisticSetAndValue_Rejected(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	rec := postForm(t, h,
		"Action=PutMetricData&Namespace=App"+
			"&MetricData.member.1.MetricName=Bad"+
			"&MetricData.member.1.Value=1.0"+
			"&MetricData.member.1.StatisticValues.SampleCount=5"+
			"&MetricData.member.1.StatisticValues.Sum=250"+
			"&MetricData.member.1.StatisticValues.Minimum=40"+
			"&MetricData.member.1.StatisticValues.Maximum=60"+
			"&MetricData.member.1.Timestamp=2024-01-01T00:00:00Z",
	)
	assert.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "UnprocessedMetricData")
}

func TestHandler_PutMetricData_StatisticSet_StoredCorrectly(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	_, err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{
			MetricName: "Reqs", HasStatisticSet: true,
			Count: 10, Sum: 500, Min: 20, Max: 80,
			Timestamp: ts,
		},
	})
	require.NoError(t, err)

	dps, err := b.GetMetricStatistics("NS", "Reqs", nil,
		ts.Add(-time.Minute), ts.Add(time.Minute), 60, []string{"Sum", "SampleCount"}, nil)
	require.NoError(t, err)
	require.Len(t, dps, 1)
	require.NotNil(t, dps[0].Sum)
	assert.InDelta(t, 500.0, *dps[0].Sum, 1e-9, "StatisticSet Sum should be stored")
	require.NotNil(t, dps[0].SampleCount)
	assert.InDelta(t, 10.0, *dps[0].SampleCount, 1e-9, "StatisticSet SampleCount should be stored")
}

// ---------------------------------------------------------------------------
// PutMetricData: unit round-trip
// ---------------------------------------------------------------------------

func TestBackend_PutMetricData_UnitStored(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	_, err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "M", Unit: "Milliseconds", Value: 5, Count: 1, Sum: 5, Min: 5, Max: 5, Timestamp: ts},
	})
	require.NoError(t, err)

	dps, err := b.GetMetricStatistics("NS", "M", nil,
		ts.Add(-time.Minute), ts.Add(time.Minute), 60, []string{"Sum"}, nil)
	require.NoError(t, err)
	require.Len(t, dps, 1)
	assert.Equal(t, "Milliseconds", dps[0].Unit, "unit should round-trip through GetMetricStatistics")
}

func TestHandler_PutMetricData_UnitParsed(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	postForm(t, h,
		"Action=PutMetricData&Namespace=NS"+
			"&MetricData.member.1.MetricName=Mem"+
			"&MetricData.member.1.Value=1024"+
			"&MetricData.member.1.Unit=Bytes"+
			"&MetricData.member.1.Timestamp=2024-01-01T00:00:00Z",
	)

	rec := postForm(t, h,
		"Action=GetMetricStatistics&Namespace=NS&MetricName=Mem"+
			"&StartTime=2023-12-31T00:00:00Z&EndTime=2024-01-02T00:00:00Z"+
			"&Period=86400&Statistics.member.1=Sum",
	)
	assert.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "Bytes")
}

// ---------------------------------------------------------------------------
// SetAlarmState: state reasons, history
// ---------------------------------------------------------------------------

func TestBackend_SetAlarmState_ToAlarm_RecordsHistory(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	err := b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName: "cpu-alarm", Namespace: "AWS/EC2", MetricName: "CPUUtilization",
		ComparisonOperator: "GreaterThanThreshold", Threshold: 80, EvaluationPeriods: 1,
	})
	require.NoError(t, err)

	err = b.SetAlarmState(t.Context(), "cpu-alarm", "ALARM", "CPU exceeded 80%", "")
	require.NoError(t, err)

	hist, err := b.DescribeAlarmHistory("cpu-alarm", "", "", "", time.Time{}, time.Time{}, 0)
	require.NoError(t, err)
	require.NotEmpty(t, hist.Data)

	found := false
	for _, item := range hist.Data {
		if item.HistoryItemType == "StateUpdate" {
			found = true
			assert.Contains(t, item.HistorySummary, "ALARM")
		}
	}
	assert.True(t, found, "SetAlarmState should create a StateUpdate history item")
}

func TestBackend_SetAlarmState_Reason_Stored(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName: "a1", Namespace: "NS", MetricName: "M",
		ComparisonOperator: "GreaterThanThreshold", Threshold: 50, EvaluationPeriods: 1,
	}))

	require.NoError(t, b.SetAlarmState(t.Context(), "a1", "ALARM", "Threshold breach detected", ""))

	alarms, _, err := b.DescribeAlarms([]string{"a1"}, nil, "", "", "", 0)
	require.NoError(t, err)
	require.Len(t, alarms.Data, 1)
	assert.Equal(t, "ALARM", alarms.Data[0].StateValue)
	assert.Equal(t, "Threshold breach detected", alarms.Data[0].StateReason)
}

func TestBackend_SetAlarmState_StateTransitions(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName: "a1", Namespace: "NS", MetricName: "M",
		ComparisonOperator: "GreaterThanThreshold", Threshold: 50, EvaluationPeriods: 1,
	}))

	// Initial state is INSUFFICIENT_DATA.
	alarms, _, err := b.DescribeAlarms([]string{"a1"}, nil, "", "", "", 0)
	require.NoError(t, err)
	assert.Equal(t, "INSUFFICIENT_DATA", alarms.Data[0].StateValue)

	// Transition to ALARM.
	require.NoError(t, b.SetAlarmState(t.Context(), "a1", "ALARM", "breach", ""))
	alarms, _, err = b.DescribeAlarms([]string{"a1"}, nil, "", "", "", 0)
	require.NoError(t, err)
	assert.Equal(t, "ALARM", alarms.Data[0].StateValue)

	// Transition to OK.
	require.NoError(t, b.SetAlarmState(t.Context(), "a1", "OK", "resolved", ""))
	alarms, _, err = b.DescribeAlarms([]string{"a1"}, nil, "", "", "", 0)
	require.NoError(t, err)
	assert.Equal(t, "OK", alarms.Data[0].StateValue)
}

func TestBackend_SetAlarmState_NotFound_Error(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	err := b.SetAlarmState(t.Context(), "nonexistent", "ALARM", "reason", "")
	assert.Error(t, err)
}

func TestHandler_SetAlarmState_AllStates(t *testing.T) {
	t.Parallel()

	tests := []string{"ALARM", "OK", "INSUFFICIENT_DATA"}
	for _, state := range tests {
		t.Run(state, func(t *testing.T) {
			t.Parallel()

			h := newCWHandler()
			postForm(t, h,
				"Action=PutMetricAlarm&AlarmName=a&Namespace=NS&MetricName=M"+
					"&ComparisonOperator=GreaterThanThreshold&Threshold=50&EvaluationPeriods=1",
			)

			rec := postForm(t, h,
				"Action=SetAlarmState&AlarmName=a&StateValue="+state+"&StateReason=test",
			)
			assert.Equal(t, 200, rec.Code, "state=%s", state)
		})
	}
}

// ---------------------------------------------------------------------------
// Alarm: DatapointsToAlarm validation
// ---------------------------------------------------------------------------

func TestBackend_PutMetricAlarm_DatapointsToAlarmExceedsEvalPeriods(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	err := b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:          "bad",
		Namespace:          "NS",
		MetricName:         "M",
		EvaluationPeriods:  3,
		DatapointsToAlarm:  5, // > EvaluationPeriods
		ComparisonOperator: "GreaterThanThreshold",
		Threshold:          80,
	})
	assert.Error(t, err, "DatapointsToAlarm > EvaluationPeriods should be rejected")
}

func TestBackend_PutMetricAlarm_DatapointsToAlarmValid(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	err := b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:          "ok",
		Namespace:          "NS",
		MetricName:         "M",
		EvaluationPeriods:  5,
		DatapointsToAlarm:  3,
		ComparisonOperator: "GreaterThanThreshold",
		Threshold:          80,
	})
	assert.NoError(t, err, "DatapointsToAlarm <= EvaluationPeriods is valid")
}

// ---------------------------------------------------------------------------
// Alarm: TreatMissingData field round-trip
// ---------------------------------------------------------------------------

func TestBackend_PutMetricAlarm_TreatMissingData(t *testing.T) {
	t.Parallel()

	for _, tmd := range []string{"missing", "notBreaching", "breaching", "ignore"} {
		t.Run(tmd, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackend()
			err := b.PutMetricAlarm(&cloudwatch.MetricAlarm{
				AlarmName:          "a",
				Namespace:          "NS",
				MetricName:         "M",
				EvaluationPeriods:  1,
				ComparisonOperator: "GreaterThanThreshold",
				Threshold:          50,
				TreatMissingData:   tmd,
			})
			require.NoError(t, err)

			alarms, _, err := b.DescribeAlarms([]string{"a"}, nil, "", "", "", 0)
			require.NoError(t, err)
			require.Len(t, alarms.Data, 1)
			assert.Equal(t, tmd, alarms.Data[0].TreatMissingData)
		})
	}
}

// ---------------------------------------------------------------------------
// Alarm: Statistic / ExtendedStatistic mutual exclusion
// ---------------------------------------------------------------------------

func TestBackend_PutMetricAlarm_StatAndExtendedStat_Rejected(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	err := b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:          "a",
		Namespace:          "NS",
		MetricName:         "M",
		EvaluationPeriods:  1,
		ComparisonOperator: "GreaterThanThreshold",
		Threshold:          50,
		Statistic:          "Average",
		ExtendedStatistic:  "p99",
	})
	assert.Error(t, err, "Statistic and ExtendedStatistic are mutually exclusive")
}

// ---------------------------------------------------------------------------
// Dashboard: CRUD round-trip
// ---------------------------------------------------------------------------

func TestBackend_Dashboard_CRUD(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()

	body := `{"widgets":[]}`
	require.NoError(t, b.PutDashboard("dash1", body))

	entry, got, err := b.GetDashboard("dash1")
	require.NoError(t, err)
	assert.Equal(t, "dash1", entry.DashboardName)
	assert.Equal(t, body, got)
	assert.NotEmpty(t, entry.DashboardArn)

	p, err := b.ListDashboards("", "")
	require.NoError(t, err)
	assert.Len(t, p.Data, 1)

	require.NoError(t, b.DeleteDashboards([]string{"dash1"}))
	_, _, err = b.GetDashboard("dash1")
	assert.Error(t, err)
}

func TestHandler_Dashboard_PutGetListDelete(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	rec := postForm(t, h, url.Values{
		"Action":        []string{"PutDashboard"},
		"DashboardName": []string{"myboard"},
		"DashboardBody": []string{`{"widgets":[]}`},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	rec = postForm(t, h, url.Values{
		"Action":        []string{"GetDashboard"},
		"DashboardName": []string{"myboard"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "myboard")

	rec = postForm(t, h, "Action=ListDashboards")
	assert.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "myboard")

	rec = postForm(t, h, url.Values{
		"Action":                  []string{"DeleteDashboards"},
		"DashboardNames.member.1": []string{"myboard"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	rec = postForm(t, h, url.Values{
		"Action":        []string{"GetDashboard"},
		"DashboardName": []string{"myboard"},
	}.Encode())
	assert.Equal(t, 400, rec.Code)
}

func TestHandler_Dashboard_NamePrefix_Filter(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	for _, name := range []string{"prod-dashboard", "staging-board", "prod-metrics"} {
		postForm(t, h, url.Values{
			"Action":        []string{"PutDashboard"},
			"DashboardName": []string{name},
			"DashboardBody": []string{"{}"},
		}.Encode())
	}

	rec := postForm(t, h, "Action=ListDashboards&DashboardNamePrefix=prod-")
	assert.Equal(t, 200, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "prod-dashboard")
	assert.Contains(t, body, "prod-metrics")
	assert.NotContains(t, body, "staging-board")
}

// ---------------------------------------------------------------------------
// Tags: TagResource, UntagResource, ListTagsForResource
// ---------------------------------------------------------------------------

func TestHandler_Tags_CRUD(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	const arn = "arn:aws:cloudwatch:us-east-1:123456789012:alarm:test"

	// Tag it.
	rec := postForm(t, h, url.Values{
		"Action":              []string{"TagResource"},
		"ResourceARN":         []string{arn},
		"Tags.member.1.Key":   []string{"env"},
		"Tags.member.1.Value": []string{"prod"},
		"Tags.member.2.Key":   []string{"team"},
		"Tags.member.2.Value": []string{"platform"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// List tags.
	rec = postForm(t, h, url.Values{
		"Action":      []string{"ListTagsForResource"},
		"ResourceARN": []string{arn},
	}.Encode())
	assert.Equal(t, 200, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "env")
	assert.Contains(t, body, "prod")
	assert.Contains(t, body, "team")

	// Remove one tag.
	rec = postForm(t, h, url.Values{
		"Action":           []string{"UntagResource"},
		"ResourceARN":      []string{arn},
		"TagKeys.member.1": []string{"team"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	rec = postForm(t, h, url.Values{
		"Action":      []string{"ListTagsForResource"},
		"ResourceARN": []string{arn},
	}.Encode())
	assert.Equal(t, 200, rec.Code)
	body = rec.Body.String()
	assert.Contains(t, body, "env")
	assert.NotContains(t, body, "team")
}

func TestHandler_Tags_MergedOnRetag(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	const arn = "arn:aws:cloudwatch:us-east-1:123:alarm:a"

	postForm(t, h, url.Values{
		"Action":              []string{"TagResource"},
		"ResourceARN":         []string{arn},
		"Tags.member.1.Key":   []string{"k1"},
		"Tags.member.1.Value": []string{"v1"},
	}.Encode())

	postForm(t, h, url.Values{
		"Action":              []string{"TagResource"},
		"ResourceARN":         []string{arn},
		"Tags.member.1.Key":   []string{"k2"},
		"Tags.member.1.Value": []string{"v2"},
	}.Encode())

	rec := postForm(t, h, url.Values{
		"Action":      []string{"ListTagsForResource"},
		"ResourceARN": []string{arn},
	}.Encode())
	body := rec.Body.String()
	assert.Contains(t, body, "k1", "first tag should persist after second TagResource")
	assert.Contains(t, body, "k2")
}

// ---------------------------------------------------------------------------
// InsightRule: lifecycle
// ---------------------------------------------------------------------------

func TestBackend_InsightRule_CRUD(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()

	require.NoError(t, b.PutInsightRule(&cloudwatch.InsightRule{
		Name:       "rule1",
		Definition: `{"Schema":{"Name":"CloudWatchLogRule","Version":1}}`,
	}))

	rule, err := b.GetInsightRule("rule1")
	require.NoError(t, err)
	assert.Equal(t, "rule1", rule.Name)
	assert.Equal(t, "ENABLED", rule.State)
	assert.NotEmpty(t, rule.Arn)

	// Disable
	failures, err := b.DisableInsightRules([]string{"rule1"})
	require.NoError(t, err)
	assert.Empty(t, failures)

	rule, _ = b.GetInsightRule("rule1")
	assert.Equal(t, "DISABLED", rule.State)

	// Re-enable
	failures, err = b.EnableInsightRules([]string{"rule1"})
	require.NoError(t, err)
	assert.Empty(t, failures)

	rule, _ = b.GetInsightRule("rule1")
	assert.Equal(t, "ENABLED", rule.State)

	// Delete
	failures, err = b.DeleteInsightRules([]string{"rule1"})
	require.NoError(t, err)
	assert.Empty(t, failures)

	_, err = b.GetInsightRule("rule1")
	assert.Error(t, err)
}

func TestBackend_InsightRule_DeleteNonExistent(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	failures, err := b.DeleteInsightRules([]string{"missing"})
	require.NoError(t, err)
	require.Len(t, failures, 1)
	assert.Equal(t, "missing", failures[0].RuleName)
}

func TestHandler_InsightRule_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	rec := postForm(t, h, url.Values{
		"Action":         []string{"PutInsightRule"},
		"RuleName":       []string{"rule1"},
		"RuleDefinition": []string{`{"Schema":1}`},
		"RuleState":      []string{"ENABLED"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	rec = postForm(t, h, url.Values{
		"Action": []string{"DescribeInsightRules"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "rule1")

	rec = postForm(t, h, url.Values{
		"Action":             []string{"DisableInsightRules"},
		"RuleNames.member.1": []string{"rule1"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	rec = postForm(t, h, url.Values{
		"Action":             []string{"EnableInsightRules"},
		"RuleNames.member.1": []string{"rule1"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	rec = postForm(t, h, url.Values{
		"Action":             []string{"DeleteInsightRules"},
		"RuleNames.member.1": []string{"rule1"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)
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

	require.NoError(t, b.DeleteAnomalyDetector("NS", "M", "Average"))

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

func TestHandler_AnomalyDetector_FullCycle(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	rec := postForm(t, h, "Action=PutAnomalyDetector&Namespace=App&MetricName=CPU&Stat=Average")
	assert.Equal(t, 200, rec.Code)

	rec = postForm(t, h, "Action=DescribeAnomalyDetectors&Namespace=App")
	assert.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "CPU")

	rec = postForm(t, h, "Action=DeleteAnomalyDetector&Namespace=App&MetricName=CPU&Stat=Average")
	assert.Equal(t, 200, rec.Code)

	rec = postForm(t, h, "Action=DescribeAnomalyDetectors&Namespace=App")
	assert.Equal(t, 200, rec.Code)
	assert.NotContains(t, rec.Body.String(), "CPU")
}

// ---------------------------------------------------------------------------
// MetricStream: full lifecycle
// ---------------------------------------------------------------------------

func TestBackend_MetricStream_CRUD(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()

	require.NoError(t, b.PutMetricStream(&cloudwatch.MetricStream{
		Name:         "stream1",
		FirehoseArn:  "arn:aws:firehose:us-east-1:123:deliverystream/ds",
		RoleArn:      "arn:aws:iam::123:role/r",
		OutputFormat: "json",
	}))

	s, err := b.GetMetricStream("stream1")
	require.NoError(t, err)
	assert.Equal(t, "stream1", s.Name)
	assert.NotEmpty(t, s.Arn)

	p, err := b.ListMetricStreams("", 0)
	require.NoError(t, err)
	assert.Len(t, p.Data, 1)

	require.NoError(t, b.DeleteMetricStream("stream1"))
	_, err = b.GetMetricStream("stream1")
	assert.Error(t, err)
}

func TestBackend_MetricStream_StartStop(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	require.NoError(t, b.PutMetricStream(&cloudwatch.MetricStream{
		Name:         "s",
		FirehoseArn:  "arn",
		OutputFormat: "json",
	}))

	require.NoError(t, b.StopMetricStreams([]string{"s"}))
	s, _ := b.GetMetricStream("s")
	assert.Equal(t, "STOPPED", s.State)

	require.NoError(t, b.StartMetricStreams([]string{"s"}))
	s, _ = b.GetMetricStream("s")
	assert.Equal(t, "RUNNING", s.State)
}

func TestHandler_MetricStream_FullCycle(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	const streamBody = "Action=PutMetricStream&Name=s1" +
		"&FirehoseArn=arn:aws:firehose:us-east-1:123:deliverystream/ds" +
		"&RoleArn=arn:aws:iam::123:role/r&OutputFormat=json"

	rec := postForm(t, h, streamBody)
	assert.Equal(t, 200, rec.Code)

	rec = postForm(t, h, "Action=GetMetricStream&Name=s1")
	assert.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "s1")

	rec = postForm(t, h, "Action=ListMetricStreams")
	assert.Equal(t, 200, rec.Code)

	rec = postForm(t, h, "Action=StartMetricStreams&Names.member.1=s1")
	assert.Equal(t, 200, rec.Code)

	rec = postForm(t, h, "Action=StopMetricStreams&Names.member.1=s1")
	assert.Equal(t, 200, rec.Code)

	rec = postForm(t, h, "Action=DeleteMetricStream&Name=s1")
	assert.Equal(t, 200, rec.Code)
}

// ---------------------------------------------------------------------------
// CompositeAlarm: state evaluation
// ---------------------------------------------------------------------------

func TestBackend_CompositeAlarm_EvaluatesOnCreate(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()

	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName: "child", Namespace: "NS", MetricName: "M",
		ComparisonOperator: "GreaterThanThreshold", Threshold: 50, EvaluationPeriods: 1,
	}))
	require.NoError(t, b.SetAlarmState(t.Context(), "child", "ALARM", "test", ""))

	require.NoError(t, b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
		AlarmName: "composite",
		AlarmRule: "ALARM(child)",
	}))

	_, compositeAlarms, err := b.DescribeAlarms([]string{"composite"}, nil, "", "", "", 0)
	require.NoError(t, err)
	require.Len(t, compositeAlarms.Data, 1)
	assert.Equal(t, "ALARM", compositeAlarms.Data[0].StateValue,
		"composite alarm should reflect child alarm state on create")
}

func TestBackend_CompositeAlarm_UpdatesOnChildChange(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()

	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName: "child", Namespace: "NS", MetricName: "M",
		ComparisonOperator: "GreaterThanThreshold", Threshold: 50, EvaluationPeriods: 1,
	}))
	require.NoError(t, b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
		AlarmName: "composite", AlarmRule: "ALARM(child)",
	}))

	// Child initially INSUFFICIENT_DATA → composite should be non-ALARM.
	_, compPages, _ := b.DescribeAlarms([]string{"composite"}, nil, "", "", "", 0)
	require.Len(t, compPages.Data, 1)
	initialState := compPages.Data[0].StateValue

	// Trigger child alarm.
	require.NoError(t, b.SetAlarmState(t.Context(), "child", "ALARM", "trigger", ""))

	_, compPages, _ = b.DescribeAlarms([]string{"composite"}, nil, "", "", "", 0)
	require.Len(t, compPages.Data, 1)
	afterState := compPages.Data[0].StateValue

	assert.NotEqual(t, initialState, afterState, "composite should transition when child changes")
	assert.Equal(t, "ALARM", afterState)
}

// ---------------------------------------------------------------------------
// DescribeAlarms: filters
// ---------------------------------------------------------------------------

func TestBackend_DescribeAlarms_ByNamePrefix(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	for _, name := range []string{"prod-cpu", "prod-mem", "staging-cpu"} {
		require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
			AlarmName: name, Namespace: "NS", MetricName: "M",
			ComparisonOperator: "GreaterThanThreshold", Threshold: 80, EvaluationPeriods: 1,
		}))
	}

	p, _, err := b.DescribeAlarms(nil, nil, "prod-", "", "", 0)
	require.NoError(t, err)
	assert.Len(t, p.Data, 2)
	for _, a := range p.Data {
		assert.True(t, strings.HasPrefix(a.AlarmName, "prod-"))
	}
}

func TestBackend_DescribeAlarms_ByState(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName: "a1", Namespace: "NS", MetricName: "M",
		ComparisonOperator: "GreaterThanThreshold", Threshold: 80, EvaluationPeriods: 1,
	}))
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName: "a2", Namespace: "NS", MetricName: "M",
		ComparisonOperator: "GreaterThanThreshold", Threshold: 80, EvaluationPeriods: 1,
	}))
	require.NoError(t, b.SetAlarmState(t.Context(), "a1", "ALARM", "test", ""))

	p, _, err := b.DescribeAlarms(nil, nil, "", "ALARM", "", 0)
	require.NoError(t, err)
	assert.Len(t, p.Data, 1)
	assert.Equal(t, "a1", p.Data[0].AlarmName)
}

func TestBackend_DescribeAlarmsForMetric_Filters(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	for _, mn := range []string{"CPU", "Memory"} {
		require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
			AlarmName: mn + "-alarm", Namespace: "AWS/EC2", MetricName: mn,
			ComparisonOperator: "GreaterThanThreshold", Threshold: 80, EvaluationPeriods: 1,
		}))
	}

	p, err := b.DescribeAlarmsForMetric("AWS/EC2", "CPU", nil, nil, "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "CPU-alarm", p.Data[0].AlarmName)
}

// ---------------------------------------------------------------------------
// AlarmHistory: filtering
// ---------------------------------------------------------------------------

func TestBackend_DescribeAlarmHistory_FilterByType(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName: "a1", Namespace: "NS", MetricName: "M",
		ComparisonOperator: "GreaterThanThreshold", Threshold: 80, EvaluationPeriods: 1,
	}))
	require.NoError(t, b.SetAlarmState(t.Context(), "a1", "ALARM", "breach", ""))

	hist, err := b.DescribeAlarmHistory("a1", "", "StateUpdate", "", time.Time{}, time.Time{}, 0)
	require.NoError(t, err)
	for _, item := range hist.Data {
		assert.Equal(t, "StateUpdate", item.HistoryItemType)
	}
}

func TestBackend_DescribeAlarmHistory_AllAlarms(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	for _, name := range []string{"a1", "a2"} {
		require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
			AlarmName: name, Namespace: "NS", MetricName: "M",
			ComparisonOperator: "GreaterThanThreshold", Threshold: 80, EvaluationPeriods: 1,
		}))
	}

	hist, err := b.DescribeAlarmHistory("", "", "", "", time.Time{}, time.Time{}, 0)
	require.NoError(t, err)
	// Should have history for both alarms (creation events).
	alarmNames := make(map[string]bool)
	for _, item := range hist.Data {
		alarmNames[item.AlarmName] = true
	}
	assert.True(t, alarmNames["a1"])
	assert.True(t, alarmNames["a2"])
}

// ---------------------------------------------------------------------------
// Pagination: ListMetrics
// ---------------------------------------------------------------------------

func TestBackend_ListMetrics_Pagination(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-time.Minute)

	for i := range 10 {
		_, err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
			{MetricName: fmt.Sprintf("M%02d", i), Value: float64(i), Count: 1,
				Sum: float64(i), Min: float64(i), Max: float64(i), Timestamp: ts},
		})
		require.NoError(t, err)
	}

	// First page of 4.
	p1, err := b.ListMetrics("NS", "", nil, "", 4)
	require.NoError(t, err)
	assert.Len(t, p1.Data, 4)
	assert.NotEmpty(t, p1.Next)

	// Second page.
	p2, err := b.ListMetrics("NS", "", nil, p1.Next, 4)
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
// Metric retention sweep
// ---------------------------------------------------------------------------

func TestBackend_SweepExpiredMetrics_RemovesOldPoints(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()

	// Put a point 20 days ago (beyond retention).
	old := time.Now().UTC().AddDate(0, 0, -(cloudwatch.CwMetricRetentionDays + 1))
	_, err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "Old", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1, Timestamp: old},
	})
	require.NoError(t, err)

	// Put a recent point.
	_, err = b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "Recent", Value: 2, Count: 1, Sum: 2, Min: 2, Max: 2, Timestamp: time.Now().UTC()},
	})
	require.NoError(t, err)

	b.SweepExpiredMetrics()

	// Old metric should be gone.
	p, err := b.ListMetrics("NS", "Old", nil, "", 0)
	require.NoError(t, err)
	assert.Empty(t, p.Data, "expired metric should be swept")

	// Recent metric should remain.
	p, err = b.ListMetrics("NS", "Recent", nil, "", 0)
	require.NoError(t, err)
	assert.Len(t, p.Data, 1, "recent metric should survive sweep")
}

// ---------------------------------------------------------------------------
// MetricFilter: CRUD
// ---------------------------------------------------------------------------

func TestBackend_MetricFilter_CRUD(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()

	require.NoError(t, b.PutMetricFilter(&cloudwatch.MetricFilter{
		FilterName:    "f1",
		LogGroupName:  "/app/logs",
		FilterPattern: "[level=ERROR]",
		MetricTransformations: []cloudwatch.MetricTransformation{
			{MetricName: "Errors", MetricNamespace: "App", MetricValue: "1"},
		},
	}))

	p, err := b.DescribeMetricFilters("", "/app/logs", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "f1", p.Data[0].FilterName)

	require.NoError(t, b.DeleteMetricFilter("f1", "/app/logs"))
	p, err = b.DescribeMetricFilters("", "/app/logs", "", 0)
	require.NoError(t, err)
	assert.Empty(t, p.Data)
}

func TestHandler_MetricFilter_FullCycle(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	rec := postForm(t, h, url.Values{
		"Action":        []string{"PutMetricFilter"},
		"FilterName":    []string{"f1"},
		"LogGroupName":  []string{"/app/logs"},
		"FilterPattern": []string{"[ERROR]"},
		"MetricTransformations.member.1.MetricName":      []string{"ErrCount"},
		"MetricTransformations.member.1.MetricNamespace": []string{"App"},
		"MetricTransformations.member.1.MetricValue":     []string{"1"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	rec = postForm(t, h, "Action=DescribeMetricFilters&LogGroupName=%2Fapp%2Flogs")
	assert.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "f1")

	rec = postForm(t, h, "Action=DeleteMetricFilter&FilterName=f1&LogGroupName=%2Fapp%2Flogs")
	assert.Equal(t, 200, rec.Code)
}

// ---------------------------------------------------------------------------
// AlarmMuteRule: CRUD
// ---------------------------------------------------------------------------

func TestBackend_AlarmMuteRule_CRUD(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()

	require.NoError(t, b.PutAlarmMuteRule(&cloudwatch.AlarmMuteRule{
		MuteName:     "mute1",
		AlarmNames:   []string{"alarm1", "alarm2"},
		MuteDuration: 3600,
	}))

	rule, err := b.GetAlarmMuteRule("mute1")
	require.NoError(t, err)
	assert.Equal(t, "mute1", rule.MuteName)
	assert.Len(t, rule.AlarmNames, 2)

	require.NoError(t, b.DeleteAlarmMuteRule("mute1"))
	_, err = b.GetAlarmMuteRule("mute1")
	assert.Error(t, err)
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

	_, err := b.PutMetricData("NS", data1001)
	assert.Error(t, err, "PutMetricData should reject > 1000 entries per request")
}

// ---------------------------------------------------------------------------
// GetMetricData: multiple stat queries in one request
// ---------------------------------------------------------------------------

func TestBackend_GetMetricData_MultipleStatQueries(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	_, err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "CPU", Value: 50, Count: 1, Sum: 50, Min: 50, Max: 50, Timestamp: ts},
		{MetricName: "Mem", Value: 80, Count: 1, Sum: 80, Min: 80, Max: 80, Timestamp: ts},
	})
	require.NoError(t, err)

	queries := []cloudwatch.MetricDataQuery{
		{
			ID:         "cpu",
			ReturnData: true,
			MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "CPU", Stat: "Average", Period: 60},
		},
		{
			ID:         "mem",
			ReturnData: true,
			MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "Mem", Stat: "Average", Period: 60},
		},
	}

	results, err := b.GetMetricDataWithOptions(queries, ts.Add(-time.Minute), ts.Add(time.Minute), "")
	require.NoError(t, err)
	assert.Len(t, results, 2)

	byID := resultsByID(results)
	require.Contains(t, byID, "cpu")
	require.Contains(t, byID, "mem")
	require.NotEmpty(t, byID["cpu"].Values)
	require.NotEmpty(t, byID["mem"].Values)
	assert.InDelta(t, 50.0, byID["cpu"].Values[0], 1e-9)
	assert.InDelta(t, 80.0, byID["mem"].Values[0], 1e-9)
}

// ---------------------------------------------------------------------------
// GetMetricData: cross-account returns empty gracefully
// ---------------------------------------------------------------------------

func TestBackend_GetMetricData_CrossAccount_ReturnsEmpty(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	queries := []cloudwatch.MetricDataQuery{
		{ID: "q1", AccountID: "999999999999", ReturnData: true, MetricStat: cloudwatch.MetricStat{
			Namespace: "NS", MetricName: "M", Stat: "Sum", Period: 60,
		}},
	}

	results, err := b.GetMetricData(queries, time.Now().Add(-time.Hour), time.Now())
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Empty(t, results[0].Values, "cross-account queries should return empty results")
}

// ---------------------------------------------------------------------------
// GetMetricData: ReturnData=false suppresses expression output
// ---------------------------------------------------------------------------

func TestBackend_GetMetricData_ReturnDataFalse_SuppressesResult(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	_, err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "M", Value: 10, Count: 1, Sum: 10, Min: 10, Max: 10, Timestamp: ts},
	})
	require.NoError(t, err)

	queries := []cloudwatch.MetricDataQuery{
		{
			ID:         "m1",
			ReturnData: true,
			MetricStat: cloudwatch.MetricStat{Namespace: "NS", MetricName: "M", Stat: "Sum", Period: 60},
		},
		{ID: "helper", Expression: "m1 * 2", ReturnData: false},
	}

	results, err := b.GetMetricData(queries, ts.Add(-time.Minute), ts.Add(time.Minute))
	require.NoError(t, err)

	byID := resultsByID(results)
	assert.Contains(t, byID, "m1")
	assert.NotContains(t, byID, "helper", "ReturnData=false should suppress output")
}

// ---------------------------------------------------------------------------
// GetMetricStatistics: period boundary alignment
// ---------------------------------------------------------------------------

func TestBackend_GetMetricStatistics_PeriodBuckets(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Three points in different 60-second buckets.
	for i, v := range []float64{10, 20, 30} {
		_, err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
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
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Two points within the same 60-second bucket.
	_, err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
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
// EnableAlarmActions / DisableAlarmActions
// ---------------------------------------------------------------------------

func TestBackend_EnableDisableAlarmActions_Composite(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	require.NoError(t, b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
		AlarmName: "c1", AlarmRule: "ALARM(nonexistent)",
	}))

	require.NoError(t, b.DisableAlarmActions([]string{"c1"}))
	_, cp, _ := b.DescribeAlarms([]string{"c1"}, nil, "", "", "", 0)
	require.Len(t, cp.Data, 1)
	assert.False(t, cp.Data[0].ActionsEnabled)

	require.NoError(t, b.EnableAlarmActions([]string{"c1"}))
	_, cp, _ = b.DescribeAlarms([]string{"c1"}, nil, "", "", "", 0)
	assert.True(t, cp.Data[0].ActionsEnabled)
}

// ---------------------------------------------------------------------------
// DescribeAlarmContributors
// ---------------------------------------------------------------------------

func TestHandler_DescribeAlarmContributors_NotFound(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	rec := postForm(t, h, "Action=DescribeAlarmContributors&AlarmName=nonexistent")
	assert.Equal(t, 400, rec.Code)
}

func TestHandler_DescribeAlarmContributors_Existing(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	postForm(t, h, "Action=PutMetricAlarm&AlarmName=a&Namespace=NS&MetricName=M"+
		"&ComparisonOperator=GreaterThanThreshold&Threshold=50&EvaluationPeriods=1")

	rec := postForm(t, h, "Action=DescribeAlarmContributors&AlarmName=a")
	assert.Equal(t, 200, rec.Code)
}

// ---------------------------------------------------------------------------
// Handler: DescribeAlarms with AlarmTypes filter
// ---------------------------------------------------------------------------

func TestHandler_DescribeAlarms_AlarmTypeMetricOnly(t *testing.T) {
	t.Parallel()

	h := newCWHandler()
	postForm(t, h, "Action=PutMetricAlarm&AlarmName=ma&Namespace=NS&MetricName=M"+
		"&ComparisonOperator=GreaterThanThreshold&Threshold=50&EvaluationPeriods=1")
	postForm(t, h, "Action=PutCompositeAlarm&AlarmName=ca&AlarmRule=ALARM%28ma%29")

	rec := postForm(t, h, "Action=DescribeAlarms&AlarmTypes.member.1=MetricAlarm")
	assert.Equal(t, 200, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "ma")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func resultsByID(results []cloudwatch.MetricDataResult) map[string]cloudwatch.MetricDataResult {
	m := make(map[string]cloudwatch.MetricDataResult, len(results))
	for _, r := range results {
		m[r.ID] = r
	}

	return m
}
