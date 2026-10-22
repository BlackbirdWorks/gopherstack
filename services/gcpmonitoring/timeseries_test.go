package gcpmonitoring_test

import (
	"testing"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/protobuf/types/known/durationpb"
)

const metricFilter = `metric.type = "` + queueDepth + `"`

func seed(t *testing.T) *env {
	t.Helper()

	e := newEnv(t)
	t0 := epoch()

	for m := 1; m <= 5; m++ {
		vals := map[string]float64{"inst-1": float64(m), "inst-2": float64(m * 10)}
		e.writeAt(t, t0.Add(time.Duration(m)*time.Minute), "gold", map[string]float64{"inst-2": vals["inst-2"]})
		e.writeAt(
			t,
			t0.Add(time.Duration(m)*time.Minute+time.Second),
			"bronze",
			map[string]float64{"inst-1": vals["inst-1"]},
		)
	}

	e.writeAt(t, t0.Add(5*time.Minute+2*time.Second), "silver", map[string]float64{"inst-3": 100})

	return e
}

func agg(
	period time.Duration,
	a monitoringpb.Aggregation_Aligner,
	r monitoringpb.Aggregation_Reducer,
	by ...string,
) *monitoringpb.Aggregation {
	return &monitoringpb.Aggregation{
		AlignmentPeriod: durationpb.New(period), PerSeriesAligner: a, CrossSeriesReducer: r, GroupByFields: by,
	}
}

func TestListTimeSeriesFilters(t *testing.T) {
	t.Parallel()

	e := seed(t)
	end := epoch().Add(6 * time.Minute)

	tests := []struct {
		name   string
		filter string
		want   []string
	}{
		{"metric type only", metricFilter, []string{"inst-1", "inst-2", "inst-3"}},
		{"label equals", metricFilter + ` AND metric.labels.tier = "gold"`, []string{"inst-2"}},
		{"label not equals", metricFilter + ` AND metric.labels.tier != "gold"`, []string{"inst-1", "inst-3"}},
		{
			"starts with",
			metricFilter + ` AND resource.labels.instance_id = starts_with("inst-")`,
			[]string{"inst-1", "inst-2", "inst-3"},
		},
		{
			"one of",
			metricFilter + ` AND resource.labels.instance_id = one_of("inst-1","inst-3")`,
			[]string{"inst-1", "inst-3"},
		},
		{"resource type", metricFilter + ` AND resource.type = "global"`, nil},
		{"has substring", metricFilter + ` AND metric.labels.tier = has_substring("ilv")`, []string{"inst-3"}},
		{
			"regex",
			metricFilter + ` AND resource.labels.instance_id = monitoring.regex.full_match("inst-[12]")`,
			[]string{"inst-1", "inst-2"},
		},
		{
			"or",
			metricFilter + ` AND (metric.labels.tier = "gold" OR metric.labels.tier = "silver")`,
			[]string{"inst-2", "inst-3"},
		},
		{"not", metricFilter + ` AND NOT metric.labels.tier = "gold"`, []string{"inst-1", "inst-3"}},
		{
			"type prefix",
			`metric.type = starts_with("custom.googleapis.com/test/")`,
			[]string{"inst-1", "inst-2", "inst-3"},
		},
		{"other metric", `metric.type = "custom.googleapis.com/test/other"`, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := e.list(t, &monitoringpb.ListTimeSeriesRequest{
				Filter: tt.filter, Interval: interval(epoch(), end),
			})

			var ids []string
			for _, ts := range got {
				ids = append(ids, ts.GetResource().GetLabels()["instance_id"])
			}

			assert.Equal(t, tt.want, ids)
		})
	}
}

func TestListTimeSeriesRawPointsNewestFirst(t *testing.T) {
	t.Parallel()

	e := seed(t)

	got := e.list(t, &monitoringpb.ListTimeSeriesRequest{
		Filter:   metricFilter + ` AND resource.labels.instance_id = "inst-1"`,
		Interval: interval(epoch(), epoch().Add(6*time.Minute)),
	})
	require.Len(t, got, 1)
	assert.Equal(t, []float64{5, 4, 3, 2, 1}, values(got[0]))
	assert.Equal(t, "gce_instance", got[0].GetResource().GetType())
	assert.Equal(t, testProject, got[0].GetResource().GetLabels()["project_id"])

	headers := e.list(t, &monitoringpb.ListTimeSeriesRequest{
		Filter: metricFilter, Interval: interval(epoch(), epoch().Add(6*time.Minute)),
		View: monitoringpb.ListTimeSeriesRequest_HEADERS,
	})
	require.Len(t, headers, 3)
	assert.Empty(t, headers[0].GetPoints())

	narrow := e.list(t, &monitoringpb.ListTimeSeriesRequest{
		Filter:   metricFilter + ` AND resource.labels.instance_id = "inst-1"`,
		Interval: interval(epoch().Add(2*time.Minute+time.Second), epoch().Add(4*time.Minute+time.Second)),
	})
	require.Len(t, narrow, 1)
	assert.Equal(t, []float64{4, 3}, values(narrow[0]))
}

func TestListTimeSeriesAggregation(t *testing.T) {
	t.Parallel()

	e := seed(t)
	end := epoch().Add(6 * time.Minute)
	const minute = time.Minute

	tests := []struct {
		agg    *monitoringpb.Aggregation
		sec    *monitoringpb.Aggregation
		want   map[string][]float64
		name   string
		filter string
	}{
		{
			name: "mean 6m", filter: metricFilter, agg: agg(6*minute, monitoringpb.Aggregation_ALIGN_MEAN, 0),
			want: map[string][]float64{"inst-1": {3}, "inst-2": {30}, "inst-3": {100}},
		},
		{
			name: "max 1m", filter: metricFilter + ` AND resource.labels.instance_id = "inst-1"`,
			agg:  agg(minute, monitoringpb.Aggregation_ALIGN_MAX, 0),
			want: map[string][]float64{"inst-1": {5, 4, 3, 2, 1}},
		},
		{
			name: "sum 6m", filter: metricFilter, agg: agg(6*minute, monitoringpb.Aggregation_ALIGN_SUM, 0),
			want: map[string][]float64{"inst-1": {15}, "inst-2": {150}, "inst-3": {100}},
		},
		{
			name: "min 6m", filter: metricFilter, agg: agg(6*minute, monitoringpb.Aggregation_ALIGN_MIN, 0),
			want: map[string][]float64{"inst-1": {1}, "inst-2": {10}, "inst-3": {100}},
		},
		{
			name: "count 6m", filter: metricFilter, agg: agg(6*minute, monitoringpb.Aggregation_ALIGN_COUNT, 0),
			want: map[string][]float64{"inst-1": {5}, "inst-2": {5}, "inst-3": {1}},
		},
		{
			name: "reduce sum", filter: metricFilter,
			agg:  agg(6*minute, monitoringpb.Aggregation_ALIGN_SUM, monitoringpb.Aggregation_REDUCE_SUM),
			want: map[string][]float64{"": {265}},
		},
		{
			name: "reduce mean", filter: metricFilter,
			agg:  agg(6*minute, monitoringpb.Aggregation_ALIGN_MEAN, monitoringpb.Aggregation_REDUCE_MEAN),
			want: map[string][]float64{"": {(3 + 30 + 100) / 3.0}},
		},
		{
			name:   "reduce max grouped by tier",
			filter: metricFilter,
			agg: agg(
				6*minute,
				monitoringpb.Aggregation_ALIGN_MAX,
				monitoringpb.Aggregation_REDUCE_MAX,
				"metric.labels.tier",
			),
			want: map[string][]float64{"bronze": {5}, "gold": {50}, "silver": {100}},
		},
		{
			name: "reduce count", filter: metricFilter,
			agg:  agg(6*minute, monitoringpb.Aggregation_ALIGN_MEAN, monitoringpb.Aggregation_REDUCE_COUNT),
			want: map[string][]float64{"": {3}},
		},
		{
			name: "secondary max over mean", filter: metricFilter + ` AND resource.labels.instance_id = "inst-1"`,
			agg:  agg(minute, monitoringpb.Aggregation_ALIGN_MEAN, 0),
			sec:  agg(6*minute, monitoringpb.Aggregation_ALIGN_MAX, 0),
			want: map[string][]float64{"inst-1": {5}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := e.list(t, &monitoringpb.ListTimeSeriesRequest{
				Filter: tt.filter, Interval: interval(epoch(), end), Aggregation: tt.agg, SecondaryAggregation: tt.sec,
			})

			byKey := map[string][]float64{}

			for _, ts := range got {
				key := ts.GetResource().GetLabels()["instance_id"]
				if tt.agg.GetCrossSeriesReducer() != 0 {
					key = ts.GetMetric().GetLabels()["tier"]
				}

				byKey[key] = values(ts)
			}

			assert.Equal(t, tt.want, byKey)
		})
	}
}

func TestListTimeSeriesDeltaAndCumulative(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	t0 := epoch()
	res := instance("inst-9")

	for m := 1; m <= 4; m++ {
		at := t0.Add(time.Duration(m) * time.Minute)
		e.clk.set(at)
		e.write(
			t,
			intSeries(
				"custom.googleapis.com/test/delta",
				metricpb.MetricDescriptor_DELTA,
				res,
				at.Add(-time.Minute),
				at,
				int64(m*60),
			),
			intSeries(
				"custom.googleapis.com/test/cum",
				metricpb.MetricDescriptor_CUMULATIVE,
				res,
				t0,
				at,
				int64(m*120),
			),
		)
	}

	end := t0.Add(5 * time.Minute)
	tests := []struct {
		name   string
		metric string
		agg    *monitoringpb.Aggregation
		want   []float64
	}{
		{"delta sum", "delta", agg(time.Minute, monitoringpb.Aggregation_ALIGN_SUM, 0), []float64{240, 180, 120, 60}},
		{"delta rate", "delta", agg(time.Minute, monitoringpb.Aggregation_ALIGN_RATE, 0), []float64{4, 3, 2, 1}},
		{
			"cumulative delta",
			"cum",
			agg(time.Minute, monitoringpb.Aggregation_ALIGN_DELTA, 0),
			[]float64{120, 120, 120, 120},
		},
		{"cumulative rate", "cum", agg(time.Minute, monitoringpb.Aggregation_ALIGN_RATE, 0), []float64{2, 2, 2, 2}},
		{"raw cumulative", "cum", nil, []float64{480, 360, 240, 120}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := e.list(t, &monitoringpb.ListTimeSeriesRequest{
				Filter:   `metric.type = "custom.googleapis.com/test/` + tt.metric + `"`,
				Interval: interval(t0, end), Aggregation: tt.agg,
			})
			require.Len(t, got, 1)
			assert.Equal(t, tt.want, values(got[0]))
		})
	}
}

func TestListTimeSeriesPagination(t *testing.T) {
	t.Parallel()

	e := seed(t)
	c := e.metric(t)

	req := &monitoringpb.ListTimeSeriesRequest{
		Name: proj, Filter: metricFilter, Interval: interval(epoch(), epoch().Add(6*time.Minute)), PageSize: 2,
	}
	it := c.ListTimeSeries(t.Context(), req)

	first, err := it.Next()
	require.NoError(t, err)
	_, err = it.Next()
	require.NoError(t, err)
	assert.NotEmpty(t, it.PageInfo().Token)

	third, err := it.Next()
	require.NoError(t, err)
	assert.NotEqual(t, first.GetResource().GetLabels()["instance_id"], third.GetResource().GetLabels()["instance_id"])
}

func TestListTimeSeriesValidation(t *testing.T) {
	t.Parallel()

	e := seed(t)
	good := interval(epoch(), epoch().Add(time.Minute))

	tests := []struct {
		req  *monitoringpb.ListTimeSeriesRequest
		name string
	}{
		{&monitoringpb.ListTimeSeriesRequest{Name: proj, Interval: good}, "no filter"},
		{
			&monitoringpb.ListTimeSeriesRequest{Name: proj, Filter: `resource.type = "gce_instance"`, Interval: good},
			"no metric type",
		},
		{&monitoringpb.ListTimeSeriesRequest{Name: proj, Filter: metricFilter}, "no interval"},
		{&monitoringpb.ListTimeSeriesRequest{Name: proj, Filter: metricFilter + " AND", Interval: good}, "bad filter"},
		{&monitoringpb.ListTimeSeriesRequest{Name: "bogus", Filter: metricFilter, Interval: good}, "bad name"},
		{&monitoringpb.ListTimeSeriesRequest{
			Name: proj, Filter: metricFilter, Interval: good,
			Aggregation: agg(10*time.Second, monitoringpb.Aggregation_ALIGN_MEAN, 0),
		}, "short period"},
		{&monitoringpb.ListTimeSeriesRequest{
			Name: proj, Filter: metricFilter, Interval: good,
			Aggregation: agg(time.Minute, 0, monitoringpb.Aggregation_REDUCE_SUM),
		}, "reducer without aligner"},
		{&monitoringpb.ListTimeSeriesRequest{
			Name: proj, Filter: metricFilter, Interval: good,
			Aggregation: agg(time.Minute, monitoringpb.Aggregation_ALIGN_RATE, 0),
		}, "rate on gauge"},
		{&monitoringpb.ListTimeSeriesRequest{
			Name: proj, Filter: metricFilter, Interval: good,
			Aggregation: agg(time.Minute, monitoringpb.Aggregation_ALIGN_FRACTION_TRUE, 0),
		}, "fraction on double"},
		{&monitoringpb.ListTimeSeriesRequest{
			Name: proj, Filter: metricFilter, Interval: good, PageToken: "!!",
		}, "bad token"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			it := e.metric(t).ListTimeSeries(t.Context(), tt.req)
			_, err := it.Next()
			require.Error(t, err)
			assert.Equal(t, "InvalidArgument", codeOf(err).String(), err.Error())
		})
	}
}
