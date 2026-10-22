package gcpmonitoring_test

import (
	"testing"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func boolPoint(at time.Time, v bool) *monitoringpb.Point {
	return &monitoringpb.Point{
		Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(at)},
		Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_BoolValue{BoolValue: v}},
	}
}

func TestCreateTimeSeriesErrors(t *testing.T) {
	t.Parallel()

	now := epoch()
	good := func() *monitoringpb.TimeSeries {
		return gaugeDouble(queueDepth, nil, instance("i-1"), now, 1)
	}

	tests := []struct {
		mutate func(*monitoringpb.TimeSeries)
		name   string
		want   string
	}{
		{
			func(ts *monitoringpb.TimeSeries) { ts.Metric.Type = "compute.googleapis.com/instance/foo" },
			"builtin type",
			"not writable",
		},
		{func(ts *monitoringpb.TimeSeries) { ts.Metric.Type = "" }, "empty type", "invalid metric type"},
		{
			func(ts *monitoringpb.TimeSeries) { ts.Resource.Type = "nope" },
			"unknown resource",
			"Unrecognized resource type",
		},
		{
			func(ts *monitoringpb.TimeSeries) { delete(ts.GetResource().GetLabels(), "zone") },
			"missing label",
			"Missing required resource label",
		},
		{
			func(ts *monitoringpb.TimeSeries) { ts.Resource.Labels["extra"] = "x" },
			"extra label",
			"Unrecognized resource label",
		},
		{
			func(ts *monitoringpb.TimeSeries) { ts.Points = append(ts.GetPoints(), ts.GetPoints()[0]) },
			"two points",
			"Only one point",
		},
		{func(ts *monitoringpb.TimeSeries) { ts.Points = nil }, "no points", "Only one point"},
		{func(ts *monitoringpb.TimeSeries) {
			ts.Points[0].Interval.StartTime = timestamppb.New(now.Add(-time.Minute))
		}, "gauge with start", "must be equal to the end time"},
		{func(ts *monitoringpb.TimeSeries) {
			ts.MetricKind = metricpb.MetricDescriptor_DELTA
		}, "delta without start", "must be before the end time"},
		{func(ts *monitoringpb.TimeSeries) {
			ts.Points[0].Interval.EndTime = timestamppb.New(now.Add(-26 * time.Hour))
		}, "too old", "too old"},
		{func(ts *monitoringpb.TimeSeries) {
			ts.Points[0].Interval.EndTime = timestamppb.New(now.Add(time.Hour))
		}, "future", "future"},
		{func(ts *monitoringpb.TimeSeries) { ts.Points[0].Interval.EndTime = nil }, "no end", "end time is required"},
		{func(ts *monitoringpb.TimeSeries) { ts.Points[0].Value = nil }, "no value", "value type"},
		{
			func(ts *monitoringpb.TimeSeries) { ts.ValueType = metricpb.MetricDescriptor_INT64 },
			"value type mismatch",
			"Value type",
		},
		{
			func(ts *monitoringpb.TimeSeries) { ts.Metric.Labels = map[string]string{"1bad": "x"} },
			"bad label key",
			"invalid label key",
		},
		{func(ts *monitoringpb.TimeSeries) {
			ts.MetricKind = metricpb.MetricDescriptor_DELTA
			ts.Points[0].Interval.StartTime = timestamppb.New(now.Add(-time.Minute))
			ts.Points[0].Value = &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_BoolValue{BoolValue: true}}
		}, "delta bool", "value type"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			ts := good()
			tt.mutate(ts)

			err := e.metric(t).CreateTimeSeries(t.Context(),
				&monitoringpb.CreateTimeSeriesRequest{Name: proj, TimeSeries: []*monitoringpb.TimeSeries{ts}})
			require.Error(t, err)
			assert.Equal(t, "InvalidArgument", codeOf(err).String())
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestCreateTimeSeriesRequestErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	c := e.metric(t)

	many := make([]*monitoringpb.TimeSeries, 201)
	for i := range many {
		many[i] = gaugeDouble(queueDepth, nil, instance("i-1"), epoch(), 1)
	}

	tests := []struct {
		req  *monitoringpb.CreateTimeSeriesRequest
		name string
	}{
		{&monitoringpb.CreateTimeSeriesRequest{Name: proj}, "empty"},
		{&monitoringpb.CreateTimeSeriesRequest{Name: proj, TimeSeries: many}, "too many"},
		{&monitoringpb.CreateTimeSeriesRequest{Name: "organizations/1", TimeSeries: many[:1]}, "bad scope"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := c.CreateTimeSeries(t.Context(), tt.req)
			require.Error(t, err)
			assert.Equal(t, "InvalidArgument", codeOf(err).String())
		})
	}
}

func TestCreateTimeSeriesOrderingAndDuplicates(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	now := epoch()
	e.write(t, gaugeDouble(queueDepth, nil, instance("i-1"), now, 1))

	tests := []struct {
		at   time.Time
		name string
		want string
		dup  bool
	}{
		{now, "same time", "in order", false},
		{now.Add(-time.Second), "older", "in order", false},
		{now.Add(time.Second), "duplicate in request", "Duplicate TimeSeries", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			batch := []*monitoringpb.TimeSeries{gaugeDouble(queueDepth, nil, instance("i-1"), tt.at, 2)}
			if tt.dup {
				batch = append(batch, gaugeDouble(queueDepth, nil, instance("i-1"), tt.at, 3))
			}

			err := e.metric(t).CreateTimeSeries(t.Context(),
				&monitoringpb.CreateTimeSeriesRequest{Name: proj, TimeSeries: batch})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestCreateTimeSeriesPartialSuccess(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	bad := gaugeDouble("compute.googleapis.com/x", nil, instance("i-1"), epoch(), 1)
	ok := gaugeDouble(queueDepth, nil, instance("i-2"), epoch(), 7)

	err := e.metric(t).CreateTimeSeries(t.Context(),
		&monitoringpb.CreateTimeSeriesRequest{Name: proj, TimeSeries: []*monitoringpb.TimeSeries{bad, ok}})
	require.Error(t, err)

	var summary *monitoringpb.CreateTimeSeriesSummary

	for _, d := range status.Convert(err).Details() {
		if s, isSummary := d.(*monitoringpb.CreateTimeSeriesSummary); isSummary {
			summary = s
		}
	}

	require.NotNil(t, summary)
	assert.EqualValues(t, 2, summary.GetTotalPointCount())
	assert.EqualValues(t, 1, summary.GetSuccessPointCount())
	require.Len(t, summary.GetErrors(), 1)

	got := e.list(t, &monitoringpb.ListTimeSeriesRequest{
		Filter: metricFilter, Interval: interval(epoch().Add(-time.Minute), epoch()),
	})
	require.Len(t, got, 1)
	assert.Equal(t, []float64{7}, values(got[0]))
}

func TestCreateTimeSeriesTypes(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	now := epoch()

	tests := []struct {
		series *monitoringpb.TimeSeries
		name   string
		vt     metricpb.MetricDescriptor_ValueType
	}{
		{&monitoringpb.TimeSeries{
			Metric: &metricpb.Metric{Type: "custom.googleapis.com/test/flag"}, Resource: instance("i-1"),
			Points: []*monitoringpb.Point{boolPoint(now, true)},
		}, "bool", metricpb.MetricDescriptor_BOOL},
		{&monitoringpb.TimeSeries{
			Metric: &metricpb.Metric{Type: "custom.googleapis.com/test/note"}, Resource: instance("i-1"),
			Points: []*monitoringpb.Point{{
				Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(now)},
				Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_StringValue{StringValue: "hi"}},
			}},
		}, "string", metricpb.MetricDescriptor_STRING},
		{&monitoringpb.TimeSeries{
			Metric: &metricpb.Metric{Type: "custom.googleapis.com/test/int"}, Resource: &monitoredres.MonitoredResource{
				Type: "global",
			},
			Points: []*monitoringpb.Point{{
				Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(now)},
				Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_Int64Value{Int64Value: 3}},
			}},
		}, "int64", metricpb.MetricDescriptor_INT64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e.write(t, tt.series)

			d, err := e.metric(t).GetMetricDescriptor(t.Context(), &monitoringpb.GetMetricDescriptorRequest{
				Name: proj + "/metricDescriptors/" + tt.series.GetMetric().GetType(),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.vt, d.GetValueType())
			assert.Equal(t, metricpb.MetricDescriptor_GAUGE, d.GetMetricKind())
		})
	}
}

func TestCreateServiceTimeSeriesWritesBuiltinMetrics(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	c := e.metric(t)
	now := epoch()
	ts := &monitoringpb.TimeSeries{
		Metric: &metricpb.Metric{
			Type: "run.googleapis.com/request_count", Labels: map[string]string{"response_code": "200"},
		},
		Resource: &monitoredres.MonitoredResource{Type: "cloud_run_revision", Labels: map[string]string{
			"service_name": "s", "revision_name": "r", "location": "us-central1", "configuration_name": "c",
		}},
		MetricKind: metricpb.MetricDescriptor_DELTA,
		Points: []*monitoringpb.Point{
			{
				Interval: &monitoringpb.TimeInterval{
					StartTime: timestamppb.New(now.Add(-time.Minute)),
					EndTime:   timestamppb.New(now),
				},
				Value: &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_Int64Value{Int64Value: 5}},
			},
		},
	}
	req := &monitoringpb.CreateTimeSeriesRequest{Name: proj, TimeSeries: []*monitoringpb.TimeSeries{ts}}

	require.Error(t, c.CreateTimeSeries(t.Context(), req))
	require.NoError(t, c.CreateServiceTimeSeries(t.Context(), req))

	got := e.list(t, &monitoringpb.ListTimeSeriesRequest{
		Filter:   `metric.type = "run.googleapis.com/request_count"`,
		Interval: interval(now.Add(-time.Hour), now),
	})
	require.Len(t, got, 1)
	assert.Equal(t, []float64{5}, values(got[0]))
}
