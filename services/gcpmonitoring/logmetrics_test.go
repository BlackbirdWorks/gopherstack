package gcpmonitoring_test

import (
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/genproto/googleapis/api/distribution"
	metricpb "google.golang.org/genproto/googleapis/api/metric"

	"github.com/blackbirdworks/gopherstack/services/gcpmonitoring"
)

const logErrors = "logging.googleapis.com/user/errors"

func logSource() *fakeLogs {
	return &fakeLogs{metrics: []gcpmonitoring.LogMetric{
		{
			Name: "errors", CreateTime: epoch().Add(-time.Hour),
			Descriptor: &metricpb.MetricDescriptor{
				MetricKind: metricpb.MetricDescriptor_DELTA, ValueType: metricpb.MetricDescriptor_INT64, Unit: "1",
			},
			Series: []gcpmonitoring.LogMetricSeries{
				{Labels: map[string]string{"svc": "api"}, Count: 7},
				{Labels: map[string]string{"svc": "web"}, Count: 2},
			},
		},
		{
			Name: "latency", CreateTime: epoch().Add(-time.Hour),
			Descriptor: &metricpb.MetricDescriptor{
				MetricKind: metricpb.MetricDescriptor_DELTA,
				ValueType:  metricpb.MetricDescriptor_DISTRIBUTION,
				Unit:       "ms",
			},
			Series: []gcpmonitoring.LogMetricSeries{
				{
					Count:        4,
					Sum:          40,
					BucketCounts: []int64{0, 4, 0},
					BucketOptions: &distribution.Distribution_BucketOptions{
						Options: &distribution.Distribution_BucketOptions_ExplicitBuckets{
							ExplicitBuckets: &distribution.Distribution_BucketOptions_Explicit{
								Bounds: []float64{5, 20},
							},
						},
					},
				},
			},
		},
	}}
}

func TestLogBasedMetricsAreQueryable(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.handler.Backend.SetLogMetricSource(logSource())
	win := interval(epoch().Add(-time.Minute), epoch())

	tests := []struct {
		req  *monitoringpb.ListTimeSeriesRequest
		want map[string][]float64
		name string
	}{
		{
			name: "all series",
			req:  &monitoringpb.ListTimeSeriesRequest{Filter: `metric.type = "` + logErrors + `"`, Interval: win},
			want: map[string][]float64{"api": {7}, "web": {2}},
		},
		{
			name: "label filter",
			req: &monitoringpb.ListTimeSeriesRequest{
				Filter:   `metric.type = "` + logErrors + `" AND metric.labels.svc = "web"`,
				Interval: win,
			},
			want: map[string][]float64{"web": {2}},
		},
		{
			name: "reduced",
			req: &monitoringpb.ListTimeSeriesRequest{
				Filter: `metric.type = "` + logErrors + `"`, Interval: win,
				Aggregation: agg(time.Minute, monitoringpb.Aggregation_ALIGN_SUM, monitoringpb.Aggregation_REDUCE_SUM),
			},
			want: map[string][]float64{"": {9}},
		},
		{
			name: "prefix",
			req: &monitoringpb.ListTimeSeriesRequest{
				Filter:   `metric.type = starts_with("logging.googleapis.com/user/err")`,
				Interval: win,
			},
			want: map[string][]float64{"api": {7}, "web": {2}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := map[string][]float64{}
			for _, ts := range e.list(t, tt.req) {
				got[ts.GetMetric().GetLabels()["svc"]] = values(ts)
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLogBasedDistributionPercentile(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.handler.Backend.SetLogMetricSource(logSource())

	got := e.list(t, &monitoringpb.ListTimeSeriesRequest{
		Filter:      `metric.type = "logging.googleapis.com/user/latency"`,
		Interval:    interval(epoch().Add(-time.Minute), epoch()),
		Aggregation: agg(time.Minute, monitoringpb.Aggregation_ALIGN_PERCENTILE_50, 0),
	})
	require.Len(t, got, 1)
	require.Len(t, got[0].GetPoints(), 1)
	assert.InDelta(t, 12.5, values(got[0])[0], 0.001)
}

func TestLogBasedDescriptors(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.handler.Backend.SetLogMetricSource(logSource())
	c := e.metric(t)

	d, err := c.GetMetricDescriptor(t.Context(), &monitoringpb.GetMetricDescriptorRequest{
		Name: proj + "/metricDescriptors/" + logErrors,
	})
	require.NoError(t, err)
	assert.Equal(t, metricpb.MetricDescriptor_DELTA, d.GetMetricKind())
	assert.Equal(t, logErrors, d.GetType())

	it := c.ListMetricDescriptors(t.Context(), &monitoringpb.ListMetricDescriptorsRequest{
		Name: proj, Filter: `metric.type = starts_with("logging.googleapis.com/user/")`,
	})

	var types []string

	for {
		md, nextErr := it.Next()
		if errors.Is(nextErr, iterator.Done) {
			break
		}

		require.NoError(t, nextErr)

		types = append(types, md.GetType())
	}

	assert.Equal(t, []string{logErrors, "logging.googleapis.com/user/latency"}, types)

	err = c.DeleteMetricDescriptor(t.Context(), &monitoringpb.DeleteMetricDescriptorRequest{Name: d.GetName()})
	assert.Equal(t, "InvalidArgument", codeOf(err).String())
}

func TestLogBasedMetricAlert(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.handler.Backend.SetLogMetricSource(logSource())

	p := queuePolicy([]string{e.pubsubChannel(t)}, monitoringpb.ComparisonType_COMPARISON_GT, 5)
	p.Conditions[0].GetConditionThreshold().Filter = `metric.type = "` + logErrors + `"`
	p.Conditions[0].GetConditionThreshold().Aggregations = []*monitoringpb.Aggregation{
		agg(time.Minute, monitoringpb.Aggregation_ALIGN_SUM, 0),
	}
	e.createPolicy(t, p)

	e.handler.Backend.Tick(t.Context())
	e.clk.advanceMinute()
	e.handler.Backend.Tick(t.Context())

	msgs := e.pub.all()
	require.Len(t, msgs, 1)
	assert.Equal(t, logErrors, decode(t, msgs[0]).Incident.Metric.Type)
	assert.Equal(t, "api", mustLabels(t, msgs[0])["svc"])
}
