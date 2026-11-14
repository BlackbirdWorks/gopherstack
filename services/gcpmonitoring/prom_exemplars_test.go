package gcpmonitoring_test

import (
	"encoding/json/v2"
	"net/http"
	"net/url"
	"testing"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	distpb "google.golang.org/genproto/googleapis/api/distribution"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

type exemplarReply struct {
	Status string `json:"status"`
	Data   []struct {
		SeriesLabels map[string]string `json:"seriesLabels"`
		Exemplars    []struct {
			Labels    map[string]string `json:"labels"`
			Value     string            `json:"value"`
			Timestamp float64           `json:"timestamp"`
		} `json:"exemplars"`
	} `json:"data"`
}

func exemplarDist(t *testing.T, at time.Time) *distpb.Distribution {
	t.Helper()

	span, err := anypb.New(
		&monitoringpb.SpanContext{SpanName: "projects/" + testProject + "/traces/abc123/spans/def456"},
	)
	require.NoError(t, err)

	dropped, err := anypb.New(&monitoringpb.DroppedLabels{Label: map[string]string{"pod": "p1"}})
	require.NoError(t, err)

	return &distpb.Distribution{
		Count: 3, Mean: 2, BucketCounts: []int64{1, 1, 1},
		BucketOptions: &distpb.Distribution_BucketOptions{Options: &distpb.Distribution_BucketOptions_ExplicitBuckets{
			ExplicitBuckets: &distpb.Distribution_BucketOptions_Explicit{Bounds: []float64{1, 5}},
		}},
		Exemplars: []*distpb.Distribution_Exemplar{
			{Value: 3, Timestamp: timestamppb.New(at.Add(-time.Minute)), Attachments: []*anypb.Any{span, dropped}},
			{Value: 9, Timestamp: timestamppb.New(at.Add(-2 * time.Minute))},
		},
	}
}

func TestPromQueryExemplars(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params func(now time.Time) url.Values
		name   string
		series int
		want   int
		status int
	}{
		{
			name:   "all",
			status: http.StatusOK,
			series: 2,
			want:   2,
			params: func(time.Time) url.Values { return url.Values{"query": {"custom_googleapis_com:test_latency_bucket"}} },
		},
		{
			name: "le selector", status: http.StatusOK, series: 1, want: 1,
			params: func(time.Time) url.Values {
				return url.Values{"query": {`custom_googleapis_com:test_latency_bucket{le="5"}`}}
			},
		},
		{
			name: "time window", status: http.StatusOK, series: 1, want: 1,
			params: func(now time.Time) url.Values {
				return url.Values{
					"query": {"custom_googleapis_com:test_latency_bucket"},
					"start": {tsParam(now.Add(-90 * time.Second))}, "end": {tsParam(now)},
				}
			},
		},
		{
			name: "other metric", status: http.StatusOK, series: 0,
			params: func(time.Time) url.Values { return url.Values{"query": {"nothing_bucket"}} },
		},
		{
			name:   "missing query",
			status: http.StatusBadRequest,
			params: func(time.Time) url.Values { return url.Values{} },
		},
		{
			name: "bad query", status: http.StatusBadRequest,
			params: func(time.Time) url.Values { return url.Values{"query": {"sum("}} },
		},
		{
			name: "bad start", status: http.StatusBadRequest,
			params: func(time.Time) url.Values { return url.Values{"query": {"x"}, "start": {"nope"}} },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			now := epoch()
			e.write(t, &monitoringpb.TimeSeries{
				Metric: &metricpb.Metric{Type: "custom.googleapis.com/test/latency"},
				Resource: &monitoredres.MonitoredResource{
					Type:   "global",
					Labels: map[string]string{"project_id": testProject},
				},
				MetricKind: metricpb.MetricDescriptor_CUMULATIVE,
				ValueType:  metricpb.MetricDescriptor_DISTRIBUTION,
				Points: []*monitoringpb.Point{{
					Interval: &monitoringpb.TimeInterval{
						StartTime: timestamppb.New(now.Add(-time.Hour)), EndTime: timestamppb.New(now),
					},
					Value: &monitoringpb.TypedValue{
						Value: &monitoringpb.TypedValue_DistributionValue{DistributionValue: exemplarDist(t, now)},
					},
				}},
			})

			code, body := e.promGet(t, promBase+"query_exemplars", tt.params(now))
			require.Equal(t, tt.status, code, string(body))

			if tt.status != http.StatusOK {
				return
			}

			var r exemplarReply
			require.NoError(t, json.Unmarshal(body, &r, jsonx.V1()))
			require.Len(t, r.Data, tt.series)

			total := 0
			for _, s := range r.Data {
				total += len(s.Exemplars)
			}

			assert.Equal(t, tt.want, total)
		})
	}
}

func TestPromQueryExemplarLabels(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	now := epoch()
	e.write(t, &monitoringpb.TimeSeries{
		Metric: &metricpb.Metric{Type: "custom.googleapis.com/test/latency"},
		Resource: &monitoredres.MonitoredResource{
			Type:   "global",
			Labels: map[string]string{"project_id": testProject},
		},
		MetricKind: metricpb.MetricDescriptor_CUMULATIVE,
		ValueType:  metricpb.MetricDescriptor_DISTRIBUTION,
		Points: []*monitoringpb.Point{{
			Interval: &monitoringpb.TimeInterval{
				StartTime: timestamppb.New(now.Add(-time.Hour)), EndTime: timestamppb.New(now),
			},
			Value: &monitoringpb.TypedValue{
				Value: &monitoringpb.TypedValue_DistributionValue{DistributionValue: exemplarDist(t, now)},
			},
		}},
	})

	code, body := e.promGet(t, promBase+"query_exemplars", url.Values{
		"query": {`custom_googleapis_com:test_latency_bucket{le="5"}`},
	})
	require.Equal(t, http.StatusOK, code)

	var r exemplarReply
	require.NoError(t, json.Unmarshal(body, &r, jsonx.V1()))
	require.Len(t, r.Data, 1)
	assert.Equal(t, "5", r.Data[0].SeriesLabels["le"])
	assert.Equal(t, "custom_googleapis_com:test_latency_bucket", r.Data[0].SeriesLabels["__name__"])

	ex := r.Data[0].Exemplars[0]
	assert.Equal(t, "3", ex.Value)
	assert.Equal(t, map[string]string{
		"project_id": testProject, "trace_id": "abc123", "span_id": "def456", "pod": "p1",
	}, ex.Labels)
	assert.InDelta(t, float64(now.Add(-time.Minute).Unix()), ex.Timestamp, 1)
}
