package gcplogging_test

import (
	"errors"
	"testing"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"cloud.google.com/go/logging/logadmin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/genproto/googleapis/api/distribution"
	"google.golang.org/genproto/googleapis/api/label"
	"google.golang.org/genproto/googleapis/api/metric"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/services/gcplogging"
)

func TestCounterMetric(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	admin := e.admin(t)

	require.NoError(
		t,
		admin.CreateMetric(t.Context(), &logadmin.Metric{ID: "errors", Description: "d", Filter: "severity>=ERROR"}),
	)
	require.Error(t, admin.CreateMetric(t.Context(), &logadmin.Metric{ID: "errors", Filter: "severity>=ERROR"}))

	got, err := admin.Metric(t.Context(), "errors")
	require.NoError(t, err)
	assert.Equal(t, "severity>=ERROR", got.Filter)

	now := e.clk.now()
	e.write(t,
		textEntry("m", "a", ltype.LogSeverity_ERROR, now),
		textEntry("m", "b", ltype.LogSeverity_ERROR, now),
		textEntry("m", "c", ltype.LogSeverity_INFO, now),
	)

	pts, err := e.handler.Backend.MetricPoints(proj + "/metrics/errors")
	require.NoError(t, err)
	require.Len(t, pts, 1)
	assert.EqualValues(t, 2, pts[0].Count)

	got.Filter = "severity>=INFO"
	require.NoError(t, admin.UpdateMetric(t.Context(), got))

	e.write(t, textEntry("m", "d", ltype.LogSeverity_INFO, now))

	pts, err = e.handler.Backend.MetricPoints(proj + "/metrics/errors")
	require.NoError(t, err)
	assert.EqualValues(t, 3, pts[0].Count)

	var ids []string

	it := admin.Metrics(t.Context())

	for {
		m, nerr := it.Next()
		if errors.Is(nerr, iterator.Done) {
			break
		}

		require.NoError(t, nerr)

		ids = append(ids, m.ID)
	}

	assert.Equal(t, []string{"errors"}, ids)
	require.NoError(t, admin.DeleteMetric(t.Context(), "errors"))

	_, err = admin.Metric(t.Context(), "errors")
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestMetricDescriptorDefaultsAndValidation(t *testing.T) {
	t.Parallel()

	mc := newEnv(t, true).metrics(t)

	created, err := mc.CreateLogMetric(t.Context(), &loggingpb.CreateLogMetricRequest{
		Parent: proj, Metric: &loggingpb.LogMetric{Name: "m1", Filter: "severity>=ERROR"},
	})
	require.NoError(t, err)
	assert.Equal(t, metric.MetricDescriptor_DELTA, created.GetMetricDescriptor().GetMetricKind())
	assert.Equal(t, metric.MetricDescriptor_INT64, created.GetMetricDescriptor().GetValueType())
	assert.Equal(t, "1", created.GetMetricDescriptor().GetUnit())
	assert.Equal(t, "logging.googleapis.com/user/m1", created.GetMetricDescriptor().GetType())
	assert.NotNil(t, created.GetCreateTime())

	tests := []struct {
		metric *loggingpb.LogMetric
		name   string
	}{
		{&loggingpb.LogMetric{Name: "x"}, "no filter"},
		{&loggingpb.LogMetric{Name: "x", Filter: "a="}, "bad filter"},
		{&loggingpb.LogMetric{Name: "a b", Filter: "a=b"}, "bad name"},
		{
			&loggingpb.LogMetric{
				Name:             "x",
				Filter:           "a=b",
				MetricDescriptor: &metric.MetricDescriptor{MetricKind: metric.MetricDescriptor_GAUGE},
			},
			"gauge kind",
		},
		{&loggingpb.LogMetric{Name: "x", Filter: "a=b", ValueExtractor: "NOPE(x)"}, "bad extractor"},
		{
			&loggingpb.LogMetric{Name: "x", Filter: "a=b", ValueExtractor: "EXTRACT(jsonPayload.v)"},
			"distribution without buckets",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, cerr := mc.CreateLogMetric(
				t.Context(),
				&loggingpb.CreateLogMetricRequest{Parent: proj, Metric: tt.metric},
			)
			assert.Equal(t, codes.InvalidArgument, status.Code(cerr))
		})
	}
}

func TestLabelledAndDistributionMetrics(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	mc := e.metrics(t)

	_, err := mc.CreateLogMetric(
		t.Context(),
		&loggingpb.CreateLogMetricRequest{Parent: proj, Metric: &loggingpb.LogMetric{
			Name: "latency", Filter: `jsonPayload.latency:*`,
			ValueExtractor:  `EXTRACT(jsonPayload.latency)`,
			LabelExtractors: map[string]string{"path": `REGEXP_EXTRACT(jsonPayload.url, "^(/[a-z]+)")`},
			MetricDescriptor: &metric.MetricDescriptor{
				Labels: []*label.LabelDescriptor{{Key: "path", ValueType: label.LabelDescriptor_STRING}},
			},
			BucketOptions: &distribution.Distribution_BucketOptions{
				Options: &distribution.Distribution_BucketOptions_LinearBuckets{
					LinearBuckets: &distribution.Distribution_BucketOptions_Linear{
						NumFiniteBuckets: 3,
						Width:            10,
						Offset:           0,
					},
				},
			},
		}},
	)
	require.NoError(t, err)

	now := timestamppb.New(e.clk.now())
	mk := func(url string, latency float64) *loggingpb.LogEntry {
		p, perr := structpb.NewStruct(map[string]any{"url": url, "latency": latency})
		require.NoError(t, perr)

		return &loggingpb.LogEntry{
			LogName: proj + "/logs/web", Timestamp: now,
			Payload: &loggingpb.LogEntry_JsonPayload{JsonPayload: p},
		}
	}

	e.write(t, mk("/home/x", 5), mk("/home/y", 15), mk("/home/z", 100), mk("/api/v1", 25))

	pts, err := e.handler.Backend.MetricPoints(proj + "/metrics/latency")
	require.NoError(t, err)
	require.Len(t, pts, 2)

	byPath := map[string]gcplogging.MetricPoint{}
	for _, p := range pts {
		byPath[p.Labels["path"]] = p
	}

	home := byPath["/home"]
	assert.EqualValues(t, 3, home.Count)
	assert.InDelta(t, 120, home.Sum, 0.001)
	// bounds 0,10,20,30: underflow, [0,10), [10,20), [20,30), overflow
	assert.Equal(t, []int64{0, 1, 1, 0, 1}, home.BucketCounts)
	assert.Equal(t, []int64{0, 0, 0, 1, 0}, byPath["/api"].BucketCounts)
}

func TestMetricUpsertOnUpdate(t *testing.T) {
	t.Parallel()

	mc := newEnv(t, true).metrics(t)

	got, err := mc.UpdateLogMetric(t.Context(), &loggingpb.UpdateLogMetricRequest{
		MetricName: proj + "/metrics/new", Metric: &loggingpb.LogMetric{Name: "new", Filter: "a=b"},
	})
	require.NoError(t, err)
	assert.Equal(t, "new", got.GetName())

	_, err = mc.UpdateLogMetric(t.Context(), &loggingpb.UpdateLogMetricRequest{
		MetricName: proj + "/metrics/new", Metric: &loggingpb.LogMetric{Name: "other", Filter: "a=b"},
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}
