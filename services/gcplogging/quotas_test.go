package gcplogging_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestListLogEntriesRateLimit(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	c := e.raw(t)
	list := func(parent string) error {
		_, err := c.ListLogEntries(t.Context(), &loggingpb.ListLogEntriesRequest{ResourceNames: []string{parent}}).
			Next()
		if err != nil && status.Code(err) == codes.OK {
			return nil
		}

		return err
	}

	for range 60 {
		err := list(proj)
		require.NotEqual(t, codes.ResourceExhausted, status.Code(err))
	}

	assert.Equal(t, codes.ResourceExhausted, status.Code(list(proj)), "61st request in a minute")
	assert.NotEqual(t, codes.ResourceExhausted, status.Code(list("projects/other")), "limit is per project")

	e.clk.advance(time.Minute)
	assert.NotEqual(t, codes.ResourceExhausted, status.Code(list(proj)), "window slides")
}

func TestWriteRequestLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		logs  int
		bytes int
		code  codes.Code
	}{
		{name: "thousand logs", logs: 1000, code: codes.OK},
		{name: "too many logs", logs: 1001, code: codes.InvalidArgument},
		{name: "too large", logs: 1, bytes: 10<<20 + 1<<10, code: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)

			var entries []*loggingpb.LogEntry
			for i := range tt.logs {
				entries = append(entries, textEntry(fmt.Sprintf("log%d", i), "x", 0, e.clk.now()))
			}

			if tt.bytes > 0 {
				entries[0].Labels = map[string]string{"k": strings.Repeat("v", tt.bytes)}
			}

			_, err := e.raw(t).WriteLogEntries(t.Context(), &loggingpb.WriteLogEntriesRequest{
				Resource: global(), Entries: entries,
			})
			assert.Equal(t, tt.code, status.Code(err))
		})
	}
}

func TestResourceCountLimits(t *testing.T) {
	t.Parallel()

	t.Run("sinks", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, true)

		for i := range 200 {
			e.createSink(t, &loggingpb.LogSink{
				Name: fmt.Sprintf("s%d", i), Destination: "pubsub.googleapis.com/projects/" + testProject + "/topics/t",
			})
		}

		_, err := e.config(t).CreateSink(t.Context(), &loggingpb.CreateSinkRequest{
			Parent: proj, Sink: &loggingpb.LogSink{
				Name: "over", Destination: "pubsub.googleapis.com/projects/" + testProject + "/topics/t",
			},
		})
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	})

	t.Run("buckets", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, true)
		c := e.config(t)

		for i := range 100 {
			_, err := c.CreateBucket(t.Context(), &loggingpb.CreateBucketRequest{
				Parent: proj + "/locations/global", BucketId: fmt.Sprintf("b%d", i),
			})
			require.NoError(t, err)
		}

		_, err := c.CreateBucket(t.Context(), &loggingpb.CreateBucketRequest{
			Parent: proj + "/locations/global", BucketId: "over",
		})
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	})

	t.Run("views", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, true)
		c := e.config(t)
		bucket := proj + "/locations/global/buckets/_Default"

		for i := range 30 {
			_, err := c.CreateView(
				t.Context(),
				&loggingpb.CreateViewRequest{Parent: bucket, ViewId: fmt.Sprintf("v%d", i)},
			)
			require.NoError(t, err)
		}

		_, err := c.CreateView(t.Context(), &loggingpb.CreateViewRequest{Parent: bucket, ViewId: "over"})
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	})
}

func TestMetricCountLimit(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	c := e.metrics(t)

	for i := range 500 {
		_, err := c.CreateLogMetric(t.Context(), &loggingpb.CreateLogMetricRequest{
			Parent: proj, Metric: &loggingpb.LogMetric{Name: fmt.Sprintf("m%d", i), Filter: `severity>=ERROR`},
		})
		require.NoError(t, err)
	}

	_, err := c.CreateLogMetric(t.Context(), &loggingpb.CreateLogMetricRequest{
		Parent: proj, Metric: &loggingpb.LogMetric{Name: "over", Filter: `severity>=ERROR`},
	})
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
}

func TestBucketIndexing(t *testing.T) {
	t.Parallel()

	many := make([]string, 21)
	for i := range many {
		many[i] = fmt.Sprintf("jsonPayload.f%d", i)
	}

	tests := []struct {
		bucket *loggingpb.LogBucket
		name   string
		code   codes.Code
	}{
		{
			name:   "ok",
			bucket: &loggingpb.LogBucket{RestrictedFields: []string{"jsonPayload.a.b", "labels"}},
			code:   codes.OK,
		},
		{
			name:   "too many restricted",
			bucket: &loggingpb.LogBucket{RestrictedFields: many},
			code:   codes.InvalidArgument,
		},
		{
			name:   "ineligible field",
			bucket: &loggingpb.LogBucket{RestrictedFields: []string{"severity"}},
			code:   codes.InvalidArgument,
		},
		{
			name:   "long field",
			bucket: &loggingpb.LogBucket{RestrictedFields: []string{"jsonPayload." + strings.Repeat("a", 800)}},
			code:   codes.InvalidArgument,
		},
		{
			name: "duplicate index",
			bucket: &loggingpb.LogBucket{IndexConfigs: []*loggingpb.IndexConfig{
				{FieldPath: "jsonPayload.a", Type: loggingpb.IndexType_INDEX_TYPE_STRING},
				{FieldPath: "jsonPayload.a", Type: loggingpb.IndexType_INDEX_TYPE_INTEGER},
			}},
			code: codes.InvalidArgument,
		},
		{
			name:   "index needs type",
			bucket: &loggingpb.LogBucket{IndexConfigs: []*loggingpb.IndexConfig{{FieldPath: "jsonPayload.a"}}},
			code:   codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			_, err := e.config(t).CreateBucket(t.Context(), &loggingpb.CreateBucketRequest{
				Parent: proj + "/locations/global", BucketId: "idx", Bucket: tt.bucket,
			})
			assert.Equal(t, tt.code, status.Code(err))
		})
	}
}

func TestIndexConfigCreateTimeAndAnalytics(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	c := e.config(t)
	parent := proj + "/locations/global"

	created, err := c.CreateBucket(t.Context(), &loggingpb.CreateBucketRequest{
		Parent: parent, BucketId: "idx2", Bucket: &loggingpb.LogBucket{
			AnalyticsEnabled: true,
			IndexConfigs: []*loggingpb.IndexConfig{{
				FieldPath: "jsonPayload.a", Type: loggingpb.IndexType_INDEX_TYPE_STRING,
				CreateTime: timestamppb.New(time.Unix(1, 0)),
			}},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, e.clk.now(), created.GetIndexConfigs()[0].GetCreateTime().AsTime(), "create_time is output only")

	_, err = c.UpdateBucket(t.Context(), &loggingpb.UpdateBucketRequest{
		Name: created.GetName(), Bucket: &loggingpb.LogBucket{AnalyticsEnabled: false},
		UpdateMask: mask("analytics_enabled"),
	})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "analytics cannot be disabled")
}

func TestTailQuotas(t *testing.T) {
	t.Parallel()

	t.Run("sessions per project", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, true)
		b := e.handler.Backend

		for range 10 {
			require.NoError(t, b.OpenTail(proj))
		}

		assert.Equal(t, codes.ResourceExhausted, status.Code(b.OpenTail(proj)))
		assert.NoError(t, b.OpenTail("projects/other"), "limit is per project")
	})

	t.Run("entries per minute", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, true)
		b := e.handler.Backend
		now := e.clk.now()

		assert.Equal(t, 60000, b.TailAllowed(proj, 60005, now))
		assert.Equal(t, 5, b.TailAllowed(proj, 5, now.Add(time.Minute)), "new window")
	})
}
