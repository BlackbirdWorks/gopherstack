package gcplogging_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"cloud.google.com/go/logging/logadmin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func (e *env) createSink(t *testing.T, sink *loggingpb.LogSink) *loggingpb.LogSink {
	t.Helper()

	out, err := e.config(t).CreateSink(t.Context(), &loggingpb.CreateSinkRequest{Parent: proj, Sink: sink})
	require.NoError(t, err)

	return out
}

func TestSinkAdminLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	admin := e.admin(t)

	created, err := admin.CreateSinkOpt(t.Context(), &logadmin.Sink{
		ID: "to-topic", Destination: "pubsub.googleapis.com/projects/p/topics/t", Filter: "severity>=ERROR",
	}, logadmin.SinkOptions{UniqueWriterIdentity: true})
	require.NoError(t, err)
	assert.Equal(t, "to-topic", created.ID)
	assert.Contains(t, created.WriterIdentity, "@gcp-sa-logging.iam.gserviceaccount.com")

	shared, err := admin.CreateSink(t.Context(), &logadmin.Sink{ID: "shared", Destination: "storage.googleapis.com/b"})
	require.NoError(t, err)
	assert.Equal(t, "serviceAccount:cloud-logs@system.gserviceaccount.com", shared.WriterIdentity)

	_, err = admin.CreateSink(t.Context(), &logadmin.Sink{ID: "shared", Destination: "storage.googleapis.com/b"})
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	got, err := admin.Sink(t.Context(), "to-topic")
	require.NoError(t, err)
	assert.Equal(t, "severity>=ERROR", got.Filter)

	got.Filter = "severity>=WARNING"
	updated, err := admin.UpdateSink(t.Context(), got)
	require.NoError(t, err)
	assert.Equal(t, "severity>=WARNING", updated.Filter)

	ids := map[string]bool{}
	it := admin.Sinks(t.Context())

	for {
		s, nerr := it.Next()
		if errors.Is(nerr, iterator.Done) {
			break
		}

		require.NoError(t, nerr)

		ids[s.ID] = true
	}

	for _, want := range []string{"_Default", "_Required", "to-topic", "shared"} {
		assert.True(t, ids[want], want)
	}

	require.NoError(t, admin.DeleteSink(t.Context(), "shared"))

	_, err = admin.Sink(t.Context(), "shared")
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestSinkValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sink *loggingpb.LogSink
		name string
	}{
		{&loggingpb.LogSink{Name: "s"}, "no destination"},
		{&loggingpb.LogSink{Name: "s", Destination: "ftp://x"}, "unsupported destination"},
		{&loggingpb.LogSink{Name: "s", Destination: "pubsub.googleapis.com/topics/t"}, "bad topic"},
		{&loggingpb.LogSink{Name: "s", Destination: "storage.googleapis.com/a/b"}, "bad bucket"},
		{&loggingpb.LogSink{Name: "s", Destination: "storage.googleapis.com/b", Filter: "severity>="}, "bad filter"},
		{&loggingpb.LogSink{Name: "a b", Destination: "storage.googleapis.com/b"}, "bad name"},
		{&loggingpb.LogSink{Name: "_Default", Destination: "storage.googleapis.com/b"}, "reserved"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := newEnv(
				t,
				false,
			).config(t).
				CreateSink(t.Context(), &loggingpb.CreateSinkRequest{Parent: proj, Sink: tt.sink})
			require.Error(t, err)
		})
	}

	e := newEnv(t, false)
	cfg := e.config(t)
	_, err := cfg.UpdateSink(t.Context(), &loggingpb.UpdateSinkRequest{
		SinkName: proj + "/sinks/_Required", Sink: &loggingpb.LogSink{Disabled: true},
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Equal(
		t,
		codes.InvalidArgument,
		status.Code(cfg.DeleteSink(t.Context(), &loggingpb.DeleteSinkRequest{SinkName: proj + "/sinks/_Required"})),
	)
}

func TestSinkUpdateMask(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	e.createSink(
		t,
		&loggingpb.LogSink{
			Name:        "m",
			Destination: "storage.googleapis.com/b",
			Filter:      "severity>=ERROR",
			Description: "keep",
		},
	)

	tests := []struct {
		check func(*testing.T, *loggingpb.LogSink)
		name  string
		mask  []string
		code  codes.Code
	}{
		{func(t *testing.T, s *loggingpb.LogSink) {
			t.Helper()
			assert.Equal(t, "severity>=INFO", s.GetFilter())
			assert.Equal(t, "keep", s.GetDescription())
		}, "filter only", []string{"filter"}, codes.OK},
		{func(t *testing.T, s *loggingpb.LogSink) {
			t.Helper()
			assert.True(t, s.GetDisabled())
		}, "disable", []string{"disabled"}, codes.OK},
		{nil, "bad path", []string{"writer_identity"}, codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := e.config(t).UpdateSink(t.Context(), &loggingpb.UpdateSinkRequest{
				SinkName:   proj + "/sinks/m",
				Sink:       &loggingpb.LogSink{Filter: "severity>=INFO", Disabled: true},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: tt.mask},
			})
			assert.Equal(t, tt.code, status.Code(err))

			if tt.check != nil {
				tt.check(t, got)
			}
		})
	}
}

func TestSinkRoutesToPubSub(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.createSink(t, &loggingpb.LogSink{
		Name: "errs", Destination: "pubsub.googleapis.com/projects/p/topics/errors", Filter: "severity>=ERROR",
	})
	e.createSink(t, &loggingpb.LogSink{
		Name: "off", Destination: "pubsub.googleapis.com/projects/p/topics/off", Disabled: true,
	})
	e.createSink(t, &loggingpb.LogSink{
		Name: "excl", Destination: "pubsub.googleapis.com/projects/p/topics/excl",
		Exclusions: []*loggingpb.LogExclusion{{Name: "noisy", Filter: `textPayload:"noisy"`}},
	})

	now := e.clk.now()
	e.write(t,
		textEntry("svc", "fine", ltype.LogSeverity_INFO, now),
		textEntry("svc", "boom", ltype.LogSeverity_ERROR, now),
		textEntry("svc", "noisy boom", ltype.LogSeverity_ERROR, now),
	)

	msgs := e.pub.topic("projects/p/topics/errors")
	require.Len(t, msgs, 2)
	assert.Contains(t, msgs[0], `"textPayload":"boom"`)
	assert.Contains(t, msgs[0], `"logName":"projects/test-project/logs/svc"`)
	assert.Equal(
		t,
		now.Format(time.RFC3339Nano),
		e.pub.attr["projects/p/topics/errors"][0]["logging.googleapis.com/timestamp"],
	)
	assert.Empty(t, e.pub.topic("projects/p/topics/off"))
	assert.Len(t, e.pub.topic("projects/p/topics/excl"), 2)
}

func TestSinkRoutesToStorageHourly(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.createSink(t, &loggingpb.LogSink{Name: "gcs", Destination: "storage.googleapis.com/archive"})

	h12 := time.Date(2030, 1, 1, 12, 10, 0, 0, time.UTC)
	e.write(t, textEntry("app", "a", 0, h12), textEntry("app", "b", 0, h12.Add(time.Minute)))
	e.write(t, textEntry("app", "c", 0, h12.Add(time.Hour)), textEntry("nested%2Flog", "d", 0, h12))

	assert.ElementsMatch(t, []string{
		"archive/app/2030/01/01/12:00:00_12:59:59_S0.json",
		"archive/app/2030/01/01/13:00:00_13:59:59_S0.json",
		"archive/nested/log/2030/01/01/12:00:00_12:59:59_S0.json",
	}, e.objs.names())

	lines := strings.Split(strings.TrimSpace(e.objs.get("archive/app/2030/01/01/12:00:00_12:59:59_S0.json")), "\n")
	require.Len(t, lines, 2)
	assert.Contains(t, lines[0], `"textPayload":"a"`)
	assert.Contains(t, lines[1], `"textPayload":"b"`)
}

func TestSinkRoutesToBigQuery(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.createSink(t, &loggingpb.LogSink{Name: "bq", Destination: "bigquery.googleapis.com/projects/data/datasets/logs"})
	e.createSink(t, &loggingpb.LogSink{
		Name:        "bqp",
		Destination: "bigquery.googleapis.com/projects/data/datasets/part",
		Options: &loggingpb.LogSink_BigqueryOptions{
			BigqueryOptions: &loggingpb.BigQueryOptions{UsePartitionedTables: true},
		},
	})

	e.write(t, textEntry("cloudaudit.googleapis.com%2Factivity", "x", ltype.LogSeverity_NOTICE, e.clk.now()))

	rows := e.tables.table("data.logs.cloudaudit_googleapis_com_activity_20300101")
	require.Len(t, rows, 1)
	assert.Equal(t, "x", rows[0]["textPayload"])
	assert.Equal(t, "NOTICE", rows[0]["severity"])
	assert.Len(t, e.tables.table("data.part.cloudaudit_googleapis_com_activity"), 1)
}

func TestSinkRoutesToLogBucket(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	_, err := e.config(t).CreateBucket(t.Context(), &loggingpb.CreateBucketRequest{
		Parent: proj + "/locations/global", BucketId: "errors", Bucket: &loggingpb.LogBucket{RetentionDays: 90},
	})
	require.NoError(t, err)

	e.createSink(t, &loggingpb.LogSink{
		Name: "to-bucket", Destination: "logging.googleapis.com/" + proj + "/locations/global/buckets/errors",
		Filter: "severity>=ERROR",
	})

	now := e.clk.now()
	e.write(
		t,
		textEntry("svc", "ok", ltype.LogSeverity_INFO, now),
		textEntry("svc", "bad", ltype.LogSeverity_ERROR, now),
	)

	view := proj + "/locations/global/buckets/errors/views/_AllLogs"
	assert.Equal(t, []string{"bad"}, texts(e.list(t, &loggingpb.ListLogEntriesRequest{ResourceNames: []string{view}})))
	assert.ElementsMatch(t, []string{"ok", "bad"}, texts(e.list(t, &loggingpb.ListLogEntriesRequest{})))
}

func TestDefaultSinkDisabledDropsEntries(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	_, err := e.config(t).UpdateSink(t.Context(), &loggingpb.UpdateSinkRequest{
		SinkName: proj + "/sinks/_Default", Sink: &loggingpb.LogSink{Disabled: true},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"disabled"}},
	})
	require.NoError(t, err)

	e.write(t, textEntry("svc", "gone", 0, e.clk.now()))
	assert.Empty(t, e.list(t, &loggingpb.ListLogEntriesRequest{}))
}

func TestExclusions(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	cfg := e.config(t)

	created, err := cfg.CreateExclusion(t.Context(), &loggingpb.CreateExclusionRequest{
		Parent:    proj,
		Exclusion: &loggingpb.LogExclusion{Name: "drop-debug", Filter: "severity<=DEBUG", Description: "d"},
	})
	require.NoError(t, err)
	assert.Equal(t, "drop-debug", created.GetName())
	assert.NotNil(t, created.GetCreateTime())

	_, err = cfg.CreateExclusion(t.Context(), &loggingpb.CreateExclusionRequest{
		Parent: proj, Exclusion: &loggingpb.LogExclusion{Name: "drop-debug", Filter: "severity<=DEBUG"},
	})
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	now := e.clk.now()
	e.write(t, textEntry("x", "dbg", ltype.LogSeverity_DEBUG, now), textEntry("x", "inf", ltype.LogSeverity_INFO, now))
	assert.Equal(t, []string{"inf"}, texts(e.list(t, &loggingpb.ListLogEntriesRequest{})))

	disabled, err := cfg.UpdateExclusion(t.Context(), &loggingpb.UpdateExclusionRequest{
		Name: proj + "/exclusions/drop-debug", Exclusion: &loggingpb.LogExclusion{Disabled: true},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"disabled"}},
	})
	require.NoError(t, err)
	assert.True(t, disabled.GetDisabled())
	assert.Equal(t, "severity<=DEBUG", disabled.GetFilter())

	e.write(t, textEntry("x", "dbg2", ltype.LogSeverity_DEBUG, now))
	assert.ElementsMatch(t, []string{"inf", "dbg2"}, texts(e.list(t, &loggingpb.ListLogEntriesRequest{})))

	var names []string

	it := cfg.ListExclusions(t.Context(), &loggingpb.ListExclusionsRequest{Parent: proj})

	for {
		x, nerr := it.Next()
		if errors.Is(nerr, iterator.Done) {
			break
		}

		require.NoError(t, nerr)

		names = append(names, x.GetName())
	}

	assert.Equal(t, []string{"drop-debug"}, names)

	_, err = cfg.UpdateExclusion(t.Context(), &loggingpb.UpdateExclusionRequest{
		Name: proj + "/exclusions/drop-debug", Exclusion: &loggingpb.LogExclusion{},
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	require.NoError(
		t,
		cfg.DeleteExclusion(t.Context(), &loggingpb.DeleteExclusionRequest{Name: proj + "/exclusions/drop-debug"}),
	)
	gone := cfg.DeleteExclusion(t.Context(), &loggingpb.DeleteExclusionRequest{Name: proj + "/exclusions/drop-debug"})
	assert.Equal(t, codes.NotFound, status.Code(gone))
}
