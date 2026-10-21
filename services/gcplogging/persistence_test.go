package gcplogging_test

import (
	"testing"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ltype "google.golang.org/genproto/googleapis/logging/type"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/gcplogging"
)

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	src := newEnv(t, true)
	cfg := src.config(t)

	src.createSink(t, &loggingpb.LogSink{Name: "s", Destination: "storage.googleapis.com/b", Filter: "severity>=ERROR"})
	_, err := cfg.CreateExclusion(t.Context(), &loggingpb.CreateExclusionRequest{
		Parent: proj, Exclusion: &loggingpb.LogExclusion{Name: "x", Filter: "severity<=DEBUG"},
	})
	require.NoError(t, err)
	_, err = cfg.CreateBucket(
		t.Context(),
		&loggingpb.CreateBucketRequest{Parent: loc, BucketId: "kept", Bucket: &loggingpb.LogBucket{RetentionDays: 77}},
	)
	require.NoError(t, err)
	_, err = cfg.CreateView(t.Context(), &loggingpb.CreateViewRequest{
		Parent: loc + "/buckets/kept", ViewId: "v", View: &loggingpb.LogView{Filter: "severity>=ERROR"},
	})
	require.NoError(t, err)
	_, err = cfg.UpdateCmekSettings(t.Context(), &loggingpb.UpdateCmekSettingsRequest{
		Name:         proj + "/cmekSettings",
		CmekSettings: &loggingpb.CmekSettings{KmsKeyName: "projects/k/locations/g/keyRings/r/cryptoKeys/c"},
	})
	require.NoError(t, err)
	_, err = src.metrics(t).CreateLogMetric(t.Context(), &loggingpb.CreateLogMetricRequest{
		Parent: proj, Metric: &loggingpb.LogMetric{Name: "m", Filter: "severity>=ERROR"},
	})
	require.NoError(t, err)

	now := src.clk.now()
	src.write(
		t,
		textEntry("svc", "boom", ltype.LogSeverity_ERROR, now),
		textEntry("svc", "fine", ltype.LogSeverity_INFO, now),
	)

	snap := src.handler.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	dst := newEnv(t, true)
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	assert.ElementsMatch(t, []string{"boom", "fine"}, texts(dst.list(t, &loggingpb.ListLogEntriesRequest{})))

	s, err := dst.config(t).GetSink(t.Context(), &loggingpb.GetSinkRequest{SinkName: proj + "/sinks/s"})
	require.NoError(t, err)
	assert.Equal(t, "severity>=ERROR", s.GetFilter())

	b, err := dst.config(t).GetBucket(t.Context(), &loggingpb.GetBucketRequest{Name: loc + "/buckets/kept"})
	require.NoError(t, err)
	assert.EqualValues(t, 77, b.GetRetentionDays())

	_, err = dst.config(t).GetView(t.Context(), &loggingpb.GetViewRequest{Name: loc + "/buckets/kept/views/v"})
	require.NoError(t, err)

	c, err := dst.config(t).
		GetCmekSettings(t.Context(), &loggingpb.GetCmekSettingsRequest{Name: proj + "/cmekSettings"})
	require.NoError(t, err)
	assert.NotEmpty(t, c.GetKmsKeyName())

	pts, err := dst.handler.Backend.MetricPoints(proj + "/metrics/m")
	require.NoError(t, err)
	require.Len(t, pts, 1)
	assert.EqualValues(t, 1, pts[0].Count)

	dst.write(t, textEntry("svc", "again", ltype.LogSeverity_ERROR, now))

	pts, err = dst.handler.Backend.MetricPoints(proj + "/metrics/m")
	require.NoError(t, err)
	assert.EqualValues(t, 2, pts[0].Count)

	dst.handler.Reset()
	assert.Empty(t, dst.list(t, &loggingpb.ListLogEntriesRequest{}))
}

func TestRestoreRejectsGarbage(t *testing.T) {
	t.Parallel()

	require.Error(t, newEnv(t, false).handler.Restore(t.Context(), []byte("not json")))
}

func TestProviderInit(t *testing.T) {
	t.Parallel()

	p := &gcplogging.Provider{}
	assert.Equal(t, "GCPLogging", p.Name())

	_, err := p.Init(nil)
	require.ErrorIs(t, err, gcplogging.ErrNilAppContext)

	reg, err := p.Init(&service.AppContext{})
	require.NoError(t, err)

	h, ok := reg.(*gcplogging.Handler)
	require.True(t, ok)
	assert.Equal(t, gcplogging.DefaultPort, h.Port)
	assert.Equal(t, "GCPLogging", h.Name())
	assert.NotEmpty(t, h.GetSupportedOperations())
}
