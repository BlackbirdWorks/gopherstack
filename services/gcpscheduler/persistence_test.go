package gcpscheduler_test

import (
	"testing"
	"time"

	"cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/services/gcpscheduler"
)

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	tg := newTarget(t, always(500))

	keep := src.job(t, "keep", httpJob(tg.srv.URL), withSchedule("0 9 * * *"), retryCfg(4, 0),
		func(j *schedulerpb.Job) { j.TimeZone = "America/New_York"; j.Description = "d" })
	paused := src.job(t, "paused", httpJob(tg.srv.URL))

	_, err := src.client.PauseJob(t.Context(), &schedulerpb.PauseJobRequest{Name: paused.GetName()})
	require.NoError(t, err)

	_, err = src.client.RunJob(t.Context(), &schedulerpb.RunJobRequest{Name: keep.GetName()})
	require.NoError(t, err)
	src.handler.Backend.Drain()

	data := src.handler.Snapshot(t.Context())
	require.NotEmpty(t, data)

	dst := newEnv(t)
	require.NoError(t, dst.handler.Restore(t.Context(), data))

	got := dst.get(t, keep.GetName())
	assert.Equal(t, "America/New_York", got.GetTimeZone())
	assert.Equal(t, "d", got.GetDescription())
	assert.EqualValues(t, 4, got.GetRetryConfig().GetRetryCount())
	assert.EqualValues(t, code.Code_INTERNAL, got.GetStatus().GetCode())
	assert.NotNil(t, got.GetLastAttemptTime())
	assert.True(t, time.Date(2030, 1, 15, 14, 0, 0, 0, time.UTC).Equal(got.GetScheduleTime().AsTime()))

	assert.Equal(t, schedulerpb.Job_PAUSED, dst.get(t, paused.GetName()).GetState())

	dst.clk.advance(11 * time.Second)
	dst.dispatch()
	assert.Equal(t, 2, tg.count(), "the pending retry survives a restore")

	_, err = dst.client.CreateJob(t.Context(), &schedulerpb.CreateJobRequest{
		Parent: location, Job: &schedulerpb.Job{
			Name: keep.GetName(), Schedule: "* * * * *",
			Target: &schedulerpb.Job_HttpTarget{HttpTarget: &schedulerpb.HttpTarget{Uri: "https://x.test"}},
		},
	})
	assert.Equal(t, codes.AlreadyExists, status.Code(err))
}

func TestRestoreRejectsGarbageAndOldVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    string
		wantErr bool
	}{
		{"garbage", "not json", true},
		{"old version starts empty", `{"version":0,"jobs":{"x":{}}}`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := gcpscheduler.NewInMemoryBackend().Restore(t.Context(), []byte(tt.data))
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestResetClearsState(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	j := e.job(t, "reset", httpJob("https://example.test"))

	e.handler.Reset()

	_, err := e.client.GetJob(t.Context(), &schedulerpb.GetJobRequest{Name: j.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))
}
