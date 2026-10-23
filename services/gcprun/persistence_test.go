package gcprun_test

import (
	"testing"

	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPersistence_RoundTrip(t *testing.T) {
	t.Parallel()

	src := newEnv(t, true)
	svc := src.createService(t, "app", "example/app:1")
	job := src.createJob(t, "etl", jobSpec("example/etl:1", 2, 2, 0))

	op, err := src.jobs.RunJob(t.Context(), &runpb.RunJobRequest{Name: job.GetName()})
	require.NoError(t, err)

	ex, err := op.Wait(t.Context())
	require.NoError(t, err)

	snap := src.handler.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	dst := newEnv(t, true)
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	got, err := dst.services.GetService(t.Context(), &runpb.GetServiceRequest{Name: svc.GetName()})
	require.NoError(t, err)
	assert.Equal(t, svc.GetUid(), got.GetUid())
	assert.Equal(t, svc.GetLatestReadyRevision(), got.GetLatestReadyRevision())

	_, err = dst.revs.GetRevision(t.Context(), &runpb.GetRevisionRequest{Name: svc.GetLatestReadyRevision()})
	require.NoError(t, err)

	gotEx, err := dst.execs.GetExecution(t.Context(), &runpb.GetExecutionRequest{Name: ex.GetName()})
	require.NoError(t, err)
	assert.Equal(t, int32(2), gotEx.GetSucceededCount())

	tasks := dst.tasks.ListTasks(t.Context(), &runpb.ListTasksRequest{Parent: ex.GetName()})
	_, err = tasks.Next()
	require.NoError(t, err)

	host := got.GetUri()[len("http://"):]
	code, body := dst.get(t, host, "/")
	assert.Equal(t, 200, code, "a restored revision starts its container on first request")
	assert.Contains(t, body, "revision=")
}

func TestPersistence_InterruptedExecutionFails(t *testing.T) {
	t.Parallel()

	src := newEnv(t, true)
	src.rt.gate = make(chan struct{})
	job := src.createJob(t, "slow", jobSpec("i", 1, 1, 0))

	_, err := src.jobs.RunJob(t.Context(), &runpb.RunJobRequest{Name: job.GetName()})
	require.NoError(t, err)

	snap := src.handler.Snapshot(t.Context())

	dst := newEnv(t, true)
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	ex, err := dst.execs.ListExecutions(t.Context(), &runpb.ListExecutionsRequest{Parent: job.GetName()}).Next()
	require.NoError(t, err)
	assert.Equal(t, int32(1), ex.GetFailedCount())
	assert.NotNil(t, ex.GetCompletionTime())

	close(src.rt.gate)
}

func TestReset(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.createService(t, "app", "i")
	e.createJob(t, "etl", jobSpec("i", 1, 1, 0))

	e.handler.Reset()

	assert.Equal(t, 0, e.rt.running())

	_, err := e.services.ListServices(t.Context(), &runpb.ListServicesRequest{Parent: parent}).Next()
	assert.Error(t, err)
}
