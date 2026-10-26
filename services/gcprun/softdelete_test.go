package gcprun_test

import (
	"testing"
	"time"

	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func names[T interface{ GetName() string }](t *testing.T, next func() (T, error)) []string {
	t.Helper()

	var out []string

	for {
		v, err := next()
		if err != nil {
			require.ErrorIs(t, err, iterator.Done)

			return out
		}

		out = append(out, v.GetName())
	}
}

func TestSoftDelete_Service(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	svc := e.createService(t, "web", "example/web:1")

	op, err := e.services.DeleteService(t.Context(), &runpb.DeleteServiceRequest{Name: svc.GetName()})
	require.NoError(t, err)

	deleted, err := op.Wait(t.Context())
	require.NoError(t, err)
	require.NotNil(t, deleted.GetDeleteTime())
	require.NotNil(t, deleted.GetExpireTime())
	assert.True(t, deleted.GetExpireTime().AsTime().After(deleted.GetDeleteTime().AsTime()))

	_, err = e.services.GetService(t.Context(), &runpb.GetServiceRequest{Name: svc.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))

	active := e.services.ListServices(t.Context(), &runpb.ListServicesRequest{Parent: parent})
	assert.Empty(t, names(t, active.Next))

	shown := e.services.ListServices(t.Context(), &runpb.ListServicesRequest{Parent: parent, ShowDeleted: true})
	assert.Equal(t, []string{svc.GetName()}, names(t, shown.Next))

	again := e.createService(t, "web", "example/web:2")
	both := e.services.ListServices(t.Context(), &runpb.ListServicesRequest{Parent: parent, ShowDeleted: true})
	assert.Len(t, names(t, both.Next), 2, "recreated service and its tombstone")
	assert.Nil(t, again.GetDeleteTime())

	e.handler.Backend.SetClock(fixedClock(time.Now().Add(400 * 24 * time.Hour)))

	expired := e.services.ListServices(t.Context(), &runpb.ListServicesRequest{Parent: parent, ShowDeleted: true})
	assert.Len(t, names(t, expired.Next), 1, "expired tombstones are gone")
}

func TestSoftDelete_JobAndExecution(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	job := e.createJob(t, "etl", jobSpec("example/etl:1", 1, 1, 0))

	run, err := e.jobs.RunJob(t.Context(), &runpb.RunJobRequest{Name: job.GetName()})
	require.NoError(t, err)

	ex, err := run.Wait(t.Context())
	require.NoError(t, err)

	delEx, err := e.execs.DeleteExecution(t.Context(), &runpb.DeleteExecutionRequest{Name: ex.GetName()})
	require.NoError(t, err)

	gone, err := delEx.Wait(t.Context())
	require.NoError(t, err)
	assert.NotNil(t, gone.GetExpireTime())

	live := e.execs.ListExecutions(t.Context(), &runpb.ListExecutionsRequest{Parent: job.GetName()})
	assert.Empty(t, names(t, live.Next))

	shown := e.execs.ListExecutions(t.Context(), &runpb.ListExecutionsRequest{Parent: job.GetName(), ShowDeleted: true})
	assert.Equal(t, []string{ex.GetName()}, names(t, shown.Next))

	delJob, err := e.jobs.DeleteJob(t.Context(), &runpb.DeleteJobRequest{Name: job.GetName()})
	require.NoError(t, err)

	gj, err := delJob.Wait(t.Context())
	require.NoError(t, err)
	assert.NotNil(t, gj.GetExpireTime())

	jobs := e.jobs.ListJobs(t.Context(), &runpb.ListJobsRequest{Parent: parent, ShowDeleted: true})
	assert.Equal(t, []string{job.GetName()}, names(t, jobs.Next))

	plain := e.jobs.ListJobs(t.Context(), &runpb.ListJobsRequest{Parent: parent})
	assert.Empty(t, names(t, plain.Next))
}
