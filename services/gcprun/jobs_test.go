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
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestJob_CRUD(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	job := e.createJob(t, "etl", jobSpec("example/etl:1", 0, 0, 1))

	assert.Equal(t, parent+"/jobs/etl", job.GetName())
	assert.Equal(t, int32(1), job.GetTemplate().GetTaskCount())
	assert.Equal(t, int32(1), job.GetTemplate().GetTemplate().GetMaxRetries())
	assert.Equal(t, int64(600), job.GetTemplate().GetTemplate().GetTimeout().GetSeconds())
	assert.Equal(t, runpb.Condition_CONDITION_SUCCEEDED, job.GetTerminalCondition().GetState())

	upd := jobSpec("example/etl:2", 4, 2, 0)
	upd.Name = job.GetName()

	op, err := e.jobs.UpdateJob(t.Context(), &runpb.UpdateJobRequest{Job: upd})
	require.NoError(t, err)

	got, err := op.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(2), got.GetGeneration())
	assert.Equal(t, int32(4), got.GetTemplate().GetTaskCount())

	_, err = e.jobs.CreateJob(
		t.Context(),
		&runpb.CreateJobRequest{Parent: parent, JobId: "etl", Job: jobSpec("i", 1, 1, 0)},
	)
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	it := e.jobs.ListJobs(t.Context(), &runpb.ListJobsRequest{Parent: parent})
	listed, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, job.GetName(), listed.GetName())

	_, err = it.Next()
	require.ErrorIs(t, err, iterator.Done)

	del, err := e.jobs.DeleteJob(t.Context(), &runpb.DeleteJobRequest{Name: job.GetName()})
	require.NoError(t, err)

	_, err = del.Wait(t.Context())
	require.NoError(t, err)

	_, err = e.jobs.GetJob(t.Context(), &runpb.GetJobRequest{Name: job.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestJob_CreateErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		job  *runpb.Job
		name string
		id   string
	}{
		{jobSpec("i", 1, 1, 0), "bad id", "Bad"},
		{&runpb.Job{}, "no template", "ok"},
		{jobSpec("", 1, 1, 0), "no image", "ok"},
		{jobSpec("i", -1, 1, 0), "negative tasks", "ok"},
		{jobSpec("i", 1, 1, 11), "too many retries", "ok"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			_, err := e.jobs.CreateJob(t.Context(), &runpb.CreateJobRequest{Parent: parent, JobId: tt.id, Job: tt.job})
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}

func TestRunJob_Tasks(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	job := e.createJob(t, "etl", jobSpec("example/etl:1", 3, 2, 0))

	op, err := e.jobs.RunJob(t.Context(), &runpb.RunJobRequest{Name: job.GetName()})
	require.NoError(t, err)

	ex, err := op.Wait(t.Context())
	require.NoError(t, err)

	assert.Equal(t, int32(3), ex.GetTaskCount())
	assert.Equal(t, int32(2), ex.GetParallelism())
	assert.Equal(t, int32(3), ex.GetSucceededCount())
	assert.Equal(t, int32(0), ex.GetFailedCount())
	assert.NotNil(t, ex.GetCompletionTime())
	assert.Equal(t, runpb.Condition_CONDITION_SUCCEEDED, ex.GetConditions()[0].GetState())

	indexes := map[string]bool{}

	for _, s := range e.rt.specs() {
		indexes[envOf(s, "CLOUD_RUN_TASK_INDEX")] = true

		assert.Equal(t, "3", envOf(s, "CLOUD_RUN_TASK_COUNT"))
		assert.Equal(t, "etl", envOf(s, "CLOUD_RUN_JOB"))
	}

	assert.Equal(t, map[string]bool{"0": true, "1": true, "2": true}, indexes)
	assert.Equal(t, 0, e.rt.running(), "task containers are removed")

	tasks := e.tasks.ListTasks(t.Context(), &runpb.ListTasksRequest{Parent: ex.GetName()})
	n := int32(0)

	for {
		task, nerr := tasks.Next()
		if nerr != nil {
			break
		}

		assert.Equal(t, n, task.GetIndex())
		assert.Equal(t, runpb.Condition_CONDITION_SUCCEEDED, task.GetConditions()[0].GetState())

		one, gerr := e.tasks.GetTask(t.Context(), &runpb.GetTaskRequest{Name: task.GetName()})
		require.NoError(t, gerr)
		assert.Equal(t, task.GetUid(), one.GetUid())

		n++
	}

	assert.Equal(t, int32(3), n)

	got, err := e.jobs.GetJob(t.Context(), &runpb.GetJobRequest{Name: job.GetName()})
	require.NoError(t, err)
	assert.Equal(t, int32(1), got.GetExecutionCount())
	assert.Equal(t, ex.GetName(), got.GetLatestCreatedExecution().GetName())
}

func TestRunJob_FailureAndRetries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		retries      int32
		wantAttempts int
	}{
		{"no retries", 0, 1},
		{"one retry", 1, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			e.rt.exitFn = func(func(string) string) int { return 3 }
			job := e.createJob(t, "bad", jobSpec("i", 1, 1, tt.retries))

			op, err := e.jobs.RunJob(t.Context(), &runpb.RunJobRequest{Name: job.GetName()})
			require.NoError(t, err)

			ex, err := op.Wait(t.Context())
			require.NoError(t, err)

			assert.Equal(t, int32(1), ex.GetFailedCount())
			assert.Equal(t, runpb.Condition_CONDITION_FAILED, ex.GetConditions()[0].GetState())
			assert.Len(t, e.rt.specs(), tt.wantAttempts)

			task, err := e.tasks.ListTasks(t.Context(), &runpb.ListTasksRequest{Parent: ex.GetName()}).Next()
			require.NoError(t, err)
			assert.Equal(t, int32(3), task.GetLastAttemptResult().GetExitCode())
			assert.Contains(t, task.GetLastAttemptResult().GetStatus().GetMessage(), "exit(3)")
		})
	}
}

func TestRunJob_Overrides(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	job := e.createJob(t, "etl", jobSpec("i", 1, 1, 0))

	op, err := e.jobs.RunJob(t.Context(), &runpb.RunJobRequest{
		Name: job.GetName(),
		Overrides: &runpb.RunJobRequest_Overrides{
			TaskCount: 2,
			Timeout:   durationpb.New(30 * time.Second),
			ContainerOverrides: []*runpb.RunJobRequest_Overrides_ContainerOverride{{
				Args: []string{"--fast"},
				Env:  []*runpb.EnvVar{{Name: "MODE", Values: &runpb.EnvVar_Value{Value: "x"}}},
			}},
		},
	})
	require.NoError(t, err)

	ex, err := op.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int32(2), ex.GetTaskCount())

	for _, s := range e.rt.specs() {
		assert.Equal(t, []string{"--fast"}, s.Cmd)
		assert.Equal(t, "x", envOf(s, "MODE"))
	}

	_, err = e.jobs.RunJob(t.Context(), &runpb.RunJobRequest{
		Name: job.GetName(),
		Overrides: &runpb.RunJobRequest_Overrides{
			ContainerOverrides: []*runpb.RunJobRequest_Overrides_ContainerOverride{{Name: "nope"}},
		},
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = e.jobs.RunJob(t.Context(), &runpb.RunJobRequest{Name: parent + "/jobs/none"})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestRunJob_NoRuntimeFailsExecution(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	job := e.createJob(t, "etl", jobSpec("i", 2, 2, 3))

	op, err := e.jobs.RunJob(t.Context(), &runpb.RunJobRequest{Name: job.GetName()})
	require.NoError(t, err)

	ex, err := op.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int32(2), ex.GetFailedCount())

	task, err := e.tasks.ListTasks(t.Context(), &runpb.ListTasksRequest{Parent: ex.GetName()}).Next()
	require.NoError(t, err)
	assert.Contains(t, task.GetConditions()[0].GetMessage(), "container runtime is unavailable")
}

func TestExecution_CancelAndDelete(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.rt.gate = make(chan struct{})
	job := e.createJob(t, "slow", jobSpec("i", 3, 1, 0))

	op, err := e.jobs.RunJob(t.Context(), &runpb.RunJobRequest{Name: job.GetName()})
	require.NoError(t, err)

	name := op.Name()
	require.NotEmpty(t, name)

	var exName string

	require.Eventually(t, func() bool {
		it := e.execs.ListExecutions(t.Context(), &runpb.ListExecutionsRequest{Parent: job.GetName()})
		ex, nerr := it.Next()
		if nerr != nil {
			return false
		}

		exName = ex.GetName()

		return ex.GetRunningCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	cancel, err := e.execs.CancelExecution(t.Context(), &runpb.CancelExecutionRequest{Name: exName})
	require.NoError(t, err)

	ex, err := cancel.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int32(3), ex.GetCancelledCount())
	assert.Equal(t, runpb.Condition_CONDITION_FAILED, ex.GetConditions()[0].GetState())

	_, err = e.execs.CancelExecution(t.Context(), &runpb.CancelExecutionRequest{Name: exName})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "completed execution cannot be cancelled")

	final, err := op.Wait(t.Context())
	require.NoError(t, err)
	assert.NotNil(t, final.GetCompletionTime())

	del, err := e.execs.DeleteExecution(t.Context(), &runpb.DeleteExecutionRequest{Name: exName})
	require.NoError(t, err)

	deleted, err := del.Wait(t.Context())
	require.NoError(t, err)
	assert.NotNil(t, deleted.GetDeleteTime())

	_, err = e.execs.GetExecution(t.Context(), &runpb.GetExecutionRequest{Name: exName})
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = e.tasks.ListTasks(t.Context(), &runpb.ListTasksRequest{Parent: exName}).Next()
	assert.Equal(t, codes.NotFound, status.Code(err))
}
