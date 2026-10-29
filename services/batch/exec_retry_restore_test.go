package batch_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

func TestExecRetryAttemptsSurviveRestore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	gate := make(chan struct{})
	runner := &fakeRunner{gate: gate, behave: func(int, batch.ContainerSpec) fakeResult { return fakeResult{exit: 1} }}
	e := newExecEnv(t, runner, execOpts{})
	job := e.submit(t, submitOpts{retry: &batch.RetryStrategy{Attempts: 2}})
	e.waitStatus(t, job.JobID, "RUNNING")

	_, err := e.b.UpdateComputeEnvironment(ctx, "ce", "DISABLED", "", nil, nil, nil)
	require.NoError(t, err)

	close(gate)

	before := e.waitStatus(t, job.JobID, "RUNNABLE")
	require.Len(t, before.Attempts, 1)

	snap := e.b.Snapshot(ctx)

	b2 := batch.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(ctx, snap))

	r2 := &fakeRunner{behave: func(int, batch.ContainerSpec) fakeResult { return fakeResult{exit: 1} }}
	b2.SetContainerRunner(r2)
	b2.SetJobLogSink(&fakeSink{})
	b2.SetExecutionEndpoint("http://host.docker.internal:4566")
	t.Cleanup(b2.StopExecution)

	_, err = b2.UpdateComputeEnvironment(ctx, "ce", "ENABLED", "", nil, nil, nil)
	require.NoError(t, err)

	b2.DispatchJobs()

	describe := func() *batch.Job {
		got := b2.DescribeJobs(ctx, []string{job.JobID})
		require.Len(t, got, 1)

		return got[0]
	}

	require.Eventually(t, func() bool { return describe().Status == "FAILED" }, 10*time.Second, 5*time.Millisecond)

	assert.Len(t, describe().Attempts, 2)
	assert.Equal(t, 1, int(r2.calls.Load()))
}
