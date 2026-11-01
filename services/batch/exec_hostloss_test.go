package batch_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

func TestExecHostLossAfterRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		retry        *batch.RetryStrategy
		name         string
		wantStatus   string
		wantReason   string
		wantAttempts int
	}{
		{
			name:         "no retry strategy fails",
			wantStatus:   "FAILED",
			wantReason:   "Host EC2 terminated",
			wantAttempts: 1,
		},
		{
			name:         "attempts requeue",
			retry:        &batch.RetryStrategy{Attempts: 3},
			wantStatus:   "RUNNING",
			wantAttempts: 2,
		},
		{
			name: "exit on host status reason",
			retry: &batch.RetryStrategy{Attempts: 3, EvaluateOnExit: []batch.EvaluateOnExit{
				{Action: "EXIT", OnStatusReason: "Host EC2*"},
			}},
			wantStatus:   "FAILED",
			wantReason:   "Host EC2 terminated",
			wantAttempts: 1,
		},
		{
			name: "retry on host status reason",
			retry: &batch.RetryStrategy{Attempts: 3, EvaluateOnExit: []batch.EvaluateOnExit{
				{Action: "RETRY", OnStatusReason: "Host EC2*"},
				{Action: "EXIT", OnReason: "*"},
			}},
			wantStatus:   "RUNNING",
			wantAttempts: 2,
		},
		{
			name:         "attempts exhausted",
			retry:        &batch.RetryStrategy{Attempts: 1},
			wantStatus:   "FAILED",
			wantReason:   "Host EC2 terminated",
			wantAttempts: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runner := &fakeRunner{behave: func(int, batch.ContainerSpec) fakeResult { return fakeResult{block: true} }}
			e := newExecEnv(t, runner, execOpts{})
			job := e.submit(t, submitOpts{retry: tt.retry})
			e.waitStatus(t, job.JobID, "RUNNING")

			snap := e.b.Snapshot(context.Background())

			b2 := batch.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b2.Restore(context.Background(), snap))
			r2 := &fakeRunner{behave: func(int, batch.ContainerSpec) fakeResult { return fakeResult{block: true} }}
			b2.SetContainerRunner(r2)
			b2.SetJobLogSink(&fakeSink{})
			b2.SetExecutionEndpoint("http://host.docker.internal:4566")
			t.Cleanup(b2.StopExecution)
			b2.DispatchJobs()

			describe := func() *batch.Job {
				got := b2.DescribeJobs(context.Background(), []string{job.JobID})
				require.Len(t, got, 1)

				return got[0]
			}

			if tt.wantStatus == "RUNNING" {
				require.Eventually(t, func() bool { return describe().Status == "RUNNING" },
					10*time.Second, time.Millisecond)
			}

			j := describe()

			assert.Equal(t, tt.wantReason, j.StatusReason)
			require.Len(t, j.Attempts, tt.wantAttempts)
			assert.Equal(t, "Host EC2 terminated", j.Attempts[0].StatusReason)
			assert.NotNil(t, j.Attempts[0].StoppedAt)

			if c := j.Attempts[0].Container; c != nil {
				assert.Nil(t, c.ExitCode)
				assert.Empty(t, c.Reason)
			}

			if tt.wantStatus == "FAILED" {
				assert.Equal(t, "FAILED", j.Status)
				assert.Zero(t, r2.calls.Load())

				return
			}

			assert.Equal(t, "RUNNING", j.Status)
			assert.Nil(t, j.Attempts[1].StoppedAt)
		})
	}
}
