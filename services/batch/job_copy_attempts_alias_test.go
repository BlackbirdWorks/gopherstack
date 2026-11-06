package batch_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testsnap"
)

// Job copies handed out by ListJobs must not alias the open attempt the
// executor closes in place when the container exits.
func TestListJobs_AttemptsNotAliasedToExecutor(t *testing.T) {
	t.Parallel()

	gate := make(chan struct{})
	runner := &fakeRunner{gate: gate}
	e := newExecEnv(t, runner, execOpts{})
	job := e.submit(t, submitOpts{})
	e.waitStatus(t, job.JobID, "RUNNING")

	jobs, _, err := e.b.ListJobs(t.Context(), e.queue, "", "", 0, nil)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.Len(t, jobs[0].Attempts, 1)

	testsnap.Unchanged(t, jobs[0], func() {
		close(gate)
		e.waitStatus(t, job.JobID, "SUCCEEDED")
	})
}
