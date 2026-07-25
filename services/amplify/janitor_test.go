package amplify_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/amplify"
)

// TestNewJanitor_DefaultInterval verifies a zero interval falls back to a
// positive default rather than creating a busy-loop ticker.
func TestNewJanitor_DefaultInterval(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	j := amplify.NewJanitor(b, 0)
	assert.Positive(t, j.Interval)
}

// TestJanitor_AdvanceJobs verifies that RUNNING jobs are advanced to SUCCEED
// (with an EndTime set) by a sweep, while jobs already in a terminal state
// are left untouched.
func TestJanitor_AdvanceJobs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		makeJob    func(t *testing.T, b *amplify.InMemoryBackend, appID, branchName string) string // returns jobID
		wantStatus amplify.JobStatus
		wantEnd    bool
	}{
		{
			name: "running_job_advances_to_succeed",
			makeJob: func(t *testing.T, b *amplify.InMemoryBackend, appID, branchName string) string {
				t.Helper()

				job, err := b.StartJob(appID, branchName, "RELEASE", "", "abc123", "msg", time.Time{})
				require.NoError(t, err)

				return job.JobID
			},
			wantStatus: amplify.JobStatusSucceed,
			wantEnd:    true,
		},
		{
			name: "cancelled_job_is_untouched",
			makeJob: func(t *testing.T, b *amplify.InMemoryBackend, appID, branchName string) string {
				t.Helper()

				job, err := b.StartJob(appID, branchName, "RELEASE", "", "abc123", "msg", time.Time{})
				require.NoError(t, err)

				_, err = b.StopJob(appID, branchName, job.JobID)
				require.NoError(t, err)

				return job.JobID
			},
			wantStatus: amplify.JobStatusCancelled,
			wantEnd:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			app, err := b.CreateApp("TestApp", "", "", "", nil)
			require.NoError(t, err)

			branch, err := b.CreateBranch(app.AppID, "main", "", "", false, nil)
			require.NoError(t, err)

			jobID := tt.makeJob(t, b, app.AppID, branch.BranchName)

			j := amplify.NewJanitor(b, time.Minute)
			j.SweepOnce(context.Background())

			got, err := b.GetJob(app.AppID, branch.BranchName, jobID)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, got.Status)
			assert.NotEmpty(t, got.JobARN)

			if tt.wantEnd {
				assert.False(t, got.EndTime.IsZero())
			}
		})
	}
}

// TestJanitor_AdvanceDomains verifies that domain associations not yet in a
// terminal state are advanced to AVAILABLE, with every subdomain marked
// verified, while a deleted (no longer existing) domain association is left
// alone.
func TestJanitor_AdvanceDomains(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	app, err := b.CreateApp("TestApp", "", "", "", nil)
	require.NoError(t, err)

	subs := []amplify.SubDomainSetting{{Prefix: "www", BranchName: "main"}}

	da, err := b.CreateDomainAssociation(app.AppID, "example.com", subs, true)
	require.NoError(t, err)
	assert.Equal(t, amplify.DomainStatusPendingVerification, da.DomainStatus)
	require.Len(t, da.SubDomains, 1)
	assert.False(t, da.SubDomains[0].Verified)

	j := amplify.NewJanitor(b, time.Minute)
	j.SweepOnce(context.Background())

	got, err := b.GetDomainAssociation(app.AppID, "example.com")
	require.NoError(t, err)
	assert.Equal(t, amplify.DomainStatusAvailable, got.DomainStatus)
	require.Len(t, got.SubDomains, 1)
	assert.True(t, got.SubDomains[0].Verified)

	// A second sweep must be a no-op: AVAILABLE is terminal.
	j.SweepOnce(context.Background())

	got2, err := b.GetDomainAssociation(app.AppID, "example.com")
	require.NoError(t, err)
	assert.Equal(t, amplify.DomainStatusAvailable, got2.DomainStatus)
}

// TestJanitor_Run_StopsOnContextCancel verifies the background loop started
// by Run exits promptly once its context is cancelled, and that it advances
// state at least once via its ticker.
func TestJanitor_Run_StopsOnContextCancel(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	app, err := b.CreateApp("TestApp", "", "", "", nil)
	require.NoError(t, err)

	branch, err := b.CreateBranch(app.AppID, "main", "", "", false, nil)
	require.NoError(t, err)

	job, err := b.StartJob(app.AppID, branch.BranchName, "RELEASE", "", "", "", time.Time{})
	require.NoError(t, err)

	const tickInterval = 10 * time.Millisecond

	j := amplify.NewJanitor(b, tickInterval)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})

	go func() {
		j.Run(ctx)
		close(done)
	}()

	require.Eventually(t, func() bool {
		got, getErr := b.GetJob(app.AppID, branch.BranchName, job.JobID)

		return getErr == nil && got.Status == amplify.JobStatusSucceed
	}, time.Second, 5*time.Millisecond)

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("janitor did not stop after context cancellation")
	}
}
