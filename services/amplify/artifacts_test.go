package amplify_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/amplify"
)

func TestAccessLogs(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	app := seedApp(t, b, "ArtifactApp")

	t.Run("nonexistent_app_errors", func(t *testing.T) {
		t.Parallel()

		_, err := b.GenerateAccessLogs("nonexistent", "", "", "")
		require.Error(t, err)
	})

	t.Run("existing_app_returns_url", func(t *testing.T) {
		t.Parallel()

		url, err := b.GenerateAccessLogs(app.AppID, "example.com", "2024-01-01", "2024-01-02")
		require.NoError(t, err)
		assert.NotEmpty(t, url)
	})
}

func TestGetArtifactURL_NotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, _, err := b.GetArtifactURL("nonexistent-artifact")
	require.Error(t, err)
}

// TestListArtifacts_ProducedByJobCompletion verifies the real producer path
// for Artifact records: nothing creates one until a job the janitor advances
// to SUCCEED does so (see janitor.go's advanceJobs / artifacts.go's
// newBuildArtifact). Before a job completes, ListArtifacts must legitimately
// return empty -- that is not the "no producer exists" bug PARITY.md used to
// flag, just a job that hasn't finished yet.
func TestListArtifacts_ProducedByJobCompletion(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	app := seedApp(t, b, "ArtifactApp")
	branch := seedMainBranch(t, b, app.AppID)

	t.Run("unknown_app_errors", func(t *testing.T) {
		t.Parallel()

		_, _, err := b.ListArtifacts("nonexistent", branch.BranchName, "job1", "", 0)
		require.Error(t, err)
	})

	t.Run("unknown_branch_errors", func(t *testing.T) {
		t.Parallel()

		_, _, err := b.ListArtifacts(app.AppID, "nonexistent", "job1", "", 0)
		require.Error(t, err)
	})

	t.Run("unknown_job_errors", func(t *testing.T) {
		t.Parallel()

		_, _, err := b.ListArtifacts(app.AppID, branch.BranchName, "nonexistent", "", 0)
		require.Error(t, err)
	})

	t.Run("running_job_has_no_artifacts_yet", func(t *testing.T) {
		t.Parallel()

		job, err := b.StartJob(app.AppID, branch.BranchName, "RELEASE", "", "", "", time.Time{})
		require.NoError(t, err)

		artifacts, _, err := b.ListArtifacts(app.AppID, branch.BranchName, job.JobID, "", 0)
		require.NoError(t, err)
		assert.Empty(t, artifacts)
	})

	t.Run("succeeded_job_has_artifacts_reachable_via_get_url", func(t *testing.T) {
		t.Parallel()

		job, err := b.StartJob(app.AppID, branch.BranchName, "RELEASE", "", "", "", time.Time{})
		require.NoError(t, err)

		j := amplify.NewJanitor(b, time.Second)
		j.SweepOnce(t.Context())

		artifacts, _, err := b.ListArtifacts(app.AppID, branch.BranchName, job.JobID, "", 0)
		require.NoError(t, err)
		require.Len(t, artifacts, 1)
		assert.Equal(t, "BUILD", artifacts[0].ArtifactType)
		assert.NotEmpty(t, artifacts[0].ArtifactFileName)

		artifactType, url, err := b.GetArtifactURL(artifacts[0].ArtifactID)
		require.NoError(t, err)
		assert.Equal(t, "BUILD", artifactType)
		assert.NotEmpty(t, url)
	})
}

// TestListArtifacts_CascadeDeletedWithJob verifies DeleteJob removes any
// artifacts it produced, and DeleteBranch/DeleteApp cascade the same way --
// otherwise a deleted job/branch/app would leave ghost artifact rows behind
// that no longer belong to anything ListApps/ListBranches/ListJobs can still
// reach.
func TestListArtifacts_CascadeDeletedWithJob(t *testing.T) {
	t.Parallel()

	t.Run("delete_job_removes_its_artifacts", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend()
		app := seedApp(t, b, "CascadeApp")
		branch := seedMainBranch(t, b, app.AppID)

		job, err := b.StartJob(app.AppID, branch.BranchName, "RELEASE", "", "", "", time.Time{})
		require.NoError(t, err)

		amplify.NewJanitor(b, time.Second).SweepOnce(t.Context())

		artifacts, _, err := b.ListArtifacts(app.AppID, branch.BranchName, job.JobID, "", 0)
		require.NoError(t, err)
		require.Len(t, artifacts, 1)

		artifactID := artifacts[0].ArtifactID

		_, err = b.DeleteJob(app.AppID, branch.BranchName, job.JobID)
		require.NoError(t, err)

		_, _, err = b.GetArtifactURL(artifactID)
		require.Error(t, err, "artifact must not survive its job's deletion")
	})

	t.Run("delete_app_removes_descendant_artifacts", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend()
		app := seedApp(t, b, "CascadeApp2")
		branch := seedMainBranch(t, b, app.AppID)

		job, err := b.StartJob(app.AppID, branch.BranchName, "RELEASE", "", "", "", time.Time{})
		require.NoError(t, err)

		amplify.NewJanitor(b, time.Second).SweepOnce(t.Context())

		artifacts, _, err := b.ListArtifacts(app.AppID, branch.BranchName, job.JobID, "", 0)
		require.NoError(t, err)
		require.Len(t, artifacts, 1)

		artifactID := artifacts[0].ArtifactID

		require.NoError(t, b.DeleteApp(app.AppID))

		_, _, err = b.GetArtifactURL(artifactID)
		require.Error(t, err, "artifact must not survive its app's deletion")
	})
}
