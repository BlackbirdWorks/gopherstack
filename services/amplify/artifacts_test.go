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

	tests := []struct {
		name         string
		domainName   string
		startTime    string
		endTime      string
		useRealApp   bool
		wantErr      bool
		wantNonEmpty bool
	}{
		{
			name:         "nonexistent_app_errors",
			useRealApp:   false,
			domainName:   "",
			startTime:    "",
			endTime:      "",
			wantErr:      true,
			wantNonEmpty: false,
		},
		{
			name:         "existing_app_returns_url",
			useRealApp:   true,
			domainName:   "example.com",
			startTime:    "2024-01-01",
			endTime:      "2024-01-02",
			wantErr:      false,
			wantNonEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			appID := "nonexistent"
			if tt.useRealApp {
				app := seedApp(t, b, "ArtifactApp")
				appID = app.AppID
			}

			url, err := b.GenerateAccessLogs(appID, tt.domainName, tt.startTime, tt.endTime)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tt.wantNonEmpty {
					assert.NotEmpty(t, url)
				}
			}
		})
	}
}

func TestGetArtifactURL_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		artifactID string
		wantErr    bool
	}{
		{
			name:       "nonexistent_artifact",
			artifactID: "nonexistent-artifact",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			_, _, err := b.GetArtifactURL(tt.artifactID)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestListArtifacts_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		appID      string
		branchName string
		jobID      string
		wantErr    bool
	}{
		{
			name:       "unknown_app_errors",
			appID:      "nonexistent",
			branchName: "main",
			jobID:      "job1",
			wantErr:    true,
		},
		{
			name:       "unknown_branch_errors",
			appID:      "valid",
			branchName: "nonexistent",
			jobID:      "job1",
			wantErr:    true,
		},
		{
			name:       "unknown_job_errors",
			appID:      "valid",
			branchName: "main",
			jobID:      "nonexistent",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			app := seedApp(t, b, "ArtifactApp")
			branch := seedMainBranch(t, b, app.AppID)

			targetApp := tt.appID
			if targetApp == "valid" {
				targetApp = app.AppID
			}
			targetBranch := tt.branchName
			if targetBranch == "main" {
				targetBranch = branch.BranchName
			}

			_, _, err := b.ListArtifacts(targetApp, targetBranch, tt.jobID, "", 0)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestListArtifacts_ProducedByJobCompletion verifies the real producer path
// for Artifact records: nothing creates one until a job the janitor advances
// to SUCCEED does so (see janitor.go's advanceJobs / artifacts.go's
// newBuildArtifact). Before a job completes, ListArtifacts must legitimately
// return empty. Each test case operates on an isolated backend instance
// to prevent SweepOnce in one parallel subtest from advancing jobs in another.
func TestListArtifacts_ProducedByJobCompletion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		runJanitor    bool
		wantArtifacts int
	}{
		{
			name:          "running_job_has_no_artifacts_yet",
			runJanitor:    false,
			wantArtifacts: 0,
		},
		{
			name:          "succeeded_job_has_artifacts_reachable_via_get_url",
			runJanitor:    true,
			wantArtifacts: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			app := seedApp(t, b, "ArtifactApp")
			branch := seedMainBranch(t, b, app.AppID)

			job, err := b.StartJob(app.AppID, branch.BranchName, "RELEASE", "", "", "", time.Time{})
			require.NoError(t, err)

			if tt.runJanitor {
				j := amplify.NewJanitor(b, time.Second)
				j.SweepOnce(t.Context())
			}

			artifacts, _, err := b.ListArtifacts(app.AppID, branch.BranchName, job.JobID, "", 0)
			require.NoError(t, err)
			assert.Len(t, artifacts, tt.wantArtifacts)

			if tt.wantArtifacts > 0 {
				assert.Equal(t, "BUILD", artifacts[0].ArtifactType)
				assert.NotEmpty(t, artifacts[0].ArtifactFileName)

				gotID, artURL, getErr := b.GetArtifactURL(artifacts[0].ArtifactID)
				require.NoError(t, getErr)
				assert.Equal(t, artifacts[0].ArtifactID, gotID)
				assert.NotEmpty(t, artURL)
			}
		})
	}
}

// TestListArtifacts_CascadeDeletedWithJob verifies DeleteJob removes any
// artifacts it produced, and DeleteBranch/DeleteApp cascade the same way --
// otherwise a deleted job/branch/app would leave ghost artifact rows behind
// that no longer belong to anything ListApps/ListBranches/ListJobs can still
// reach.
func TestListArtifacts_CascadeDeletedWithJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		deleteType string
	}{
		{
			name:       "delete_job_removes_its_artifacts",
			deleteType: "job",
		},
		{
			name:       "delete_app_removes_descendant_artifacts",
			deleteType: "app",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

			switch tt.deleteType {
			case "job":
				_, err = b.DeleteJob(app.AppID, branch.BranchName, job.JobID)
				require.NoError(t, err)
			case "app":
				_, err = b.DeleteApp(app.AppID)
				require.NoError(t, err)
			}

			_, _, err = b.GetArtifactURL(artifactID)
			require.Error(t, err, "artifact must not survive deletion")
		})
	}
}
