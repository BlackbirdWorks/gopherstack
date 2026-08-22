package amplify_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/amplify"
)

func TestInMemoryBackend_CreateBranch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs           error
		setup           func(*amplify.InMemoryBackend) string
		tags            map[string]string
		name            string
		branchName      string
		description     string
		stage           string
		enableAutoBuild bool
		wantErr         bool
	}{
		{
			name: "creates_branch_for_existing_app",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return app.AppID
			},
			branchName:      "main",
			description:     "Main branch",
			stage:           "PRODUCTION",
			enableAutoBuild: true,
			tags:            map[string]string{"branch": "main"},
		},
		{
			name: "returns_not_found_for_missing_app",
			setup: func(_ *amplify.InMemoryBackend) string {
				return "nonexistent"
			},
			branchName: "main",
			wantErr:    true,
			errIs:      awserr.ErrNotFound,
		},
		{
			name: "returns_already_exists_for_duplicate_branch",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)
				_, _ = b.CreateBranch(app.AppID, "main", "", "", false, nil)

				return app.AppID
			},
			branchName: "main",
			wantErr:    true,
			errIs:      awserr.ErrAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			appID := tt.setup(b)
			branch, err := b.CreateBranch(appID, tt.branchName, tt.description, tt.stage, tt.enableAutoBuild, tt.tags)

			if tt.wantErr {
				require.Error(t, err)

				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.branchName, branch.BranchName)
			assert.Equal(t, appID, branch.AppID)
			assert.Equal(t, tt.enableAutoBuild, branch.EnableAutoBuild)
			assert.Equal(t, amplify.Stage(tt.stage), branch.Stage)
			assert.NotEmpty(t, branch.BranchARN)
			assert.Contains(t, branch.BranchARN, appID)
			assert.Contains(t, branch.BranchARN, tt.branchName)
			assert.False(t, branch.CreateTime.IsZero())
		})
	}
}

func TestInMemoryBackend_GetBranch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs   error
		setup   func(*amplify.InMemoryBackend) (string, string)
		name    string
		wantErr bool
	}{
		{
			name: "returns_existing_branch",
			setup: func(b *amplify.InMemoryBackend) (string, string) {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)
				_, _ = b.CreateBranch(app.AppID, "main", "", "", false, nil)

				return app.AppID, "main"
			},
		},
		{
			name: "returns_not_found_for_missing_branch",
			setup: func(b *amplify.InMemoryBackend) (string, string) {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return app.AppID, "nonexistent"
			},
			wantErr: true,
			errIs:   awserr.ErrNotFound,
		},
		{
			name: "returns_not_found_for_missing_app",
			setup: func(_ *amplify.InMemoryBackend) (string, string) {
				return "nonexistent", "main"
			},
			wantErr: true,
			errIs:   awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			appID, branchName := tt.setup(b)
			branch, err := b.GetBranch(appID, branchName)

			if tt.wantErr {
				require.Error(t, err)

				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, branchName, branch.BranchName)
			assert.Equal(t, appID, branch.AppID)
		})
	}
}

func TestInMemoryBackend_ListBranches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs     error
		setup     func(*amplify.InMemoryBackend) string
		name      string
		wantCount int
		wantErr   bool
	}{
		{
			name: "returns_empty_list_for_new_app",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return app.AppID
			},
			wantCount: 0,
		},
		{
			name: "returns_all_branches",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)
				_, _ = b.CreateBranch(app.AppID, "main", "", "", false, nil)
				_, _ = b.CreateBranch(app.AppID, "dev", "", "", true, nil)

				return app.AppID
			},
			wantCount: 2,
		},
		{
			name: "returns_not_found_for_missing_app",
			setup: func(_ *amplify.InMemoryBackend) string {
				return "nonexistent"
			},
			wantErr: true,
			errIs:   awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			appID := tt.setup(b)
			branches, _, err := b.ListBranches(appID, "", 0)

			if tt.wantErr {
				require.Error(t, err)

				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)
			assert.Len(t, branches, tt.wantCount)
		})
	}
}

func TestInMemoryBackend_ListBranchesPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		nextToken     string
		maxResults    int
		wantCount     int
		wantNextToken bool
	}{
		{
			name:       "no_limit_returns_all",
			maxResults: 0,
			wantCount:  4,
		},
		{
			name:          "first_page",
			maxResults:    2,
			wantCount:     2,
			wantNextToken: true,
		},
		{
			name:       "second_page",
			maxResults: 2,
			nextToken:  "2",
			wantCount:  2,
		},
		{
			name:       "token_beyond_end",
			maxResults: 2,
			nextToken:  "100",
			wantCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			app, err := b.CreateApp("PaginationApp", "", "", "", nil)
			require.NoError(t, err)

			for _, name := range []string{"br1", "br2", "br3", "br4"} {
				_, err = b.CreateBranch(app.AppID, name, "", "", false, nil)
				require.NoError(t, err)
			}

			branches, outToken, err := b.ListBranches(app.AppID, tt.nextToken, tt.maxResults)
			require.NoError(t, err)
			assert.Len(t, branches, tt.wantCount)

			if tt.wantNextToken {
				assert.NotEmpty(t, outToken)
			} else {
				assert.Empty(t, outToken)
			}
		})
	}
}

func TestInMemoryBackend_DeleteBranch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs   error
		setup   func(*amplify.InMemoryBackend) (string, string)
		name    string
		wantErr bool
	}{
		{
			name: "deletes_existing_branch",
			setup: func(b *amplify.InMemoryBackend) (string, string) {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)
				_, _ = b.CreateBranch(app.AppID, "main", "", "", false, nil)

				return app.AppID, "main"
			},
		},
		{
			name: "returns_not_found_for_missing_branch",
			setup: func(b *amplify.InMemoryBackend) (string, string) {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return app.AppID, "nonexistent"
			},
			wantErr: true,
			errIs:   awserr.ErrNotFound,
		},
		{
			name: "returns_not_found_for_missing_app",
			setup: func(_ *amplify.InMemoryBackend) (string, string) {
				return "nonexistent", "main"
			},
			wantErr: true,
			errIs:   awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			appID, branchName := tt.setup(b)
			deleted, err := b.DeleteBranch(appID, branchName)

			if tt.wantErr {
				require.Error(t, err)

				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)
			require.NotNil(t, deleted)
			assert.Equal(t, branchName, deleted.BranchName)

			_, getErr := b.GetBranch(appID, branchName)
			require.Error(t, getErr)
		})
	}
}

// TestInMemoryBackend_CreateBranch_InvalidStage verifies real Amplify's
// server-side Stage enum validation: CreateBranch rejects any value outside
// PRODUCTION/BETA/DEVELOPMENT/EXPERIMENTAL/PULL_REQUEST with a
// BadRequestException.
func TestInMemoryBackend_CreateBranch_InvalidStage(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	app := seedApp(t, b, "BadStageApp")

	_, err := b.CreateBranch(app.AppID, "main", "", "STAGING", false, nil)
	require.Error(t, err, "STAGING is not a real Amplify Stage value -- BETA is")
	require.ErrorIs(t, err, awserr.ErrInvalidParameter)
}

// TestInMemoryBackend_CreateBranch_FieldParity verifies the full Branch
// field set added for real-Amplify parity: create-time defaults (DisplayName
// defaults to the branch name, TTL defaults to "5", EnvironmentVariables
// defaults to a non-nil empty map) and that BranchOptions fields round-trip.
func TestInMemoryBackend_CreateBranch_FieldParity(t *testing.T) {
	t.Parallel()

	t.Run("defaults_when_opts_omitted", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend()
		app := seedApp(t, b, "DefaultsApp")

		branch, err := b.CreateBranch(app.AppID, "main", "", "", false, nil)
		require.NoError(t, err)

		assert.Equal(t, "main", branch.DisplayName)
		assert.Equal(t, "5", branch.TTL)
		assert.NotNil(t, branch.EnvironmentVariables)
		assert.Empty(t, branch.EnvironmentVariables)
		assert.Equal(t, "0", branch.TotalNumberOfJobs)
		assert.Empty(t, branch.ActiveJobID)
	})

	t.Run("opts_round_trip", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend()
		app := seedApp(t, b, "OptsApp")
		displayName := "custom-display"
		framework := "React"

		branch, err := b.CreateBranch(app.AppID, "main", "", "", false, nil, amplify.BranchOptions{
			DisplayName: &displayName,
			Framework:   &framework,
			EnvironmentVariables: map[string]string{
				"NODE_ENV": "production",
			},
		})
		require.NoError(t, err)

		assert.Equal(t, "custom-display", branch.DisplayName)
		assert.Equal(t, "React", branch.Framework)
		assert.Equal(t, map[string]string{"NODE_ENV": "production"}, branch.EnvironmentVariables)
	})
}

// TestInMemoryBackend_UpdateBranch_PartialSemantics verifies UpdateBranch's
// partial-update contract: a BranchOptions field left nil leaves the
// existing value unchanged.
func TestInMemoryBackend_UpdateBranch_PartialSemantics(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	app := seedApp(t, b, "UpdateBranchApp")
	framework := "React"

	branch, err := b.CreateBranch(app.AppID, "main", "", "", false, nil, amplify.BranchOptions{
		Framework: &framework,
	})
	require.NoError(t, err)
	require.Equal(t, "React", branch.Framework)

	updated, err := b.UpdateBranch(app.AppID, "main", "new description", "", false)
	require.NoError(t, err)

	assert.Equal(t, "new description", updated.Description)
	assert.Equal(t, "React", updated.Framework, "unset opts field must not be reset")
}

// TestInMemoryBackend_UpdateBranch_InvalidStage mirrors
// TestInMemoryBackend_CreateBranch_InvalidStage for UpdateBranch.
func TestInMemoryBackend_UpdateBranch_InvalidStage(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	app := seedApp(t, b, "BadStageUpdateApp")
	branch := seedMainBranch(t, b, app.AppID)

	_, err := b.UpdateBranch(app.AppID, branch.BranchName, "", "STAGING", false)
	require.Error(t, err)
	require.ErrorIs(t, err, awserr.ErrInvalidParameter)
}

// TestInMemoryBackend_TotalNumberOfJobsAndActiveJobID verifies these two
// computed-at-read-time Branch fields (see InMemoryBackend.branchView):
// TotalNumberOfJobs counts every job under the branch, and ActiveJobID names
// the most recently started one.
func TestInMemoryBackend_TotalNumberOfJobsAndActiveJobID(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	app := seedApp(t, b, "JobCountApp")
	branch := seedMainBranch(t, b, app.AppID)

	job1, err := b.StartJob(app.AppID, branch.BranchName, "RELEASE", "", "", "", time.Time{})
	require.NoError(t, err)

	got, err := b.GetBranch(app.AppID, branch.BranchName)
	require.NoError(t, err)
	assert.Equal(t, "1", got.TotalNumberOfJobs)
	assert.Equal(t, job1.JobID, got.ActiveJobID)

	job2, err := b.StartJob(app.AppID, branch.BranchName, "RELEASE", "", "", "", time.Time{})
	require.NoError(t, err)

	got, err = b.GetBranch(app.AppID, branch.BranchName)
	require.NoError(t, err)
	assert.Equal(t, "2", got.TotalNumberOfJobs)
	assert.Equal(t, job2.JobID, got.ActiveJobID, "ActiveJobID tracks the most recently started job")
}

// TestInMemoryBackend_ProductionBranch verifies App.ProductionBranch (see
// InMemoryBackend.productionBranchFor): nil until the app has a
// PRODUCTION-stage branch, then reflecting that branch's most recent job.
func TestInMemoryBackend_ProductionBranch(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	app := seedApp(t, b, "ProdApp")

	noBranch, err := b.GetApp(app.AppID)
	require.NoError(t, err)
	assert.Nil(t, noBranch.ProductionBranch, "no branches at all yet")

	_, err = b.CreateBranch(app.AppID, "dev", "", "DEVELOPMENT", false, nil)
	require.NoError(t, err)

	stillNone, err := b.GetApp(app.AppID)
	require.NoError(t, err)
	assert.Nil(t, stillNone.ProductionBranch, "no PRODUCTION-stage branch yet")

	_, err = b.CreateBranch(app.AppID, "main", "", "PRODUCTION", false, nil)
	require.NoError(t, err)

	withProdBranchNoJobs, err := b.GetApp(app.AppID)
	require.NoError(t, err)
	require.NotNil(t, withProdBranchNoJobs.ProductionBranch)
	assert.Equal(t, "main", withProdBranchNoJobs.ProductionBranch.BranchName)
	assert.Empty(t, withProdBranchNoJobs.ProductionBranch.Status, "no job yet -> no status")

	job, err := b.StartJob(app.AppID, "main", "RELEASE", "", "", "", time.Time{})
	require.NoError(t, err)

	withJob, err := b.GetApp(app.AppID)
	require.NoError(t, err)
	require.NotNil(t, withJob.ProductionBranch)
	assert.Equal(t, "main", withJob.ProductionBranch.BranchName)
	assert.Equal(t, string(amplify.JobStatusRunning), withJob.ProductionBranch.Status)
	assert.Equal(t, job.StartTime.Unix(), withJob.ProductionBranch.LastDeployTime.Unix())
}
