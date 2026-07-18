package amplify_test

import (
	"testing"

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
			err := b.DeleteBranch(appID, branchName)

			if tt.wantErr {
				require.Error(t, err)

				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)

			_, getErr := b.GetBranch(appID, branchName)
			require.Error(t, getErr)
		})
	}
}
