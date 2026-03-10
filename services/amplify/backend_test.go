package amplify_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/amplify"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestBackend() *amplify.InMemoryBackend {
	return amplify.NewInMemoryBackend("000000000000", "us-east-1")
}

// ---- App tests ----

func TestInMemoryBackend_CreateApp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		appName      string
		description  string
		repository   string
		platform     string
		tags         map[string]string
		wantName     string
		wantPlatform amplify.Platform
		wantErr      bool
	}{
		{
			name:         "creates_app_with_all_fields",
			appName:      "MyApp",
			description:  "My application",
			repository:   "https://github.com/example/repo",
			platform:     "WEB",
			tags:         map[string]string{"env": "test"},
			wantName:     "MyApp",
			wantPlatform: amplify.PlatformWEB,
		},
		{
			name:         "creates_app_with_default_platform",
			appName:      "DefaultPlatformApp",
			wantName:     "DefaultPlatformApp",
			wantPlatform: amplify.PlatformWEB,
		},
		{
			name:         "creates_app_with_compute_platform",
			appName:      "ComputeApp",
			platform:     "WEB_COMPUTE",
			wantName:     "ComputeApp",
			wantPlatform: amplify.PlatformWEBCOMPUTE,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			app, err := b.CreateApp(tt.appName, tt.description, tt.repository, tt.platform, tt.tags)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantName, app.Name)
			assert.Equal(t, tt.wantPlatform, app.Platform)
			assert.NotEmpty(t, app.AppID)
			assert.NotEmpty(t, app.ARN)
			assert.Contains(t, app.ARN, app.AppID)
			assert.Contains(t, app.DefaultDomain, app.AppID)
			assert.False(t, app.CreateTime.IsZero())
			assert.False(t, app.UpdateTime.IsZero())
		})
	}
}

func TestInMemoryBackend_GetApp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs   error
		setup   func(*amplify.InMemoryBackend) string
		name    string
		appID   string
		wantErr bool
	}{
		{
			name: "returns_existing_app",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return app.AppID
			},
			wantErr: false,
		},
		{
			name:    "returns_not_found_for_missing_app",
			setup:   func(_ *amplify.InMemoryBackend) string { return "nonexistent" },
			appID:   "nonexistent",
			wantErr: true,
			errIs:   awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			appID := tt.setup(b)

			if tt.appID != "" {
				appID = tt.appID
			}

			app, err := b.GetApp(appID)

			if tt.wantErr {
				require.Error(t, err)

				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, appID, app.AppID)
		})
	}
}

func TestInMemoryBackend_ListApps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*amplify.InMemoryBackend)
		name      string
		wantCount int
	}{
		{
			name:      "returns_empty_list",
			setup:     func(_ *amplify.InMemoryBackend) {},
			wantCount: 0,
		},
		{
			name: "returns_all_apps",
			setup: func(b *amplify.InMemoryBackend) {
				_, _ = b.CreateApp("App1", "", "", "", nil)
				_, _ = b.CreateApp("App2", "", "", "", nil)
			},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			tt.setup(b)
			apps, err := b.ListApps()

			require.NoError(t, err)
			assert.Len(t, apps, tt.wantCount)
		})
	}
}

func TestInMemoryBackend_DeleteApp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs   error
		setup   func(*amplify.InMemoryBackend) string
		name    string
		wantErr bool
	}{
		{
			name: "deletes_existing_app",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("ToDelete", "", "", "", nil)

				return app.AppID
			},
		},
		{
			name: "deletes_app_with_branches",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("AppWithBranch", "", "", "", nil)
				_, _ = b.CreateBranch(app.AppID, "main", "", "", false, nil)

				return app.AppID
			},
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
			err := b.DeleteApp(appID)

			if tt.wantErr {
				require.Error(t, err)

				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)

			_, getErr := b.GetApp(appID)
			require.Error(t, getErr)
		})
	}
}

// ---- Branch tests ----

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
			branches, err := b.ListBranches(appID)

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

// ---- Tagging tests ----

func TestInMemoryBackend_TagResource_App(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs   error
		setup   func(*amplify.InMemoryBackend) string
		tagMap  map[string]string
		name    string
		wantErr bool
	}{
		{
			name: "tags_existing_app",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return app.ARN
			},
			tagMap: map[string]string{"env": "prod", "team": "backend"},
		},
		{
			name: "returns_not_found_for_invalid_arn",
			setup: func(_ *amplify.InMemoryBackend) string {
				return "arn:aws:amplify:us-east-1:000000000000:apps/nonexistent"
			},
			tagMap:  map[string]string{"env": "test"},
			wantErr: true,
			errIs:   awserr.ErrNotFound,
		},
		{
			name: "returns_not_found_for_malformed_arn",
			setup: func(_ *amplify.InMemoryBackend) string {
				return "invalid-arn"
			},
			tagMap:  map[string]string{"env": "test"},
			wantErr: true,
			errIs:   awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			arn := tt.setup(b)
			err := b.TagResource(arn, tt.tagMap)

			if tt.wantErr {
				require.Error(t, err)

				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)

			tags, listErr := b.ListTagsForResource(arn)
			require.NoError(t, listErr)

			for k, v := range tt.tagMap {
				assert.Equal(t, v, tags[k])
			}
		})
	}
}

func TestInMemoryBackend_TagResource_Branch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs   error
		setup   func(*amplify.InMemoryBackend) string
		name    string
		wantErr bool
	}{
		{
			name: "tags_existing_branch",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)
				branch, _ := b.CreateBranch(app.AppID, "main", "", "", false, nil)

				return branch.BranchARN
			},
		},
		{
			name: "returns_not_found_for_nonexistent_branch_arn",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return "arn:aws:amplify:us-east-1:000000000000:apps/" + app.AppID + "/branches/nonexistent"
			},
			wantErr: true,
			errIs:   awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			branchARN := tt.setup(b)
			err := b.TagResource(branchARN, map[string]string{"tagged": "yes"})

			if tt.wantErr {
				require.Error(t, err)

				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs   error
		setup   func(*amplify.InMemoryBackend) string
		name    string
		tagKeys []string
		wantErr bool
	}{
		{
			name: "removes_specified_tags",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", map[string]string{
					"env":  "prod",
					"team": "backend",
				})

				return app.ARN
			},
			tagKeys: []string{"env"},
		},
		{
			name: "returns_not_found_for_invalid_arn",
			setup: func(_ *amplify.InMemoryBackend) string {
				return "arn:aws:amplify:us-east-1:000000000000:apps/nonexistent"
			},
			tagKeys: []string{"env"},
			wantErr: true,
			errIs:   awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			arn := tt.setup(b)
			err := b.UntagResource(arn, tt.tagKeys)

			if tt.wantErr {
				require.Error(t, err)

				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)

			tags, listErr := b.ListTagsForResource(arn)
			require.NoError(t, listErr)

			for _, k := range tt.tagKeys {
				_, exists := tags[k]
				assert.False(t, exists, "tag %s should have been removed", k)
			}
		})
	}
}

func TestInMemoryBackend_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs    error
		setup    func(*amplify.InMemoryBackend) string
		wantTags map[string]string
		name     string
		wantErr  bool
	}{
		{
			name: "returns_tags_for_app",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", map[string]string{"env": "test"})

				return app.ARN
			},
			wantTags: map[string]string{"env": "test"},
		},
		{
			name: "returns_empty_tags",
			setup: func(b *amplify.InMemoryBackend) string {
				app, _ := b.CreateApp("TestApp", "", "", "", nil)

				return app.ARN
			},
			wantTags: map[string]string{},
		},
		{
			name: "returns_not_found_for_invalid_arn",
			setup: func(_ *amplify.InMemoryBackend) string {
				return "arn:aws:amplify:us-east-1:000000000000:apps/nonexistent"
			},
			wantErr: true,
			errIs:   awserr.ErrNotFound,
		},
		{
			name: "returns_not_found_for_unsupported_arn",
			setup: func(_ *amplify.InMemoryBackend) string {
				return "arn:aws:amplify:us-east-1:000000000000:domains/example"
			},
			wantErr: true,
			errIs:   awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			arn := tt.setup(b)
			tags, err := b.ListTagsForResource(arn)

			if tt.wantErr {
				require.Error(t, err)

				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantTags, tags)
		})
	}
}

func TestInMemoryBackend_TagResource_BranchMissingApp(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	err := b.TagResource(
		"arn:aws:amplify:us-east-1:000000000000:apps/nonexistent/branches/main",
		map[string]string{"k": "v"},
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}
