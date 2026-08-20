package amplify_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/amplify"
)

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
			apps, _, err := b.ListApps("", 0)

			require.NoError(t, err)
			assert.Len(t, apps, tt.wantCount)
		})
	}
}

func TestInMemoryBackend_ListAppsPagination(t *testing.T) {
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

			for _, name := range []string{"App1", "App2", "App3", "App4"} {
				_, err := b.CreateApp(name, "", "", "", nil)
				require.NoError(t, err)
			}

			apps, outToken, err := b.ListApps(tt.nextToken, tt.maxResults)
			require.NoError(t, err)
			assert.Len(t, apps, tt.wantCount)

			if tt.wantNextToken {
				assert.NotEmpty(t, outToken)
			} else {
				assert.Empty(t, outToken)
			}
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
			deleted, err := b.DeleteApp(appID)

			if tt.wantErr {
				require.Error(t, err)

				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)
			require.NotNil(t, deleted)
			assert.Equal(t, appID, deleted.AppID)

			_, getErr := b.GetApp(appID)
			require.Error(t, getErr)
		})
	}
}

// TestInMemoryBackend_CreateApp_InvalidPlatform verifies real Amplify's
// server-side Platform enum validation: CreateApp rejects any value outside
// WEB/WEB_COMPUTE/WEB_DYNAMIC with a BadRequestException, rather than
// silently accepting an arbitrary string.
func TestInMemoryBackend_CreateApp_InvalidPlatform(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.CreateApp("BadPlatformApp", "", "", "NOT_A_REAL_PLATFORM", nil)
	require.Error(t, err)
	require.ErrorIs(t, err, awserr.ErrInvalidParameter)
}

// TestInMemoryBackend_CreateApp_FieldParity verifies the full App field set
// added for real-Amplify parity: create-time defaults for fields the caller
// doesn't set (EnableBranchAutoBuild defaults true, EnvironmentVariables
// defaults to a non-nil empty map, RepositoryCloneMethod is derived from
// Repository), and that every optional field passed via AppOptions round
// -trips onto the created App.
func TestInMemoryBackend_CreateApp_FieldParity(t *testing.T) {
	t.Parallel()

	t.Run("defaults_when_opts_omitted", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend()
		app, err := b.CreateApp("DefaultsApp", "", "", "", nil)
		require.NoError(t, err)

		assert.True(t, app.EnableBranchAutoBuild, "EnableBranchAutoBuild must default true")
		assert.False(t, app.EnableBasicAuth)
		assert.NotNil(t, app.EnvironmentVariables)
		assert.Empty(t, app.EnvironmentVariables)
		assert.Empty(t, app.RepositoryCloneMethod, "no repository configured -> empty clone method")
		assert.Nil(t, app.ProductionBranch, "no PRODUCTION branch exists yet")
	})

	t.Run("repository_clone_method_derived_from_repository", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend()
		app, err := b.CreateApp("RepoApp", "", "https://github.com/example/repo", "", nil)
		require.NoError(t, err)

		assert.Equal(t, "TOKEN", app.RepositoryCloneMethod)
	})

	t.Run("opts_round_trip", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend()
		enableBranchAutoBuild := false
		enableBasicAuth := true
		buildSpec := "version: 1"

		app, err := b.CreateApp("OptsApp", "", "", "", nil, amplify.AppOptions{
			EnvironmentVariables: map[string]string{"FOO": "bar"},
			BuildSpec:            &buildSpec,
			CustomRules: []amplify.CustomRule{
				{Source: "/a", Target: "/b", Status: "301"},
			},
			EnableBranchAutoBuild: &enableBranchAutoBuild,
			EnableBasicAuth:       &enableBasicAuth,
		})
		require.NoError(t, err)

		assert.Equal(t, map[string]string{"FOO": "bar"}, app.EnvironmentVariables)
		assert.Equal(t, "version: 1", app.BuildSpec)
		require.Len(t, app.CustomRules, 1)
		assert.Equal(t, "/a", app.CustomRules[0].Source)
		assert.False(t, app.EnableBranchAutoBuild)
		assert.True(t, app.EnableBasicAuth)
	})
}

// TestInMemoryBackend_UpdateApp_PartialSemantics verifies UpdateApp's
// partial-update contract: a field omitted from AppOptions (nil pointer)
// leaves the existing value unchanged, matching real Amplify's UpdateApp
// (never resetting unspecified fields to their zero value).
func TestInMemoryBackend_UpdateApp_PartialSemantics(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	buildSpec := "version: 1"
	enableBasicAuth := true

	app, err := b.CreateApp("UpdateApp", "", "", "", nil, amplify.AppOptions{
		BuildSpec:       &buildSpec,
		EnableBasicAuth: &enableBasicAuth,
	})
	require.NoError(t, err)
	require.Equal(t, "version: 1", app.BuildSpec)
	require.True(t, app.EnableBasicAuth)

	// Update only the name, via an empty AppOptions -- BuildSpec/
	// EnableBasicAuth must survive untouched.
	updated, err := b.UpdateApp(app.AppID, "RenamedApp", "", "", "")
	require.NoError(t, err)

	assert.Equal(t, "RenamedApp", updated.Name)
	assert.Equal(t, "version: 1", updated.BuildSpec, "unset opts field must not be reset")
	assert.True(t, updated.EnableBasicAuth, "unset opts field must not be reset")

	// Now explicitly clear BuildSpec.
	cleared := ""
	updated2, err := b.UpdateApp(app.AppID, "", "", "", "", amplify.AppOptions{BuildSpec: &cleared})
	require.NoError(t, err)
	assert.Empty(t, updated2.BuildSpec)
	assert.True(t, updated2.EnableBasicAuth, "still untouched by an unrelated update")
}

// TestInMemoryBackend_UpdateApp_InvalidPlatform mirrors
// TestInMemoryBackend_CreateApp_InvalidPlatform for UpdateApp.
func TestInMemoryBackend_UpdateApp_InvalidPlatform(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	app := seedApp(t, b, "PlatformUpdateApp")

	_, err := b.UpdateApp(app.AppID, "", "", "", "NOT_A_REAL_PLATFORM")
	require.Error(t, err)
	require.ErrorIs(t, err, awserr.ErrInvalidParameter)
}
