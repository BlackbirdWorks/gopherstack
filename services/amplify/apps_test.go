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
