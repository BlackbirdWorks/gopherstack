package elasticbeanstalk_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/elasticbeanstalk"
)

func newTestBackend() *elasticbeanstalk.InMemoryBackend {
	return elasticbeanstalk.NewInMemoryBackend("123456789012", "us-east-1")
}

func TestBackend_Application(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs   error
		setup       func(b *elasticbeanstalk.InMemoryBackend)
		name        string
		appName     string
		description string
		wantErr     bool
	}{
		{
			name:        "create success",
			appName:     "my-app",
			description: "test app",
		},
		{
			name:    "create duplicate",
			appName: "dup-app",
			setup: func(b *elasticbeanstalk.InMemoryBackend) {
				_, _ = b.CreateApplication("dup-app", "", nil)
			},
			wantErr:   true,
			wantErrIs: awserr.ErrAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()

			if tt.setup != nil {
				tt.setup(b)
			}

			app, err := b.CreateApplication(tt.appName, tt.description, map[string]string{"env": "test"})

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.appName, app.ApplicationName)
			assert.Equal(t, tt.description, app.Description)
			assert.Contains(t, app.ApplicationARN, tt.appName)
		})
	}
}

func TestBackend_DescribeApplications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filter    []string
		wantCount int
	}{
		{
			name:      "list all",
			filter:    nil,
			wantCount: 2,
		},
		{
			name:      "filter by name",
			filter:    []string{"app-a"},
			wantCount: 1,
		},
		{
			name:      "filter missing",
			filter:    []string{"nonexistent"},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			_, _ = b.CreateApplication("app-a", "", nil)
			_, _ = b.CreateApplication("app-b", "", nil)

			apps := b.DescribeApplications(tt.filter)
			assert.Len(t, apps, tt.wantCount)
		})
	}
}

func TestBackend_DeleteApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		name      string
		appName   string
		wantErr   bool
	}{
		{
			name:    "delete existing",
			appName: "del-app",
		},
		{
			name:      "delete not found",
			appName:   "nonexistent",
			wantErr:   true,
			wantErrIs: awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()

			if tt.appName == "del-app" {
				_, _ = b.CreateApplication("del-app", "", nil)
			}

			err := b.DeleteApplication(tt.appName)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}

			require.NoError(t, err)
			apps := b.DescribeApplications([]string{tt.appName})
			assert.Empty(t, apps)
		})
	}
}

func TestBackend_Environment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		setup     func(b *elasticbeanstalk.InMemoryBackend)
		name      string
		appName   string
		envName   string
		wantErr   bool
	}{
		{
			name:    "create success",
			appName: "my-app",
			envName: "my-env",
		},
		{
			name:    "create duplicate",
			appName: "my-app",
			envName: "dup-env",
			setup: func(b *elasticbeanstalk.InMemoryBackend) {
				_, _ = b.CreateEnvironment("my-app", "dup-env", "", "", nil)
			},
			wantErr:   true,
			wantErrIs: awserr.ErrAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()

			if tt.setup != nil {
				tt.setup(b)
			}

			env, err := b.CreateEnvironment(tt.appName, tt.envName, "64bit Amazon Linux", "test env", nil)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.appName, env.ApplicationName)
			assert.Equal(t, tt.envName, env.EnvironmentName)
			assert.Equal(t, "Ready", env.Status)
			assert.Equal(t, "Green", env.Health)
			assert.Contains(t, env.EnvironmentARN, tt.appName)
		})
	}
}

func TestBackend_DescribeEnvironments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		appFilter string
		envFilter []string
		envIDs    []string
		wantCount int
	}{
		{
			name:      "list all",
			wantCount: 3,
		},
		{
			name:      "filter by app",
			appFilter: "app-a",
			wantCount: 2,
		},
		{
			name:      "filter by env name",
			envFilter: []string{"env-1"},
			wantCount: 1,
		},
		{
			name:      "filter by env id",
			envIDs:    []string{"e-00000001"},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			_, _ = b.CreateEnvironment("app-a", "env-1", "", "", nil)
			_, _ = b.CreateEnvironment("app-a", "env-2", "", "", nil)
			_, _ = b.CreateEnvironment("app-b", "env-3", "", "", nil)

			envs := b.DescribeEnvironments(tt.appFilter, tt.envFilter, tt.envIDs)
			assert.Len(t, envs, tt.wantCount)
		})
	}
}

func TestBackend_TerminateEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		name      string
		appName   string
		envName   string
		wantErr   bool
	}{
		{
			name:    "terminate existing",
			appName: "my-app",
			envName: "my-env",
		},
		{
			name:      "terminate not found",
			appName:   "my-app",
			envName:   "nonexistent",
			wantErr:   true,
			wantErrIs: awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()

			if tt.envName == "my-env" {
				_, _ = b.CreateEnvironment("my-app", "my-env", "", "", nil)
			}

			env, err := b.TerminateEnvironment(tt.appName, tt.envName)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "Terminated", env.Status)
			// Verify it's gone.
			envs := b.DescribeEnvironments("my-app", []string{"my-env"}, nil)
			assert.Empty(t, envs)
		})
	}
}

func TestBackend_ApplicationVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs    error
		setup        func(b *elasticbeanstalk.InMemoryBackend)
		name         string
		appName      string
		versionLabel string
		wantErr      bool
	}{
		{
			name:         "create success",
			appName:      "my-app",
			versionLabel: "v1",
		},
		{
			name:         "create duplicate",
			appName:      "my-app",
			versionLabel: "v1",
			setup: func(b *elasticbeanstalk.InMemoryBackend) {
				_, _ = b.CreateApplicationVersion("my-app", "v1", "", nil)
			},
			wantErr:   true,
			wantErrIs: awserr.ErrAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()

			if tt.setup != nil {
				tt.setup(b)
			}

			ver, err := b.CreateApplicationVersion(tt.appName, tt.versionLabel, "version desc", nil)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.appName, ver.ApplicationName)
			assert.Equal(t, tt.versionLabel, ver.VersionLabel)
			assert.Equal(t, "Processed", ver.Status)
			assert.Contains(t, ver.ApplicationVersionARN, tt.appName)
		})
	}
}

func TestBackend_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs  error
		name       string
		wantErr    bool
		useRealARN bool
	}{
		{
			name:       "list tags for app",
			useRealARN: true,
		},
		{
			name:      "list tags for nonexistent",
			wantErr:   true,
			wantErrIs: awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			app, _ := b.CreateApplication("tag-app", "", map[string]string{"key1": "val1"})

			resourceARN := "nonexistent-arn"
			if tt.useRealARN {
				resourceARN = app.ApplicationARN
			}

			tags, err := b.ListTagsForResource(resourceARN)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "val1", tags["key1"])
		})
	}
}

func TestBackend_UpdateTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		addTags    map[string]string
		removeTags map[string]string
		wantTags   map[string]string
		name       string
		wantErr    bool
	}{
		{
			name:     "add tags",
			addTags:  map[string]string{"k2": "v2"},
			wantTags: map[string]string{"k1": "v1", "k2": "v2"},
		},
		{
			name:       "remove tags",
			removeTags: map[string]string{"k1": ""},
			wantTags:   map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			app, _ := b.CreateApplication("tag-app", "", map[string]string{"k1": "v1"})

			err := b.UpdateTagsForResource(app.ApplicationARN, tt.addTags, tt.removeTags)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			tags, _ := b.ListTagsForResource(app.ApplicationARN)

			for k, v := range tt.wantTags {
				assert.Equal(t, v, tags[k])
			}

			for k := range tt.removeTags {
				_, exists := tags[k]
				assert.False(t, exists, "removed tag should not exist")
			}
		})
	}
}
