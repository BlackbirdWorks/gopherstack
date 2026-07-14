package elasticbeanstalk_test

import (
	"context"
	"testing"
	"time"

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
				_, _ = b.CreateApplication(context.Background(), "dup-app", "", nil)
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

			app, err := b.CreateApplication(
				context.Background(), tt.appName, tt.description, map[string]string{"env": "test"},
			)

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
			_, _ = b.CreateApplication(context.Background(), "app-a", "", nil)
			_, _ = b.CreateApplication(context.Background(), "app-b", "", nil)

			apps := b.DescribeApplications(context.Background(), tt.filter)
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
				_, _ = b.CreateApplication(context.Background(), "del-app", "", nil)
			}

			err := b.DeleteApplication(context.Background(), tt.appName)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}

			require.NoError(t, err)
			apps := b.DescribeApplications(context.Background(), []string{tt.appName})
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
				_, _ = b.CreateEnvironment(
					context.Background(), "my-app", "dup-env", "", "", nil,
					elasticbeanstalk.CreateEnvironmentParams{},
				)
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

			env, err := b.CreateEnvironment(
				context.Background(),
				tt.appName,
				tt.envName,
				"64bit Amazon Linux",
				"test env",
				nil,
				elasticbeanstalk.CreateEnvironmentParams{},
			)

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
			ctx := context.Background()
			params := elasticbeanstalk.CreateEnvironmentParams{}
			_, _ = b.CreateEnvironment(ctx, "app-a", "env-1", "", "", nil, params)
			_, _ = b.CreateEnvironment(ctx, "app-a", "env-2", "", "", nil, params)
			_, _ = b.CreateEnvironment(ctx, "app-b", "env-3", "", "", nil, params)

			envs := b.DescribeEnvironments(context.Background(), tt.appFilter, tt.envFilter, tt.envIDs)
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
				_, _ = b.CreateEnvironment(
					context.Background(), "my-app", "my-env", "", "", nil,
					elasticbeanstalk.CreateEnvironmentParams{},
				)
			}

			env, err := b.TerminateEnvironment(context.Background(), tt.appName, tt.envName)

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
			envs := b.DescribeEnvironments(context.Background(), "my-app", []string{"my-env"}, nil)
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
			setup: func(b *elasticbeanstalk.InMemoryBackend) {
				_, _ = b.CreateApplication(context.Background(), "my-app", "", nil)
			},
		},
		{
			name:         "create duplicate",
			appName:      "my-app",
			versionLabel: "v1",
			setup: func(b *elasticbeanstalk.InMemoryBackend) {
				_, _ = b.CreateApplication(context.Background(), "my-app", "", nil)
				_, _ = b.CreateApplicationVersion(context.Background(), "my-app", "v1", "", "", "", nil)
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

			ver, err := b.CreateApplicationVersion(
				context.Background(), tt.appName, tt.versionLabel, "version desc", "", "", nil,
			)

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
			app, _ := b.CreateApplication(context.Background(), "tag-app", "", map[string]string{"key1": "val1"})

			resourceARN := "nonexistent-arn"
			if tt.useRealARN {
				resourceARN = app.ApplicationARN
			}

			tags, err := b.ListTagsForResource(context.Background(), resourceARN)

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
			app, _ := b.CreateApplication(context.Background(), "tag-app", "", map[string]string{"k1": "v1"})

			err := b.UpdateTagsForResource(context.Background(), app.ApplicationARN, tt.addTags, tt.removeTags)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			tags, _ := b.ListTagsForResource(context.Background(), app.ApplicationARN)

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

// TestBackend_TagsReachConfigTemplateAndPlatformVersion verifies that tags
// supplied to CreateConfigurationTemplate and CreatePlatformVersion (real AWS:
// "Elastic Beanstalk supports tagging of all of its resources") are actually
// reachable through List/UpdateTagsForResource, not just stored and orphaned.
func TestBackend_TagsReachConfigTemplateAndPlatformVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		buildARN func(b *elasticbeanstalk.InMemoryBackend) string
		name     string
	}{
		{
			name: "configuration template",
			buildARN: func(b *elasticbeanstalk.InMemoryBackend) string {
				ctx := context.Background()
				_, _ = b.CreateApplication(ctx, "tmpl-app", "", nil)
				tmpl, err := b.CreateConfigurationTemplate(
					ctx, "tmpl-app", "tmpl1", "", "", map[string]string{"k1": "v1"},
				)
				require.NoError(t, err)

				return "arn:aws:elasticbeanstalk:us-east-1:123456789012:configurationtemplate/tmpl-app/" +
					tmpl.TemplateName
			},
		},
		{
			name: "platform version",
			buildARN: func(b *elasticbeanstalk.InMemoryBackend) string {
				pv, err := b.CreatePlatformVersion(
					context.Background(), "my-platform", "1.0.0", map[string]string{"k1": "v1"},
				)
				require.NoError(t, err)

				return pv.PlatformArn
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			resourceARN := tt.buildARN(b)

			tags, err := b.ListTagsForResource(context.Background(), resourceARN)
			require.NoError(t, err)
			assert.Equal(t, "v1", tags["k1"])

			err = b.UpdateTagsForResource(
				context.Background(), resourceARN, map[string]string{"k2": "v2"}, map[string]string{"k1": ""},
			)
			require.NoError(t, err)

			tags, err = b.ListTagsForResource(context.Background(), resourceARN)
			require.NoError(t, err)
			assert.Equal(t, "v2", tags["k2"])
			_, hasK1 := tags["k1"]
			assert.False(t, hasK1, "removed tag should not exist")
		})
	}
}

// TestBackend_CreatePlatformVersionARNIncludesAccountID verifies that a custom
// platform's ARN carries the caller's account ID, matching the documented
// "arn:aws:elasticbeanstalk:{region}:{account-id}:platform/{name}/{version}"
// resource-path pattern -- an empty account ID would produce a malformed
// "::platform/..." ARN that a real client constructing the ARN itself would
// never match.
func TestBackend_CreatePlatformVersionARNIncludesAccountID(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pv, err := b.CreatePlatformVersion(context.Background(), "my-platform", "1.0.0", nil)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:elasticbeanstalk:us-east-1:123456789012:platform/my-platform/1.0.0", pv.PlatformArn)
}

// TestBackend_UpdateOpsBumpDateUpdated verifies that Update* backend methods
// advance DateUpdated on every mutation, not just at creation time.
func TestBackend_UpdateOpsBumpDateUpdated(t *testing.T) {
	t.Parallel()

	t.Run("UpdateApplication", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend()
		app, err := b.CreateApplication(context.Background(), "app1", "orig", nil)
		require.NoError(t, err)
		created := app.DateUpdated

		time.Sleep(time.Second)

		updated, err := b.UpdateApplication(context.Background(), "app1", "new desc")
		require.NoError(t, err)
		assert.NotEqual(t, created, updated.DateUpdated)
	})

	t.Run("UpdateApplicationVersion", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend()
		_, err := b.CreateApplication(context.Background(), "app2", "", nil)
		require.NoError(t, err)
		ver, err := b.CreateApplicationVersion(context.Background(), "app2", "v1", "orig", "", "", nil)
		require.NoError(t, err)
		created := ver.DateUpdated

		time.Sleep(time.Second)

		updated, err := b.UpdateApplicationVersion(context.Background(), "app2", "v1", "new desc")
		require.NoError(t, err)
		assert.NotEqual(t, created, updated.DateUpdated)
	})

	t.Run("UpdateConfigurationTemplate", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend()
		_, err := b.CreateApplication(context.Background(), "app3", "", nil)
		require.NoError(t, err)
		tmpl, err := b.CreateConfigurationTemplate(context.Background(), "app3", "tmpl1", "orig", "", nil)
		require.NoError(t, err)
		created := tmpl.DateUpdated

		time.Sleep(time.Second)

		updated, err := b.UpdateConfigurationTemplate(context.Background(), "app3", "tmpl1", "new desc")
		require.NoError(t, err)
		assert.NotEqual(t, created, updated.DateUpdated)
	})
}

// TestBackend_CreateApplicationVersionRequiresApplication verifies AWS's
// documented CreateApplicationVersion behavior: if no application is found
// with this name, and AutoCreateApplication is false, returns an
// InvalidParameterValue error.
func TestBackend_CreateApplicationVersionRequiresApplication(t *testing.T) {
	t.Parallel()

	t.Run("missing application without AutoCreateApplication errors", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend()
		_, err := b.CreateApplicationVersionWithParams(
			context.Background(), "ghost-app", "v1", elasticbeanstalk.ApplicationVersionParams{},
		)
		require.Error(t, err)
		require.ErrorIs(t, err, awserr.ErrInvalidParameter)
	})

	t.Run("missing application with AutoCreateApplication succeeds", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend()
		ver, err := b.CreateApplicationVersionWithParams(
			context.Background(), "auto-app", "v1",
			elasticbeanstalk.ApplicationVersionParams{AutoCreateApplication: true},
		)
		require.NoError(t, err)
		assert.Equal(t, "auto-app", ver.ApplicationName)

		apps := b.DescribeApplications(context.Background(), []string{"auto-app"})
		require.Len(t, apps, 1)
	})
}
