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

func TestInMemoryBackend_CreateApplication(t *testing.T) {
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

func TestInMemoryBackend_DescribeApplications(t *testing.T) {
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

func TestInMemoryBackend_DeleteApplication(t *testing.T) {
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

// TestInMemoryBackend_UpdateApplication_BumpsDateUpdated verifies that UpdateApplication
// advances DateUpdated on every mutation, not just at creation time.
func TestInMemoryBackend_UpdateApplication_BumpsDateUpdated(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	app, err := b.CreateApplication(context.Background(), "app1", "orig", nil)
	require.NoError(t, err)
	created := app.DateUpdated

	time.Sleep(time.Second)

	updated, err := b.UpdateApplication(context.Background(), "app1", "new desc")
	require.NoError(t, err)
	assert.NotEqual(t, created, updated.DateUpdated)
}
