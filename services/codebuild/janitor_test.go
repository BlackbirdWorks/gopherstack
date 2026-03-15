package codebuild_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codebuild"
)

func newTestBackend(t *testing.T) *codebuild.InMemoryBackend {
	t.Helper()

	return codebuild.NewInMemoryBackend("123456789012", "us-east-1")
}

// TestJanitor_SweepCompletedBuilds verifies that the janitor removes builds in
// terminal states whose EndTime is past the configured TTL.
func TestJanitor_SweepCompletedBuilds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		status      string
		endOffset   time.Duration // negative = in the past
		ttl         time.Duration
		wantEvicted bool
	}{
		{
			name:        "evict_succeeded_past_ttl",
			status:      "SUCCEEDED",
			endOffset:   -25 * time.Hour,
			ttl:         24 * time.Hour,
			wantEvicted: true,
		},
		{
			name:        "evict_failed_past_ttl",
			status:      "FAILED",
			endOffset:   -25 * time.Hour,
			ttl:         24 * time.Hour,
			wantEvicted: true,
		},
		{
			name:        "evict_stopped_past_ttl",
			status:      "STOPPED",
			endOffset:   -25 * time.Hour,
			ttl:         24 * time.Hour,
			wantEvicted: true,
		},
		{
			name:        "evict_fault_past_ttl",
			status:      "FAULT",
			endOffset:   -25 * time.Hour,
			ttl:         24 * time.Hour,
			wantEvicted: true,
		},
		{
			name:        "keep_succeeded_within_ttl",
			status:      "SUCCEEDED",
			endOffset:   -1 * time.Hour,
			ttl:         24 * time.Hour,
			wantEvicted: false,
		},
		{
			name:        "keep_in_progress",
			status:      "IN_PROGRESS",
			endOffset:   0,
			ttl:         24 * time.Hour,
			wantEvicted: false,
		},
		{
			name:        "keep_no_endtime",
			status:      "SUCCEEDED",
			endOffset:   0,
			ttl:         24 * time.Hour,
			wantEvicted: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)

			_, err := backend.CreateProject(
				"proj",
				"",
				codebuild.ProjectSource{Type: "NO_SOURCE"},
				codebuild.ProjectArtifacts{Type: "NO_ARTIFACTS"},
				codebuild.ProjectEnvironment{
					Type:        "LINUX_CONTAINER",
					Image:       "img",
					ComputeType: "BUILD_GENERAL1_SMALL",
				},
				"",
				nil,
			)
			require.NoError(t, err)

			build, err := backend.StartBuild("proj")
			require.NoError(t, err)

			if tt.endOffset != 0 {
				backend.SetBuildEndTime(build.ID, tt.status, time.Now().Add(tt.endOffset))
			} else if tt.status != "IN_PROGRESS" {
				// SUCCEEDED with no end time: set status only, leave EndTime=0.
				backend.SetBuildEndTime(build.ID, tt.status, time.Time{})
			}

			janitor := codebuild.NewJanitor(backend, time.Hour, tt.ttl)
			janitor.SweepOnce(t.Context())

			builds, err := backend.ListBuildsForProject("proj")
			require.NoError(t, err)

			if tt.wantEvicted {
				assert.NotContains(t, builds, build.ID, "build should be evicted")
				assert.Equal(t, 0, backend.BuildCount())
			} else {
				assert.Contains(t, builds, build.ID, "build should be preserved")
			}
		})
	}
}

// TestDeleteProject_CleanupBuilds verifies that deleting a project removes
// all associated builds.
func TestDeleteProject_CleanupBuilds(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)

	_, err := backend.CreateProject("proj", "", codebuild.ProjectSource{Type: "NO_SOURCE"},
		codebuild.ProjectArtifacts{Type: "NO_ARTIFACTS"},
		codebuild.ProjectEnvironment{Type: "LINUX_CONTAINER", Image: "img", ComputeType: "BUILD_GENERAL1_SMALL"},
		"", nil)
	require.NoError(t, err)

	_, err = backend.StartBuild("proj")
	require.NoError(t, err)

	_, err = backend.StartBuild("proj")
	require.NoError(t, err)

	assert.Equal(t, 2, backend.BuildCount(), "should have 2 builds before delete")

	err = backend.DeleteProject("proj")
	require.NoError(t, err)

	assert.Equal(t, 0, backend.BuildCount(), "all builds should be removed after project deletion")
}

// TestJanitor_SweepCleansARNIndex verifies that sweeping builds also removes
// their entries from the buildARNIndex so tag operations on evicted builds
// return ErrNotFound instead of looking up a deleted resource.
func TestJanitor_SweepCleansARNIndex(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)

	_, err := backend.CreateProject(
		"proj",
		"",
		codebuild.ProjectSource{Type: "NO_SOURCE"},
		codebuild.ProjectArtifacts{Type: "NO_ARTIFACTS"},
		codebuild.ProjectEnvironment{Type: "LINUX_CONTAINER", Image: "img", ComputeType: "BUILD_GENERAL1_SMALL"},
		"",
		nil,
	)
	require.NoError(t, err)

	build, err := backend.StartBuild("proj")
	require.NoError(t, err)

	// Mark build as terminal and past the TTL.
	backend.SetBuildEndTime(build.ID, "SUCCEEDED", time.Now().Add(-25*time.Hour))

	assert.Equal(t, 1, backend.BuildARNIndexSize(), "ARN index should have 1 entry before sweep")

	janitor := codebuild.NewJanitor(backend, time.Hour, 24*time.Hour)
	janitor.SweepOnce(t.Context())

	assert.Equal(t, 0, backend.BuildCount(), "build should be evicted")
	assert.Equal(t, 0, backend.BuildARNIndexSize(), "ARN index should be empty after sweep")

	// Tag op on the evicted build's ARN must return ErrNotFound.
	err = backend.TagResource(build.Arn, map[string]string{"key": "val"})
	require.ErrorIs(t, err, codebuild.ErrNotFound)
}

// TestJanitor_RunContext verifies that the janitor stops when context is cancelled.
func TestCodeBuildJanitor_RunContext(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	janitor := codebuild.NewJanitor(backend, 10*time.Millisecond, time.Hour)

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})

	go func() {
		janitor.Run(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "janitor did not stop after context cancellation")
	}
}
