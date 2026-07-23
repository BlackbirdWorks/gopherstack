package kinesisanalyticsv2_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

func newTestBackend(t *testing.T) *kinesisanalyticsv2.InMemoryBackend {
	t.Helper()

	return kinesisanalyticsv2.NewInMemoryBackend("000000000000", "us-east-1")
}

func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(*kinesisanalyticsv2.InMemoryBackend)
		name  string
	}{
		{
			name:  "empty backend",
			setup: func(_ *kinesisanalyticsv2.InMemoryBackend) {},
		},
		{
			name: "with applications",
			setup: func(b *kinesisanalyticsv2.InMemoryBackend) {
				ctx := context.Background()
				_, _ = b.CreateApplication(ctx, "app-1", "FLINK-1_18", "", "", "", nil)
				_, _ = b.CreateApplication(ctx, "app-2", "FLINK-1_18", "", "", "", nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			tt.setup(b)
			b.Reset()

			assert.Zero(t, kinesisanalyticsv2.ApplicationCount(b))

			_, err := b.CreateApplication(context.Background(), "post-reset", "FLINK-1_18", "", "", "", nil)
			require.NoError(t, err)
		})
	}
}

func TestBackend_ResetMultipleCycles(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)

	for range 3 {
		_, _ = b.CreateApplication(ctx, "temp", "FLINK-1_18", "", "", "", nil)
		b.Reset()
		assert.Zero(t, kinesisanalyticsv2.ApplicationCount(b))
	}
}

// TestBackend_AddApplicationInternal verifies the test-only seed helper
// stores an application directly, bypassing CreateApplication.
func TestBackend_AddApplicationInternal(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)
	appARN := b.GenerateApplicationARN("seeded-app")

	b.AddApplicationInternal(ctx, &kinesisanalyticsv2.Application{
		ApplicationARN:       appARN,
		ApplicationName:      "seeded-app",
		ApplicationStatus:    "READY",
		RuntimeEnvironment:   "FLINK-1_18",
		ApplicationVersionID: 1,
	})

	assert.Equal(t, 1, kinesisanalyticsv2.ApplicationCount(b))

	app, err := b.DescribeApplication(ctx, "seeded-app")
	require.NoError(t, err)
	assert.Equal(t, "seeded-app", app.ApplicationName)
}

// TestBackend_ApplicationAndSnapshotCounts exercises the ApplicationCount and
// SnapshotCount test-export helpers end to end.
func TestBackend_ApplicationAndSnapshotCounts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)

	assert.Zero(t, kinesisanalyticsv2.ApplicationCount(b))
	assert.Zero(t, kinesisanalyticsv2.SnapshotCount(b))

	_, err := b.CreateApplication(ctx, "count-app", "FLINK-1_18", "", "", "", nil)
	require.NoError(t, err)

	assert.Equal(t, 1, kinesisanalyticsv2.ApplicationCount(b))

	_, err = b.StartApplication(ctx, "count-app", nil)
	require.NoError(t, err)

	_, err = b.CreateApplicationSnapshot(ctx, "count-app", "snap-1")
	require.NoError(t, err)

	assert.Equal(t, 1, kinesisanalyticsv2.SnapshotCount(b))
}
