package kinesisanalyticsv2_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

// TestBackend_AddApplicationCloudWatchLoggingOption_ConcurrentModification
// verifies that supplying a stale CurrentApplicationVersionId to a config
// Add* op fails with ErrConcurrentModification rather than silently applying.
func TestBackend_AddApplicationCloudWatchLoggingOption_ConcurrentModification(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)

	_, err := b.CreateApplication(ctx, "ver-app", "FLINK-1_18", "", "", "", nil)
	require.NoError(t, err)

	// version check: wrong version should fail
	err = b.AddApplicationCloudWatchLoggingOption(ctx, "ver-app", 99,
		"arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s", "")
	require.ErrorIs(t, err, kinesisanalyticsv2.ErrConcurrentModification)
}
