package kinesisanalyticsv2_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

// TestBackend_RollbackApplication verifies that RollbackApplication reverts
// an application to its previous configuration version. Before
// checkAndBumpVersion started recording version history (see store.go),
// b.versions only ever held the version-1 snapshot from CreateApplication, so
// RollbackApplication's "at least 2 versions" guard could never be satisfied
// by real traffic and this operation always failed.
func TestBackend_RollbackApplication(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)

		app, err := b.CreateApplication(ctx, "rollback-app", "FLINK-1_18", "", "first description", "", nil)
		require.NoError(t, err)
		require.Equal(t, int64(1), app.ApplicationVersionID)

		updated, opID, err := b.UpdateApplication(ctx, kinesisanalyticsv2.UpdateApplicationParams{
			Name:                        "rollback-app",
			CurrentApplicationVersionID: 1,
			ApplicationDescription:      "second description",
		})
		require.NoError(t, err)
		require.Equal(t, int64(2), updated.ApplicationVersionID)
		require.NotEmpty(t, opID)

		rolledBack, rollbackOpID, err := b.RollbackApplication(ctx, "rollback-app", 2)
		require.NoError(t, err)
		assert.NotEmpty(t, rollbackOpID)
		assert.Equal(t, int64(3), rolledBack.ApplicationVersionID)
		assert.Equal(t, "first description", rolledBack.ApplicationDescription)

		op, err := b.DescribeApplicationOperation(ctx, "rollback-app", rollbackOpID)
		require.NoError(t, err)
		assert.Equal(t, "RollbackApplication", op.Operation)
		assert.Equal(t, kinesisanalyticsv2.OperationStatusSuccessful, op.OperationStatus)
	})

	t.Run("not enough version history", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)

		_, err := b.CreateApplication(ctx, "rollback-fresh-app", "FLINK-1_18", "", "", "", nil)
		require.NoError(t, err)

		_, _, err = b.RollbackApplication(ctx, "rollback-fresh-app", 0)
		require.Error(t, err)
	})

	t.Run("version mismatch", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)

		_, err := b.CreateApplication(ctx, "rollback-mismatch-app", "FLINK-1_18", "", "", "", nil)
		require.NoError(t, err)

		_, _, err = b.UpdateApplication(ctx, kinesisanalyticsv2.UpdateApplicationParams{
			Name:                        "rollback-mismatch-app",
			CurrentApplicationVersionID: 1,
			ApplicationDescription:      "second description",
		})
		require.NoError(t, err)

		_, _, err = b.RollbackApplication(ctx, "rollback-mismatch-app", 99)
		require.ErrorIs(t, err, kinesisanalyticsv2.ErrConcurrentModification)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)

		_, _, err := b.RollbackApplication(ctx, "missing-rollback-app", 0)
		require.ErrorIs(t, err, kinesisanalyticsv2.ErrNotFound)
	})
}

// TestBackend_ApplicationVersionHistory verifies that ListApplicationVersions
// and DescribeApplicationVersion reflect every version-bumping change, not
// just the version-1 snapshot from CreateApplication.
func TestBackend_ApplicationVersionHistory(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)

	_, err := b.CreateApplication(ctx, "version-history-app", "FLINK-1_18", "", "", "", nil)
	require.NoError(t, err)

	_, err = b.AddApplicationCloudWatchLoggingOption(ctx, "version-history-app", 0,
		"arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s", "")
	require.NoError(t, err)

	versions, _, err := b.ListApplicationVersions(ctx, "version-history-app", "")
	require.NoError(t, err)
	require.Len(t, versions, 2, "expected a version-history entry for both CreateApplication and the Add* call")
	assert.Equal(t, int64(1), versions[0].ApplicationVersionID)
	assert.Equal(t, int64(2), versions[1].ApplicationVersionID)

	v2, err := b.DescribeApplicationVersion(ctx, "version-history-app", 2)
	require.NoError(t, err)
	assert.Len(t, v2.CloudWatchLoggingOptionDescs, 1)
}
