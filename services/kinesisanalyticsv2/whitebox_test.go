package kinesisanalyticsv2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBackend_UpdateApplication_ConditionalToken verifies that
// UpdateApplication's ConditionalToken implements the same
// optimistic-concurrency check as CurrentApplicationVersionId (real AWS: "you
// must provide the CurrentApplicationVersionId or the ConditionalToken"),
// and that a mismatched token is rejected with ErrConcurrentModification
// without mutating the application or bumping its version.
func TestBackend_UpdateApplication_ConditionalToken(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("valid token succeeds and rotates", func(t *testing.T) {
		t.Parallel()

		b := NewInMemoryBackend("000000000000", "us-east-1")
		app, err := b.CreateApplication(ctx, "token-app", "FLINK-1_18", "", "", "", nil)
		require.NoError(t, err)

		tok := conditionalToken(app)

		updated, opID, err := b.UpdateApplication(ctx, UpdateApplicationParams{
			Name:                       "token-app",
			ConditionalToken:           tok,
			ServiceExecutionRoleUpdate: "arn:aws:iam::000000000000:role/updated-via-token",
		})
		require.NoError(t, err)
		assert.NotEmpty(t, opID)
		assert.Equal(t, int64(2), updated.ApplicationVersionID)
		assert.NotEqual(t, tok, conditionalToken(updated), "token must rotate on version bump")
	})

	t.Run("stale token rejected", func(t *testing.T) {
		t.Parallel()

		b := NewInMemoryBackend("000000000000", "us-east-1")
		app, err := b.CreateApplication(ctx, "stale-token-app", "FLINK-1_18", "", "orig", "", nil)
		require.NoError(t, err)

		staleTok := conditionalToken(app)

		// Bump the version once via a normal update so staleTok no longer matches.
		_, _, err = b.UpdateApplication(ctx, UpdateApplicationParams{
			Name:                       "stale-token-app",
			ServiceExecutionRoleUpdate: "arn:aws:iam::000000000000:role/first-update",
		})
		require.NoError(t, err)

		_, _, err = b.UpdateApplication(ctx, UpdateApplicationParams{
			Name:                       "stale-token-app",
			ConditionalToken:           staleTok,
			ServiceExecutionRoleUpdate: "arn:aws:iam::000000000000:role/should-not-apply",
		})
		require.ErrorIs(t, err, ErrConcurrentModification)

		current, err := b.DescribeApplication(ctx, "stale-token-app")
		require.NoError(t, err)
		assert.Equal(t, "arn:aws:iam::000000000000:role/first-update", current.ServiceExecutionRole,
			"rejected update must not mutate state")
	})
}
