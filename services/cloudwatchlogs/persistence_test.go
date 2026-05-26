package cloudwatchlogs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *cloudwatchlogs.InMemoryBackend) string
		verify func(t *testing.T, b *cloudwatchlogs.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *cloudwatchlogs.InMemoryBackend) string {
				_, err := b.CreateLogGroup("test-group", "", "")
				if err != nil {
					return ""
				}

				return "test-group"
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend, id string) {
				t.Helper()

				groups, _, err := b.DescribeLogGroups("", "", 100)
				require.NoError(t, err)
				require.Len(t, groups, 1)
				assert.Equal(t, id, groups[0].LogGroupName)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *cloudwatchlogs.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend, _ string) {
				t.Helper()

				groups, _, err := b.DescribeLogGroups("", "", 100)
				require.NoError(t, err)
				assert.Empty(t, groups)
			},
		},
		{
			name: "round_trip_preserves_subscription_filters",
			setup: func(b *cloudwatchlogs.InMemoryBackend) string {
				_, err := b.CreateLogGroup("sub-grp", "", "")
				if err != nil {
					return ""
				}
				_ = b.PutSubscriptionFilter(
					"sub-grp", "my-filter", "ERROR",
					"arn:aws:lambda:us-east-1:123456789012:function:target",
					"", "",
				)

				return "sub-grp"
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend, id string) {
				t.Helper()

				filters, _, err := b.DescribeSubscriptionFilters(id, "", "", 100)
				require.NoError(t, err)
				require.Len(t, filters, 1)
				assert.Equal(t, "my-filter", filters[0].FilterName)
				assert.Equal(t, "ERROR", filters[0].FilterPattern)
				assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:target", filters[0].DestinationArn)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := cloudwatchlogs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := cloudwatchlogs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}

func TestHandler_SnapshotRestore_PreservesTags(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	_, err := b.CreateLogGroup("tagged-group", "", "")
	require.NoError(t, err)

	h := cloudwatchlogs.NewHandler(b)

	// Set tags on the log group via the handler so the tag-serialization path is exercised.
	h.SetTagsForTest("tagged-group", map[string]string{"env": "prod", "team": "ops"})

	snap := h.Snapshot()
	require.NotNil(t, snap)

	// Restore into a fresh handler.
	b2 := cloudwatchlogs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	h2 := cloudwatchlogs.NewHandler(b2)
	require.NoError(t, h2.Restore(snap))

	// Log group should be present in the restored backend.
	groups, _, gErr := b2.DescribeLogGroups("", "", 100)
	require.NoError(t, gErr)
	require.Len(t, groups, 1)
	assert.Equal(t, "tagged-group", groups[0].LogGroupName)

	// Tags should have been restored.
	restoredTags := h2.GetTagsForTest("tagged-group")
	assert.Equal(t, "prod", restoredTags["env"])
	assert.Equal(t, "ops", restoredTags["team"])
}

func TestHandler_SnapshotRestore_StaleTagsCleared(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	_, err := b.CreateLogGroup("g", "", "")
	require.NoError(t, err)

	// Original handler has a tag.
	h := cloudwatchlogs.NewHandler(b)
	h.SetTagsForTest("g", map[string]string{"stale": "yes"})

	// Snapshot a second handler that has no tags.
	b2 := cloudwatchlogs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	_, err = b2.CreateLogGroup("g", "", "")
	require.NoError(t, err)
	h2 := cloudwatchlogs.NewHandler(b2)
	snap := h2.Snapshot()
	require.NotNil(t, snap)

	// Restore the snapshot into h — stale tags should be cleared.
	require.NoError(t, h.Restore(snap))
	restoredTags := h.GetTagsForTest("g")
	assert.Empty(t, restoredTags)
}

func TestHandler_SnapshotRestore_InvalidData(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	h := cloudwatchlogs.NewHandler(b)
	err := h.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}

func TestInMemoryBackend_SnapshotRestore_PreservesRetention(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	_, err := b.CreateLogGroup("ret-grp", "", "")
	require.NoError(t, err)
	require.NoError(t, b.SetRetentionPolicy("ret-grp", func() *int32 {
		v := int32(14)

		return &v
	}()))

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := cloudwatchlogs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	groups, _, err := b2.DescribeLogGroups("", "", 100)
	require.NoError(t, err)
	require.Len(t, groups, 1)
	require.NotNil(t, groups[0].RetentionInDays)
	assert.Equal(t, int32(14), *groups[0].RetentionInDays)
}
