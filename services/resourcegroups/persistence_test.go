package resourcegroups_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

func TestResourceGroups_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *resourcegroups.InMemoryBackend)
		verify func(t *testing.T, b *resourcegroups.InMemoryBackend)
		name   string
	}{
		{
			name:  "empty_backend",
			setup: func(_ *testing.T, _ *resourcegroups.InMemoryBackend) {},
			verify: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				t.Helper()

				groups, _ := b.ListGroups(context.Background(), nil, "", 0)
				assert.Empty(t, groups)
			},
		},
		{
			name: "group_preserved",
			setup: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateGroup(context.Background(),
					"my-group",
					"test description",
					&resourcegroups.ResourceQuery{Type: "TAG_FILTERS_1_0", Query: "{}"},
					nil,
					nil,
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				t.Helper()

				groups, _ := b.ListGroups(context.Background(), nil, "", 0)
				require.Len(t, groups, 1)
				assert.Equal(t, "my-group", groups[0].Name)
				assert.Equal(t, "test description", groups[0].Description)

				// Name-based lookup.
				g, err := b.GetGroup(context.Background(), "my-group")
				require.NoError(t, err)
				assert.Equal(t, "my-group", g.Name)
			},
		},
		{
			name: "arn_index_rebuilt",
			setup: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateGroup(context.Background(), "indexed-group", "desc", nil, nil, nil)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				t.Helper()

				groups, _ := b.ListGroups(context.Background(), nil, "", 0)
				require.Len(t, groups, 1)

				// ARN-based tag lookup validates ARN index was rebuilt.
				tagMap, err := b.GetTagsByARN(context.Background(), groups[0].ARN)
				require.NoError(t, err)
				assert.NotNil(t, tagMap)
			},
		},
		{
			name: "tags_preserved",
			setup: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				t.Helper()

				g, err := b.CreateGroup(context.Background(), "tagged-group", "", nil, nil, nil)
				require.NoError(t, err)

				_, err = b.AddTagsByARN(context.Background(), g.ARN, map[string]string{"owner": "alice"})
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				t.Helper()

				groups, _ := b.ListGroups(context.Background(), nil, "", 0)
				require.Len(t, groups, 1)

				tagMap, err := b.GetTagsByARN(context.Background(), groups[0].ARN)
				require.NoError(t, err)
				assert.Equal(t, "alice", tagMap["owner"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := resourcegroups.NewInMemoryBackend("123456789012", "us-east-1")
			tt.setup(t, b)

			snap := b.Snapshot()
			require.NotNil(t, snap)

			b2 := resourcegroups.NewInMemoryBackend("123456789012", "us-east-1")
			err := b2.Restore(snap)
			require.NoError(t, err)

			tt.verify(t, b2)
		})
	}
}
