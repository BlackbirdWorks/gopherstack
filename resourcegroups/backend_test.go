package resourcegroups_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/resourcegroups"
)

func TestResourceGroupsBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T, b *resourcegroups.InMemoryBackend)
	}{
		{
			name: "CreateGroup",
			run: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				g, err := b.CreateGroup("my-group", "test description", nil)
				require.NoError(t, err)
				assert.Equal(t, "my-group", g.Name)
				assert.Contains(t, g.ARN, "arn:aws:resource-groups:")
				assert.Equal(t, "test description", g.Description)
			},
		},
		{
			name: "CreateGroup/AlreadyExists",
			run: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				_, err := b.CreateGroup("my-group", "", nil)
				require.NoError(t, err)

				_, err = b.CreateGroup("my-group", "", nil)
				require.Error(t, err)
				assert.ErrorIs(t, err, resourcegroups.ErrAlreadyExists)
			},
		},
		{
			name: "DeleteGroup",
			run: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				_, err := b.CreateGroup("my-group", "", nil)
				require.NoError(t, err)

				err = b.DeleteGroup("my-group")
				require.NoError(t, err)

				groups := b.ListGroups()
				assert.Empty(t, groups)
			},
		},
		{
			name: "DeleteGroup/NotFound",
			run: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				err := b.DeleteGroup("nonexistent")
				require.Error(t, err)
				assert.ErrorIs(t, err, resourcegroups.ErrNotFound)
			},
		},
		{
			name: "GetGroup",
			run: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				_, err := b.CreateGroup("my-group", "desc", map[string]string{"env": "test"})
				require.NoError(t, err)

				g, err := b.GetGroup("my-group")
				require.NoError(t, err)
				assert.Equal(t, "my-group", g.Name)
				assert.Equal(t, "test", g.Tags["env"])
			},
		},
		{
			name: "GetGroup/NotFound",
			run: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				_, err := b.GetGroup("nonexistent")
				require.Error(t, err)
				assert.ErrorIs(t, err, resourcegroups.ErrNotFound)
			},
		},
		{
			name: "ListGroups",
			run: func(t *testing.T, b *resourcegroups.InMemoryBackend) {
				_, _ = b.CreateGroup("group-a", "", nil)
				_, _ = b.CreateGroup("group-b", "", nil)

				groups := b.ListGroups()
				assert.Len(t, groups, 2)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
			tt.run(t, b)
		})
	}
}
