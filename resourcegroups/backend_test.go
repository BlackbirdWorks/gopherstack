package resourcegroups_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/resourcegroups"
)

func TestResourceGroupsCreateGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr     error
		setup       func(b *resourcegroups.InMemoryBackend)
		tags        map[string]string
		name        string
		groupName   string
		description string
	}{
		{
			name:        "success",
			groupName:   "my-group",
			description: "test description",
		},
		{
			name:      "already_exists",
			groupName: "my-group",
			setup: func(b *resourcegroups.InMemoryBackend) {
				_, _ = b.CreateGroup("my-group", "", nil)
			},
			wantErr: resourcegroups.ErrAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}
			g, err := b.CreateGroup(tt.groupName, tt.description, tt.tags)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.groupName, g.Name)
			assert.Contains(t, g.ARN, "arn:aws:resource-groups:")
			assert.Equal(t, tt.description, g.Description)
		})
	}
}

func TestResourceGroupsDeleteGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(b *resourcegroups.InMemoryBackend)
		name      string
		groupName string
	}{
		{
			name:      "success",
			groupName: "my-group",
			setup: func(b *resourcegroups.InMemoryBackend) {
				_, _ = b.CreateGroup("my-group", "", nil)
			},
		},
		{
			name:      "not_found",
			groupName: "nonexistent",
			wantErr:   resourcegroups.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}
			err := b.DeleteGroup(tt.groupName)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			groups := b.ListGroups()
			assert.Empty(t, groups)
		})
	}
}

func TestResourceGroupsGetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(b *resourcegroups.InMemoryBackend)
		groupName string
		wantErr   error
		wantTag   string
	}{
		{
			name:      "success",
			groupName: "my-group",
			setup: func(b *resourcegroups.InMemoryBackend) {
				_, _ = b.CreateGroup("my-group", "desc", map[string]string{"env": "test"})
			},
			wantTag: "test",
		},
		{
			name:      "not_found",
			groupName: "nonexistent",
			wantErr:   resourcegroups.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}
			g, err := b.GetGroup(tt.groupName)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.groupName, g.Name)
			if tt.wantTag != "" {
				assert.Equal(t, tt.wantTag, g.Tags["env"])
			}
		})
	}
}

func TestResourceGroupsListGroups(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateGroup("group-a", "", nil)
	_, _ = b.CreateGroup("group-b", "", nil)

	groups := b.ListGroups()
	assert.Len(t, groups, 2)
}
