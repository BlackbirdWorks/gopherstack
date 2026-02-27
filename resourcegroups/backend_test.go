package resourcegroups_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/resourcegroups"
)

func TestResourceGroups_CreateGroup(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	g, err := b.CreateGroup("my-group", "test description", nil)
	require.NoError(t, err)
	assert.Equal(t, "my-group", g.Name)
	assert.Contains(t, g.ARN, "arn:aws:resource-groups:")
	assert.Equal(t, "test description", g.Description)
}

func TestResourceGroups_CreateGroup_AlreadyExists(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateGroup("my-group", "", nil)
	require.NoError(t, err)

	_, err = b.CreateGroup("my-group", "", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroups.ErrAlreadyExists)
}

func TestResourceGroups_DeleteGroup(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateGroup("my-group", "", nil)
	require.NoError(t, err)

	err = b.DeleteGroup("my-group")
	require.NoError(t, err)

	groups := b.ListGroups()
	assert.Empty(t, groups)
}

func TestResourceGroups_DeleteGroup_NotFound(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.DeleteGroup("nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroups.ErrNotFound)
}

func TestResourceGroups_GetGroup(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateGroup("my-group", "desc", map[string]string{"env": "test"})
	require.NoError(t, err)

	g, err := b.GetGroup("my-group")
	require.NoError(t, err)
	assert.Equal(t, "my-group", g.Name)
	assert.Equal(t, "test", g.Tags["env"])
}

func TestResourceGroups_GetGroup_NotFound(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.GetGroup("nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroups.ErrNotFound)
}

func TestResourceGroups_ListGroups(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateGroup("group-a", "", nil)
	_, _ = b.CreateGroup("group-b", "", nil)

	groups := b.ListGroups()
	assert.Len(t, groups, 2)
}
