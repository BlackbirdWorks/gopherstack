package resourcegroups_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
	"github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

func TestResourceGroupsCreateGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr     error
		setup       func(b *resourcegroups.InMemoryBackend)
		tags        *tags.Tags
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
				_, _ = b.CreateGroup(context.Background(), "my-group", "", nil, nil, nil)
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
			g, err := b.CreateGroup(context.Background(), tt.groupName, tt.description, nil, tt.tags, nil)
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
				_, _ = b.CreateGroup(context.Background(), "my-group", "", nil, nil, nil)
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
			err := b.DeleteGroup(context.Background(), tt.groupName)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			groups, _ := b.ListGroups(context.Background(), nil, "", 0)
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
		wantName  string // expected g.Name; defaults to groupName when empty
		wantErr   error
		wantTag   string
	}{
		{
			name:      "success",
			groupName: "my-group",
			setup: func(b *resourcegroups.InMemoryBackend) {
				tgs := tags.FromMap("test.rg", map[string]string{"env": "test"})
				_, _ = b.CreateGroup(context.Background(), "my-group", "desc", nil, tgs, nil)
			},
			wantTag: "test",
		},
		{
			name:      "not_found",
			groupName: "nonexistent",
			wantErr:   resourcegroups.ErrNotFound,
		},
		{
			name:      "arn_lookup",
			groupName: "arn:aws:resource-groups:us-east-1:000000000000:group/my-group",
			wantName:  "my-group",
			setup: func(b *resourcegroups.InMemoryBackend) {
				_, _ = b.CreateGroup(context.Background(), "my-group", "desc", nil, nil, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}
			g, err := b.GetGroup(context.Background(), tt.groupName)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			wantName := tt.groupName
			if tt.wantName != "" {
				wantName = tt.wantName
			}
			assert.Equal(t, wantName, g.Name)
			if tt.wantTag != "" {
				v, _ := g.Tags.Get("env")
				assert.Equal(t, tt.wantTag, v)
			}
		})
	}
}

func TestResourceGroupsListGroups(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateGroup(context.Background(), "group-a", "", nil, nil, nil)
	_, _ = b.CreateGroup(context.Background(), "group-b", "", nil, nil, nil)

	groups, _ := b.ListGroups(context.Background(), nil, "", 0)
	assert.Len(t, groups, 2)
}

func TestResourceGroupsGetTagsByARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  error
		setup    func(b *resourcegroups.InMemoryBackend) string
		wantTags map[string]string
		name     string
	}{
		{
			name: "success",
			setup: func(b *resourcegroups.InMemoryBackend) string {
				g, _ := b.CreateGroup(context.Background(),
					"my-group",
					"",
					nil,
					tags.FromMap("test.rg", map[string]string{"env": "prod"}),
					nil,
				)

				return g.ARN
			},
			wantTags: map[string]string{"env": "prod"},
		},
		{
			name: "not_found",
			setup: func(_ *resourcegroups.InMemoryBackend) string {
				return "arn:aws:resource-groups:us-east-1:000000000000:group/nonexistent"
			},
			wantErr: resourcegroups.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
			arn := tt.setup(b)
			got, err := b.GetTagsByARN(context.Background(), arn)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantTags, got)
		})
	}
}

func TestResourceGroupsAddTagsByARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  error
		addTags  map[string]string
		wantTags map[string]string
		name     string
	}{
		{
			name:     "success",
			addTags:  map[string]string{"team": "platform"},
			wantTags: map[string]string{"env": "prod", "team": "platform"},
		},
		{
			name:    "not_found",
			addTags: map[string]string{"k": "v"},
			wantErr: resourcegroups.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
			var groupARN string
			if tt.wantErr == nil {
				g, _ := b.CreateGroup(context.Background(),
					"my-group",
					"",
					nil,
					tags.FromMap("test.rg", map[string]string{"env": "prod"}),
					nil,
				)
				groupARN = g.ARN
			} else {
				groupARN = "arn:aws:resource-groups:us-east-1:000000000000:group/nonexistent"
			}
			got, err := b.AddTagsByARN(context.Background(), groupARN, tt.addTags)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantTags, got)
		})
	}
}

func TestResourceGroupsRemoveTagsByARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		name       string
		removeKeys []string
	}{
		{
			name:       "success",
			removeKeys: []string{"env"},
		},
		{
			name:       "not_found",
			removeKeys: []string{"env"},
			wantErr:    resourcegroups.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
			var groupARN string
			if tt.wantErr == nil {
				g, _ := b.CreateGroup(context.Background(),
					"my-group",
					"",
					nil,
					tags.FromMap("test.rg", map[string]string{"env": "prod"}),
					nil,
				)
				groupARN = g.ARN
			} else {
				groupARN = "arn:aws:resource-groups:us-east-1:000000000000:group/nonexistent"
			}
			err := b.RemoveTagsByARN(context.Background(), groupARN, tt.removeKeys)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			got, _ := b.GetTagsByARN(context.Background(), groupARN)
			assert.NotContains(t, got, "env")
		})
	}
}

func TestResourceGroupsSnapshotRestore(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateGroup(context.Background(), "snap-group", "desc", &resourcegroups.ResourceQuery{
		Type:  "TAG_FILTERS_1_0",
		Query: `{}`,
	}, tags.FromMap("test.rg", map[string]string{"env": "test"}), nil)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	g, err := b2.GetGroup(context.Background(), "snap-group")
	require.NoError(t, err)
	assert.Equal(t, "snap-group", g.Name)
	assert.Equal(t, "desc", g.Description)
	v, ok := g.Tags.Get("env")
	assert.True(t, ok)
	assert.Equal(t, "test", v)
}

func TestResourceGroupsRestoreInvalidJSON(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-json"))
	require.Error(t, err)
}
