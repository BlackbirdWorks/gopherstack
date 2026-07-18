package resourcegroups_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
	"github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

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

// TestCreateGroupRejectsReservedTag verifies backend-level tag validation:
// CreateGroup must reject tag keys with the "aws:" reserved prefix.
func TestCreateGroupRejectsReservedTag(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateGroup(context.Background(),
		"my-group",
		"desc",
		nil,
		tags.FromMap("test.rg", map[string]string{"aws:reserved": "v"}),
		nil,
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroups.ErrValidation)
}
