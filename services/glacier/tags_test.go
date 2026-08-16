package glacier_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glacier"
)

func TestTagsStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		addTags    map[string]string
		wantTags   map[string]string
		name       string
		removeTags []string
	}{
		{
			name:     "add_tags",
			addTags:  map[string]string{"env": "test", "team": "infra"},
			wantTags: map[string]string{"env": "test", "team": "infra"},
		},
		{
			name:       "add_and_remove_tags",
			addTags:    map[string]string{"env": "test", "team": "infra"},
			removeTags: []string{"team"},
			wantTags:   map[string]string{"env": "test"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()
			_, err := bk.CreateVault(testAccountID, testRegion, "vault")
			require.NoError(t, err)

			err = bk.AddTagsToVault(testAccountID, testRegion, "vault", tt.addTags)
			require.NoError(t, err)

			if len(tt.removeTags) > 0 {
				err = bk.RemoveTagsFromVault(testAccountID, testRegion, "vault", tt.removeTags)
				require.NoError(t, err)
			}

			tags, err := bk.ListTagsForVault(testAccountID, testRegion, "vault")
			require.NoError(t, err)
			assert.Equal(t, tt.wantTags, tags)
		})
	}
}
