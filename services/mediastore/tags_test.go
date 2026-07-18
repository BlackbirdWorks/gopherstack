package mediastore_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags        map[string]string
		wantTags    map[string]string
		name        string
		container   string
		removeKeys  []string
		createFirst bool
	}{
		{
			name:        "tag and untag resource",
			container:   "tag-me",
			tags:        map[string]string{"env": "test", "team": "backend"},
			removeKeys:  []string{"team"},
			wantTags:    map[string]string{"env": "test"},
			createFirst: true,
		},
		{
			name:        "list tags on empty resource",
			container:   "empty-tags",
			tags:        nil,
			wantTags:    map[string]string{},
			createFirst: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			_, err := b.CreateContainer(context.Background(), testAccountID, tt.container, nil)
			require.NoError(t, err)

			c, descErr := b.DescribeContainer(context.Background(), tt.container)
			require.NoError(t, descErr)

			if len(tt.tags) > 0 {
				err = b.TagResource(context.Background(), c.ARN, tt.tags)
				require.NoError(t, err)
			}

			if len(tt.removeKeys) > 0 {
				err = b.UntagResource(context.Background(), c.ARN, tt.removeKeys)
				require.NoError(t, err)
			}

			tags, err := b.ListTagsForResource(context.Background(), c.ARN)
			require.NoError(t, err)
			assert.Equal(t, tt.wantTags, tags)
		})
	}
}
