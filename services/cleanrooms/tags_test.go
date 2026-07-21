package cleanrooms_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/cleanrooms"
)

func TestTags_ResourceARNExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{"Tags_ResourceARNExists"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := cleanrooms.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
			seed := seedFullState(t, b)

			arns := []string{
				seed.collab.Arn,
				seed.membership.Arn,
				seed.table.Arn,
				seed.assoc.Arn,
				seed.template.Arn,
				seed.budget.Arn,
				seed.idMapping.Arn,
				seed.idNamespace.Arn,
				seed.cama.Arn,
			}

			for _, arn := range arns {
				// Get existing tags
				tags, err := b.ListTagsForResource(arn)
				require.NoError(t, err)

				// Remove all tags
				var keys []string
				for k := range tags {
					keys = append(keys, k)
				}
				if len(keys) > 0 {
					err = b.UntagResource(arn, keys)
					require.NoError(t, err)
				}

				// Now ListTagsForResource should hit resourceARNExists
				emptyTags, err := b.ListTagsForResource(arn)
				require.NoError(t, err, "arn: %s", arn)
				assert.Empty(t, emptyTags)
			}

			// TagResource should work on an empty resource
			err := b.TagResource(seed.table.Arn, map[string]string{"foo": "bar"})
			require.NoError(t, err)

			tags, err := b.ListTagsForResource(seed.table.Arn)
			require.NoError(t, err)
			assert.Equal(t, "bar", tags["foo"])

			// UntagResource should work
			err = b.UntagResource(seed.table.Arn, []string{"foo"})
			require.NoError(t, err)

			tags, err = b.ListTagsForResource(seed.table.Arn)
			require.NoError(t, err)
			assert.Empty(t, tags)

			// Invalid ARN should return ResourceNotFoundException
			_, err = b.ListTagsForResource("arn:aws:cleanrooms:us-east-1:123:invalid/123")
			require.Error(t, err)

			err = b.TagResource("arn:aws:cleanrooms:us-east-1:123:invalid/123", map[string]string{"a": "b"})
			require.Error(t, err)

			err = b.UntagResource("arn:aws:cleanrooms:us-east-1:123:invalid/123", []string{"a"})
			require.Error(t, err)
		})
	}
}
