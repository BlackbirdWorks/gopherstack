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

	type args struct {
		arn string
	}
	type wants struct {
		err bool
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name:  "collab",
			args:  args{arn: "collab"},
			wants: wants{err: false},
		},
		{
			name:  "membership",
			args:  args{arn: "membership"},
			wants: wants{err: false},
		},
		{
			name:  "table",
			args:  args{arn: "table"},
			wants: wants{err: false},
		},
		{
			name:  "assoc",
			args:  args{arn: "assoc"},
			wants: wants{err: false},
		},
		{
			name:  "template",
			args:  args{arn: "template"},
			wants: wants{err: false},
		},
		{
			name:  "budget",
			args:  args{arn: "budget"},
			wants: wants{err: false},
		},
		{
			name:  "idMapping",
			args:  args{arn: "idMapping"},
			wants: wants{err: false},
		},
		{
			name:  "idNamespace",
			args:  args{arn: "idNamespace"},
			wants: wants{err: false},
		},
		{
			name:  "cama",
			args:  args{arn: "cama"},
			wants: wants{err: false},
		},
		{
			name:  "invalid",
			args:  args{arn: "arn:aws:cleanrooms:us-east-1:123:invalid/123"},
			wants: wants{err: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := cleanrooms.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
			seed := seedFullState(t, b)

			arn := tt.args.arn
			switch arn {
			case "collab":
				arn = seed.collab.Arn
			case "membership":
				arn = seed.membership.Arn
			case "table":
				arn = seed.table.Arn
			case "assoc":
				arn = seed.assoc.Arn
			case "template":
				arn = seed.template.Arn
			case "budget":
				arn = seed.budget.Arn
			case "idMapping":
				arn = seed.idMapping.Arn
			case "idNamespace":
				arn = seed.idNamespace.Arn
			case "cama":
				arn = seed.cama.Arn
			}

			if tt.wants.err {
				_, err := b.ListTagsForResource(arn)
				require.Error(t, err)

				err = b.TagResource(arn, map[string]string{"a": "b"})
				require.Error(t, err)

				err = b.UntagResource(arn, []string{"a"})
				require.Error(t, err)

				return
			}

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

			// TagResource should work on an empty resource
			err = b.TagResource(arn, map[string]string{"foo": "bar"})
			require.NoError(t, err)

			tags, err = b.ListTagsForResource(arn)
			require.NoError(t, err)
			assert.Equal(t, "bar", tags["foo"])

			// UntagResource should work
			err = b.UntagResource(arn, []string{"foo"})
			require.NoError(t, err)

			tags, err = b.ListTagsForResource(arn)
			require.NoError(t, err)
			assert.Empty(t, tags)
		})
	}
}
