package dlm_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dlm"
)

// ---------------------------------------------------------------------------
// UntagResource: multiple keys
// ---------------------------------------------------------------------------

func TestBackend_UntagResource_MultipleKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		initTags   map[string]string
		removeKeys []string
		wantKeys   []string
	}{
		{
			name:       "remove two of three tags",
			initTags:   map[string]string{"a": "1", "b": "2", "c": "3"},
			removeKeys: []string{"a", "b"},
			wantKeys:   []string{"c"},
		},
		{
			name:       "remove non-existent key is safe",
			initTags:   map[string]string{"x": "1"},
			removeKeys: []string{"y"},
			wantKeys:   []string{"x"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := dlm.NewInMemoryBackend("000000000000", "us-east-1")
			p, err := b.CreateLifecyclePolicy("desc", "role", "ENABLED", tc.initTags, nil)
			require.NoError(t, err)

			err = b.UntagResource(p.PolicyArn, tc.removeKeys)
			require.NoError(t, err)

			tags, err := b.ListTagsForResource(p.PolicyArn)
			require.NoError(t, err)
			for _, k := range tc.wantKeys {
				assert.Contains(t, tags, k)
			}
			for _, k := range tc.removeKeys {
				// Only assert removed if it was initially present.
				if _, had := tc.initTags[k]; had {
					assert.NotContains(t, tags, k)
				}
			}
		})
	}
}

// TestBackend_Tags_SurviveSnapshotRestore verifies tags applied via
// TagResource/UntagResource persist across a Snapshot -> Restore cycle and
// remain mutable on the restored backend.
func TestBackend_Tags_SurviveSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "tags_via_tagresource"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b1 := dlm.NewInMemoryBackend("000000000000", "us-east-1")

			p, err := b1.CreateLifecyclePolicy("test", "arn:aws:iam::000000000000:role/r", "ENABLED", nil, nil)
			require.NoError(t, err)

			// Tag via TagResource.
			require.NoError(t, b1.TagResource(p.PolicyArn, map[string]string{"env": "prod", "team": "infra"}))

			// Verify tags appear via GetLifecyclePolicy (reads p.Tags).
			got, err := b1.GetLifecyclePolicy(p.PolicyID)
			require.NoError(t, err)
			assert.Equal(t, "prod", got.Tags["env"])
			assert.Equal(t, "infra", got.Tags["team"])

			// Snapshot + restore.
			snap := b1.Snapshot(context.Background())
			b2 := dlm.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b2.Restore(context.Background(), snap))

			// Tags must survive restore.
			got2, err := b2.GetLifecyclePolicy(p.PolicyID)
			require.NoError(t, err)
			assert.Equal(t, "prod", got2.Tags["env"])

			// TagResource on restored backend must also affect GetLifecyclePolicy.
			require.NoError(t, b2.TagResource(p.PolicyArn, map[string]string{"new": "tag"}))

			got3, err := b2.GetLifecyclePolicy(p.PolicyID)
			require.NoError(t, err)
			assert.Equal(t, "tag", got3.Tags["new"])
			assert.Equal(t, "prod", got3.Tags["env"])
		})
	}
}

// TestBackend_Tags_UntagAndList verifies UntagResource removes the requested
// keys and ListTagsForResource reflects the remaining set.
func TestBackend_Tags_UntagAndList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantTags map[string]string
		tagKeys  []string
	}{
		{
			name:     "untag_one_key",
			tagKeys:  []string{"env"},
			wantTags: map[string]string{"team": "infra"},
		},
		{
			name:     "untag_all_keys",
			tagKeys:  []string{"env", "team"},
			wantTags: map[string]string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := dlm.NewInMemoryBackend("000000000000", "us-east-1")

			p, err := b.CreateLifecyclePolicy("t", "arn:aws:iam::000000000000:role/r", "ENABLED",
				map[string]string{"env": "prod", "team": "infra"}, nil)
			require.NoError(t, err)

			require.NoError(t, b.UntagResource(p.PolicyArn, tc.tagKeys))

			tags, err := b.ListTagsForResource(p.PolicyArn)
			require.NoError(t, err)
			assert.Equal(t, tc.wantTags, tags)
		})
	}
}
