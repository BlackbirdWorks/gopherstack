package pinnedid_test

import (
	"strconv"
	"testing"

	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func errCode(t *testing.T, err error) string {
	t.Helper()

	var apiErr smithy.APIError

	require.ErrorAs(t, err, &apiErr)

	return apiErr.ErrorCode()
}

// TestPinnedID drives every service that honours the reserved gopherstack:id
// create-time tag through the real SDK: the ID is pinned, the tag is never
// stored or echoed, bad IDs and reused IDs are rejected, and TagResource
// refuses the reserved key.
func TestPinnedID(t *testing.T) {
	t.Parallel()

	c := newClients(t)

	for _, tc := range allCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			next := func() string { return strconv.FormatInt(c.seq.Add(1), 10) }

			got, err := tc.create(ctx, c, tc.valid, "a"+next())
			require.NoError(t, err)
			assert.Equal(t, tc.valid, got.id, "pinned ID")
			assert.NotContains(t, got.createTags, tags.PinnedIDKey, "create response echoed the reserved tag")

			keys, err := tc.listTags(ctx, c, got.handle)
			require.NoError(t, err)
			assert.NotContains(t, keys, tags.PinnedIDKey, "tag listing leaked the reserved tag")

			_, err = tc.create(ctx, c, tc.invalid, "b"+next())
			require.Error(t, err, "invalid pinned ID")
			assert.Equal(t, tc.invalidCode, errCode(t, err))

			if tc.conflictCode != "" {
				_, err = tc.create(ctx, c, tc.valid, "c"+next())
				require.Error(t, err, "reused pinned ID")
				assert.Equal(t, tc.conflictCode, errCode(t, err))
			}

			require.Error(t, tc.retag(ctx, c, got.handle), "TagResource must refuse the reserved key")

			keys, err = tc.listTags(ctx, c, got.handle)
			require.NoError(t, err)
			assert.NotContains(t, keys, tags.PinnedIDKey)
		})
	}
}
