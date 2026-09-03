package kms

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// paginateTagList and parseMarker back this package's offset-token
// pagination: the same start/end/marker structure is duplicated inline
// (not via a shared function) across custom_key_stores.go, aliases.go,
// grants.go (x2), keys.go, key_policies.go and rotation.go -- eight
// operations sharing the same shape as paginateTagList tested here, and the
// same parseMarker. All match pkgs/page's algorithm (offset decodes safely
// to 0 on empty/invalid/negative input, and every call site clamps
// start >= len(...) before slicing), so this is a near-duplicate of
// pkgs/page rather than a bug: verified directly here rather than assumed
// from the reading.

func tagEntries(keys ...string) []kmsTagEntry {
	out := make([]kmsTagEntry, 0, len(keys))
	for _, k := range keys {
		out = append(out, kmsTagEntry{TagKey: k, TagValue: "v-" + k})
	}

	return out
}

func tagKeys(entries []kmsTagEntry) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.TagKey)
	}

	return out
}

func TestParseMarker_EmptyInvalidNegative(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 0, parseMarker(""))
	assert.Equal(t, 0, parseMarker("not-a-number"))
	assert.Equal(t, 0, parseMarker("-5"))
	assert.Equal(t, 3, parseMarker("3"))
}

func TestPaginateTagList_BoundaryWalk(t *testing.T) {
	t.Parallel()

	keys := []string{"k0", "k1", "k2", "k3", "k4", "k5", "k6"}
	all := tagEntries(keys...)

	var collected []string

	marker := ""
	limit := int32(3)

	for {
		out := paginateTagList(all, marker, &limit)
		collected = append(collected, tagKeys(out.Tags)...)

		if out.NextMarker == "" {
			break
		}

		marker = out.NextMarker
	}

	require.Equal(t, keys, collected)
}

func TestPaginateTagList_ExactDivisionNoTrailingCursor(t *testing.T) {
	t.Parallel()

	all := tagEntries("k0", "k1", "k2", "k3")
	limit := int32(2)

	out1 := paginateTagList(all, "", &limit)
	require.Equal(t, []string{"k0", "k1"}, tagKeys(out1.Tags))
	require.NotEmpty(t, out1.NextMarker)
	require.True(t, out1.Truncated)

	out2 := paginateTagList(all, out1.NextMarker, &limit)
	assert.Equal(t, []string{"k2", "k3"}, tagKeys(out2.Tags))
	assert.Empty(t, out2.NextMarker)
	assert.False(t, out2.Truncated)
}

func TestPaginateTagList_SinglePageNoCursor(t *testing.T) {
	t.Parallel()

	all := tagEntries("k0", "k1")
	limit := int32(10)

	out := paginateTagList(all, "", &limit)
	assert.Equal(t, []string{"k0", "k1"}, tagKeys(out.Tags))
	assert.Empty(t, out.NextMarker)
}

func TestPaginateTagList_EmptyCollectionNoCursor(t *testing.T) {
	t.Parallel()

	limit := int32(10)
	out := paginateTagList(nil, "", &limit)
	assert.Empty(t, out.Tags)
	assert.Empty(t, out.NextMarker)
}

func TestPaginateTagList_CursorRoundTrip(t *testing.T) {
	t.Parallel()

	all := tagEntries("k0", "k1", "k2")
	limit := int32(10)

	out := paginateTagList(all, strconv.Itoa(1), &limit)
	assert.Equal(t, []string{"k1", "k2"}, tagKeys(out.Tags))
}

// TestPaginateTagList_StaleCursor_PastEnd reproduces the case a retention
// sweep or deletion (or a tampered/replayed marker) triggers: the marker
// decodes to an offset beyond the current tag count. paginateTagList must
// clamp to an empty page, never slice with start > end.
func TestPaginateTagList_StaleCursor_PastEnd(t *testing.T) {
	t.Parallel()

	all := tagEntries("k0", "k1", "k2")
	limit := int32(10)

	require.NotPanics(t, func() {
		out := paginateTagList(all, strconv.Itoa(100), &limit)
		assert.Empty(t, out.Tags)
		assert.Empty(t, out.NextMarker)
		assert.False(t, out.Truncated)
	})
}
