package fsx

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// letterKeys returns n single/double-letter ascending keys, e.g. "a".."z",
// "za".."zz", suitable for the keyFn contract paginate expects (sorted,
// ascending).
func letterKeys(n int) []string {
	out := make([]string, 0, n)
	for i := range n {
		if i < 26 {
			out = append(out, string(rune('a'+i)))
		} else {
			out = append(out, string(rune('a'+i/26-1))+string(rune('a'+i%26)))
		}
	}

	return out
}

func TestPaginate_BoundaryWalk(t *testing.T) {
	t.Parallel()

	keys := letterKeys(17)
	keyFn := func(i int) string { return keys[i] }

	var collected []string

	token := ""
	for {
		start, end, next := paginate(len(keys), 4, token, keyFn)
		collected = append(collected, keys[start:end]...)

		if next == "" {
			break
		}

		token = next
	}

	require.Equal(t, keys, collected)
}

func TestPaginate_FinalPageEmptyCursor(t *testing.T) {
	t.Parallel()

	keys := []string{"a", "b", "c"}
	keyFn := func(i int) string { return keys[i] }

	start, end, next := paginate(len(keys), 2, "", keyFn)
	require.Equal(t, []string{"a", "b"}, keys[start:end])
	require.NotEmpty(t, next)

	start, end, next = paginate(len(keys), 2, next, keyFn)
	assert.Equal(t, []string{"c"}, keys[start:end])
	assert.Empty(t, next)
}

func TestPaginate_SinglePageNoCursor(t *testing.T) {
	t.Parallel()

	keys := []string{"a", "b"}
	keyFn := func(i int) string { return keys[i] }

	start, end, next := paginate(len(keys), 10, "", keyFn)
	assert.Equal(t, keys, keys[start:end])
	assert.Empty(t, next)
}

func TestPaginate_EmptyCollectionNoCursor(t *testing.T) {
	t.Parallel()

	keyFn := func(int) string { return "" }

	start, end, next := paginate(0, 10, "", keyFn)
	assert.Equal(t, 0, start)
	assert.Equal(t, 0, end)
	assert.Empty(t, next)
}

func TestPaginate_ExactDivisionNoTrailingCursor(t *testing.T) {
	t.Parallel()

	keys := []string{"a", "b", "c", "d"}
	keyFn := func(i int) string { return keys[i] }

	start, end, next := paginate(len(keys), 2, "", keyFn)
	require.Equal(t, []string{"a", "b"}, keys[start:end])
	require.NotEmpty(t, next)

	start, end, next = paginate(len(keys), 2, next, keyFn)
	assert.Equal(t, []string{"c", "d"}, keys[start:end])
	assert.Empty(t, next, "last full page must not emit a cursor pointing past the end")
}

func TestPaginate_CursorRoundTrip(t *testing.T) {
	t.Parallel()

	keys := []string{"a", "b", "c", "d", "e"}
	keyFn := func(i int) string { return keys[i] }

	_, _, next := paginate(len(keys), 2, "", keyFn)
	require.Equal(t, "c", next, "cursor is the opaque key of the first item on the next page")

	start, end, _ := paginate(len(keys), 2, next, keyFn)
	assert.Equal(t, []string{"c", "d"}, keys[start:end])
}

// TestPaginate_StaleCursor_DeletedItem reproduces the case a retention sweep
// or deletion triggers: the item the cursor names is gone by the time the
// next page is fetched. paginate must resume after where that item would
// have sorted, not silently restart at index 0 -- restarting means a client
// following the cursor gets page one, forever.
func TestPaginate_StaleCursor_DeletedItem(t *testing.T) {
	t.Parallel()

	// "c" was the resume point but has since been deleted from the
	// collection; the caller still presents the token it was given.
	remaining := []string{"a", "b", "d", "e"}
	remainingKeyFn := func(i int) string { return remaining[i] }

	start, end, next := paginate(len(remaining), 10, "c", remainingKeyFn)

	got := remaining[start:end]
	assert.Equal(t, []string{"d", "e"}, got,
		"must resume after the deleted item's sort position, not restart at page one")
	assert.Empty(t, next)
}

// TestPaginate_TamperedCursor_NoMatch is the same shape but for a cursor
// that never named a real item (client-constructed, or the collection was
// entirely replaced). A safe helper must terminate (empty result), never
// spin: repeatedly calling with the same unmatched token must not keep
// returning items[0:limit].
func TestPaginate_TamperedCursor_NoMatch(t *testing.T) {
	t.Parallel()

	keys := []string{"a", "b", "c"}
	keyFn := func(i int) string { return keys[i] }

	start, end, next := paginate(len(keys), 10, "zzz-does-not-exist", keyFn)

	assert.Equal(t, []string{}, keys[start:end])
	assert.Empty(t, next)
}
