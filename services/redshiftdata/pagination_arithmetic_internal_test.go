package redshiftdata

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── paginateStrings (ListDatabases, ListSchemas) ─────────────────────────

func TestPaginateStrings_BoundaryWalk(t *testing.T) {
	t.Parallel()

	all := []string{"s0", "s1", "s2", "s3", "s4", "s5", "s6"}

	var collected []string

	token := ""
	for {
		page, next := paginateStrings(all, token, 3, 100)
		collected = append(collected, page...)

		if next == "" {
			break
		}

		token = next
	}

	require.Equal(t, all, collected)
}

func TestPaginateStrings_ExactDivisionNoTrailingCursor(t *testing.T) {
	t.Parallel()

	all := []string{"s0", "s1", "s2", "s3"}

	page1, tok1 := paginateStrings(all, "", 2, 100)
	require.Equal(t, []string{"s0", "s1"}, page1)
	require.NotEmpty(t, tok1)

	page2, tok2 := paginateStrings(all, tok1, 2, 100)
	assert.Equal(t, []string{"s2", "s3"}, page2)
	assert.Empty(t, tok2)
}

func TestPaginateStrings_SinglePageNoCursor(t *testing.T) {
	t.Parallel()

	all := []string{"s0", "s1"}
	page, tok := paginateStrings(all, "", 10, 100)
	assert.Equal(t, all, page)
	assert.Empty(t, tok)
}

func TestPaginateStrings_EmptyCollectionNoCursor(t *testing.T) {
	t.Parallel()

	page, tok := paginateStrings(nil, "", 10, 100)
	assert.Empty(t, page)
	assert.Empty(t, tok)
}

// TestPaginateStrings_CursorRoundTrip: the cursor names the first item of
// the page being resumed (that's what the encoder emits: the first item
// past the previous page's boundary), so decoding must resume AT it,
// inclusive -- not after it.
func TestPaginateStrings_CursorRoundTrip(t *testing.T) {
	t.Parallel()

	all := []string{"s0", "s1", "s2"}
	page, _ := paginateStrings(all, "s1", 10, 100)
	assert.Equal(t, []string{"s1", "s2"}, page)
}

func TestPaginateStrings_DefaultMaxWhenUnset(t *testing.T) {
	t.Parallel()

	all := []string{"s0", "s1", "s2"}
	page, tok := paginateStrings(all, "", 0, 2)
	assert.Equal(t, []string{"s0", "s1"}, page)
	assert.NotEmpty(t, tok)
}

// TestPaginateStrings_TamperedCursor_NoMatch reproduces the case a
// tampered/garbage nextToken triggers: the name it names was never in the
// collection. paginateStrings must terminate (empty page, no cursor), not
// silently restart at index 0 -- restarting means a client following the
// cursor gets page one, forever.
func TestPaginateStrings_TamperedCursor_NoMatch(t *testing.T) {
	t.Parallel()

	all := []string{"s0", "s1", "s2"}
	page, tok := paginateStrings(all, "does-not-exist", 10, 100)
	assert.Empty(t, page, "an unmatched cursor must not restart at page one")
	assert.Empty(t, tok)
}

// ── paginateMaps (ListTables) ────────────────────────────────────────────

func namedMaps(names ...string) []map[string]any {
	out := make([]map[string]any, 0, len(names))
	for _, n := range names {
		out = append(out, map[string]any{keyName: n})
	}

	return out
}

func mapNames(maps []map[string]any) []string {
	out := make([]string, 0, len(maps))
	for _, m := range maps {
		out = append(out, m[keyName].(string))
	}

	return out
}

func TestPaginateMaps_BoundaryWalk(t *testing.T) {
	t.Parallel()

	names := []string{"t0", "t1", "t2", "t3", "t4"}
	all := namedMaps(names...)

	var collected []string

	token := ""
	for {
		page, next := paginateMaps(all, token, 2, 100)
		collected = append(collected, mapNames(page)...)

		if next == "" {
			break
		}

		token = next
	}

	require.Equal(t, names, collected)
}

func TestPaginateMaps_ExactDivisionNoTrailingCursor(t *testing.T) {
	t.Parallel()

	all := namedMaps("t0", "t1", "t2", "t3")

	page1, tok1 := paginateMaps(all, "", 2, 100)
	require.Equal(t, []string{"t0", "t1"}, mapNames(page1))
	require.NotEmpty(t, tok1)

	page2, tok2 := paginateMaps(all, tok1, 2, 100)
	assert.Equal(t, []string{"t2", "t3"}, mapNames(page2))
	assert.Empty(t, tok2)
}

func TestPaginateMaps_SinglePageNoCursor(t *testing.T) {
	t.Parallel()

	all := namedMaps("t0", "t1")
	page, tok := paginateMaps(all, "", 10, 100)
	assert.Equal(t, []string{"t0", "t1"}, mapNames(page))
	assert.Empty(t, tok)
}

func TestPaginateMaps_EmptyCollectionNoCursor(t *testing.T) {
	t.Parallel()

	page, tok := paginateMaps(nil, "", 10, 100)
	assert.Empty(t, page)
	assert.Empty(t, tok)
}

// TestPaginateMaps_CursorRoundTrip: same inclusive-resume contract as
// paginateStrings above.
func TestPaginateMaps_CursorRoundTrip(t *testing.T) {
	t.Parallel()

	all := namedMaps("t0", "t1", "t2")
	page, _ := paginateMaps(all, "t1", 10, 100)
	assert.Equal(t, []string{"t1", "t2"}, mapNames(page))
}

func TestPaginateMaps_TamperedCursor_NoMatch(t *testing.T) {
	t.Parallel()

	all := namedMaps("t0", "t1", "t2")
	page, tok := paginateMaps(all, "does-not-exist", 10, 100)
	assert.Empty(t, page, "an unmatched cursor must not restart at page one")
	assert.Empty(t, tok)
}
