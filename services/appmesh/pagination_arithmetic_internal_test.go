package appmesh

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// paginateStrings (store.go) backs 7 List operations (meshes.go,
// virtual_gateways.go x2, virtual_services.go, virtual_nodes.go,
// virtual_routers.go x2). Unlike the equality-cursor shape found buggy
// elsewhere in this campaign, it searches for the first sorted name
// strictly greater than nextToken -- a threshold, not an exact match -- so
// a name deleted since the token was issued still resolves to the correct
// resume point (the next surviving name), and an exhausted/tampered
// cursor terminates via its explicit "nothing greater, and nothing at or
// before nextToken remains" empty-return guard, never a restart at 0.

func TestPaginateStrings_BoundaryWalk(t *testing.T) {
	t.Parallel()

	names := []string{"m0", "m1", "m2", "m3", "m4", "m5", "m6"}

	var collected []string

	token := ""
	for {
		page, next := paginateStrings(names, token, 3)
		collected = append(collected, page...)

		if next == "" {
			break
		}

		token = next
	}

	require.Equal(t, names, collected)
}

func TestPaginateStrings_ExactDivisionNoTrailingCursor(t *testing.T) {
	t.Parallel()

	names := []string{"m0", "m1", "m2", "m3"}

	page1, tok1 := paginateStrings(names, "", 2)
	require.Equal(t, []string{"m0", "m1"}, page1)
	require.NotEmpty(t, tok1)

	page2, tok2 := paginateStrings(names, tok1, 2)
	assert.Equal(t, []string{"m2", "m3"}, page2)
	assert.Empty(t, tok2)
}

func TestPaginateStrings_SinglePageNoCursor(t *testing.T) {
	t.Parallel()

	names := []string{"m0", "m1"}
	page, tok := paginateStrings(names, "", 10)
	assert.Equal(t, names, page)
	assert.Empty(t, tok)
}

func TestPaginateStrings_EmptyCollectionNoCursor(t *testing.T) {
	t.Parallel()

	page, tok := paginateStrings(nil, "", 10)
	assert.Empty(t, page)
	assert.Empty(t, tok)
}

func TestPaginateStrings_CursorRoundTrip(t *testing.T) {
	t.Parallel()

	names := []string{"m0", "m1", "m2"}

	_, tok := paginateStrings(names, "", 1)
	require.Equal(t, "m0", tok, "the token is the opaque name of the last item on this page")

	page, _ := paginateStrings(names, tok, 10)
	assert.Equal(t, []string{"m1", "m2"}, page)
}

// TestPaginateStrings_StaleCursor_DeletedItem reproduces the case a
// deletion between calls triggers: the name the cursor points past is gone
// from the current set. Because the search is threshold-based ("first name
// > token"), not equality-based, it must resume at the next surviving
// name -- neither skipping nor repeating any item.
func TestPaginateStrings_StaleCursor_DeletedItem(t *testing.T) {
	t.Parallel()

	// m1 was the cursor's boundary but has since been deleted.
	remaining := []string{"m0", "m2", "m3"}

	page, _ := paginateStrings(remaining, "m1", 10)
	assert.Equal(t, []string{"m2", "m3"}, page)
}

// TestPaginateStrings_TamperedCursor_PastEnd is the exhaustion case: every
// remaining name is <= the token (the collection shrank so nothing sorts
// after it any more, or the token was hand-built past the real end). Must
// return no items and no cursor -- not the full list from index 0.
func TestPaginateStrings_TamperedCursor_PastEnd(t *testing.T) {
	t.Parallel()

	names := []string{"m0", "m1", "m2"}

	page, tok := paginateStrings(names, "zzz-past-everything", 10)
	assert.Empty(t, page, "an exhausted/tampered cursor must not restart at page one")
	assert.Empty(t, tok)
}
