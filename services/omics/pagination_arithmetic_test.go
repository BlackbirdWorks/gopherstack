package omics_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/omics"
)

// TestPaginateStrings_BoundaryWalk verifies that walking a collection in
// pages of K, where K does not divide N, and concatenating every page
// reproduces the original sorted collection exactly: no item dropped, none
// duplicated, order preserved. This is the check that would have caught
// memorydb's off-by-one.
func TestPaginateStrings_BoundaryWalk(t *testing.T) {
	t.Parallel()

	ids := make([]string, 0, 23)
	for i := range 23 {
		ids = append(ids, string(rune('a'+i)))
	}

	var collected []string

	token := ""
	for {
		page, next := omics.PaginateStringsForTest(ids, token, 5)
		collected = append(collected, page...)

		if next == "" {
			break
		}

		token = next
	}

	require.Equal(t, ids, collected)
}

func TestPaginateStrings_ExactDivisionNoTrailingCursor(t *testing.T) {
	t.Parallel()

	ids := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}

	page1, tok1 := omics.PaginateStringsForTest(ids, "", 5)
	require.Equal(t, []string{"a", "b", "c", "d", "e"}, page1)
	require.NotEmpty(t, tok1)

	page2, tok2 := omics.PaginateStringsForTest(ids, tok1, 5)
	require.Equal(t, []string{"f", "g", "h", "i", "j"}, page2)
	assert.Empty(t, tok2, "last full page must not emit a cursor pointing past the end")
}

func TestPaginateStrings_SinglePage(t *testing.T) {
	t.Parallel()

	ids := []string{"a", "b", "c"}

	page, tok := omics.PaginateStringsForTest(ids, "", 10)
	require.Equal(t, ids, page)
	assert.Empty(t, tok)
}

func TestPaginateStrings_Empty(t *testing.T) {
	t.Parallel()

	page, tok := omics.PaginateStringsForTest(nil, "", 10)
	assert.Empty(t, page)
	assert.Empty(t, tok)
}

// TestPaginateStrings_StaleCursorAfterDeletion demonstrates that when the id
// named by nextToken has since been deleted (e.g. the resource it names was
// removed between calls), paginateStrings must resume at the first
// remaining id greater than or equal to the cursor -- not silently restart
// pagination from the beginning. Restarting at 0 hands the caller duplicate
// items it already consumed on the prior page.
func TestPaginateStrings_StaleCursorAfterDeletion(t *testing.T) {
	t.Parallel()

	all := []string{"a", "b", "c", "d", "e"}

	page1, tok := omics.PaginateStringsForTest(all, "", 2)
	require.Equal(t, []string{"a", "b"}, page1)
	require.Equal(t, "c", tok, "token names the first id of the next page")

	// "c" is deleted between calls.
	remaining := []string{"a", "b", "d", "e"}

	page2, tok2 := omics.PaginateStringsForTest(remaining, tok, 2)
	assert.Equal(t, []string{"d", "e"}, page2, "must resume after the deleted cursor, not restart from the beginning")
	assert.Empty(t, tok2)
}

// TestPaginateStrings_CursorPastEnd verifies a cursor beyond the last item
// (deleted tail, or an exhausted final page) returns an empty page and no
// further cursor rather than looping forever.
func TestPaginateStrings_CursorPastEnd(t *testing.T) {
	t.Parallel()

	all := []string{"a", "b", "c"}

	page, tok := omics.PaginateStringsForTest(all, "z", 10)
	assert.Empty(t, page)
	assert.Empty(t, tok)
}
