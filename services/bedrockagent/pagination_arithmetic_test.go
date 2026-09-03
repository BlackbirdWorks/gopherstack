package bedrockagent_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrockagent"
)

// idsN returns n sorted, distinct string IDs ("id-000".."id-00N").
func idsN(n int) []string {
	out := make([]string, n)
	for i := range n {
		out[i] = fmt.Sprintf("id-%03d", i)
	}

	return out
}

// TestPaginate_BoundaryWalk walks the full collection in fixed-size pages
// where the page size does not divide the collection size, and asserts the
// concatenation of every page reproduces the original collection exactly:
// nothing dropped, nothing duplicated, order preserved.
func TestPaginate_BoundaryWalk(t *testing.T) {
	t.Parallel()

	all := idsN(7)
	const pageSize = 3

	var got []string

	token := ""
	for range len(all) + 1 {
		var page []string
		page, token = bedrockagent.PaginateForTest(all, token, pageSize)
		got = append(got, page...)

		if token == "" {
			break
		}
	}

	assert.Equal(t, all, got, "concatenation of every page must reproduce the collection exactly")
}

// TestPaginate_FinalPage asserts the final page returns the remainder and an
// empty token, never one yielding an empty page forever.
func TestPaginate_FinalPage(t *testing.T) {
	t.Parallel()

	all := idsN(7)

	page1, token1 := bedrockagent.PaginateForTest(all, "", 3)
	require.Len(t, page1, 3)
	require.NotEmpty(t, token1)

	page2, token2 := bedrockagent.PaginateForTest(all, token1, 3)
	require.Len(t, page2, 3)
	require.NotEmpty(t, token2)

	page3, token3 := bedrockagent.PaginateForTest(all, token2, 3)
	assert.Len(t, page3, 1)
	assert.Empty(t, token3, "final page must not carry a cursor")
}

// TestPaginate_SinglePage asserts a collection smaller than one page returns
// everything with no cursor.
func TestPaginate_SinglePage(t *testing.T) {
	t.Parallel()

	all := idsN(2)

	page, token := bedrockagent.PaginateForTest(all, "", 10)
	assert.Equal(t, all, page)
	assert.Empty(t, token)
}

// TestPaginate_EmptyCollection asserts an empty collection returns no items
// and no cursor.
func TestPaginate_EmptyCollection(t *testing.T) {
	t.Parallel()

	page, token := bedrockagent.PaginateForTest(nil, "", 10)
	assert.Empty(t, page)
	assert.Empty(t, token)
}

// TestPaginate_ExactDivision asserts that when the page size evenly divides
// the collection size, the last full page does not emit a cursor pointing
// past the end.
func TestPaginate_ExactDivision(t *testing.T) {
	t.Parallel()

	all := idsN(6)

	page1, token1 := bedrockagent.PaginateForTest(all, "", 3)
	require.Len(t, page1, 3)
	require.NotEmpty(t, token1)

	page2, token2 := bedrockagent.PaginateForTest(all, token1, 3)
	assert.Len(t, page2, 3)
	assert.Empty(t, token2, "exact-division last page must not emit a cursor")
}

// TestPaginate_CursorRoundTrip asserts a token that encodes an item's ID
// resumes exactly at that item.
func TestPaginate_CursorRoundTrip(t *testing.T) {
	t.Parallel()

	all := idsN(5)

	page1, token1 := bedrockagent.PaginateForTest(all, "", 2)
	require.Len(t, page1, 2)
	require.Equal(t, all[2], token1, "token must name the first item of the next page")

	page2, _ := bedrockagent.PaginateForTest(all, token1, 2)
	require.NotEmpty(t, page2)
	assert.Equal(t, all[2], page2[0], "resuming with the token must land exactly on the item it named")
}

// TestPaginate_StaleCursor is the check that finds Class A/B/C bugs: the
// token names an item that has since been deleted from the collection. The
// pre-fix helper left start at its zero value on a scan miss, so a client
// following a stale cursor got page one forever (Class B: infinite loop,
// cursor matched by equality). The fix must terminate cleanly instead.
func TestPaginate_StaleCursor(t *testing.T) {
	t.Parallel()

	all := idsN(5)

	// A token for an item that no longer exists in the collection (as if the
	// item it named was deleted between calls).
	staleToken := "id-999"

	page, token := bedrockagent.PaginateForTest(all, staleToken, 2)

	assert.Empty(t, token, "a stale cursor must not produce another cursor (no infinite loop)")
	assert.Empty(t, page, "a stale cursor must default to the end of the collection, not the start")
}
