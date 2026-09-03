package databrew

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPaginateKeys_BoundaryWalk(t *testing.T) {
	t.Parallel()

	keys := make([]string, 0, 19)
	for i := range 19 {
		keys = append(keys, string(rune('a'+i)))
	}

	var collected []string

	token := ""
	for {
		page, next := paginateKeys(keys, 4, token)
		collected = append(collected, page...)

		if next == "" {
			break
		}

		token = next
	}

	require.Equal(t, keys, collected)
}

func TestPaginateKeys_ExactDivisionNoTrailingCursor(t *testing.T) {
	t.Parallel()

	keys := []string{"a", "b", "c", "d"}

	page1, tok1 := paginateKeys(keys, 2, "")
	require.Equal(t, []string{"a", "b"}, page1)
	require.NotEmpty(t, tok1)

	page2, tok2 := paginateKeys(keys, 2, tok1)
	require.Equal(t, []string{"c", "d"}, page2)
	assert.Empty(t, tok2)
}

func TestPaginateKeys_SinglePage(t *testing.T) {
	t.Parallel()

	keys := []string{"a", "b"}

	page, tok := paginateKeys(keys, 10, "")
	require.Equal(t, keys, page)
	assert.Empty(t, tok)
}

func TestPaginateKeys_Empty(t *testing.T) {
	t.Parallel()

	page, tok := paginateKeys(nil, 10, "")
	assert.Empty(t, page)
	assert.Empty(t, tok)
}

// TestPaginateKeys_DeletionTolerant confirms paginateKeys resumes correctly
// even when the key the cursor was minted from has since been deleted: it
// searches for the first remaining key strictly greater than the cursor
// rather than an exact match, so it naturally skips past a deleted key
// instead of restarting or getting stuck.
func TestPaginateKeys_DeletionTolerant(t *testing.T) {
	t.Parallel()

	all := []string{"a", "b", "c", "d", "e"}

	page1, tok := paginateKeys(all, 2, "")
	require.Equal(t, []string{"a", "b"}, page1)
	require.Equal(t, "b", tok)

	// "c" is deleted between calls; cursor still names "b" (the last item of
	// the previous page), which still exists.
	remaining := []string{"a", "b", "d", "e"}

	page2, tok2 := paginateKeys(remaining, 2, tok)
	assert.Equal(t, []string{"d", "e"}, page2)
	assert.Empty(t, tok2)
}

func TestPaginateKeys_CursorPastEnd(t *testing.T) {
	t.Parallel()

	page, tok := paginateKeys([]string{"a", "b", "c"}, 10, "z")
	assert.Empty(t, page)
	assert.Empty(t, tok)
}

func TestPaginateKeys_DefaultLimitOnNonPositive(t *testing.T) {
	t.Parallel()

	keys := make([]string, 0, 150)
	for i := range 150 {
		keys = append(keys, string(rune('a'))+string(rune(i)))
	}

	page, _ := paginateKeys(keys, 0, "")
	assert.Len(t, page, 100, "maxResults<=0 must fall back to the default page size of 100")
}
