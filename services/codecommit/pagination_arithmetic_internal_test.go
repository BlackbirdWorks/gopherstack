package codecommit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPaginateStrings_BoundaryWalk(t *testing.T) {
	t.Parallel()

	items := make([]string, 0, 25)
	for i := range 25 {
		items = append(items, string(rune('a'+i)))
	}

	var collected []string

	token := ""
	for {
		page, next := paginateStrings(items, token, 6)
		collected = append(collected, page...)

		if next == "" {
			break
		}

		token = next
	}

	require.Equal(t, items, collected)
}

func TestPaginateStrings_ExactDivisionNoTrailingCursor(t *testing.T) {
	t.Parallel()

	items := []string{"a", "b", "c", "d"}

	page1, tok1 := paginateStrings(items, "", 2)
	require.Equal(t, []string{"a", "b"}, page1)
	require.NotEmpty(t, tok1)

	page2, tok2 := paginateStrings(items, tok1, 2)
	require.Equal(t, []string{"c", "d"}, page2)
	assert.Empty(t, tok2)
}

func TestPaginateStrings_SinglePage(t *testing.T) {
	t.Parallel()

	items := []string{"a", "b"}

	page, tok := paginateStrings(items, "", 10)
	require.Equal(t, items, page)
	assert.Empty(t, tok)
}

func TestPaginateStrings_Empty(t *testing.T) {
	t.Parallel()

	page, tok := paginateStrings(nil, "", 10)
	assert.Empty(t, page)
	assert.Empty(t, tok)
}

// TestPaginateStrings_TokenIsAPlainOffset documents this helper's cursor
// contract: the token is a decimal slice offset, not an opaque or
// item-identity-derived value. A stale token surviving a deletion still
// works arithmetically (it just skips or repeats the one item whose
// position shifted), but it is not tamper-evident and a caller could pass an
// arbitrary integer directly. Contrast with pkgs/page, which encodes the
// same offset contract but wraps it in base64 to signal "opaque, do not
// construct by hand" -- this helper accepts a bare decimal string.
func TestPaginateStrings_TokenIsAPlainOffset(t *testing.T) {
	t.Parallel()

	items := []string{"a", "b", "c", "d", "e"}

	page, tok := paginateStrings(items, "2", 2)
	assert.Equal(t, []string{"c", "d"}, page)
	assert.Equal(t, "4", tok)
}

func TestPaginateStrings_CursorPastEnd(t *testing.T) {
	t.Parallel()

	page, tok := paginateStrings([]string{"a", "b", "c"}, "100", 10)
	assert.Empty(t, page)
	assert.Empty(t, tok)
}

func TestPaginateStrings_NegativeOrMalformedTokenResetsToStart(t *testing.T) {
	t.Parallel()

	items := []string{"a", "b", "c"}

	page, _ := paginateStrings(items, "-5", 2)
	assert.Equal(t, []string{"a", "b"}, page, "negative offset is invalid, so it must reset to the start")

	page2, _ := paginateStrings(items, "not-a-number", 2)
	assert.Equal(t, []string{"a", "b"}, page2, "malformed offset must reset to the start")
}
