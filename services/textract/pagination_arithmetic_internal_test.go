package textract

import (
	"encoding/base64"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func blockPage(n int) []Block {
	out := make([]Block, n)
	for i := range out {
		out[i] = Block{ID: strconv.Itoa(i)}
	}

	return out
}

func blockIDs(bs []Block) []string {
	out := make([]string, len(bs))
	for i, b := range bs {
		out[i] = b.ID
	}

	return out
}

func TestPaginateBlocks_BoundaryWalk(t *testing.T) {
	t.Parallel()

	blocks := blockPage(23)
	want := blockIDs(blocks)

	var collected []string

	token := ""
	for {
		page, next := paginateBlocks(blocks, 5, token)
		collected = append(collected, blockIDs(page)...)

		if next == "" {
			break
		}

		token = next
	}

	require.Equal(t, want, collected)
}

func TestPaginateBlocks_ExactDivisionNoTrailingCursor(t *testing.T) {
	t.Parallel()

	blocks := blockPage(4)

	page1, tok1 := paginateBlocks(blocks, 2, "")
	require.Equal(t, []string{"0", "1"}, blockIDs(page1))
	require.NotEmpty(t, tok1)

	page2, tok2 := paginateBlocks(blocks, 2, tok1)
	require.Equal(t, []string{"2", "3"}, blockIDs(page2))
	assert.Empty(t, tok2)
}

func TestPaginateBlocks_SinglePage(t *testing.T) {
	t.Parallel()

	blocks := blockPage(2)

	page, tok := paginateBlocks(blocks, 10, "")
	require.Equal(t, blockIDs(blocks), blockIDs(page))
	assert.Empty(t, tok)
}

func TestPaginateBlocks_Empty(t *testing.T) {
	t.Parallel()

	page, tok := paginateBlocks(nil, 10, "")
	assert.Empty(t, page)
	assert.Empty(t, tok)
}

func TestPaginateBlocks_TokenRoundTrip(t *testing.T) {
	t.Parallel()

	blocks := blockPage(5)

	_, tok := paginateBlocks(blocks, 2, "")
	decoded, err := base64.StdEncoding.DecodeString(tok)
	require.NoError(t, err)
	assert.Equal(t, "2", string(decoded))
}

// TestPaginateBlocks_CursorPastEndDoesNotRestart demonstrates that a token
// decoding to an offset at or beyond the current block count must yield an
// empty page and no cursor -- not silently restart at page one. The inner
// decode guard ("n >= 0 && n < len(blocks)") rejects any out-of-range n and
// leaves offset at its zero value, which the outer "offset >= len(blocks)"
// check can then never observe, since offset is 0 again by the time it runs.
func TestPaginateBlocks_CursorPastEndDoesNotRestart(t *testing.T) {
	t.Parallel()

	blocks := blockPage(3)
	staleToken := base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(100)))

	page, tok := paginateBlocks(blocks, 10, staleToken)
	assert.Empty(t, page, "a token past the end must not restart pagination from the beginning")
	assert.Empty(t, tok)
}

// TestPaginateBlocks_CursorExactlyAtEndDoesNotRestart is the boundary
// variant of the above: a token equal to len(blocks) (the value this helper
// itself would have emitted had the list been one shorter) must also yield
// empty, not page one.
func TestPaginateBlocks_CursorExactlyAtEndDoesNotRestart(t *testing.T) {
	t.Parallel()

	blocks := blockPage(3)
	tokenAtEnd := base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(len(blocks))))

	page, tok := paginateBlocks(blocks, 10, tokenAtEnd)
	assert.Empty(t, page)
	assert.Empty(t, tok)
}
