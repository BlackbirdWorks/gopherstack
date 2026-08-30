package guardduty

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// paginate[T] (pagination.go) backs 14 List/Describe operations across this
// package (entity_sets.go x2, filters.go, findings.go, investigations.go,
// members.go x2, organization.go, publishing_destinations.go,
// ip_and_threatintel_sets.go x2, malware_protection.go x2 via
// paginateMalwareScans). It's an offset-token paginator matching pkgs/page's
// algorithm exactly (this package hand-rolls it rather than importing
// pkgs/page): decodeToken defaults 0 on empty/invalid input, and paginate
// clamps offset >= len(items) before slicing.

func TestPaginate_BoundaryWalk(t *testing.T) {
	t.Parallel()

	items := []string{"a0", "a1", "a2", "a3", "a4", "a5", "a6"}

	var collected []string

	tok := ""
	for {
		offset, err := decodeToken(tok)
		require.NoError(t, err)

		page, next := paginate(items, offset, 3)
		collected = append(collected, page...)

		if next == "" {
			break
		}

		tok = next
	}

	require.Equal(t, items, collected)
}

func TestPaginate_ExactDivisionNoTrailingCursor(t *testing.T) {
	t.Parallel()

	items := []string{"a0", "a1", "a2", "a3"}

	page1, tok1 := paginate(items, 0, 2)
	require.Equal(t, []string{"a0", "a1"}, page1)
	require.NotEmpty(t, tok1)

	off2, err := decodeToken(tok1)
	require.NoError(t, err)

	page2, tok2 := paginate(items, off2, 2)
	assert.Equal(t, []string{"a2", "a3"}, page2)
	assert.Empty(t, tok2)
}

func TestPaginate_SinglePageNoCursor(t *testing.T) {
	t.Parallel()

	items := []string{"a0", "a1"}
	page, tok := paginate(items, 0, 10)
	assert.Equal(t, items, page)
	assert.Empty(t, tok)
}

func TestPaginate_EmptyCollectionNoCursor(t *testing.T) {
	t.Parallel()

	page, tok := paginate([]string{}, 0, 10)
	assert.Empty(t, page)
	assert.Empty(t, tok)
}

func TestPaginate_CursorRoundTrip(t *testing.T) {
	t.Parallel()

	items := []string{"a0", "a1", "a2", "a3", "a4"}

	_, tok := paginate(items, 0, 2)
	require.Equal(t, encodeToken(2), tok)

	off, err := decodeToken(tok)
	require.NoError(t, err)
	assert.Equal(t, 2, off)
}

// TestPaginate_StaleOffset_PastEnd reproduces a token decoding to an offset
// beyond the current item count -- a collection that shrank between calls,
// or a hand-built/replayed token. Must clamp to an empty page, not panic.
func TestPaginate_StaleOffset_PastEnd(t *testing.T) {
	t.Parallel()

	items := []string{"a0", "a1", "a2"}

	require.NotPanics(t, func() {
		page, tok := paginate(items, 100, 10)
		assert.Empty(t, page)
		assert.Empty(t, tok)
	})
}

func TestDecodeToken_EmptyInvalidRoundTrip(t *testing.T) {
	t.Parallel()

	off, err := decodeToken("")
	require.NoError(t, err)
	assert.Equal(t, 0, off)

	_, err = decodeToken("not-valid-base64!!!")
	require.Error(t, err)

	off, err = decodeToken(encodeToken(42))
	require.NoError(t, err)
	assert.Equal(t, 42, off)
}

func TestResolvePageSize_DefaultAndCap(t *testing.T) {
	t.Parallel()

	assert.Equal(t, standardPageSize, resolvePageSize(0))
	assert.Equal(t, standardPageSize, resolvePageSize(-1))
	assert.Equal(t, standardPageSize, resolvePageSize(9999))
	assert.Equal(t, 10, resolvePageSize(10))
}

// paginateMalwareScans (malware_protection.go) wraps decodeToken + paginate
// for DescribeMalwareScans/ListMalwareScans; verified directly since it has
// its own error path (ErrValidation on a malformed token) on top of the
// shared paginate.

func TestPaginateMalwareScans_StaleOffset_PastEnd(t *testing.T) {
	t.Parallel()

	scans := []*MalwareScan{{ScanID: "s0"}, {ScanID: "s1"}}

	require.NotPanics(t, func() {
		page, next, err := paginateMalwareScans(scans, MalwareScanQuery{NextToken: encodeToken(100)})
		require.NoError(t, err)
		assert.Empty(t, page)
		assert.Empty(t, next)
	})
}

func TestPaginateMalwareScans_MalformedToken_Errors(t *testing.T) {
	t.Parallel()

	scans := []*MalwareScan{{ScanID: "s0"}}

	_, _, err := paginateMalwareScans(scans, MalwareScanQuery{NextToken: "not-valid-base64!!!"})
	require.Error(t, err)
}
