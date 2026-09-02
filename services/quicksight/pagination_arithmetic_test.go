package quicksight_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// These tests target the pagination arithmetic inside InMemoryBackend's
// List* helpers, independent of the HTTP/wire layer. Three code shapes are
// exercised, each duplicated across many quicksight List operations:
//
//   - "equality-cursor" (e.g. paginateGroups behind ListGroups): the cursor
//     names an item by ID and the helper does a linear equality scan,
//     defaulting to index 0 when the named item is missing.
//   - "index-cursor" (e.g. paginateFolders behind ListFolders, or
//     ListTemplates inline): the cursor is an opaque encoded integer offset
//     with no upper-bound clamp against the current collection length.
//   - "unsorted" (ListGroups itself): the collection is paginated without
//     ever being sorted, so store.Table.All()'s unspecified map order can
//     reorder items between two calls with no mutation in between.

// TestListGroupsPaginationBoundaryWalk exercises check 1 (boundary walk) and
// check 5 (exact division) against ListGroups, whose paginateGroups helper
// has both the equality-cursor bug and (since ListGroups never sorts before
// calling it) the unsorted-collection bug.
func TestListGroupsPaginationBoundaryWalk(t *testing.T) {
	t.Parallel()

	b := quicksight.NewInMemoryBackend("000000000000", "us-east-1")

	const n = 7 // does not divide page size 3: exercises a non-aligned final page
	want := make(map[string]bool, n)
	for i := range n {
		name := fmt.Sprintf("group-%02d", i)
		_, err := b.CreateGroup("000000000000", "default", name, "")
		require.NoError(t, err)
		want[name] = true
	}

	got := make(map[string]bool, n)
	nextToken := ""

	for range n + 2 { // bounded: a real infinite loop would exceed this and fail via missing items
		page, next, err := b.ListGroups("000000000000", "default", 3, nextToken)
		require.NoError(t, err)

		for _, g := range page {
			assert.Falsef(t, got[g.GroupName],
				"group %s returned twice across pages: pagination duplicated an item", g.GroupName)
			got[g.GroupName] = true
		}

		if next == "" {
			break
		}
		nextToken = next
	}

	assert.Equal(t, want, got, "concatenation of every page must reproduce the collection exactly")
}

// TestListGroupsPaginationStaleCursor exercises check 7: a cursor naming an
// item deleted since it was issued must not silently resume from the start
// of the collection (Class B: infinite loop, cursor matched by equality).
func TestListGroupsPaginationStaleCursor(t *testing.T) {
	t.Parallel()

	b := quicksight.NewInMemoryBackend("000000000000", "us-east-1")

	for i := range 5 {
		_, err := b.CreateGroup("000000000000", "default", fmt.Sprintf("group-%02d", i), "")
		require.NoError(t, err)
	}

	// A cursor for an item that both sorts after and was since deleted.
	staleToken := "group-99"

	page, _, err := b.ListGroups("000000000000", "default", 2, staleToken)
	require.NoError(t, err)

	for _, g := range page {
		assert.NotEqual(t, "group-00", g.GroupName,
			"stale cursor must not reset pagination to the first item of the collection")
	}
}

// TestListFoldersPaginationStaleCursor exercises check 7 against the
// index-cursor shape (Class A: panic). A token encoding an offset that is
// no longer valid once items are deleted must not panic when sliced.
func TestListFoldersPaginationStaleCursor(t *testing.T) {
	t.Parallel()

	b := quicksight.NewInMemoryBackend("000000000000", "us-east-1")

	for i := range 5 {
		_, err := b.CreateFolder(
			"000000000000",
			fmt.Sprintf("f%02d", i),
			fmt.Sprintf("Folder%02d", i),
			"",
			"",
			"",
			nil,
			nil,
		)
		require.NoError(t, err)
	}

	page1, next, err := b.ListFolders("000000000000", 2, "")
	require.NoError(t, err)
	require.Len(t, page1, 2)
	require.NotEmpty(t, next, "expected a continuation token after page 1 of 5")

	// Shrink the collection strictly below the encoded offset (2) before the
	// client returns for page 2 -- the exact stale-cursor scenario Class A
	// misses. Deleting only down to len==offset is not enough: start==end is
	// a legal empty slice, the bug needs start > len(all).
	for i := range 5 {
		require.NoError(t, b.DeleteFolder("000000000000", fmt.Sprintf("f%02d", i)))
	}
	_, err = b.CreateFolder("000000000000", "fzz", "FolderZZ", "", "", "", nil, nil)
	require.NoError(t, err)

	assert.NotPanics(t, func() {
		page2, _, listErr := b.ListFolders("000000000000", 2, next)
		require.NoError(t, listErr)
		assert.Empty(t, page2, "offset past the shrunk collection should yield an empty page, not a panic")
	})
}

// TestListTemplatesPaginationStaleCursor is the same Class A check against
// ListTemplates' own inline index-cursor pagination (it does not go through
// paginateFolders, but duplicates the identical unclamped-offset shape).
func TestListTemplatesPaginationStaleCursor(t *testing.T) {
	t.Parallel()

	b := quicksight.NewInMemoryBackend("000000000000", "us-east-1")

	for i := range 5 {
		_, err := b.CreateTemplate(
			"000000000000",
			fmt.Sprintf("t%02d", i),
			fmt.Sprintf("Template%02d", i),
			"",
			"",
			nil,
			nil,
			nil,
		)
		require.NoError(t, err)
	}

	page1, next, err := b.ListTemplates("000000000000", 2, "")
	require.NoError(t, err)
	require.Len(t, page1, 2)
	require.NotEmpty(t, next)

	for i := range 5 {
		require.NoError(t, b.DeleteTemplate("000000000000", fmt.Sprintf("t%02d", i), 0))
	}
	_, err = b.CreateTemplate("000000000000", "tzz", "TemplateZZ", "", "", nil, nil, nil)
	require.NoError(t, err)

	assert.NotPanics(t, func() {
		page2, _, listErr := b.ListTemplates("000000000000", 2, next)
		require.NoError(t, listErr)
		assert.Empty(t, page2)
	})
}

// TestListAnalysesPaginationBoundaryWalk covers the third shape: an inline
// equality-cursor List function (not routed through a shared paginate*
// helper) that additionally never sorts its collection before paginating.
func TestListAnalysesPaginationBoundaryWalk(t *testing.T) {
	t.Parallel()

	b := quicksight.NewInMemoryBackend("000000000000", "us-east-1")

	const n = 7
	want := make(map[string]bool, n)
	for i := range n {
		id := fmt.Sprintf("an-%02d", i)
		_, err := b.CreateAnalysis("000000000000", id, id, "", map[string]any{"x": 1}, nil, nil)
		require.NoError(t, err)
		want[id] = true
	}

	got := make(map[string]bool, n)
	nextToken := ""

	for range n + 2 {
		page, next, err := b.ListAnalyses("000000000000", 3, nextToken)
		require.NoError(t, err)

		for _, a := range page {
			assert.Falsef(t, got[a.AnalysisID], "analysis %s returned twice across pages", a.AnalysisID)
			got[a.AnalysisID] = true
		}

		if next == "" {
			break
		}
		nextToken = next
	}

	assert.Equal(t, want, got, "concatenation of every page must reproduce the collection exactly")
}

// TestListAnalysesPaginationFinalPageAndEmpty covers checks 2, 3, and 4
// (final page terminates, a collection smaller than one page returns
// everything with no cursor, and an empty collection returns no cursor).
func TestListAnalysesPaginationFinalPageAndEmpty(t *testing.T) {
	t.Parallel()

	b := quicksight.NewInMemoryBackend("000000000000", "us-east-1")

	page, next, err := b.ListAnalyses("000000000000", 10, "")
	require.NoError(t, err)
	assert.Empty(t, page)
	assert.Empty(t, next, "empty collection must not emit a cursor")

	for i := range 3 {
		id := fmt.Sprintf("an-%02d", i)
		_, cerr := b.CreateAnalysis("000000000000", id, id, "", map[string]any{"x": 1}, nil, nil)
		require.NoError(t, cerr)
	}

	page, next, err = b.ListAnalyses("000000000000", 10, "")
	require.NoError(t, err)
	assert.Len(t, page, 3, "collection smaller than one page must return everything")
	assert.Empty(t, next, "must not emit a cursor when nothing remains")
}
