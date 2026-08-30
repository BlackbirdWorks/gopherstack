package lakeformation

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// paginate[T] (store.go) is a thin wrapper directly over pkgs/page.New --
// verified by reading, and pkgs/page carries its own exhaustive test suite
// (pkgs/page/page_test.go), so it is not re-derived here. This file covers
// the two helpers this package hand-rolls instead of using it:
// paginateTaggedTables and paginateTaggedDatabases (lf_tags.go), which
// parse an offset token via a manual decimal-digit loop rather than
// strconv/pkgs/page, but land on the same offset-clamp algorithm.

func taggedTables(names ...string) []TaggedTable {
	out := make([]TaggedTable, 0, len(names))
	for _, n := range names {
		out = append(out, TaggedTable{Table: &TableResource{Name: n}})
	}

	return out
}

func taggedTableNames(list []TaggedTable) []string {
	out := make([]string, 0, len(list))
	for _, t := range list {
		out = append(out, t.Table.Name)
	}

	return out
}

func TestPaginateTaggedTables_BoundaryWalk(t *testing.T) {
	t.Parallel()

	names := []string{"t0", "t1", "t2", "t3", "t4", "t5", "t6"}
	all := taggedTables(names...)

	var collected []string

	token := ""
	for {
		page, next := paginateTaggedTables(all, 3, token)
		collected = append(collected, taggedTableNames(page)...)

		if next == "" {
			break
		}

		token = next
	}

	require.Equal(t, names, collected)
}

func TestPaginateTaggedTables_ExactDivisionNoTrailingCursor(t *testing.T) {
	t.Parallel()

	all := taggedTables("t0", "t1", "t2", "t3")

	page1, tok1 := paginateTaggedTables(all, 2, "")
	require.Equal(t, []string{"t0", "t1"}, taggedTableNames(page1))
	require.NotEmpty(t, tok1)

	page2, tok2 := paginateTaggedTables(all, 2, tok1)
	assert.Equal(t, []string{"t2", "t3"}, taggedTableNames(page2))
	assert.Empty(t, tok2)
}

func TestPaginateTaggedTables_SinglePageNoCursor(t *testing.T) {
	t.Parallel()

	all := taggedTables("t0", "t1")
	page, tok := paginateTaggedTables(all, 10, "")
	assert.Equal(t, []string{"t0", "t1"}, taggedTableNames(page))
	assert.Empty(t, tok)
}

func TestPaginateTaggedTables_EmptyCollectionNoCursor(t *testing.T) {
	t.Parallel()

	page, tok := paginateTaggedTables(nil, 10, "")
	assert.Empty(t, page)
	assert.Empty(t, tok)
}

func TestPaginateTaggedTables_CursorRoundTrip(t *testing.T) {
	t.Parallel()

	all := taggedTables("t0", "t1", "t2")
	page, _ := paginateTaggedTables(all, 10, strconv.Itoa(1))
	assert.Equal(t, []string{"t1", "t2"}, taggedTableNames(page))
}

// TestPaginateTaggedTables_StaleCursor_PastEnd reproduces a token decoding
// to an offset beyond the current count (list shrank, or a hand-built
// token): must clamp to an empty page, not panic or restart at page one.
func TestPaginateTaggedTables_StaleCursor_PastEnd(t *testing.T) {
	t.Parallel()

	all := taggedTables("t0", "t1", "t2")

	require.NotPanics(t, func() {
		page, tok := paginateTaggedTables(all, 10, strconv.Itoa(100))
		assert.Empty(t, page)
		assert.Empty(t, tok)
	})
}

func taggedDatabases(names ...string) []TaggedDatabase {
	out := make([]TaggedDatabase, 0, len(names))
	for _, n := range names {
		out = append(out, TaggedDatabase{Database: &DatabaseResource{Name: n}})
	}

	return out
}

func taggedDatabaseNames(list []TaggedDatabase) []string {
	out := make([]string, 0, len(list))
	for _, d := range list {
		out = append(out, d.Database.Name)
	}

	return out
}

func TestPaginateTaggedDatabases_BoundaryWalk(t *testing.T) {
	t.Parallel()

	names := []string{"d0", "d1", "d2", "d3", "d4"}
	all := taggedDatabases(names...)

	var collected []string

	token := ""
	for {
		page, next := paginateTaggedDatabases(all, 2, token)
		collected = append(collected, taggedDatabaseNames(page)...)

		if next == "" {
			break
		}

		token = next
	}

	require.Equal(t, names, collected)
}

func TestPaginateTaggedDatabases_ExactDivisionNoTrailingCursor(t *testing.T) {
	t.Parallel()

	all := taggedDatabases("d0", "d1", "d2", "d3")

	page1, tok1 := paginateTaggedDatabases(all, 2, "")
	require.Equal(t, []string{"d0", "d1"}, taggedDatabaseNames(page1))
	require.NotEmpty(t, tok1)

	page2, tok2 := paginateTaggedDatabases(all, 2, tok1)
	assert.Equal(t, []string{"d2", "d3"}, taggedDatabaseNames(page2))
	assert.Empty(t, tok2)
}

func TestPaginateTaggedDatabases_EmptyCollectionNoCursor(t *testing.T) {
	t.Parallel()

	page, tok := paginateTaggedDatabases(nil, 10, "")
	assert.Empty(t, page)
	assert.Empty(t, tok)
}

func TestPaginateTaggedDatabases_StaleCursor_PastEnd(t *testing.T) {
	t.Parallel()

	all := taggedDatabases("d0", "d1", "d2")

	require.NotPanics(t, func() {
		page, tok := paginateTaggedDatabases(all, 10, strconv.Itoa(100))
		assert.Empty(t, page)
		assert.Empty(t, tok)
	})
}
