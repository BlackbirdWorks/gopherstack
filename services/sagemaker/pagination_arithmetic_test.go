package sagemaker //nolint:testpackage // needs access to unexported pagination helpers

import (
	"fmt"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type paginationTestItem struct {
	id string
}

func clonePaginationTestItem(v *paginationTestItem) *paginationTestItem {
	c := *v

	return &c
}

func makePaginationTestItems(n int) []*paginationTestItem {
	items := make([]*paginationTestItem, n)
	for i := range items {
		items[i] = &paginationTestItem{id: fmt.Sprintf("item-%03d", i)}
	}

	return items
}

// walkAll drains a paginator to completion given a fixed page count, returning
// every id seen across every page in the order returned.
func walkAll(t *testing.T, pageOf func(nextToken string) ([]*paginationTestItem, string), maxPages int) []string {
	t.Helper()

	var (
		got   []string
		token string
	)

	for range maxPages + 1 {
		page, next := pageOf(token)
		for _, it := range page {
			got = append(got, it.id)
		}

		if next == "" {
			return got
		}

		token = next
	}

	t.Fatalf("paginator did not terminate within %d pages (possible infinite loop)", maxPages)

	return nil
}

func idsOf(items []*paginationTestItem) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.id
	}

	return out
}

func TestPaginateSlice_SevenChecks(t *testing.T) {
	t.Parallel()

	page := func(items []*paginationTestItem, maxResults int32) func(string) ([]*paginationTestItem, string) {
		return func(token string) ([]*paginationTestItem, string) {
			return paginateSlice(items, token, maxResults)
		}
	}

	t.Run("boundary walk non-dividing page size", func(t *testing.T) {
		t.Parallel()

		items := makePaginationTestItems(23)
		got := walkAll(t, page(items, 5), 20)
		assert.Equal(t, idsOf(items), got)
	})

	t.Run("exact division", func(t *testing.T) {
		t.Parallel()

		items := makePaginationTestItems(20)
		got := walkAll(t, page(items, 5), 20)
		assert.Equal(t, idsOf(items), got)
	})

	t.Run("single page", func(t *testing.T) {
		t.Parallel()

		items := makePaginationTestItems(3)
		out, next := paginateSlice(items, "", 10)
		assert.Equal(t, idsOf(items), idsOf(out))
		assert.Empty(t, next)
	})

	t.Run("final page", func(t *testing.T) {
		t.Parallel()

		items := makePaginationTestItems(12)
		out, next := paginateSlice(items, "10", 5)
		assert.Equal(t, []string{"item-010", "item-011"}, idsOf(out))
		assert.Empty(t, next)
	})

	t.Run("empty collection", func(t *testing.T) {
		t.Parallel()

		out, next := paginateSlice([]*paginationTestItem{}, "", 5)
		assert.Empty(t, out)
		assert.Empty(t, next)
	})

	t.Run("cursor round trip", func(t *testing.T) {
		t.Parallel()

		items := makePaginationTestItems(10)
		page1, next1 := paginateSlice(items, "", 4)
		require.NotEmpty(t, next1)
		page2, next2 := paginateSlice(items, next1, 4)
		require.NotEmpty(t, next2)
		page3, next3 := paginateSlice(items, next2, 4)
		assert.Empty(t, next3)

		all := append(append(idsOf(page1), idsOf(page2)...), idsOf(page3)...)
		assert.Equal(t, idsOf(items), all)
	})

	t.Run("stale cursor past end", func(t *testing.T) {
		t.Parallel()

		items := makePaginationTestItems(5)
		out, next := paginateSlice(items, "999", 5)
		assert.Empty(t, out)
		assert.Empty(t, next)
	})
}

func TestSagemakerListKeyPagedMap_SevenChecks(t *testing.T) {
	t.Parallel()

	newMap := func(n int) map[string]*paginationTestItem {
		m := make(map[string]*paginationTestItem, n)
		for _, it := range makePaginationTestItems(n) {
			m[it.id] = it
		}

		return m
	}

	page := func(m map[string]*paginationTestItem, maxResults int32) func(string) ([]*paginationTestItem, string) {
		return func(token string) ([]*paginationTestItem, string) {
			return sagemakerListKeyPagedMap(m, token, clonePaginationTestItem, maxResults)
		}
	}

	t.Run("boundary walk non-dividing page size", func(t *testing.T) {
		t.Parallel()

		m := newMap(23)
		got := walkAll(t, page(m, 5), 20)
		assert.ElementsMatch(t, idsOf(makePaginationTestItems(23)), got)
		assert.Len(t, got, 23)
	})

	t.Run("exact division", func(t *testing.T) {
		t.Parallel()

		m := newMap(20)
		got := walkAll(t, page(m, 5), 20)
		assert.Len(t, got, 20)
	})

	t.Run("single page", func(t *testing.T) {
		t.Parallel()

		m := newMap(3)
		out, next := sagemakerListKeyPagedMap(m, "", clonePaginationTestItem, 10)
		assert.Len(t, out, 3)
		assert.Empty(t, next)
	})

	t.Run("final page", func(t *testing.T) {
		t.Parallel()

		m := newMap(12)
		_, next := sagemakerListKeyPagedMap(m, "", clonePaginationTestItem, 10)
		require.NotEmpty(t, next)
		out, next2 := sagemakerListKeyPagedMap(m, next, clonePaginationTestItem, 10)
		assert.Len(t, out, 2)
		assert.Empty(t, next2)
	})

	t.Run("empty collection", func(t *testing.T) {
		t.Parallel()

		out, next := sagemakerListKeyPagedMap(map[string]*paginationTestItem{}, "", clonePaginationTestItem, 5)
		assert.Empty(t, out)
		assert.Empty(t, next)
	})

	t.Run("cursor round trip", func(t *testing.T) {
		t.Parallel()

		m := newMap(10)
		got := walkAll(t, page(m, 4), 20)
		assert.ElementsMatch(t, idsOf(makePaginationTestItems(10)), got)
	})

	t.Run("stale cursor names deleted item", func(t *testing.T) {
		t.Parallel()

		m := newMap(10)
		// A cursor naming an item that no longer exists (deleted between
		// calls) must not silently restart at the beginning: that serves
		// page one forever to a client following the cursor (Class B).
		out, next := sagemakerListKeyPagedMap(m, "item-999-deleted", clonePaginationTestItem, 5)
		assert.Empty(t, out, "a stale cursor must not replay page one")
		assert.Empty(t, next)
	})
}

func TestSagemakerListKeyPagedN_SevenChecks(t *testing.T) {
	t.Parallel()

	newTable := func(n int) *store.Table[paginationTestItem] {
		tbl := store.New(func(v *paginationTestItem) string { return v.id })
		for _, it := range makePaginationTestItems(n) {
			tbl.Put(it)
		}

		return tbl
	}

	keyFn := func(v *paginationTestItem) string { return v.id }

	page := func(tbl *store.Table[paginationTestItem], maxResults int32) func(string) ([]*paginationTestItem, string) {
		return func(token string) ([]*paginationTestItem, string) {
			return sagemakerListKeyPagedN(tbl, token, maxResults, clonePaginationTestItem, keyFn)
		}
	}

	t.Run("boundary walk non-dividing page size", func(t *testing.T) {
		t.Parallel()

		tbl := newTable(23)
		got := walkAll(t, page(tbl, 5), 20)
		assert.Equal(t, idsOf(makePaginationTestItems(23)), got)
	})

	t.Run("exact division", func(t *testing.T) {
		t.Parallel()

		tbl := newTable(20)
		got := walkAll(t, page(tbl, 5), 20)
		assert.Equal(t, idsOf(makePaginationTestItems(20)), got)
	})

	t.Run("single page", func(t *testing.T) {
		t.Parallel()

		tbl := newTable(3)
		out, next := sagemakerListKeyPagedN(tbl, "", 10, clonePaginationTestItem, keyFn)
		assert.Len(t, out, 3)
		assert.Empty(t, next)
	})

	t.Run("final page", func(t *testing.T) {
		t.Parallel()

		tbl := newTable(12)
		_, next := sagemakerListKeyPagedN(tbl, "", 10, clonePaginationTestItem, keyFn)
		require.NotEmpty(t, next)
		out, next2 := sagemakerListKeyPagedN(tbl, next, 10, clonePaginationTestItem, keyFn)
		assert.Len(t, out, 2)
		assert.Empty(t, next2)
	})

	t.Run("empty collection", func(t *testing.T) {
		t.Parallel()

		tbl := store.New(func(v *paginationTestItem) string { return v.id })
		out, next := sagemakerListKeyPagedN(tbl, "", 5, clonePaginationTestItem, keyFn)
		assert.Empty(t, out)
		assert.Empty(t, next)
	})

	t.Run("cursor round trip", func(t *testing.T) {
		t.Parallel()

		tbl := newTable(10)
		got := walkAll(t, page(tbl, 4), 20)
		assert.Equal(t, idsOf(makePaginationTestItems(10)), got)
	})

	t.Run("stale cursor names deleted item", func(t *testing.T) {
		t.Parallel()

		tbl := newTable(10)
		// Same Class B shape as sagemakerListKeyPagedMap: a token naming an
		// item deleted since it was issued must not silently resume at 0.
		out, next := sagemakerListKeyPagedN(tbl, "item-999-deleted", 5, clonePaginationTestItem, keyFn)
		assert.Empty(t, out, "a stale cursor must not replay page one")
		assert.Empty(t, next)
	})
}

func TestSagemakerListPagedSlice_SevenChecks(t *testing.T) {
	t.Parallel()

	less := func(a, b *paginationTestItem) bool { return a.id < b.id }

	page := func(items []*paginationTestItem) func(string) ([]*paginationTestItem, string) {
		return func(token string) ([]*paginationTestItem, string) {
			return sagemakerListPagedSlice(items, token, clonePaginationTestItem, less)
		}
	}

	t.Run("boundary walk non-dividing page size", func(t *testing.T) {
		t.Parallel()

		items := makePaginationTestItems(sagemakerDefaultPageSize*2 + 7)
		got := walkAll(t, page(items), sagemakerDefaultPageSize*4)
		assert.Equal(t, idsOf(items), got)
	})

	t.Run("single page", func(t *testing.T) {
		t.Parallel()

		items := makePaginationTestItems(3)
		out, next := sagemakerListPagedSlice(items, "", clonePaginationTestItem, less)
		assert.Len(t, out, 3)
		assert.Empty(t, next)
	})

	t.Run("empty collection", func(t *testing.T) {
		t.Parallel()

		out, next := sagemakerListPagedSlice([]*paginationTestItem{}, "", clonePaginationTestItem, less)
		assert.Empty(t, out)
		assert.Empty(t, next)
	})

	t.Run("stale cursor past end", func(t *testing.T) {
		t.Parallel()

		items := makePaginationTestItems(5)
		out, next := sagemakerListPagedSlice(items, "999999", clonePaginationTestItem, less)
		assert.Empty(t, out, "a stale offset past the collection length must not panic or wrap")
		assert.Empty(t, next)
	})

	t.Run("cursor round trip exact division", func(t *testing.T) {
		t.Parallel()

		items := makePaginationTestItems(sagemakerDefaultPageSize * 3)
		got := walkAll(t, page(items), sagemakerDefaultPageSize*4)
		assert.Equal(t, idsOf(items), got)
	})
}

// TestFilterSortPaginateByName_TiedSortKeyAcrossSeparateCalls probes whether
// paginating over a comparator with ties survives two independent calls that
// each re-collect and re-sort their input in a different order — exactly
// what happens across two real List* HTTP calls, since each rebuilds `all`
// fresh from a store.Table.All() whose Go map iteration order is
// unspecified and re-randomized per call.
func TestFilterSortPaginateByName_TiedSortKeyAcrossSeparateCalls(t *testing.T) {
	t.Parallel()

	tied := time.Unix(1000, 0)
	mk := func(name string) *paginationTestItem2 { return &paginationTestItem2{name: name, created: tied} }

	orderA := []*paginationTestItem2{mk("a"), mk("b"), mk("c"), mk("d")}
	orderB := []*paginationTestItem2{mk("d"), mk("c"), mk("b"), mk("a")}

	filter := nameTimeFilter{MaxResults: 2}
	nameOf := func(v *paginationTestItem2) string { return v.name }
	createdOf := func(v *paginationTestItem2) time.Time { return v.created }

	page1, next1 := filterSortPaginateByName(orderA, "", filter, false, nameOf, createdOf)
	require.NotEmpty(t, next1)

	page2, _ := filterSortPaginateByName(orderB, next1, filter, false, nameOf, createdOf)

	seen := map[string]bool{}
	for _, it := range page1 {
		seen[it.name] = true
	}

	for _, it := range page2 {
		assert.False(t, seen[it.name],
			"item %q duplicated across pages when a tied sort key is re-sorted from a different input order", it.name)
	}
}

type paginationTestItem2 struct {
	created time.Time
	name    string
}

// TestFilterSortPaginateByNameOrTime_TiedSortKeyAcrossSeparateCalls is the
// same probe as TestFilterSortPaginateByName_TiedSortKeyAcrossSeparateCalls,
// against the ListContexts/ListActions pagination helper.
func TestFilterSortPaginateByNameOrTime_TiedSortKeyAcrossSeparateCalls(t *testing.T) {
	t.Parallel()

	tied := time.Unix(2000, 0)
	mk := func(name string) *paginationTestItem2 { return &paginationTestItem2{name: name, created: tied} }

	orderA := []*paginationTestItem2{mk("a"), mk("b"), mk("c"), mk("d")}
	orderB := []*paginationTestItem2{mk("d"), mk("c"), mk("b"), mk("a")}

	nameOf := func(v *paginationTestItem2) string { return v.name }
	createdOf := func(v *paginationTestItem2) time.Time { return v.created }
	noop := func(_ *paginationTestItem2) string { return "" }
	clone := func(v *paginationTestItem2) *paginationTestItem2 {
		c := *v

		return &c
	}

	params := nameOrTimeSortParams{MaxResults: 2}

	page1, next1 := filterSortPaginateByNameOrTime(orderA, params, noop, noop, nameOf, createdOf, clone)
	require.NotEmpty(t, next1)

	params2 := params
	params2.NextToken = next1
	page2, _ := filterSortPaginateByNameOrTime(orderB, params2, noop, noop, nameOf, createdOf, clone)

	seen := map[string]bool{}
	for _, it := range page1 {
		seen[it.name] = true
	}

	for _, it := range page2 {
		assert.False(t, seen[it.name],
			"item %q duplicated across pages when a tied sort key is re-sorted from a different input order", it.name)
	}
}
