package securityhub //nolint:testpackage // needs access to unexported pagination helpers

import (
	"fmt"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeStrPage(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("item-%03d", i)
	}

	return out
}

func walkPaginateSlice(t *testing.T, items []string, maxResults, maxCap, maxPages int) []string {
	t.Helper()

	var (
		got   []string
		token string
	)

	for range maxPages + 1 {
		page, next := paginateSlice(items, token, maxResults, maxCap)
		got = append(got, page...)

		if next == "" {
			return got
		}

		token = next
	}

	t.Fatalf("paginateSlice did not terminate within %d pages (possible infinite loop)", maxPages)

	return nil
}

func TestPaginateSlice_SevenChecks(t *testing.T) {
	t.Parallel()

	t.Run("boundary walk non-dividing page size", func(t *testing.T) {
		t.Parallel()

		items := makeStrPage(23)
		got := walkPaginateSlice(t, items, 5, 100, 20)
		assert.Equal(t, items, got)
	})

	t.Run("exact division", func(t *testing.T) {
		t.Parallel()

		items := makeStrPage(20)
		got := walkPaginateSlice(t, items, 5, 100, 20)
		assert.Equal(t, items, got)
	})

	t.Run("single page", func(t *testing.T) {
		t.Parallel()

		items := makeStrPage(3)
		out, next := paginateSlice(items, "", 10, 100)
		assert.Equal(t, items, out)
		assert.Empty(t, next)
	})

	t.Run("final page", func(t *testing.T) {
		t.Parallel()

		items := makeStrPage(12)
		out, next := paginateSlice(items, "10", 5, 100)
		assert.Equal(t, []string{"item-010", "item-011"}, out)
		assert.Empty(t, next)
	})

	t.Run("empty collection", func(t *testing.T) {
		t.Parallel()

		out, next := paginateSlice([]string{}, "", 5, 100)
		assert.Empty(t, out)
		assert.Empty(t, next)
	})

	t.Run("cursor round trip", func(t *testing.T) {
		t.Parallel()

		items := makeStrPage(10)
		page1, next1 := paginateSlice(items, "", 4, 100)
		require.NotEmpty(t, next1)
		page2, next2 := paginateSlice(items, next1, 4, 100)
		require.NotEmpty(t, next2)
		page3, next3 := paginateSlice(items, next2, 4, 100)
		assert.Empty(t, next3)

		all := append(append(page1, page2...), page3...)
		assert.Equal(t, items, all)
	})

	t.Run("stale cursor past end", func(t *testing.T) {
		t.Parallel()

		items := makeStrPage(5)
		out, next := paginateSlice(items, "999999", 5, 100)
		assert.Empty(t, out)
		assert.Empty(t, next)
	})

	t.Run("maxCap clamps maxResults", func(t *testing.T) {
		t.Parallel()

		items := makeStrPage(10)
		out, next := paginateSlice(items, "", 999, 3)
		assert.Len(t, out, 3)
		assert.NotEmpty(t, next)
	})

	// negative-offset token: decodeToken has no `< 0` guard, and paginateSlice's
	// `start >= len(results)` check does not catch a negative start, so
	// results[start:end] previously panicked with a negative slice bound.
	t.Run("negative offset token", func(t *testing.T) {
		t.Parallel()

		items := makeStrPage(5)

		require.NotPanics(t, func() {
			out, next := paginateSlice(items, "-5", 5, 100)
			assert.Equal(t, items, out, "a negative-offset token must be treated like start=0")
			assert.Empty(t, next)
		})
	})
}

// TestFilterOrAll_ListAllIsSorted proves filterOrAll's "list everything"
// branch (arns empty) returns a deterministic order across repeated calls,
// not raw store.Table.All() map order. Without this, a caller that
// paginates the result (DescribeActionTargets, GetEnabledStandards) can
// drop or duplicate an item across two separate calls when Go's map
// iteration order shifts between them (Class E: unsorted collection).
func TestFilterOrAll_ListAllIsSorted(t *testing.T) {
	t.Parallel()

	tbl := store.New(func(v *string) string { return *v })
	for i := range 25 {
		s := fmt.Sprintf("arn:%03d", i)
		tbl.Put(&s)
	}

	first := filterOrAll(nil, tbl)
	for range 10 {
		again := filterOrAll(nil, tbl)
		require.Len(t, again, len(first))

		for i := range first {
			assert.Equal(t, *first[i], *again[i], "filterOrAll(nil, ...) order must be deterministic across calls")
		}
	}
}

// TestSortFindings_EmptyCriteriaIsDeterministic proves that with no
// SortCriteria (the common GetFindings/GetFindingsV2 call shape), findings
// still come out in a stable, repeatable order. b.findings is a
// map[string]map[string]any, so a caller collecting []map[string]any via a
// bare `for _, f := range b.findings` gets a different order on every call
// (Go map iteration is randomized per range) unless sortFindings imposes a
// deterministic order even when sortCriteria is empty.
func TestSortFindings_EmptyCriteriaIsDeterministic(t *testing.T) {
	t.Parallel()

	mk := func(productArn, id string) map[string]any {
		return map[string]any{keyProductArn: productArn, "Id": id}
	}

	// Two different input orderings of the same finding set, simulating two
	// separate map-order reads.
	orderA := []map[string]any{
		mk("p1", "a"), mk("p1", "b"), mk("p1", "c"), mk("p1", "d"), mk("p1", "e"),
	}
	orderB := []map[string]any{
		mk("p1", "e"), mk("p1", "d"), mk("p1", "c"), mk("p1", "b"), mk("p1", "a"),
	}

	sortFindings(orderA, nil)
	sortFindings(orderB, nil)

	idsOf := func(fs []map[string]any) []string {
		out := make([]string, len(fs))
		for i, f := range fs {
			out[i], _ = f["Id"].(string)
		}

		return out
	}

	assert.Equal(t, idsOf(orderA), idsOf(orderB),
		"sortFindings with no criteria must still impose a deterministic order")
}
