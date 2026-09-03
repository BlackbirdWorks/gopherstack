package iot //nolint:testpackage // needs access to unexported pagination helpers

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makePaginationProbeItems(n int) []string {
	items := make([]string, n)
	for i := range items {
		items[i] = fmt.Sprintf("item-%03d", i)
	}

	return items
}

func walkPaginateMaps(t *testing.T, items []string, pageSize, maxPages int) []string {
	t.Helper()

	var (
		got   []string
		token string
	)

	for range maxPages + 1 {
		page, next := paginateMaps(items, pageSize, searchStartOffset(token))
		got = append(got, page...)

		if next == "" {
			return got
		}

		token = next
	}

	t.Fatalf("paginateMaps did not terminate within %d pages (possible infinite loop)", maxPages)

	return nil
}

func TestPaginateMaps_SevenChecks(t *testing.T) {
	t.Parallel()

	t.Run("boundary walk non-dividing page size", func(t *testing.T) {
		t.Parallel()

		items := makePaginationProbeItems(23)
		got := walkPaginateMaps(t, items, 5, 20)
		assert.Equal(t, items, got)
	})

	t.Run("exact division", func(t *testing.T) {
		t.Parallel()

		items := makePaginationProbeItems(20)
		got := walkPaginateMaps(t, items, 5, 20)
		assert.Equal(t, items, got)
	})

	t.Run("single page", func(t *testing.T) {
		t.Parallel()

		items := makePaginationProbeItems(3)
		out, next := paginateMaps(items, 10, 0)
		assert.Equal(t, items, out)
		assert.Empty(t, next)
	})

	t.Run("final page", func(t *testing.T) {
		t.Parallel()

		items := makePaginationProbeItems(12)
		out, next := paginateMaps(items, 5, 10)
		assert.Equal(t, []string{"item-010", "item-011"}, out)
		assert.Empty(t, next)
	})

	t.Run("empty collection", func(t *testing.T) {
		t.Parallel()

		out, next := paginateMaps([]string{}, 5, 0)
		assert.Empty(t, out)
		assert.Empty(t, next)
	})

	t.Run("cursor round trip", func(t *testing.T) {
		t.Parallel()

		items := makePaginationProbeItems(10)
		page1, next1 := paginateMaps(items, 4, 0)
		require.NotEmpty(t, next1)
		page2, next2 := paginateMaps(items, 4, searchStartOffset(next1))
		require.NotEmpty(t, next2)
		page3, next3 := paginateMaps(items, 4, searchStartOffset(next2))
		assert.Empty(t, next3)

		all := append(append(page1, page2...), page3...)
		assert.Equal(t, items, all)
	})

	t.Run("stale cursor past end does not panic", func(t *testing.T) {
		t.Parallel()

		items := makePaginationProbeItems(5)
		out, next := paginateMaps(items, 5, searchStartOffset("999999"))
		assert.Empty(t, out)
		assert.Empty(t, next)
	})
}

func TestSearchStartOffset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		token string
		want  int
	}{
		{name: "empty", token: "", want: 0},
		{name: "zero", token: "0", want: 0},
		{name: "positive", token: "42", want: 42},
		{name: "negative", token: "-1", want: 0},
		{name: "not a number", token: "not-a-number", want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, searchStartOffset(tc.token))
		})
	}
}
