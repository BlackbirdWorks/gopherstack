package servicediscovery

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestApplyPaginationNamespaces_NegativeOffsetToken reproduces a nextToken
// decoding to a negative offset. decodeCursor has no `< 0` guard, and
// applyPaginationNamespaces' `offset >= len(items)` check does not catch a
// negative offset, so items[offset:end] previously panicked with a negative
// slice bound.
func TestApplyPaginationNamespaces_NegativeOffsetToken(t *testing.T) {
	t.Parallel()

	items := []Namespace{{ID: "ns-0"}, {ID: "ns-1"}, {ID: "ns-2"}}

	require.NotPanics(t, func() {
		page, next := applyPaginationNamespaces(items, encodeCursor(-5), 10)
		assert.Equal(t, items, page, "a negative-offset token must be treated like offset=0")
		assert.Empty(t, next)
	})
}

// TestApplyPaginationInstances_NegativeOffsetToken is the same reproduction
// for applyPaginationInstances, which shares the identical decodeCursor bug.
func TestApplyPaginationInstances_NegativeOffsetToken(t *testing.T) {
	t.Parallel()

	items := []Instance{{ID: "i-0"}, {ID: "i-1"}}

	require.NotPanics(t, func() {
		page, next := applyPaginationInstances(items, encodeCursor(-5), 10)
		assert.Equal(t, items, page, "a negative-offset token must be treated like offset=0")
		assert.Empty(t, next)
	})
}
