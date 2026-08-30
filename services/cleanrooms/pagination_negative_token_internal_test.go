package cleanrooms

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPaginate_NegativeOffsetToken reproduces a nextToken decoding to a
// negative offset. paginate parses nextToken with a bare fmt.Sscanf and no
// `< 0` guard, and its `start >= len(items)` check does not catch a
// negative offset, so items[start:end] previously panicked with a negative
// slice bound. paginate backs every List op in this package via
// listItems/listNestedItems.
func TestPaginate_NegativeOffsetToken(t *testing.T) {
	t.Parallel()

	items := []string{"a", "b", "c"}

	require.NotPanics(t, func() {
		page, next := paginate(items, "", "-5")
		assert.Equal(t, items, page, "a negative-offset token must be treated like start=0")
		assert.Empty(t, next)
	})
}
