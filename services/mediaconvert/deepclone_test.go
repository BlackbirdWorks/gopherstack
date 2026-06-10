package mediaconvert_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediaconvert"
)

// TestDeepCloneMap_PreservesDeeplyNestedSettings verifies that deepCloneMap clones a
// settings document without silently truncating deeply-nested values to nil. Previously
// any value nested beyond depth 20 was dropped, corrupting legitimate job settings.
func TestDeepCloneMap_PreservesDeeplyNestedSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		depth int
	}{
		{name: "shallow", depth: 5},
		{name: "at_old_limit", depth: 20},
		{name: "beyond_old_limit", depth: 30},
		{name: "deep", depth: 50},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Build a chain of nested maps of the requested depth, with a leaf marker.
			leafKey := "leaf"
			leafVal := "preserved"

			current := map[string]any{leafKey: leafVal}
			for range tt.depth {
				current = map[string]any{"child": current}
			}

			cloned := mediaconvert.DeepCloneMapForTest(current)

			// Walk the clone down to the leaf and assert the marker survived.
			node := cloned
			for range tt.depth {
				next, ok := node["child"].(map[string]any)
				require.Truef(t, ok, "expected nested map at each level, missing one (depth %d)", tt.depth)
				node = next
			}

			assert.Equal(t, leafVal, node[leafKey])
		})
	}
}
