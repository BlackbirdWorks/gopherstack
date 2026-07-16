package sns_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/sns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newR1Backend(t *testing.T) *sns.InMemoryBackend {
	t.Helper()

	return sns.NewInMemoryBackendWithContext(t.Context(), "000000000000", "us-east-1")
}

// TestRefinement1_SetPublishEmitter verifies the emitter can be set.
func TestSetPublishEmitter(t *testing.T) {
	t.Parallel()

	b := newR1Backend(t)
	// SetPublishEmitter accepts nil - just exercises the covered path.
	b.SetPublishEmitter(nil)
}

// TestRefinement1_StorageBackendInterface verifies var_ assertion compiles.
func TestStorageBackendInterface(t *testing.T) {
	t.Parallel()

	var _ sns.StorageBackend = (*sns.InMemoryBackend)(nil)
}

// TestRefinement1_HandlerOpsLen verifies GetSupportedOperations count.
func TestHandlerOpsLen(t *testing.T) {
	t.Parallel()

	b := newR1Backend(t)
	h := sns.NewHandler(b)
	assert.Len(t, h.GetSupportedOperations(), 42)
}

// TestRefinement1_SDKOpsSorted verifies GetSupportedOperations is sorted.
func TestSDKOpsSorted(t *testing.T) {
	t.Parallel()

	b := newR1Backend(t)
	h := sns.NewHandler(b)
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i],
			"ops not sorted at index %d: %s > %s", i, ops[i-1], ops[i])
	}
}
