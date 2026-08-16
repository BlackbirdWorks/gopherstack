package sns_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

// TestSetPublishEmitter verifies the emitter can be set.
func TestSetPublishEmitter(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	// SetPublishEmitter accepts nil - just exercises the covered path.
	b.SetPublishEmitter(nil)
}

// TestStorageBackendInterface verifies var_ assertion compiles.
func TestStorageBackendInterface(t *testing.T) {
	t.Parallel()

	var _ sns.StorageBackend = (*sns.InMemoryBackend)(nil)
}

// TestHandlerOpsLen verifies GetSupportedOperations count.
func TestHandlerOpsLen(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	h := sns.NewHandler(b)
	assert.Len(t, h.GetSupportedOperations(), 42)
}

// TestSDKOpsSorted verifies GetSupportedOperations is sorted.
func TestSDKOpsSorted(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	h := sns.NewHandler(b)
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i],
			"ops not sorted at index %d: %s > %s", i, ops[i-1], ops[i])
	}
}
