package memorydb_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

// TestRefinement1_VarCompileTimeAssertion verifies var _ StorageBackend = (*InMemoryBackend)(nil) compiles.
func TestStorageBackendCompileTimeAssertion(t *testing.T) {
	t.Parallel()

	var _ memorydb.StorageBackend = (*memorydb.InMemoryBackend)(nil)
}
