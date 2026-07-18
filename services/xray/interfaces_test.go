package xray_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/xray"
)

// TestStorageBackendInterface verifies var_ assertion compiles.
func TestStorageBackendInterface(t *testing.T) {
	t.Parallel()

	var _ xray.StorageBackend = (*xray.InMemoryBackend)(nil)
}
