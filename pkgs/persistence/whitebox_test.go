package persistence

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	errSimulatedSync    = errors.New("simulated sync failure")
	errSimulatedDirSync = errors.New("simulated dir sync failure")
)

// withFileSyncFn returns a copy of s with the file-sync function replaced by fn.
func withFileSyncFn(s *FileStore, fn func(*os.File) error) *FileStore {
	dup := *s
	dup.fileSyncFn = fn

	return &dup
}

// withDirSyncFn returns a copy of s with the directory-sync function replaced by fn.
func withDirSyncFn(s *FileStore, fn func(string) error) *FileStore {
	dup := *s
	dup.dirSyncFn = fn

	return &dup
}

// TestFileStore_Save_SyncError covers the tmp.Sync() error branch inside Save.
// It verifies that the error is wrapped correctly and that the temp file is
// cleaned up (does not remain in the service directory).
func TestFileStore_Save_SyncError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	base, err := NewFileStore(dir)
	require.NoError(t, err)

	s := withFileSyncFn(base, func(_ *os.File) error { return errSimulatedSync })

	saveErr := s.Save("svc", "key", []byte(`{}`))
	require.Error(t, saveErr)
	require.ErrorContains(t, saveErr, "persistence: sync")
	require.ErrorIs(t, saveErr, errSimulatedSync)

	// The temp file must not linger.
	svcDir := filepath.Join(dir, "svc")
	entries, _ := os.ReadDir(svcDir)
	for _, e := range entries {
		assert.False(t, strings.HasPrefix(e.Name(), ".tmp-"),
			"temp file %s was not cleaned up after sync failure", e.Name())
	}
}

// TestFileStore_Save_DirSyncError covers the syncDirectory() error branch inside Save.
// The rename has already happened, so the key file should exist, but the
// wrapped error is returned.
func TestFileStore_Save_DirSyncError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	base, err := NewFileStore(dir)
	require.NoError(t, err)

	s := withDirSyncFn(base, func(_ string) error { return errSimulatedDirSync })

	saveErr := s.Save("svc", "key", []byte(`{}`))
	require.Error(t, saveErr)
	require.ErrorContains(t, saveErr, "persistence: sync dir")
	require.ErrorIs(t, saveErr, errSimulatedDirSync)
}
