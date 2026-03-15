package persistence_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

var (
	errSimulatedSync    = errors.New("simulated sync failure")
	errSimulatedDirSync = errors.New("simulated dir sync failure")
)

// TestNewFileStore_CreatesNestedDirs verifies that NewFileStore creates the
// base directory (and its parents) when they do not already exist.
func TestNewFileStore_CreatesNestedDirs(t *testing.T) {
	t.Parallel()

	base := filepath.Join(t.TempDir(), "a", "b", "c")
	s, err := persistence.NewFileStore(base)

	require.NoError(t, err)
	require.NotNil(t, s)

	info, statErr := os.Stat(base)
	require.NoError(t, statErr)
	assert.True(t, info.IsDir())
}

// TestNewFileStore_Error ensures NewFileStore returns an error when the base
// directory cannot be created (e.g. a regular file blocks a parent component).
func TestNewFileStore_Error(t *testing.T) {
	t.Parallel()

	// Create a regular file, then attempt to use a subdirectory of it as the
	// base. os.MkdirAll must fail because the path component is a file.
	dir := t.TempDir()
	blockingFile := filepath.Join(dir, "not-a-dir")
	require.NoError(t, os.WriteFile(blockingFile, []byte("data"), 0o600))

	_, err := persistence.NewFileStore(filepath.Join(blockingFile, "subdir"))
	require.Error(t, err)
}

// TestFileStore_ListKeys_SkipsNonJSONAndDirs ensures ListKeys ignores
// subdirectories and files that do not carry the ".json" extension.
func TestFileStore_ListKeys_SkipsNonJSONAndDirs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := persistence.NewFileStore(dir)
	require.NoError(t, err)

	// Save a legitimate key so we have at least one .json file.
	require.NoError(t, s.Save("svc", "valid", []byte(`{}`)))

	// Manually plant a non-.json file and a subdirectory.
	svcDir := filepath.Join(dir, "svc")
	require.NoError(t, os.WriteFile(filepath.Join(svcDir, "ignored.txt"), []byte("x"), 0o600))
	require.NoError(t, os.MkdirAll(filepath.Join(svcDir, "subdir"), 0o700))

	keys, listErr := s.ListKeys("svc")
	require.NoError(t, listErr)
	assert.ElementsMatch(t, []string{"valid"}, keys)
}

// TestFileStore_ListKeys_MissingServiceDir verifies that ListKeys returns nil
// (not an error) when the service directory has never been created.
func TestFileStore_ListKeys_MissingServiceDir(t *testing.T) {
	t.Parallel()

	s, err := persistence.NewFileStore(t.TempDir())
	require.NoError(t, err)

	keys, listErr := s.ListKeys("never-existed")
	require.NoError(t, listErr)
	assert.Nil(t, keys)
}

// TestFileStore_SanitizeDotSegments checks that service and key values of "."
// and ".." are sanitised and do not escape the base directory.
func TestFileStore_SanitizeDotSegments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		service string
		key     string
	}{
		{name: "dot_service", service: ".", key: "snapshot"},
		{name: "dotdot_service", service: "..", key: "snapshot"},
		{name: "dot_key", service: "svc", key: "."},
		{name: "dotdot_key", service: "svc", key: ".."},
		{name: "both_dot", service: ".", key: "."},
		{name: "both_dotdot", service: "..", key: ".."},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			baseDir := t.TempDir()
			s, err := persistence.NewFileStore(baseDir)
			require.NoError(t, err)

			data := []byte(`{"safe":true}`)
			require.NoError(t, s.Save(tt.service, tt.key, data))

			got, loadErr := s.Load(tt.service, tt.key)
			require.NoError(t, loadErr)
			assert.Equal(t, data, got)

			// Verify that no file escaped the base directory.
			err = filepath.WalkDir(baseDir, func(path string, _ os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				rel, relErr := filepath.Rel(baseDir, path)
				require.NoError(t, relErr)
				assert.NotContains(t, rel, "..", "file escaped base dir: %s", path)

				return nil
			})
			require.NoError(t, err)
		})
	}
}

// TestFileStore_Load_ErrorOnUnreadableFile covers the non-ErrNotExist error
// branch of Load by making a directory appear where a file is expected.
func TestFileStore_Load_ErrorOnUnreadableFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := persistence.NewFileStore(dir)
	require.NoError(t, err)

	// Plant a *directory* where the .json file would normally live so that
	// os.ReadFile returns an error that is not os.ErrNotExist.
	fakePath := filepath.Join(dir, "svc", "snap.json")
	require.NoError(t, os.MkdirAll(fakePath, 0o700))

	_, loadErr := s.Load("svc", "snap")
	require.Error(t, loadErr)
	assert.NotErrorIs(t, loadErr, persistence.ErrKeyNotFound)
}

// TestFileStore_Delete_Idempotent checks that deleting a non-existent key is
// not an error (the idempotent contract of the Store interface).
func TestFileStore_Delete_Idempotent(t *testing.T) {
	t.Parallel()

	s, err := persistence.NewFileStore(t.TempDir())
	require.NoError(t, err)

	require.NoError(t, s.Delete("nosvc", "nokey"))
}

// TestFileStore_Save_MkdirError covers the MkdirAll error branch inside Save.
func TestFileStore_Save_MkdirError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := persistence.NewFileStore(dir)
	require.NoError(t, err)

	// Plant a regular file where the service directory would be created.
	// os.MkdirAll will fail because the path component is an existing regular file.
	svcFile := filepath.Join(dir, "svc")
	require.NoError(t, os.WriteFile(svcFile, []byte("blocker"), 0o600))

	saveErr := s.Save("svc", "key", []byte(`{}`))
	require.Error(t, saveErr)
}

// TestFileStore_Save_RenameError covers the [os.Rename] error branch inside Save
// by replacing the destination path with a directory so the rename cannot complete.
func TestFileStore_Save_RenameError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := persistence.NewFileStore(dir)
	require.NoError(t, err)

	// First save creates the service directory and the key.json file.
	require.NoError(t, s.Save("svc", "key", []byte(`{"v":1}`)))

	// Replace key.json with a directory so the atomic rename fails.
	keyPath := filepath.Join(dir, "svc", "key.json")
	require.NoError(t, os.Remove(keyPath))
	require.NoError(t, os.Mkdir(keyPath, 0o700))

	saveErr := s.Save("svc", "key", []byte(`{"v":2}`))
	require.Error(t, saveErr)
}

// TestFileStore_ListKeys_ReadDirError covers the non-ErrNotExist error branch
// of ListKeys by placing a regular file where the service directory should be.
func TestFileStore_ListKeys_ReadDirError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := persistence.NewFileStore(dir)
	require.NoError(t, err)

	// Plant a regular file at the service directory path; os.ReadDir returns an
	// error that is not os.ErrNotExist, exercising the wrapped-error branch.
	svcPath := filepath.Join(dir, "svc")
	require.NoError(t, os.WriteFile(svcPath, []byte("not-a-dir"), 0o600))

	_, listErr := s.ListKeys("svc")
	require.Error(t, listErr)
	assert.NotErrorIs(t, listErr, persistence.ErrKeyNotFound)
}

// TestFileStore_Delete_Error covers the error return branch of Delete when
// the file exists but cannot be removed (e.g. read-only parent directory).
// The test is skipped when running as root because root bypasses permissions.
func TestFileStore_Delete_Error(t *testing.T) {
	t.Parallel()

	if os.Getuid() == 0 {
		t.Skip("root bypasses permission checks")
	}

	dir := t.TempDir()
	s, err := persistence.NewFileStore(dir)
	require.NoError(t, err)

	require.NoError(t, s.Save("svc", "key", []byte(`{}`)))

	// Make the service directory read-only so os.Remove cannot delete the file.
	svcDir := filepath.Join(dir, "svc")
	require.NoError(t, os.Chmod(svcDir, 0o500))

	t.Cleanup(func() {
		_ = os.Chmod(svcDir, 0o700)
	})

	deleteErr := s.Delete("svc", "key")
	require.Error(t, deleteErr)
}

// TestFileStore_Save_RoundTrip verifies that data written with Save is
// exactly recovered by Load across various content types.
func TestFileStore_Save_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data []byte
	}{
		{name: "json_object", data: []byte(`{"service":"test","count":42}`)},
		{name: "empty_bytes", data: []byte{}},
		{name: "binary", data: []byte{0x00, 0xFF, 0x7F, 0x80}},
		{name: "unicode", data: []byte(`{"emoji":"🚀","text":"日本語"}`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s, err := persistence.NewFileStore(t.TempDir())
			require.NoError(t, err)

			require.NoError(t, s.Save("svc", "key", tt.data))

			got, loadErr := s.Load("svc", "key")
			require.NoError(t, loadErr)
			assert.Equal(t, tt.data, got)
		})
	}
}

// TestFileStore_Save_SyncError covers the tmp.Sync() error branch inside Save.
// It verifies that the error is wrapped correctly and that the temp file is
// cleaned up (does not remain in the service directory).
func TestFileStore_Save_SyncError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	base, err := persistence.NewFileStore(dir)
	require.NoError(t, err)

	s := base.WithFileSyncFn(func(_ *os.File) error { return errSimulatedSync })

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
	base, err := persistence.NewFileStore(dir)
	require.NoError(t, err)

	s := base.WithDirSyncFn(func(_ string) error { return errSimulatedDirSync })

	saveErr := s.Save("svc", "key", []byte(`{}`))
	require.Error(t, saveErr)
	require.ErrorContains(t, saveErr, "persistence: sync dir")
	require.ErrorIs(t, saveErr, errSimulatedDirSync)
}
