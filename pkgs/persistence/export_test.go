package persistence

import "os"

// WithFileSyncFn returns a copy of s with the file-sync function replaced by fn.
// Used in unit tests to simulate Sync() failures.
func (s *FileStore) WithFileSyncFn(fn func(*os.File) error) *FileStore {
	dup := *s
	dup.fileSyncFn = fn

	return &dup
}

// WithDirSyncFn returns a copy of s with the directory-sync function replaced by fn.
// Used in unit tests to simulate directory Sync() failures.
func (s *FileStore) WithDirSyncFn(fn func(string) error) *FileStore {
	dup := *s
	dup.dirSyncFn = fn

	return &dup
}
