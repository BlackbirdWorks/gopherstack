package persistence

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// FileStore persists blobs as JSON files on the local file system.
// Data is stored at {baseDir}/{service}/{key}.json.
type FileStore struct {
	baseDir string
}

// NewFileStore creates a FileStore rooted at baseDir.
// The directory is created (with parents) if it does not exist.
func NewFileStore(baseDir string) (*FileStore, error) {
	if err := os.MkdirAll(baseDir, 0o700); err != nil {
		return nil, fmt.Errorf("persistence: create base dir: %w", err)
	}

	return &FileStore{baseDir: baseDir}, nil
}

// sanitizeSegment makes a service or key name safe to use as a single path
// component. Both forward- and back-slashes are replaced, and any segment
// that is "." or ".." is rewritten to "_" to prevent directory traversal.
func sanitizeSegment(s string) string {
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, "\\", "_")

	if s == "." || s == ".." {
		return "_"
	}

	return s
}

// path returns the file path for the given service and key.
func (f *FileStore) path(service, key string) string {
	return filepath.Join(f.baseDir, sanitizeSegment(service), sanitizeSegment(key)+".json")
}

// Save writes data to {baseDir}/{service}/{key}.json.
func (f *FileStore) Save(service, key string, data []byte) error {
	p := f.path(service, key)
	dir := filepath.Dir(p)

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("persistence: mkdir %s: %w", dir, err)
	}

	// Write to a unique temp file in the same directory to ensure atomicity.
	// Using a unique name avoids clobbering concurrent writes for the same key.
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("persistence: create temp file: %w", err)
	}

	// os.CreateTemp always returns an absolute path; Clean normalises it.
	// The nolint directives below suppress G703 (path traversal via taint
	// analysis): tmpName is derived from os.CreateTemp with a controlled dir
	// and is not user-supplied.
	tmpName := filepath.Clean(tmp.Name())

	if _, writeErr := tmp.Write(data); writeErr != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName) //nolint:gosec // G703: path from CreateTemp within sanitized dir

		return fmt.Errorf("persistence: write %s: %w", tmpName, writeErr)
	}

	if closeErr := tmp.Close(); closeErr != nil {
		_ = os.Remove(tmpName) //nolint:gosec // G703: path from CreateTemp within sanitized dir

		return fmt.Errorf("persistence: close %s: %w", tmpName, closeErr)
	}

	if renameErr := os.Rename( //nolint:gosec // G703: path from CreateTemp within sanitized dir
		tmpName,
		p,
	); renameErr != nil {
		_ = os.Remove(tmpName) //nolint:gosec // G703: path from CreateTemp within sanitized dir

		return fmt.Errorf("persistence: rename %s -> %s: %w", tmpName, p, renameErr)
	}

	return nil
}

// Load reads data from {baseDir}/{service}/{key}.json.
// Returns ErrKeyNotFound when the file does not exist.
func (f *FileStore) Load(service, key string) ([]byte, error) {
	p := f.path(service, key)

	data, err := os.ReadFile(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrKeyNotFound
		}

		return nil, fmt.Errorf("persistence: read %s: %w", p, err)
	}

	return data, nil
}

// Delete removes {baseDir}/{service}/{key}.json.
// It is not an error if the file does not exist.
func (f *FileStore) Delete(service, key string) error {
	p := f.path(service, key)

	if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("persistence: delete %s: %w", p, err)
	}

	return nil
}

// ListKeys returns the base names (without .json extension) of all files
// under {baseDir}/{service}/. Temp files created during atomic saves are excluded.
func (f *FileStore) ListKeys(service string) ([]string, error) {
	dir := filepath.Join(f.baseDir, sanitizeSegment(service))

	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}

		return nil, fmt.Errorf("persistence: list %s: %w", dir, err)
	}

	keys := make([]string, 0, len(entries))

	for _, e := range entries {
		if e.IsDir() {
			continue
		}

		name := e.Name()
		if before, ok := strings.CutSuffix(name, ".json"); ok {
			keys = append(keys, before)
		}
	}

	return keys, nil
}
