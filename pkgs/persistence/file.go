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

// path returns the file path for the given service and key.
func (f *FileStore) path(service, key string) string {
	// Sanitise: replace path separators so callers cannot traverse directories.
	svc := strings.ReplaceAll(service, string(os.PathSeparator), "_")
	k := strings.ReplaceAll(key, string(os.PathSeparator), "_")

	return filepath.Join(f.baseDir, svc, k+".json")
}

// Save writes data to {baseDir}/{service}/{key}.json.
func (f *FileStore) Save(service, key string, data []byte) error {
	p := f.path(service, key)

	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return fmt.Errorf("persistence: mkdir %s: %w", filepath.Dir(p), err)
	}

	// Write to a temp file first to ensure atomicity.
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("persistence: write %s: %w", tmp, err)
	}

	if err := os.Rename(tmp, p); err != nil {
		_ = os.Remove(tmp)

		return fmt.Errorf("persistence: rename %s -> %s: %w", tmp, p, err)
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
// under {baseDir}/{service}/.
func (f *FileStore) ListKeys(service string) ([]string, error) {
	dir := filepath.Join(f.baseDir, strings.ReplaceAll(service, string(os.PathSeparator), "_"))

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
