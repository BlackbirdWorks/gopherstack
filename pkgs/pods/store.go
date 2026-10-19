// Package pods saves and loads named, versioned whole-emulator snapshots
// ("pods") to a local directory or an S3-compatible bucket.
package pods

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// ErrNotFound is returned when a pod, version or blob does not exist.
var ErrNotFound = errors.New("pod not found")

// BlobStore is the minimal object storage pods need. Keys use "/" separators.
type BlobStore interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	// List returns every key that starts with prefix, sorted.
	List(ctx context.Context, prefix string) ([]string, error)
	Delete(ctx context.Context, key string) error
}

// DirStore stores blobs as files under a root directory.
type DirStore struct {
	root string
}

// NewDirStore returns a DirStore rooted at dir, creating it if needed.
func NewDirStore(dir string) (*DirStore, error) {
	const dirPerm = 0o750

	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, fmt.Errorf("create pods dir: %w", err)
	}

	return &DirStore{root: dir}, nil
}

func (d *DirStore) path(key string) (string, error) {
	if key == "" || strings.Contains(key, "..") || filepath.IsAbs(key) {
		return "", fmt.Errorf("%w: key %q", ErrInvalidName, key)
	}

	return filepath.Join(d.root, filepath.FromSlash(key)), nil
}

// Put writes data atomically (temp file + rename).
func (d *DirStore) Put(_ context.Context, key string, data []byte) error {
	const (
		dirPerm  = 0o750
		filePerm = 0o600
	)

	p, err := d.path(key)
	if err != nil {
		return err
	}

	if err = os.MkdirAll(filepath.Dir(p), dirPerm); err != nil {
		return fmt.Errorf("create pod dir: %w", err)
	}

	tmp := p + ".tmp"
	if err = os.WriteFile(tmp, data, filePerm); err != nil {
		return fmt.Errorf("write pod blob: %w", err)
	}

	if err = os.Rename(tmp, p); err != nil {
		_ = os.Remove(tmp)

		return fmt.Errorf("commit pod blob: %w", err)
	}

	return nil
}

// Get reads a blob.
func (d *DirStore) Get(_ context.Context, key string) ([]byte, error) {
	p, err := d.path(key)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNotFound
	}

	if err != nil {
		return nil, fmt.Errorf("read pod blob: %w", err)
	}

	return data, nil
}

// List returns keys under root that start with prefix.
func (d *DirStore) List(_ context.Context, prefix string) ([]string, error) {
	var keys []string

	err := filepath.WalkDir(d.root, func(p string, ent fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if ent.IsDir() || strings.HasSuffix(p, ".tmp") {
			return nil
		}

		rel, relErr := filepath.Rel(d.root, p)
		if relErr != nil {
			return relErr
		}

		if key := filepath.ToSlash(rel); strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}

	sort.Strings(keys)

	return keys, nil
}

// Delete removes a blob; a missing blob is not an error.
func (d *DirStore) Delete(_ context.Context, key string) error {
	p, err := d.path(key)
	if err != nil {
		return err
	}

	if err = os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("delete pod blob: %w", err)
	}

	_ = os.Remove(filepath.Dir(p))

	return nil
}
