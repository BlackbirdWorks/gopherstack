package azurecontainerregistry

import (
	"context"
	"errors"
	"io/fs"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// SetExportAllowed permits or refuses importing images out of the registry; Learn data-loss-prevention.
func (b *InMemoryBackend) SetExportAllowed(name string, allowed bool) error {
	b.mu.Lock("SetExportAllowed")
	defer b.mu.Unlock()

	r, ok := b.regs[strs.Fold(name)]
	if !ok {
		return ErrRegistryNotFound
	}

	r.ExportDisabled = !allowed

	return nil
}

func (b *InMemoryBackend) exportAllowed(name string) bool {
	b.mu.RLock("exportAllowed")
	defer b.mu.RUnlock()

	r, ok := b.regs[strs.Fold(name)]

	return !ok || !r.ExportDisabled
}

// UsedBytes returns the bytes of blobs and manifests the registry stores; blobs are content-addressed, so a
// layer shared by several images counts once.
func (b *InMemoryBackend) UsedBytes(name string) (int64, error) {
	if _, ok := b.app(name); !ok {
		return 0, ErrRegistryNotFound
	}

	b.mu.RLock("UsedBytes")

	r := b.regs[strs.Fold(name)]
	if r == nil || r.driver == nil {
		b.mu.RUnlock()

		return 0, ErrRegistryNotFound
	}

	drv, ctx := r.driver, b.ctx
	b.mu.RUnlock()

	return storedBytes(ctx, drv)
}

func storedBytes(ctx context.Context, drv storagedriver.StorageDriver) (int64, error) {
	var total int64

	err := drv.Walk(ctx, storageRoot+"/blobs", func(info storagedriver.FileInfo) error {
		if !info.IsDir() {
			total += info.Size()
		}

		return nil
	})
	if err != nil && !errors.As(err, &storagedriver.PathNotFoundError{}) && !errors.Is(err, fs.ErrNotExist) {
		return 0, err
	}

	return total, nil
}
