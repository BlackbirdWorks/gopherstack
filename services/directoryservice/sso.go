package directoryservice

import (
	"context"
)

// EnableSso enables single sign-on for a directory.
func (b *InMemoryBackend) EnableSso(ctx context.Context, directoryID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("EnableSso")
	defer b.mu.Unlock()

	d, ok := b.directoryGet(region, directoryID)
	if !ok {
		return ErrDirectoryNotFound
	}

	d.SsoEnabled = true

	return nil
}

// DisableSso disables single sign-on for a directory.
func (b *InMemoryBackend) DisableSso(ctx context.Context, directoryID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DisableSso")
	defer b.mu.Unlock()

	d, ok := b.directoryGet(region, directoryID)
	if !ok {
		return ErrDirectoryNotFound
	}

	d.SsoEnabled = false

	return nil
}
