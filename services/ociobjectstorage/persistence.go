package ociobjectstorage

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to Bucket/Object would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type backendSnapshot struct {
	Buckets      map[string]*Bucket      `json:"buckets"`
	WorkRequests map[string]*WorkRequest `json:"workRequests"`
	NSMeta       nsMetaWire              `json:"nsMeta"`
	Keys         []ociauth.KeyRecord     `json:"keys"`
	Version      int                     `json:"version"`
}

// Snapshot serialises the backend state, including registered API keys.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	keys := b.Auth.Records()

	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "ociobjectstorage", backendSnapshot{
		Version: snapshotVersion, Buckets: b.buckets, WorkRequests: b.workRequests, NSMeta: b.nsMeta, Keys: keys,
	})
}

// Restore loads backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "ociobjectstorage", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "ociobjectstorage: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)

		b.buckets, b.workRequests = map[string]*Bucket{}, map[string]*WorkRequest{}

		return nil
	}

	for name, bk := range snap.Buckets {
		if bk == nil {
			return fmt.Errorf("%w: bucket %q", ErrSnapshotNull, name)
		}

		normalize(bk)
	}

	if snap.Buckets == nil {
		snap.Buckets = map[string]*Bucket{}
	}

	if snap.WorkRequests == nil {
		snap.WorkRequests = map[string]*WorkRequest{}
	}

	b.buckets, b.workRequests, b.nsMeta = snap.Buckets, snap.WorkRequests, snap.NSMeta
	b.Auth.Load(snap.Keys)

	return nil
}

func normalize(bk *Bucket) {
	if bk.Objects == nil {
		bk.Objects = map[string][]*Object{}
	}

	if bk.Uploads == nil {
		bk.Uploads = map[string]*Upload{}
	}

	if bk.PARs == nil {
		bk.PARs = map[string]*PAR{}
	}

	if bk.Retention == nil {
		bk.Retention = map[string]*Retention{}
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("ociobjectstorage: restore snapshot: %w", err)
	}

	return nil
}
