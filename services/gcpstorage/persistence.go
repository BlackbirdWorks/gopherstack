package gcpstorage

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to Bucket/Object would make an
// older snapshot unsafe to decode.
const snapshotVersion = 1

type backendSnapshot struct {
	Buckets map[string]*Bucket `json:"buckets"`
	Version int                `json:"version"`
}

// Snapshot serialises the backend state. In-flight resumable sessions are not persisted.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "gcpstorage",
		backendSnapshot{Version: snapshotVersion, Buckets: b.buckets})
}

// Restore loads backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcpstorage", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.uploads = make(map[string]*uploadSession)

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "gcpstorage: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)

		b.buckets = make(map[string]*Bucket)

		return nil
	}

	var maxGen int64

	for name, bk := range snap.Buckets {
		if bk == nil {
			return fmt.Errorf("%w: %q", ErrSnapshotBucketNull, name)
		}

		if bk.Objects == nil {
			bk.Objects = make(map[string][]*Object)
		}

		for _, versions := range bk.Objects {
			for _, o := range versions {
				if o == nil {
					return fmt.Errorf("%w: object in bucket %q", ErrSnapshotBucketNull, name)
				}

				maxGen = max(maxGen, o.Generation)
			}
		}
	}

	if snap.Buckets == nil {
		snap.Buckets = make(map[string]*Bucket)
	}

	b.buckets = snap.Buckets
	b.lastGen = max(b.lastGen, maxGen)

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcpstorage: restore snapshot: %w", err)
	}

	return nil
}
