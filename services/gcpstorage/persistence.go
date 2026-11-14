package gcpstorage

import (
	"context"
	"fmt"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to Bucket/Object would make an
// older snapshot unsafe to decode.
const snapshotVersion = 1

type backendSnapshot struct {
	Buckets  map[string]*Bucket  `json:"buckets"`
	HMACKeys map[string]*HMACKey `json:"hmacKeys,omitempty"`
	SoftBkts []*Bucket           `json:"softDeletedBuckets,omitempty"`
	Version  int                 `json:"version"`
}

// Snapshot serialises the backend state. In-flight resumable sessions are not persisted.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "gcpstorage",
		backendSnapshot{Version: snapshotVersion, Buckets: b.buckets, HMACKeys: b.hmac, SoftBkts: b.softBkts})
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
		b.hmac = make(map[string]*HMACKey)
		b.softBkts = nil

		return nil
	}

	maxGen, err := validateSnapshot(&snap)
	if err != nil {
		return err
	}

	b.buckets = snap.Buckets
	b.hmac = snap.HMACKeys
	b.softBkts = snap.SoftBkts

	if b.hmac == nil {
		b.hmac = make(map[string]*HMACKey)
	}

	b.lastGen = max(b.lastGen, maxGen)

	return nil
}

func validateSoftBuckets(in []*Bucket) (int64, error) {
	var maxGen int64

	for i, bk := range in {
		if bk == nil {
			return 0, fmt.Errorf("%w: soft-deleted bucket %d", ErrSnapshotBucketNull, i)
		}

		if bk.Objects == nil {
			bk.Objects = make(map[string][]*Object)
		}

		maxGen = max(maxGen, bk.Generation)
	}

	return maxGen, nil
}

func validateSnapshot(snap *backendSnapshot) (int64, error) {
	var maxGen int64

	for name, bk := range snap.Buckets {
		if bk == nil {
			return 0, fmt.Errorf("%w: %q", ErrSnapshotBucketNull, name)
		}

		if bk.Objects == nil {
			bk.Objects = make(map[string][]*Object)
		}

		all := slices.Clone(bk.SoftDeleted)
		for _, versions := range bk.Objects {
			all = append(all, versions...)
		}

		for _, o := range all {
			if o == nil {
				return 0, fmt.Errorf("%w: object in bucket %q", ErrSnapshotBucketNull, name)
			}

			maxGen = max(maxGen, o.Generation)
		}
	}

	for id, k := range snap.HMACKeys {
		if k == nil {
			return 0, fmt.Errorf("%w: hmac key %q", ErrSnapshotBucketNull, id)
		}
	}

	softGen, err := validateSoftBuckets(snap.SoftBkts)
	if err != nil {
		return 0, err
	}

	maxGen = max(maxGen, softGen)

	if snap.Buckets == nil {
		snap.Buckets = make(map[string]*Bucket)
	}

	return maxGen, nil
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
