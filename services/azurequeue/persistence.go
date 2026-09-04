package azurequeue

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// azureQueueSnapshotVersion identifies the shape of backendSnapshot. Must be
// bumped whenever a change to storedQueue/storedMessage would make an older
// snapshot unsafe to decode as the current shape; Restore compares this
// against the persisted value and discards (rather than partially decodes)
// any mismatch, mirroring services/azureblob and services/sqs.
const azureQueueSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Azure Queue
// backend. Queues serialises directly (no DTO layer): storedQueue/
// storedMessage have no unexported fields, so encoding/json round-trips
// them as-is.
type backendSnapshot struct {
	Queues  map[string]*storedQueue `json:"queues"`
	Version int                     `json:"version"`
}

// Snapshot serialises the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Version: azureQueueSnapshotVersion,
		Queues:  b.queues,
	}

	return persistence.MarshalSnapshot(ctx, "azurequeue", snap)
}

// Restore loads backend state from a JSON snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "azurequeue", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != azureQueueSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- discard cleanly and start
		// empty instead of erroring, since this is an expected, recoverable
		// condition (e.g. upgrading gopherstack across a snapshot-format
		// change), not data corruption. Mirrors services/azureblob and
		// services/sqs.
		logger.Load(ctx).WarnContext(ctx,
			"azurequeue: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", azureQueueSnapshotVersion)

		b.queues = make(map[string]*storedQueue)

		return nil
	}

	if snap.Queues == nil {
		snap.Queues = make(map[string]*storedQueue)
	}

	for name, q := range snap.Queues {
		// A JSON `null` value at "queues"[name] decodes to a nil
		// *storedQueue without error; leaving it in place would panic the
		// first time anything dereferences it. Reject the whole snapshot
		// rather than silently dropping or fabricating an entry.
		if q == nil {
			return fmt.Errorf("%w: %q", ErrSnapshotQueueNull, name)
		}

		for i, msg := range q.Messages {
			if msg == nil {
				return fmt.Errorf("%w: index %d in queue %q", ErrSnapshotMessageNull, i, name)
			}
		}
	}

	b.queues = snap.Queues

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	type snapshotter interface {
		Snapshot(ctx context.Context) []byte
	}
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	type restorer interface {
		Restore(context.Context, []byte) error
	}
	if r, ok := h.Backend.(restorer); ok {
		if err := r.Restore(ctx, data); err != nil {
			return fmt.Errorf("azurequeue: restore snapshot: %w", err)
		}
	}

	return nil
}
