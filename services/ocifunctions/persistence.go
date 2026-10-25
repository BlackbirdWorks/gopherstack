package ocifunctions

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the stored types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type backendSnapshot struct {
	Apps     map[string]*Application `json:"applications"`
	Funcs    map[string]*Function    `json:"functions"`
	WorkReqs map[string]*WorkRequest `json:"workRequests"`
	Auth     []ociauth.KeyRecord     `json:"authKeys"`
	Version  int                     `json:"version"`
	Seq      int64                   `json:"seq"`
}

// Snapshot serialises the backend state, including registered API keys. Containers are not persisted.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	keys := b.Auth.Records()

	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "ocifunctions", backendSnapshot{
		Version: snapshotVersion, Apps: b.apps, Funcs: b.funcs, WorkReqs: b.workReqs, Auth: keys, Seq: b.seq,
	})
}

// Restore loads backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "ocifunctions", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "ocifunctions: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	if err := checkSnapshot(&snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	b.apps, b.funcs, b.workReqs, b.seq = snap.Apps, snap.Funcs, snap.WorkReqs, snap.Seq
	b.mu.Unlock()
	b.Auth.Load(snap.Auth)
	b.exec.drain()

	return nil
}

func nullCheck[T any](kind string, m *map[string]*T) error {
	if *m == nil {
		*m = map[string]*T{}
	}

	for id, v := range *m {
		if v == nil {
			return fmt.Errorf("%w: %s %q", ErrSnapshotNull, kind, id)
		}
	}

	return nil
}

func checkSnapshot(snap *backendSnapshot) error {
	for _, err := range []error{
		nullCheck("application", &snap.Apps), nullCheck("function", &snap.Funcs),
		nullCheck("work request", &snap.WorkReqs),
	} {
		if err != nil {
			return err
		}
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("ocifunctions: restore snapshot: %w", err)
	}

	return nil
}
