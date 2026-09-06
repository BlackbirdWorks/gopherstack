package cosmosdb

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// cosmosdbSnapshotVersion identifies the shape of backendSnapshot. Must be
// bumped whenever a change to storedDatabase/storedContainer/storedDocument
// would make an older snapshot unsafe to decode as the current shape;
// Restore compares this against the persisted value and discards (rather
// than partially decodes) any mismatch, mirroring services/azuretable.
const cosmosdbSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Cosmos DB backend.
type backendSnapshot struct {
	Databases map[string]*storedDatabase `json:"databases"`
	Version   int                        `json:"version"`
}

// Snapshot serialises the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Version:   cosmosdbSnapshotVersion,
		Databases: b.databases,
	}

	return persistence.MarshalSnapshot(ctx, "cosmosdb", snap)
}

// Restore loads backend state from a JSON snapshot. It implements
// persistence.Persistable. A snapshot's "databases" map, or any database's
// "Containers" map, or any container's "Documents" map, may legally be JSON
// null (a nil Go map after decode) rather than absent -- this is tolerated
// by initializing each to an empty map rather than leaving it nil, since a
// nil map is safe to range/read but panics on insertion (this was a real M2
// bug in services/azuretable's Restore -- see AZURE.md's process rules).
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "cosmosdb", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != cosmosdbSnapshotVersion {
		logger.Load(ctx).WarnContext(ctx,
			"cosmosdb: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", cosmosdbSnapshotVersion)

		b.databases = make(map[string]*storedDatabase)

		return nil
	}

	if snap.Databases == nil {
		snap.Databases = make(map[string]*storedDatabase)
	}

	if err := validateSnapshotDatabases(snap.Databases); err != nil {
		return err
	}

	b.databases = snap.Databases

	return nil
}

// validateSnapshotDatabases rejects a snapshot whose "databases" map, any
// database's "Containers" map, or any container's "Documents" map holds a
// JSON null entry -- which decodes to a nil pointer that would panic on
// first dereference if stored as-is -- and initializes any nil nested map
// (legal JSON `null`, decodes to a nil Go map, not a nil pointer, so it
// isn't rejected here) to an empty one.
func validateSnapshotDatabases(databases map[string]*storedDatabase) error {
	for dbName, d := range databases {
		if d == nil {
			return fmt.Errorf("%w: %q", ErrSnapshotDatabaseNull, dbName)
		}

		if d.Containers == nil {
			d.Containers = make(map[string]*storedContainer)
		}

		if err := validateSnapshotContainers(dbName, d.Containers); err != nil {
			return err
		}
	}

	return nil
}

func validateSnapshotContainers(dbName string, containers map[string]*storedContainer) error {
	for collName, c := range containers {
		if c == nil {
			return fmt.Errorf("%w: %s/%s", ErrSnapshotContainerNull, dbName, collName)
		}

		if c.Documents == nil {
			c.Documents = make(map[documentCompositeKey]*storedDocument)
		}

		for key, doc := range c.Documents {
			if doc == nil {
				return fmt.Errorf("%w: %s/%s key %v", ErrSnapshotDocumentNull, dbName, collName, key)
			}
		}
	}

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
			return fmt.Errorf("cosmosdb: restore snapshot: %w", err)
		}
	}

	return nil
}
