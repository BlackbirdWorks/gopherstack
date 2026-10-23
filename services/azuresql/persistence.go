package azuresql

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

const snapshotVersion = 1

type serverSnap struct {
	Spec      Spec       `json:"spec"`
	Databases []Database `json:"databases,omitempty"`
}

type backendSnapshot struct {
	Servers map[string]serverSnap `json:"servers"`
	Version int                   `json:"version"`
}

// Snapshot serialises every server's spec and database list. Engine data is not persisted: containers start
// empty on restore and the tracked databases are created again.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{Version: snapshotVersion, Servers: make(map[string]serverSnap, len(b.servers))}

	for k, s := range b.servers {
		dbs := make([]Database, 0, len(s.dbs))
		for _, d := range s.dbs {
			dbs = append(dbs, d)
		}

		slices.SortFunc(dbs, func(a, c Database) int { return strings.Compare(a.Name, c.Name) })
		snap.Servers[k] = serverSnap{Spec: s.spec, Databases: dbs}
	}

	return persistence.MarshalSnapshot(ctx, "azuresql", snap)
}

// Restore loads a snapshot, starting every server again on a fresh port.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "azuresql", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "azuresql: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	b.Reset()

	for k, s := range snap.Servers {
		if err := b.restoreServer(k, s); err != nil {
			b.Reset()

			return fmt.Errorf("azuresql: restore server %q: %w", k, err)
		}
	}

	return nil
}

func (b *InMemoryBackend) restoreServer(key string, s serverSnap) error {
	if _, err := b.Ensure(key, s.Spec); err != nil {
		return err
	}

	for _, d := range s.Databases {
		if err := b.EnsureDatabase(key, d.Name, d.Collation); err != nil {
			return err
		}
	}

	return nil
}
