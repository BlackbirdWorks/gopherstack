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
	Spec       Spec              `json:"spec"`
	EngineData map[string][]byte `json:"engineData,omitempty"`
	Databases  []Database        `json:"databases,omitempty"`
}

type backendSnapshot struct {
	Servers map[string]serverSnap `json:"servers"`
	Version int                   `json:"version"`
}

// Snapshot serialises every server's spec and database list, plus a backup of each database's content when
// the engine can export one; a database whose backup fails or exceeds the size caps is saved without data.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	snap := backendSnapshot{Version: snapshotVersion, Servers: map[string]serverSnap{}}

	b.life.RLock("Snapshot")
	defer b.life.RUnlock()

	type target struct {
		engine Engine
		snap   serverSnap
	}

	b.mu.RLock("Snapshot")

	targets := make(map[string]target, len(b.servers))

	for k, s := range b.servers {
		dbs := make([]Database, 0, len(s.dbs))
		for _, d := range s.dbs {
			dbs = append(dbs, d)
		}

		slices.SortFunc(dbs, func(a, c Database) int { return strings.Compare(a.Name, c.Name) })
		targets[k] = target{engine: s.engine, snap: serverSnap{Spec: s.spec, Databases: dbs}}
	}

	b.mu.RUnlock()

	budget := int64(maxSnapshotBytes)

	for k, t := range targets {
		t.snap.EngineData = exportData(ctx, t.engine, t.snap.Databases, &budget)
		snap.Servers[k] = t.snap
	}

	return persistence.MarshalSnapshot(ctx, "azuresql", snap)
}

func exportData(ctx context.Context, eng Engine, dbs []Database, budget *int64) map[string][]byte {
	de, ok := eng.(DataEngine)
	if !ok {
		return nil
	}

	out := map[string][]byte{}

	for _, d := range dbs {
		bctx, cancel := context.WithTimeout(ctx, execDeadline)
		data, err := de.BackupDatabase(bctx, d.Name, min(int64(maxBackupBytes), *budget))

		cancel()

		if err != nil {
			logger.Load(ctx).WarnContext(ctx, "azuresql: database content not saved", "database", d.Name, "error", err)

			continue
		}

		*budget -= int64(len(data))
		out[d.Name] = data
	}

	return out
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

		if data := s.EngineData[d.Name]; len(data) > 0 {
			if err := b.restoreData(key, d.Name, data); err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *InMemoryBackend) restoreData(id, name string, data []byte) error {
	b.life.Lock("restoreData")
	defer b.life.Unlock()

	s, ok := b.lookup(id)
	if !ok {
		return ErrNotFound
	}

	de, ok := s.engine.(DataEngine)
	if !ok {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), execDeadline)
	defer cancel()

	return de.RestoreDatabase(ctx, name, data)
}
