package ocimysql

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
	Systems  map[string]*DBSystem      `json:"dbSystems"`
	Backups  map[string]*Backup        `json:"backups"`
	Channels map[string]*Channel       `json:"channels"`
	Configs  map[string]*Configuration `json:"configurations"`
	WorkReqs map[string]*WorkRequest   `json:"workRequests"`
	Auth     []ociauth.KeyRecord       `json:"authKeys"`
	Version  int                       `json:"version"`
	Seq      int64                     `json:"seq"`
}

// Snapshot serialises the backend state, including registered API keys.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	keys := b.Auth.Records()

	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "ocimysql", backendSnapshot{
		Version: snapshotVersion, Systems: b.systems, Backups: b.backups, Channels: b.channels, Configs: b.configs,
		WorkReqs: b.workReqs, Auth: keys, Seq: b.seq,
	})
}

// Restore loads backend state from a snapshot and relaunches engine servers.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "ocimysql", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "ocimysql: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	if err := checkSnapshot(&snap); err != nil {
		return err
	}

	b.stop()
	b.wg.Wait()

	b.mu.Lock("Restore")
	lives := b.detachAllLocked()
	b.systems, b.backups, b.channels, b.configs = snap.Systems, snap.Backups, snap.Channels, snap.Configs
	b.workReqs, b.seq = snap.WorkReqs, snap.Seq

	for _, s := range b.systems {
		s.EngineHost, s.EnginePort = "", 0
	}

	b.runCtx, b.stop = contextWithCancel()
	b.relaunchLocked()
	e := b.eng
	b.mu.Unlock()

	e.closeAll(lives, false)
	b.Auth.Load(snap.Auth)

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
		nullCheck("DB system", &snap.Systems), nullCheck("backup", &snap.Backups),
		nullCheck("channel", &snap.Channels), nullCheck("configuration", &snap.Configs),
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
		return fmt.Errorf("ocimysql: restore snapshot: %w", err)
	}

	return nil
}
