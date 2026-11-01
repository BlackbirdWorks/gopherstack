package azurefunctions

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// snapshotVersion must be bumped when a change to the snapshot types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type backendSnapshot struct {
	Apps    map[string]appState `json:"apps"`
	Version int                 `json:"version"`
}

// Snapshot serialises apps, settings, keys, deployments and package bytes; running hosts are not part of it.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{Version: snapshotVersion, Apps: make(map[string]appState, len(b.apps))}
	for k, a := range b.apps {
		snap.Apps[k] = a.appState
	}

	return persistence.MarshalSnapshot(ctx, "azurefunctions", snap)
}

// Restore loads a snapshot, replacing every app; hosts start again on first use.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "azurefunctions", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "azurefunctions: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	b.mu.Lock("Restore")
	boxes := b.takeAllLocked()
	b.apps = make(map[string]*app, len(snap.Apps))

	for _, st := range snap.Apps {
		if st.Settings == nil {
			st.Settings = map[string]string{}
		}

		if st.HostKeys == nil {
			st.HostKeys = map[string]string{}
		}

		if st.SystemKeys == nil {
			st.SystemKeys = map[string]string{}
		}

		if st.FunctionKeys == nil {
			st.FunctionKeys = map[string]map[string]string{}
		}

		b.apps[strs.Fold(st.key())] = &app{appState: st}
	}
	b.mu.Unlock()

	for _, x := range boxes {
		b.teardown(x)
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("azurefunctions: restore snapshot: %w", err)
	}

	return nil
}
