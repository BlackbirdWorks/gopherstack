package azureredis

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

const snapshotVersion = 1

type cacheSnap struct {
	Spec     Spec   `json:"spec"`
	Primary  string `json:"primary"`
	Second   string `json:"secondary"`
	Keyspace []byte `json:"keyspace,omitempty"`
}

type backendSnapshot struct {
	Caches  map[string]cacheSnap `json:"caches"`
	Version int                  `json:"version"`
}

// Snapshot serialises every cache with its keys and keyspace; servers and ports are rebuilt on restore.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{Version: snapshotVersion, Caches: make(map[string]cacheSnap, len(b.caches))}

	for k, c := range b.caches {
		blob, _ := c.engine.Snapshot()
		keys := c.keys.get()
		snap.Caches[k] = cacheSnap{Spec: c.spec, Primary: keys.Primary, Second: keys.Secondary, Keyspace: blob}
	}

	return persistence.MarshalSnapshot(ctx, "azureredis", snap)
}

// Restore loads a snapshot, starting every cache again on fresh ports.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "azureredis", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "azureredis: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	fresh, err := b.rebuild(snap.Caches)
	if err != nil {
		return err
	}

	old := b.caches
	b.caches = fresh

	for _, c := range old {
		c.close()
	}

	return nil
}

func (b *InMemoryBackend) rebuild(snaps map[string]cacheSnap) (map[string]*cache, error) {
	out := make(map[string]*cache, len(snaps))

	for k, s := range snaps {
		c, err := b.startCache(s.Spec, Keys{Primary: s.Primary, Secondary: s.Second})
		if err == nil {
			err = c.engine.Restore(s.Keyspace)
		}

		if err != nil {
			for _, started := range out {
				started.close()
			}

			return nil, fmt.Errorf("azureredis: restore cache %q: %w", k, err)
		}

		out[k] = c
	}

	return out, nil
}
