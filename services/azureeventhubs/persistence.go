package azureeventhubs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

const snapshotVersion = 1

var errNullEntry = errors.New("azureeventhubs: null snapshot entry")

type backendSnapshot struct {
	Namespaces map[string]*namespace `json:"namespaces"`
	Version    int                   `json:"version"`
}

// Snapshot implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "azureeventhubs", backendSnapshot{
		Version: snapshotVersion, Namespaces: b.namespaces,
	})
}

// Restore implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "azureeventhubs", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "azureeventhubs: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)

		b.namespaces = map[string]*namespace{}

		return nil
	}

	if err := normalizeSnapshot(snap.Namespaces); err != nil {
		return err
	}

	for key := range b.receivers {
		b.kickAllLocked(key)
	}

	b.namespaces = snap.Namespaces
	if b.namespaces == nil {
		b.namespaces = map[string]*namespace{}
	}

	return nil
}

func normalizeSnapshot(nss map[string]*namespace) error {
	for name, ns := range nss {
		if ns == nil {
			return fmt.Errorf("%w: namespace %q", errNullEntry, name)
		}

		if ns.Hubs == nil {
			ns.Hubs = map[string]*hub{}
		}

		if ns.Rules == nil {
			ns.Rules = map[string]*rule{}
		}

		for hubName, h := range ns.Hubs {
			if err := normalizeHub(hubName, h); err != nil {
				return err
			}
		}
	}

	return nil
}

func normalizeHub(name string, h *hub) error {
	if h == nil {
		return fmt.Errorf("%w: hub %q", errNullEntry, name)
	}

	if h.ConsumerGroups == nil {
		h.ConsumerGroups = map[string]time.Time{}
	}

	for i, p := range h.Partitions {
		if p == nil {
			return fmt.Errorf("%w: hub %q partition %d", errNullEntry, name, i)
		}

		p.notify = make(chan struct{})
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
