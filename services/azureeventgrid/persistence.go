package azureeventgrid

import (
	"context"
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

const snapshotVersion = 1

var errNullEntry = errors.New("azureeventgrid: null snapshot entry")

type backendSnapshot struct {
	Topics     map[string]*topic     `json:"topics"`
	Namespaces map[string]*namespace `json:"namespaces"`
	Version    int                   `json:"version"`
}

// Snapshot implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "azureeventgrid", backendSnapshot{
		Version: snapshotVersion, Topics: b.topics, Namespaces: b.namespaces,
	})
}

// Restore implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "azureeventgrid", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "azureeventgrid: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)

		b.topics, b.namespaces = map[string]*topic{}, map[string]*namespace{}

		return nil
	}

	if err := normalize(&snap); err != nil {
		return err
	}

	b.topics, b.namespaces = snap.Topics, snap.Namespaces

	return nil
}

func normalize(snap *backendSnapshot) error {
	if snap.Topics == nil {
		snap.Topics = map[string]*topic{}
	}

	if snap.Namespaces == nil {
		snap.Namespaces = map[string]*namespace{}
	}

	for name, t := range snap.Topics {
		if t == nil {
			return fmt.Errorf("%w: topic %q", errNullEntry, name)
		}

		if t.Subs == nil {
			t.Subs = map[string]*subscription{}
		}

		for sname, s := range t.Subs {
			if s == nil {
				return fmt.Errorf("%w: subscription %q", errNullEntry, sname)
			}
		}
	}

	return normalizeNamespaces(snap.Namespaces)
}

func normalizeNamespaces(nss map[string]*namespace) error {
	for name, ns := range nss {
		if ns == nil {
			return fmt.Errorf("%w: namespace %q", errNullEntry, name)
		}

		if ns.Topics == nil {
			ns.Topics = map[string]*nsTopic{}
		}

		for tname, t := range ns.Topics {
			if t == nil {
				return fmt.Errorf("%w: namespace topic %q", errNullEntry, tname)
			}

			if t.Subs == nil {
				t.Subs = map[string]*pullSub{}
			}

			for sname, s := range t.Subs {
				if s == nil {
					return fmt.Errorf("%w: namespace subscription %q", errNullEntry, sname)
				}
			}
		}
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
