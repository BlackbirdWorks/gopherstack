package azuresearch

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

const snapshotVersion = 1

type backendSnapshot struct {
	Services map[string]*Service `json:"services"`
	Version  int                 `json:"version"`
}

// Snapshot implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(
		ctx,
		"azuresearch",
		backendSnapshot{Version: snapshotVersion, Services: b.services},
	)
}

// Restore implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "azuresearch", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "azuresearch: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)

		b.services = map[string]*Service{}

		return nil
	}

	if err := normalize(&snap); err != nil {
		return err
	}

	b.services = snap.Services

	return nil
}

func normalize(snap *backendSnapshot) error {
	if snap.Services == nil {
		snap.Services = map[string]*Service{}
	}

	for name, s := range snap.Services {
		if s == nil {
			return fmt.Errorf("%w: service %q", errNullEntry, name)
		}

		if err := normalizeService(s); err != nil {
			return err
		}
	}

	return nil
}

func normalizeService(s *Service) error {
	if s.Indexes == nil {
		s.Indexes = map[string]*Index{}
	}

	if s.Runtime == nil {
		s.Runtime = map[string]*IndexerRuntime{}
	}

	if s.Objects == nil {
		s.Objects = newObjects()
	}

	for _, k := range objectKindList() {
		if s.Objects[k] == nil {
			s.Objects[k] = map[string]Object{}
		}
	}

	for name, ix := range s.Indexes {
		if err := normalizeIndex(name, ix); err != nil {
			return err
		}
	}

	for name, rt := range s.Runtime {
		if rt == nil {
			return fmt.Errorf("%w: indexer runtime %q", errNullEntry, name)
		}

		if rt.Seen == nil {
			rt.Seen = map[string]string{}
		}
	}

	return nil
}

func normalizeIndex(name string, ix *Index) error {
	if ix == nil || ix.Def == nil {
		return fmt.Errorf("%w: index %q", errNullEntry, name)
	}

	if ix.Docs == nil {
		ix.Docs = map[string]*Doc{}
	}

	for k, d := range ix.Docs {
		if d == nil {
			return fmt.Errorf("%w: document %q", errNullEntry, k)
		}
	}

	if err := ix.rebuild(); err != nil {
		return fmt.Errorf("azuresearch: restore index %q: %w", name, err)
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
