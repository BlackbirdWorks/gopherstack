package kinesisanalytics

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

type backendSnapshot struct {
	Apps   map[string]map[string]*Application `json:"apps"`
	NextID int64                              `json:"next_id"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	appsCopy := make(map[string]map[string]*Application, len(b.apps))

	for region, regionApps := range b.apps {
		appsCopy[region] = make(map[string]*Application, len(regionApps))

		for k, v := range regionApps {
			appsCopy[region][k] = appCopy(v)
		}
	}

	snap := backendSnapshot{
		Apps:   appsCopy,
		NextID: b.nextID,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "kinesisanalytics: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if snap.Apps == nil {
		snap.Apps = make(map[string]map[string]*Application)
	}

	b.apps = snap.Apps
	b.nextID = snap.NextID

	// Rebuild ARN index from restored applications.
	b.appsByARN = make(map[string]map[string]*Application, len(b.apps))

	for region, regionApps := range b.apps {
		for _, app := range regionApps {
			if b.appsByARN[region] == nil {
				b.appsByARN[region] = make(map[string]*Application)
			}

			b.appsByARN[region][app.ApplicationARN] = app
		}
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	if sb, ok := h.Backend.(*InMemoryBackend); ok {
		return sb.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if sb, ok := h.Backend.(*InMemoryBackend); ok {
		return sb.Restore(ctx, data)
	}

	return nil
}
