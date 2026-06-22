package kinesis

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// backendSnapshot is the persisted form of the backend. Streams and
// ResourcePolicies are nested by region (outer key = region) to match the
// region-isolated in-memory layout.
type backendSnapshot struct {
	Streams          map[string]map[string]*Stream `json:"streams"`
	ResourcePolicies map[string]map[string]string  `json:"resourcePolicies,omitempty"`
	AccountID        string                        `json:"accountID"`
	Region           string                        `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
// Note: shard sequence number counters are now serialised via the NextSeq field.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Streams:          b.streams,
		ResourcePolicies: b.resourcePolicies,
		AccountID:        b.accountID,
		Region:           b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "kinesis: snapshot serialization failed", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "kinesis", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Streams == nil {
		snap.Streams = make(map[string]map[string]*Stream)
	}

	if snap.ResourcePolicies == nil {
		snap.ResourcePolicies = make(map[string]map[string]string)
	}

	for region, regionStreams := range snap.Streams {
		for name, stream := range regionStreams {
			if stream == nil {
				delete(regionStreams, name)

				continue
			}
			initializeStreamRuntime(stream, name)
		}

		if len(regionStreams) == 0 {
			delete(snap.Streams, region)
		}
	}

	b.streams = snap.Streams
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.resourcePolicies = snap.ResourcePolicies

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
		return r.Restore(ctx, data)
	}

	return nil
}
