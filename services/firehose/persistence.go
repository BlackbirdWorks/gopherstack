package firehose

import (
	"encoding/json"
	"log/slog"
	"time"
)

type backendSnapshot struct {
	Streams   map[string]map[string]*DeliveryStream `json:"streams"`
	AccountID string                                `json:"accountID"`
	Region    string                                `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Streams:   b.streams,
		AccountID: b.accountID,
		Region:    b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		// Snapshot has no context parameter; fall back to the default logger.
		slog.Default().Warn("firehose: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	// Close Tags on any streams that are being replaced to prevent
	// Prometheus registry leaks.
	for _, streams := range b.streams {
		for _, s := range streams {
			if s.Tags != nil {
				s.Tags.Close()
			}
		}
	}

	if snap.Streams == nil {
		snap.Streams = make(map[string]map[string]*DeliveryStream)
	}

	now := time.Now()
	for _, streams := range snap.Streams {
		for _, s := range streams {
			s.lastFlush = now

			// Recalculate bufferSizeBytes because it is not persisted (unexported field).
			// Without this, size-based flush thresholds would never fire after a restore.
			s.bufferSizeBytes = 0
			for _, rec := range s.Records {
				s.bufferSizeBytes += len(rec)
			}
		}
	}

	b.streams = snap.Streams
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}
