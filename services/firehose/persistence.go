package firehose

import (
	"encoding/json"
	"time"
)

type backendSnapshot struct {
	Streams   map[string]*DeliveryStream `json:"streams"`
	AccountID string                     `json:"accountID"`
	Region    string                     `json:"region"`
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
	for _, s := range b.streams {
		if s.Tags != nil {
			s.Tags.Close()
		}
	}

	if snap.Streams == nil {
		snap.Streams = make(map[string]*DeliveryStream)
	}

	now := time.Now()
	for _, s := range snap.Streams {
		s.lastFlush = now

		// Recalculate bufferSizeBytes because it is not persisted (unexported field).
		// Without this, size-based flush thresholds would never fire after a restore.
		s.bufferSizeBytes = 0
		for _, rec := range s.Records {
			s.bufferSizeBytes += len(rec)
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
