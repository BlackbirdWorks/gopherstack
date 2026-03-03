package kinesis

import (
	"encoding/json"
)

type backendSnapshot struct {
	Streams   map[string]*Stream `json:"streams"`
	AccountID string             `json:"accountID"`
	Region    string             `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
// Note: shard sequence number counters (nextSeq) are not serialised; they
// restart from 0 after restore. Existing records retain their stored sequence
// numbers, so no in-flight duplicates occur for already-stored records.
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

	if snap.Streams == nil {
		snap.Streams = make(map[string]*Stream)
	}

	b.streams = snap.Streams
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}
