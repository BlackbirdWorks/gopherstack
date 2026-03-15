package dynamodb

import (
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

type dbSnapshot struct {
	Tables        map[string]map[string]*Table `json:"Tables"`
	Backups       map[string]*Backup           `json:"Backups,omitempty"`
	DefaultRegion string                       `json:"DefaultRegion"`
	AccountID     string                       `json:"AccountID"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
// Note: per-table stream sequence counters (streamSeq) are not serialised; they
// restart from 0 after restore. Existing StreamRecords retain their stored
// sequence numbers, so no in-flight duplicates occur for already-stored records.
func (db *InMemoryDB) Snapshot() []byte {
	db.mu.RLock("Snapshot")
	defer db.mu.RUnlock()

	snap := dbSnapshot{
		Tables:        db.Tables,
		Backups:       db.Backups,
		DefaultRegion: db.defaultRegion,
		AccountID:     db.accountID,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (db *InMemoryDB) Restore(data []byte) error {
	var snap dbSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	db.mu.Lock("Restore")
	defer db.mu.Unlock()

	if snap.Tables == nil {
		snap.Tables = make(map[string]map[string]*Table)
	}

	if snap.Backups == nil {
		snap.Backups = make(map[string]*Backup)
	}

	// Reinitialise per-table mutexes and rebuild indexes.
	for _, regionTables := range snap.Tables {
		for _, t := range regionTables {
			if t.mu == nil {
				t.mu = lockmetrics.New("ddb-table")
			}

			t.rebuildIndexes()
		}
	}

	db.Tables = snap.Tables
	db.Backups = snap.Backups
	db.defaultRegion = snap.DefaultRegion
	db.accountID = snap.AccountID

	// Rebuild the stream ARN reverse index from the restored tables.
	db.streamARNIndex = make(map[string]*Table)

	for _, regionTables := range db.Tables {
		for _, t := range regionTables {
			if t.StreamARN != "" {
				db.streamARNIndex[t.StreamARN] = t
			}
		}
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *DynamoDBHandler) Snapshot() []byte {
	type snapshotter interface{ Snapshot() []byte }
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *DynamoDBHandler) Restore(data []byte) error {
	type restorer interface{ Restore([]byte) error }
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(data)
	}

	return nil
}
