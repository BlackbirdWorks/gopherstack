package dynamodb

import (
	"encoding/json"
	"log/slog"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

type dbSnapshot struct {
	Tables        map[string]map[string]*Table  `json:"Tables"`
	Backups       map[string]*Backup            `json:"Backups,omitempty"`
	GlobalTables  map[string]*StoredGlobalTable `json:"GlobalTables,omitempty"`
	DefaultRegion string                        `json:"DefaultRegion"`
	AccountID     string                        `json:"AccountID"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
// Per-table stream sequence counters (streamSeq) are unexported and therefore
// not serialised directly; they are reconstructed during Restore from the
// highest SequenceNumber found in each table's StreamRecords ring buffer.
func (db *InMemoryDB) Snapshot() []byte {
	db.mu.RLock("Snapshot")
	defer db.mu.RUnlock()

	snap := dbSnapshot{
		Tables:        db.Tables,
		Backups:       db.Backups,
		GlobalTables:  db.GlobalTables,
		DefaultRegion: db.defaultRegion,
		AccountID:     db.accountID,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		// Log the marshal failure so operators can detect data-loss scenarios.
		slog.Default().Warn("DynamoDB: failed to serialise snapshot; state will not be persisted",
			slog.String("error", err.Error()),
		)

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

	if snap.GlobalTables == nil {
		snap.GlobalTables = make(map[string]*StoredGlobalTable)
	}

	// Reinitialise per-table mutexes and rebuild indexes.
	for _, regionTables := range snap.Tables {
		for _, t := range regionTables {
			if t.mu == nil {
				t.mu = lockmetrics.New("ddb-table")
			}

			t.rebuildIndexes()
			restoreStreamSeq(t)
		}
	}

	db.Tables = snap.Tables
	db.Backups = snap.Backups
	db.GlobalTables = snap.GlobalTables
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

// restoreStreamSeq sets t.streamSeq to the maximum sequence number found in the
// table's persisted StreamRecords. This ensures that newly-appended stream
// records receive monotonically increasing sequence numbers after a restore.
// Sequence numbers are stored as zero-padded decimal strings (see appendStreamRecord).
func restoreStreamSeq(t *Table) {
	var maxSeq int64
	for i := range t.StreamRecords {
		if t.StreamRecords[i].SequenceNumber == "" {
			continue
		}
		n, err := strconv.ParseInt(t.StreamRecords[i].SequenceNumber, 10, 64)
		if err == nil && n > maxSeq {
			maxSeq = n
		}
	}
	t.streamSeq = maxSeq
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
