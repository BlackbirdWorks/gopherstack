package dynamodb

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// dynamodbSnapshotVersion identifies dbSnapshot's shape. Bump it ONLY when a
// field change would make an older snapshot unsafe to decode as-is: Restore
// discards (never partially decodes) any version mismatch, so a reflexive
// bump silently throws away every user's persisted tables.
//
// Only tables/backups/globalTables are persisted; deletingTables is a
// transient staging area and the rest are short-lived caches not worth
// persisting.
const dynamodbSnapshotVersion = 1

type dbSnapshot struct {
	DefaultRegion string               `json:"defaultRegion"`
	AccountID     string               `json:"accountID"`
	Tables        []*Table             `json:"tables"`
	Backups       []*Backup            `json:"backups,omitempty"`
	GlobalTables  []*StoredGlobalTable `json:"globalTables,omitempty"`
	Version       int                  `json:"version"`
}

// Snapshot serialises the backend state to JSON; implements persistence.Persistable.
// streamSeq is unexported and not serialised -- Restore reconstructs it from
// the highest SequenceNumber in each table's StreamRecords ring buffer.
func (db *InMemoryDB) Snapshot(ctx context.Context) []byte {
	db.mu.RLock("Snapshot")
	defer db.mu.RUnlock()

	snap := dbSnapshot{
		Version:       dynamodbSnapshotVersion,
		Tables:        db.tables.Snapshot(),
		Backups:       db.backups.Snapshot(),
		GlobalTables:  db.globalTables.Snapshot(),
		DefaultRegion: db.defaultRegion,
		AccountID:     db.accountID,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx,
			"DynamoDB: failed to serialise snapshot; state will not be persisted",
			slog.String("error", err.Error()),
		)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (db *InMemoryDB) Restore(ctx context.Context, data []byte) error {
	var snap dbSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "dynamodb", data, &snap); err != nil {
		return err
	}

	// Reinitialise per-table mutexes and rebuild indexes before taking db.mu --
	// this only touches the freshly unmarshaled Table values, not backend state.
	for _, t := range snap.Tables {
		if t.mu == nil {
			t.mu = lockmetrics.New("ddb-table")
		}

		t.rebuildIndexes()
		restoreStreamSeq(t)
	}

	db.mu.Lock("Restore")
	defer db.mu.Unlock()

	if snap.Version != dynamodbSnapshotVersion {
		// Never partially decode a version mismatch -- discard and start empty
		// instead of erroring; this is an expected upgrade condition, not corruption.
		logger.Load(ctx).WarnContext(ctx,
			"DynamoDB: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", dynamodbSnapshotVersion,
		)

		db.registry.ResetAll()

		return nil
	}

	db.tables.Restore(snap.Tables)
	db.backups.Restore(snap.Backups)
	db.globalTables.Restore(snap.GlobalTables)
	db.defaultRegion = snap.DefaultRegion
	db.accountID = snap.AccountID

	db.streamARNIndex.Reset()

	for _, t := range db.tables.All() {
		if t.StreamARN != "" {
			db.streamARNIndex.Put(t)
		}
	}

	return nil
}

// restoreStreamSeq sets t.streamSeq to the max sequence number in the table's
// persisted StreamRecords, so post-restore appends stay monotonically
// increasing (sequence numbers are zero-padded decimal strings, see appendStreamRecord).
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
func (h *DynamoDBHandler) Snapshot(ctx context.Context) []byte {
	type snapshotter interface {
		Snapshot(ctx context.Context) []byte
	}
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *DynamoDBHandler) Restore(ctx context.Context, data []byte) error {
	type restorer interface {
		Restore(context.Context, []byte) error
	}
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(ctx, data)
	}

	return nil
}
