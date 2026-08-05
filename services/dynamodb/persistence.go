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

// dynamodbSnapshotVersion identifies the shape of dbSnapshot. It must be
// bumped whenever a change to Table, Backup, StoredGlobalTable, or
// dbSnapshot itself would make an older snapshot unsafe to decode as the
// current shape. Restore compares this against the persisted value and
// discards (rather than attempts to partially decode) any mismatch -- see
// Restore below. This mirrors the services/sqs pilot (commit 0f09d77c) and
// the services/ec2 conversion (commit 12e611a4).
//
// Only tables/backups/globalTables are persisted here, matching the
// pre-refactor behavior: deletingTables, exports, imports, txnTokens,
// txnPending, and fisReplicationPaused were never part of the snapshot
// either (deletingTables is a transient staging area drained by the
// janitor; the rest are short-lived caches/metadata not worth persisting).
//
// Do NOT bump this reflexively whenever a field is added to Table, Backup, or
// StoredGlobalTable. encoding/json is decode-compatible with new fields: an
// older snapshot missing a newly-added field decodes fine, leaving it at its
// zero value. Bumping the version on every such change makes Restore hit the
// mismatch branch above and discard (registry.ResetAll()) every table,
// backup, and global table a user already had persisted -- i.e. it destroys
// their data on the very upgrade meant to add a harmless new field. Only bump
// this when a change is genuinely decode-incompatible: a field's type
// changes, a field is removed and its old meaning can't be safely defaulted,
// or existing field semantics change in a way that misinterprets old data.
// (Table.PITRSnapshots, added to fix PITR snapshots not surviving a restart,
// is the case that prompted this comment: exporting the field was pure
// decode-compatible addition and did not warrant a bump.)
const dynamodbSnapshotVersion = 1

type dbSnapshot struct {
	DefaultRegion string               `json:"defaultRegion"`
	AccountID     string               `json:"accountID"`
	Tables        []*Table             `json:"tables"`
	Backups       []*Backup            `json:"backups,omitempty"`
	GlobalTables  []*StoredGlobalTable `json:"globalTables,omitempty"`
	Version       int                  `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
// Per-table stream sequence counters (streamSeq) are unexported and therefore
// not serialised directly; they are reconstructed during Restore from the
// highest SequenceNumber found in each table's StreamRecords ring buffer.
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
		// Log the marshal failure so operators can detect data-loss scenarios.
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

	// Reinitialise per-table mutexes and rebuild indexes before taking db.mu,
	// matching the pre-refactor ordering (this touches only the freshly
	// unmarshaled Table values, not any backend state).
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
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption. Mirrors the services/sqs pilot (commit 0f09d77c).
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

	// Rebuild the stream ARN reverse index from the restored tables.
	db.streamARNIndex.Reset()

	for _, t := range db.tables.All() {
		if t.StreamARN != "" {
			db.streamARNIndex.Put(t)
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
