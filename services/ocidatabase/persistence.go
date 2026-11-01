package ocidatabase

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the stored types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type backendSnapshot struct {
	Databases map[string]*ADB         `json:"databases"`
	Backups   map[string]*Backup      `json:"backups"`
	WorkReqs  map[string]*WorkRequest `json:"workRequests"`
	Tokens    map[string]string       `json:"retryTokens"`
	Auth      []ociauth.KeyRecord     `json:"authKeys"`
	Version   int                     `json:"version"`
	Seq       int64                   `json:"seq"`
}

// Snapshot serialises the backend state, including registered API keys. The ADMIN password, the TLS material and
// the running databases are not part of it.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	keys := b.Auth.Records()

	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "ocidatabase", backendSnapshot{
		Version: snapshotVersion, Databases: b.dbs, Backups: b.backups, WorkReqs: b.workReqs, Tokens: b.tokens,
		Auth: keys, Seq: b.seq,
	})
}

// Restore loads backend state. Engine databases come back STOPPED: their containers do not survive a restart.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "ocidatabase", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "ocidatabase: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	if err := checkSnapshot(&snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	b.dbs, b.backups, b.workReqs, b.tokens, b.seq = snap.Databases, snap.Backups, snap.WorkReqs, snap.Tokens, snap.Seq

	for _, db := range b.dbs {
		b.settleRestored(db)
	}

	for _, wr := range b.workReqs {
		if wr.Status == wrInProgress && wr.FinishAt == nil {
			b.failWR(wr, "Interrupted", "restored from a snapshot")
		}
	}
	b.mu.Unlock()
	b.Auth.Load(snap.Auth)

	return nil
}

// settleRestored resolves engine-driven states that cannot continue after a restart. Callers hold the write lock.
func (b *InMemoryBackend) settleRestored(db *ADB) {
	if !b.engineOn(db) || db.TransitionAt != nil {
		return
	}

	const details = "Restored from a snapshot; start the database to run it again"

	switch db.State {
	case stateAvailable, stateUpdating, stateScaling, stateStarting, stateRestarting, stateStopping, stateRestoring,
		stateBackingUp:
		db.State, db.Details = stateStopped, details
	case statePending:
		db.State, db.Details = stateUnavailable, "Restored from a snapshot before provisioning finished"
	case stateTerminating:
		db.State = stateTerminated
	}

	db.Target = ""
}

func checkSnapshot(snap *backendSnapshot) error {
	if snap.Databases == nil {
		snap.Databases = map[string]*ADB{}
	}

	if snap.Backups == nil {
		snap.Backups = map[string]*Backup{}
	}

	if snap.WorkReqs == nil {
		snap.WorkReqs = map[string]*WorkRequest{}
	}

	if snap.Tokens == nil {
		snap.Tokens = map[string]string{}
	}

	for id, db := range snap.Databases {
		if db == nil {
			return fmt.Errorf("%w: database %q", ErrSnapshotNull, id)
		}
	}

	for id, bk := range snap.Backups {
		if bk == nil {
			return fmt.Errorf("%w: backup %q", ErrSnapshotNull, id)
		}
	}

	for id, wr := range snap.WorkReqs {
		if wr == nil {
			return fmt.Errorf("%w: work request %q", ErrSnapshotNull, id)
		}
	}

	return nil
}
