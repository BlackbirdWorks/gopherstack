package emrserverless

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// emrserverlessSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to backendSnapshot (or a value type held
// by one of the registered tables) would make an older snapshot unsafe to
// decode as the current shape. Restore compares this against the persisted
// value and discards (ResetAll, not a partial decode) any mismatch -- see
// Restore. The pre-Phase-3.3 snapshot format had no version field at all, so
// an old snapshot decodes with Version == 0, which is guaranteed to mismatch
// emrserverlessSnapshotVersion and is discarded the same way any other
// incompatible snapshot is.
const emrserverlessSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the EMR Serverless
// backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// b.registry.SnapshotAll() -- applications, jobRuns, and sessions are all
// "clean" tables keyed by their own real identity field (see
// store_setup.go's file doc), so no ephemeral DTO registry is needed.
// SessionTokens is left as a plain map (not store.Table-backed, see
// store_setup.go) and is carried alongside the tables. Version guards
// against decoding a snapshot from an incompatible (older or newer) build of
// this backend as though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables        map[string]json.RawMessage   `json:"tables"`
	SessionTokens map[string]map[string]string `json:"sessionTokens"`
	AccountID     string                       `json:"accountID"`
	Region        string                       `json:"region"`
	Version       int                          `json:"version"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "emrserverless: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:       emrserverlessSnapshotVersion,
		Tables:        tables,
		SessionTokens: b.sessionTokens,
		AccountID:     b.accountID,
		Region:        b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "emrserverless: Snapshot marshal failure", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot produced by Snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "emrserverless", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != emrserverlessSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"emrserverless: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", emrserverlessSnapshotVersion)

		b.registry.ResetAll()
		b.sessionTokens = make(map[string]map[string]string)

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("emrserverless: restore snapshot tables: %w", err)
	}

	b.ensureNonNilTags()

	if snap.SessionTokens == nil {
		snap.SessionTokens = make(map[string]map[string]string)
	}

	for appID, tokens := range snap.SessionTokens {
		if tokens == nil {
			snap.SessionTokens[appID] = make(map[string]string)
		}
	}

	b.sessionTokens = snap.SessionTokens
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// ensureNonNilTags guarantees every restored Application, JobRun, and
// Session has a non-nil Tags map. Tags is `json:",omitempty"` on all three
// types, so a resource created with no tags round-trips through
// Snapshot/Restore with a nil Tags field; TagResource/UntagResource mutate
// the stored map in place via findTagsByARN (maps.Copy/delete into a nil map
// panics), so this must run once after every successful RestoreAll.
func (b *InMemoryBackend) ensureNonNilTags() {
	b.applications.Range(func(app *Application) bool {
		if app.Tags == nil {
			app.Tags = make(map[string]string)
		}

		return true
	})

	b.jobRuns.Range(func(jr *JobRun) bool {
		if jr.Tags == nil {
			jr.Tags = make(map[string]string)
		}

		return true
	})

	b.sessions.Range(func(session *Session) bool {
		if session.Tags == nil {
			session.Tags = make(map[string]string)
		}

		return true
	})
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
