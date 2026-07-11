package forecast

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// forecastSnapshotVersion identifies the shape of [backendSnapshot]. It must
// be bumped whenever a change to backendSnapshot (or a value type held by
// one of the registered tables) would make an older snapshot unsafe to
// decode as the current shape. Restore compares this against the persisted
// value and discards (registry.ResetAll plus resetting the raw maps, not a
// partial decode) any mismatch -- see Restore. Forecast had no persistence
// at all before Phase 3.3 -- Handler had no Snapshot/Restore, and neither
// did InMemoryBackend, so cli.go's generic setupPersistence (which
// type-asserts the registered service.Registerable, i.e. the Handler, for a
// Snapshot/Restore pair) never picked Forecast up at all: dead wiring, with
// no persistence underneath it either (see the Handler.Snapshot/Restore
// delegation at the bottom of this file). So there is no legacy snapshot
// shape to be compatible with -- any snapshot without a matching Version
// (including one with no version field, which decodes as 0) is discarded
// the same way any other incompatible snapshot is.
const forecastSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Forecast backend.
//
// Tables holds one JSON-encoded array per registered per-kind resources
// table (see store_setup.go), produced directly by b.registry.SnapshotAll():
// every Resource is keyed by its own Name field, so no DTO wrapper is
// needed. Evaluations and Tags are the two remaining raw (non-store.Table)
// maps -- their values are not *T -- and are persisted directly here.
//
// ArnIndex is deliberately NOT part of the snapshot: it is a pure
// cross-table reverse index fully derivable from each Resource's own
// ARN/Kind fields (see rebuildARNIndex), so Restore rebuilds it from the
// restored tables instead of trusting a persisted copy that could otherwise
// drift out of sync with the tables.
type backendSnapshot struct {
	Tables      map[string]json.RawMessage     `json:"tables"`
	Evaluations map[string][]MonitorEvaluation `json:"evaluations"`
	Tags        map[string]map[string]string   `json:"tags"`
	AccountID   string                         `json:"accountID"`
	Region      string                         `json:"region"`
	Version     int                            `json:"version"`
}

func ensureNonNilMaps(s *backendSnapshot) {
	if s.Evaluations == nil {
		s.Evaluations = make(map[string][]MonitorEvaluation)
	}

	if s.Tags == nil {
		s.Tags = make(map[string]map[string]string)
	}
}

// rebuildARNIndex rebuilds the ARN -> {kind, name} reverse index from the
// current contents of every per-kind resources table. It is the inverse of
// what create populates incrementally, used after Restore replaces the
// tables wholesale.
func (b *InMemoryBackend) rebuildARNIndex() {
	arnIndex := make(map[string]arnEntry)

	for kind, table := range b.resources {
		table.Range(func(r *Resource) bool {
			arnIndex[r.ARN] = arnEntry{kind: kind, name: r.Name}

			return true
		})
	}

	b.arnIndex = arnIndex
}

// Snapshot serializes the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "forecast: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:     forecastSnapshotVersion,
		Tables:      tables,
		Evaluations: b.evaluations,
		Tags:        b.tags,
		AccountID:   b.accountID,
		Region:      b.region,
	}

	return persistence.MarshalSnapshot(ctx, "forecast", snap)
}

// Restore deserializes backend state from a snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "forecast", data, &snap); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if snap.Version != forecastSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"forecast: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", forecastSnapshotVersion)

		b.registry.ResetAll()
		b.evaluations = make(map[string][]MonitorEvaluation)
		b.tags = make(map[string]map[string]string)
		b.arnIndex = make(map[string]arnEntry)
		b.accountID = snap.AccountID
		b.region = snap.Region

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("forecast: restore snapshot tables: %w", err)
	}

	ensureNonNilMaps(&snap)

	b.evaluations = snap.Evaluations
	b.tags = snap.Tags
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.rebuildARNIndex()

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
// Handler previously had no Snapshot/Restore of its own -- and neither did
// InMemoryBackend -- so cli.go's generic setupPersistence never picked
// Forecast up at all: dead wiring, with no persistence underneath it either.
// This delegation (matching the cleanrooms/codecommit/codepipeline/emr/
// workmail pattern) is what wires Forecast into persistence for the first
// time.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
