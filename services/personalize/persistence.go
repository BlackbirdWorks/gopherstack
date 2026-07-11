package personalize

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// personalizeSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to a registered table's value type or to
// backendSnapshot itself would make an older snapshot unsafe to decode as the
// current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll, not a partial decode) any mismatch -- see
// Restore below. This is the first version: Personalize had no persistence at
// all before Phase 3.3, so there is no legacy snapshot shape to be compatible
// with -- any snapshot without a matching Version (including one with no
// version field, which decodes as 0) is discarded the same way any other
// incompatible snapshot is.
const personalizeSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Personalize backend.
//
// Tables holds one JSON-encoded array per table registered on b.registry in
// store_setup.go's registerAllTables -- all 16 of them (datasetGroups,
// datasets, schemas, solutions, solutionVersions, campaigns,
// datasetImportJobs, datasetExportJobs, batchInferenceJobs,
// batchSegmentJobs, eventTrackers, filters, recommenders,
// metricAttributions, dataDeletionJobs, featureTransformations). Every value
// type already carries its own real (non-json:"-") identity field, so none
// of them need the "dirty" DTO-registry treatment services/codecommit and
// services/ses use for hidden identity fields.
//
// Tags (map[string]map[string]string) is the one resource-holding field left
// as a plain map by store_setup.go (its value type is map[string]string, not
// *T, so it does not fit store.Table's keyed-by-identity-value shape) and is
// persisted here directly instead.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage   `json:"tables"`
	Tags      map[string]map[string]string `json:"tags"`
	AccountID string                       `json:"accountId"`
	Region    string                       `json:"region"`
	Version   int                          `json:"version"`
}

// Snapshot serializes current state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "personalize: snapshot table marshal failed", "error", err)

		return nil
	}

	s := backendSnapshot{
		Version:   personalizeSnapshotVersion,
		Tables:    tables,
		Tags:      b.tags,
		AccountID: b.accountID,
		Region:    b.region,
	}

	return persistence.MarshalSnapshot(ctx, "personalize", s)
}

// Restore deserializes state from JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var s backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "personalize", data, &s); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if s.Version != personalizeSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"personalize: discarding incompatible snapshot version, starting empty",
			"gotVersion", s.Version, "wantVersion", personalizeSnapshotVersion)

		b.registry.ResetAll()
		b.tags = make(map[string]map[string]string)
		// featureTransformations are read-only builtins re-seeded by Reset();
		// registry.ResetAll() cleared the table along with every other one,
		// so it must be re-seeded here too.
		b.seedBuiltinFeatureTransformations()

		return nil
	}

	if err := b.registry.RestoreAll(s.Tables); err != nil {
		return fmt.Errorf("personalize: restore snapshot tables: %w", err)
	}

	b.tags = s.Tags
	if b.tags == nil {
		b.tags = make(map[string]map[string]string)
	}

	b.accountID = s.AccountID
	b.region = s.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
// Before this, Handler had no Snapshot/Restore of its own and InMemoryBackend
// had no persistence at all -- Personalize was entirely excluded from
// Gopherstack's persistence.Manager (it type-asserts each service's
// Registerable to persistence.Persistable). This is the Phase 3.3
// dead-wiring fix: Personalize now round-trips full state like every other
// converted service.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
