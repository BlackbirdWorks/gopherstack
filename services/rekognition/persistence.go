package rekognition

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// rekognitionSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to a registered table's value type, or to
// backendSnapshot itself, would make an older snapshot unsafe to decode as
// the current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll, not a partial decode) any mismatch -- see
// Restore below. This is the first version: pre-Phase-3.3 snapshots covered
// only collections/faces/streamProcessors/tags (see git history), so any
// older snapshot -- and any snapshot with no version field at all, which
// decodes as 0 -- is discarded the same way any other incompatible snapshot
// is, rather than partially/incorrectly restored.
const rekognitionSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Rekognition backend.
//
// Tables holds one JSON-encoded array per table registered on b.registry
// (collections, faces, streamProcessors, projects, projectVersions,
// projectPolicies, datasets, users, livenessSessions, asyncJobs,
// mediaAnalysisJobs) -- see store_setup.go's registerAllTables. Tags and
// DatasetEntries are the two plain (non-store.Table) maps left by Phase 3.3
// (see backend.go's field comments for why): both persisted directly here,
// exactly as tags already was pre-refactor. Version guards against decoding
// a snapshot from an incompatible (older/newer/absent) build of this backend
// as though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables         map[string]json.RawMessage   `json:"tables"`
	Tags           map[string]map[string]string `json:"tags"`
	DatasetEntries map[string][]string          `json:"datasetEntries"`
	AccountID      string                       `json:"accountId"`
	Region         string                       `json:"region"`
	Version        int                          `json:"version"`
}

// Snapshot serializes the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		// The registered tables are plain JSON-friendly structs, so a
		// marshal failure here would indicate a programming error rather
		// than bad input data. Log and skip the snapshot rather than panic,
		// matching the persistence.Persistable contract (nil is skipped by
		// the Manager).
		logger.Load(ctx).WarnContext(ctx, "rekognition: snapshot table marshal failed", "error", err)

		return nil
	}

	tagsCopy := make(map[string]map[string]string, len(b.tags))
	for arn, t := range b.tags {
		tc := make(map[string]string, len(t))
		maps.Copy(tc, t)
		tagsCopy[arn] = tc
	}

	entriesCopy := make(map[string][]string, len(b.datasetEntries))
	for k, v := range b.datasetEntries {
		entriesCopy[k] = slices.Clone(v)
	}

	snap := backendSnapshot{
		Version:        rekognitionSnapshotVersion,
		Tables:         tables,
		Tags:           tagsCopy,
		DatasetEntries: entriesCopy,
		AccountID:      b.accountID,
		Region:         b.region,
	}

	return persistence.MarshalSnapshot(ctx, "rekognition", snap)
}

// Restore deserializes backend state from JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "rekognition", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != rekognitionSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never
		// be partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"rekognition: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", rekognitionSnapshotVersion)

		b.registry.ResetAll()
		b.tags = make(map[string]map[string]string)
		b.datasetEntries = make(map[string][]string)
		b.accountID = snap.AccountID
		b.region = snap.Region

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("rekognition: restore snapshot tables: %w", err)
	}

	b.tags = snap.Tags
	if b.tags == nil {
		b.tags = make(map[string]map[string]string)
	}

	b.datasetEntries = snap.DatasetEntries
	if b.datasetEntries == nil {
		b.datasetEntries = make(map[string][]string)
	}

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
//
// This delegation is itself the Phase 3.3 dead-wiring fix: before this
// change Handler had no Snapshot/Restore of its own, even though
// InMemoryBackend (via StorageBackend) always implemented both. cli.go's
// generic setupPersistence type-asserts the registered service.Registerable
// (the Handler returned by Provider.Init, not the backend) against a
// Snapshot/Restore-shaped interface, so without these two methods
// Rekognition was silently never persisted at all, matching the
// codecommit/codepipeline/emr pattern.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
// See Handler.Snapshot for why this delegation exists.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
