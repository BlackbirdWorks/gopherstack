package transcribe

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// transcribeSnapshotVersion identifies the shape of [backendSnapshot]. It must
// be bumped whenever a change to backendSnapshot (or a value type held by one
// of the registered tables) would make an older snapshot unsafe to decode as
// the current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll, not a partial decode) any mismatch -- see
// Restore. The pre-Phase-3.3 snapshot format had no version field at all, so
// an old snapshot decodes with Version == 0, which is guaranteed to mismatch
// transcribeSnapshotVersion and is discarded the same way any other
// incompatible snapshot is.
const transcribeSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Transcribe backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// b.registry.SnapshotAll() (jobs, callAnalyticsCategories, languageModels,
// medicalVocabularies, vocabularies, vocabularyFilters, callAnalyticsJobs,
// medicalScribeJobs, medicalTranscriptionJobs -- see store_setup.go). Every
// registered table is a "clean" table keyed directly off a real
// (non-json:"-") identity field, so no ephemeral DTO registry is needed here.
// ResourceTags (map[string]map[string]string, keyed by ARN rather than *T) is
// left as a plain field: its values are plain string maps, not *T, so it does
// not fit store.Table's keyed-collection shape. Version guards against
// decoding a snapshot from an incompatible (older or newer) build of this
// backend as though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables       map[string]json.RawMessage   `json:"tables"`
	ResourceTags map[string]map[string]string `json:"resourceTags"`
	Version      int                          `json:"version"`
}

func ensureNonNilMaps(s *backendSnapshot) {
	if s.ResourceTags == nil {
		s.ResourceTags = make(map[string]map[string]string)
	}
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")

	tables, tblErr := b.registry.SnapshotAll()

	snap := backendSnapshot{
		Version:      transcribeSnapshotVersion,
		Tables:       tables,
		ResourceTags: b.resourceTags,
	}

	b.mu.RUnlock()

	if tblErr != nil {
		logger.Load(ctx).WarnContext(ctx, "transcribe: snapshot table marshal failed", "error", tblErr)

		return nil
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "transcribe: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "transcribe", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != transcribeSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"transcribe: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", transcribeSnapshotVersion)

		b.registry.ResetAll()
		b.resourceTags = make(map[string]map[string]string)

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("transcribe: restore snapshot tables: %w", err)
	}

	ensureNonNilMaps(&snap)

	b.resourceTags = snap.ResourceTags

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
