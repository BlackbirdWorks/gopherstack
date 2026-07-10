package accessanalyzer

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// accessanalyzerSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to a DTO type, a registered table's value
// type, or backendSnapshot itself would make an older snapshot unsafe to
// decode as the current shape. Restore compares this against the persisted
// value and discards (registry.ResetAll, not a partial decode) any mismatch
// -- see Restore below. The pre-Phase-3.3 snapshot format only covered
// analyzers/archiveRules/findings/tags (and was never actually reachable --
// see the Handler.Snapshot/Handler.Restore doc comment below), so there is no
// legacy shape to preserve compatibility with: any snapshot without a
// matching Version (including one with no version field, which decodes as 0)
// is discarded the same way any other incompatible snapshot is.
const accessanalyzerSnapshotVersion = 1

// archiveRuleSnapshot is a DTO used ONLY for Snapshot/Restore. It mirrors
// ArchiveRule field for field, except AnalyzerName -- tagged json:"-" on the
// live type because it is never part of the CreateArchiveRule/GetArchiveRule
// API response -- is given a real JSON tag here. Marshaling the live type
// directly would silently drop it, and unmarshaling into it would leave it
// permanently empty, corrupting the flat archiveRules table's composite key
// and archiveRulesByAnalyzer index on restore. This is the same DTO
// technique services/codecommit uses for PullRequestApprovalRule.PRID,
// applied here to archiveRules -- the one "dirty" table from
// store_setup.go's registerAllTables.
type archiveRuleSnapshot struct {
	Filter       map[string]FilterCriterion `json:"filter"`
	CreatedAt    time.Time                  `json:"createdAt"`
	UpdatedAt    time.Time                  `json:"updatedAt"`
	RuleName     string                     `json:"ruleName"`
	AnalyzerName string                     `json:"analyzerName"`
}

func archiveRuleSnapshotKeyFn(v *archiveRuleSnapshot) string {
	return archiveRuleKey(v.AnalyzerName, v.RuleName)
}

func toArchiveRuleSnapshot(r *ArchiveRule) *archiveRuleSnapshot {
	return &archiveRuleSnapshot{
		RuleName:     r.RuleName,
		AnalyzerName: r.AnalyzerName,
		Filter:       r.Filter,
		CreatedAt:    r.CreatedAt,
		UpdatedAt:    r.UpdatedAt,
	}
}

func fromArchiveRuleSnapshot(s *archiveRuleSnapshot) *ArchiveRule {
	return &ArchiveRule{
		RuleName:     s.RuleName,
		AnalyzerName: s.AnalyzerName,
		Filter:       s.Filter,
		CreatedAt:    s.CreatedAt,
		UpdatedAt:    s.UpdatedAt,
	}
}

// buildPersistenceDTORegistry constructs the ephemeral DTO registry used by
// both Snapshot and Restore for the one "dirty" table (archiveRules). It is
// built fresh on every call, mirroring services/codecommit's
// buildPersistenceDTORegistry.
func buildPersistenceDTORegistry() (*store.Registry, *store.Table[archiveRuleSnapshot]) {
	dtoReg := store.NewRegistry()
	archiveRuleDTOs := store.Register(dtoReg, "archiveRules", store.New(archiveRuleSnapshotKeyFn))

	return dtoReg, archiveRuleDTOs
}

// backendSnapshot is the top-level on-disk shape for the Access Analyzer
// backend.
//
// Tables holds one JSON-encoded array per registered table name: the five
// "clean" tables on b.registry (analyzers, findings, analyzedResources,
// policyGenerations, accessPreviews, findingRecommendations) plus the one DTO
// table built above for the "dirty" one (archiveRules) -- see
// store_setup.go's registerAllTables doc for the clean/dirty split.
// policyGenerations, accessPreviews, and findingRecommendations were not part
// of the pre-Phase-3.3 snapshot at all; registering them here is a
// persistence-coverage expansion (see this file's Version doc comment), not
// a behavior change to any operation.
//
// Tags is the plain map left unconverted by Phase 3.3 (see the field comment
// on InMemoryBackend in backend.go): its value (map[string]string) has no
// identity of its own, so it is persisted here directly, matching its
// pre-refactor shape exactly.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage   `json:"tables"`
	Tags      map[string]map[string]string `json:"tags"`
	AccountID string                       `json:"accountID"`
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
		logger.Load(ctx).WarnContext(ctx, "accessanalyzer: snapshot table marshal failed", "error", err)

		return nil
	}

	dtoReg, archiveRuleDTOs := buildPersistenceDTORegistry()

	for _, r := range b.archiveRules.Snapshot() {
		archiveRuleDTOs.Put(toArchiveRuleSnapshot(r))
	}

	dtoTables, err := dtoReg.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "accessanalyzer: snapshot DTO table marshal failed", "error", err)

		return nil
	}

	maps.Copy(tables, dtoTables)

	snap := backendSnapshot{
		Version:   accessanalyzerSnapshotVersion,
		Tables:    tables,
		Tags:      b.tags,
		AccountID: b.accountID,
		Region:    b.region,
	}

	return persistence.MarshalSnapshot(ctx, "accessanalyzer", snap)
}

// Restore deserializes state from JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "accessanalyzer", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != accessanalyzerSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"accessanalyzer: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", accessanalyzerSnapshotVersion)

		b.registry.ResetAll()
		b.archiveRules.Reset()
		b.tags = make(map[string]map[string]string)

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("accessanalyzer: restore snapshot tables: %w", err)
	}

	if err := b.restoreArchiveRules(snap.Tables); err != nil {
		return err
	}

	b.tags = snap.Tags
	if b.tags == nil {
		b.tags = make(map[string]map[string]string)
	}

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// restoreArchiveRules rebuilds the archiveRules "dirty" table (see
// store_setup.go's registerAllTables doc) from tables via the ephemeral DTO
// registry, factored out of Restore to keep Restore's own cognitive
// complexity low. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restoreArchiveRules(tables map[string]json.RawMessage) error {
	dtoReg, archiveRuleDTOs := buildPersistenceDTORegistry()
	if err := dtoReg.RestoreAll(tables); err != nil {
		return fmt.Errorf("accessanalyzer: restore snapshot DTO tables: %w", err)
	}

	b.archiveRules.Reset()
	for _, r := range archiveRuleDTOs.Snapshot() {
		b.archiveRules.Put(fromArchiveRuleSnapshot(r))
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
//
// Before this Phase 3.3 conversion, InMemoryBackend already implemented
// Snapshot/Restore, but Handler never delegated to them -- setupPersistence
// in cli.go only registers a service.Registerable with the
// persistence.Manager if the Handler itself (the value actually returned by
// Provider.Init and passed to setupPersistence) satisfies the Snapshot/
// Restore duck-typed interface, so Access Analyzer state was silently never
// persisted at all. Adding this method (and Restore below) is the
// dead-wiring fix that makes the backend's (now registry-backed) persistence
// reachable.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
// See Snapshot's doc comment for why this delegation was previously missing.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
