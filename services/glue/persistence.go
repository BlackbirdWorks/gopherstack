package glue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// glueSnapshotVersion identifies the shape of backendSnapshot's Tables blob
// (i.e. the set/shape of resources registered on b.registry -- see
// registerAllTables in store_setup.go). It must be bumped whenever a change
// there would make an older snapshot unsafe to decode as the current shape.
// Restore compares this against the persisted value and discards (rather than
// attempts to partially decode) any mismatch -- see Restore below. This
// mirrors the services/ec2 (commit 12e611a4) and services/sqs (commit
// 0f09d77c) conversions.
const glueSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Glue backend.
//
// Tables holds one JSON-encoded array per registered store.Table, produced by
// store.Registry.SnapshotAll (see registerAllTables in store_setup.go). Every
// remaining field is a resource collection that was NOT converted to
// store.Table -- see the comment above registerAllTables for why each one's
// key is not a pure function of its value (or, for the *AtRest/*Config
// singletons and JobRun*At timer maps, is not a map[string]*T at all) -- and
// so is still persisted directly, exactly as before this refactor.
type backendSnapshot struct {
	Tables                    map[string]json.RawMessage                `json:"tables"`
	PartitionIndexes          map[string]*PartitionIndex                `json:"partitionIndexes"`
	TableColumnStats          map[string]*ColumnStatistics              `json:"tableColumnStats"`
	PartitionColumnStats      map[string]*ColumnStatistics              `json:"partitionColumnStats"`
	ResourcePolicies          map[string]*resourcePolicyEntry           `json:"resourcePolicies"`
	CatalogEncryptionSettings map[string]*DataCatalogEncryptionSettings `json:"catalogEncryptionSettings"`
	CatalogImports            map[string]*CatalogImportStatus           `json:"catalogImports"`
	JobRuns                   map[string][]*JobRun                      `json:"jobRuns"`
	WorkflowRuns              map[string][]*WorkflowRun                 `json:"workflowRuns"`
	SchemaVersions            map[string][]*SchemaVersion               `json:"schemaVersions"`
	SessionStatements         map[string][]*Statement                   `json:"sessionStatements"`
	CrawlHistory              map[string][]*CrawlHistoryEntry           `json:"crawlHistory"`
	SchemaVersionMetadata     map[string]map[string]string              `json:"schemaVersionMetadata"`
	IterableFormItems         iterableFormItemsMap                      `json:"iterableFormItems"`
	GlueIdentityCenterConfig  *IdentityCenterConfig                     `json:"glueIdentityCenterConfig,omitempty"`
	Version                   int                                       `json:"version"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		// The registered tables are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the Manager).
		logger.Load(ctx).WarnContext(ctx, "glue: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:                   glueSnapshotVersion,
		Tables:                    tables,
		PartitionIndexes:          b.partitionIndexes,
		TableColumnStats:          b.tableColumnStats,
		PartitionColumnStats:      b.partitionColumnStats,
		ResourcePolicies:          b.resourcePolicies,
		CatalogEncryptionSettings: b.catalogEncryptionSettings,
		CatalogImports:            b.catalogImports,
		JobRuns:                   b.jobRuns,
		WorkflowRuns:              b.workflowRuns,
		SchemaVersions:            b.schemaVersions,
		SessionStatements:         b.sessionStatements,
		CrawlHistory:              b.crawlHistory,
		SchemaVersionMetadata:     b.schemaVersionMetadata,
		GlueIdentityCenterConfig:  b.glueIdentityCenterConfig,
		IterableFormItems:         b.iterableFormItems,
	}

	return persistence.MarshalSnapshot(ctx, "glue", snap)
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "glue", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != glueSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption. Mirrors the services/ec2 and services/sqs conversions.
		logger.Load(ctx).WarnContext(ctx,
			"glue: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", glueSnapshotVersion)

		b.registry.ResetAll()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("glue: restore snapshot tables: %w", err)
	}

	initSnapshotDefaults(&snap)
	b.restoreFromSnapshot(snap)

	return nil
}

// initSnapshotDefaults ensures every raw (non-store.Table) snapshot map is
// non-nil. The registered store.Table fields need no such treatment --
// store.Table.Restore already leaves an empty (never nil) table when its
// input is nil or absent.
func initSnapshotDefaults(snap *backendSnapshot) {
	if snap.PartitionIndexes == nil {
		snap.PartitionIndexes = make(map[string]*PartitionIndex)
	}
	if snap.TableColumnStats == nil {
		snap.TableColumnStats = make(map[string]*ColumnStatistics)
	}
	if snap.PartitionColumnStats == nil {
		snap.PartitionColumnStats = make(map[string]*ColumnStatistics)
	}
	if snap.ResourcePolicies == nil {
		snap.ResourcePolicies = make(map[string]*resourcePolicyEntry)
	}
	if snap.CatalogEncryptionSettings == nil {
		snap.CatalogEncryptionSettings = make(map[string]*DataCatalogEncryptionSettings)
	}
	if snap.CatalogImports == nil {
		snap.CatalogImports = make(map[string]*CatalogImportStatus)
	}

	initSnapshotListDefaults(snap)
}

// initSnapshotListDefaults initialises the one-to-many (map[string][]*T)
// snapshot fields to non-nil. Split out from initSnapshotDefaults to stay
// under the funlen limit.
func initSnapshotListDefaults(snap *backendSnapshot) {
	if snap.JobRuns == nil {
		snap.JobRuns = make(map[string][]*JobRun)
	}
	if snap.WorkflowRuns == nil {
		snap.WorkflowRuns = make(map[string][]*WorkflowRun)
	}
	if snap.SchemaVersions == nil {
		snap.SchemaVersions = make(map[string][]*SchemaVersion)
	}
	if snap.SessionStatements == nil {
		snap.SessionStatements = make(map[string][]*Statement)
	}
	if snap.CrawlHistory == nil {
		snap.CrawlHistory = make(map[string][]*CrawlHistoryEntry)
	}
	if snap.SchemaVersionMetadata == nil {
		snap.SchemaVersionMetadata = make(map[string]map[string]string)
	}
	if snap.IterableFormItems == nil {
		snap.IterableFormItems = make(iterableFormItemsMap)
	}
}

// restoreFromSnapshot copies the raw (non-store.Table) snapshot data into the
// backend. Caller holds b.mu; the registered store.Table fields are already
// restored by b.registry.RestoreAll in Restore above.
func (b *InMemoryBackend) restoreFromSnapshot(snap backendSnapshot) {
	b.partitionIndexes = snap.PartitionIndexes
	b.tableColumnStats = snap.TableColumnStats
	b.partitionColumnStats = snap.PartitionColumnStats
	b.resourcePolicies = snap.ResourcePolicies
	b.catalogEncryptionSettings = snap.CatalogEncryptionSettings
	b.catalogImports = snap.CatalogImports
	b.jobRuns = snap.JobRuns
	b.workflowRuns = snap.WorkflowRuns
	b.schemaVersions = snap.SchemaVersions
	b.sessionStatements = snap.SessionStatements
	b.crawlHistory = snap.CrawlHistory
	b.schemaVersionMetadata = snap.SchemaVersionMetadata
	b.glueIdentityCenterConfig = snap.GlueIdentityCenterConfig
	b.iterableFormItems = snap.IterableFormItems
}

// Snapshot implements Snapshottable by delegating to the backend when it
// supports it.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Snapshot(ctx)
	}

	return nil
}

// Restore implements Snapshottable by delegating to the backend when it
// supports it.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Restore(ctx, data)
	}

	return nil
}
