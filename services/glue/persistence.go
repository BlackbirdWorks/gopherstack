package glue

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// glueSnapshotVersion identifies the shape of backendSnapshot's Tables blob
// (i.e. the set/shape of resources registered on b.registry -- see
// registerAllTables in store_setup.go). It must be bumped whenever a change
// there would make an older snapshot unsafe to decode as the current shape.
// Restore compares this against the persisted value and discards (rather than
// attempts to partially decode) any mismatch -- see Restore below. This
// mirrors the services/ec2 (commit 12e611a4) and services/sqs (commit
// 0f09d77c) conversions.
//
// Bumped 1 -> 2 (gopherstack-5mvf): the tableOptimizers table's stored value
// changed from a flat TableOptimizer (with CatalogID/DatabaseName/TableName
// as its own fields) to tableOptimizerRecord, which nests the now-slimmer
// TableOptimizer one level deeper under an "Optimizer" key. An old snapshot
// has no "Optimizer" key at all, so decoding it as the new shape would
// silently zero out every optimizer's Type/Configuration/LastRun rather than
// erroring -- not a case encoding/json's default zero-value-on-missing-field
// behavior can absorb safely.
//
// Bumped 2 -> 3 (clientcoverage-driven audit, 2026-08-23):
// ColumnStatisticsTaskRun.StartedOn's json tag changed from "StartedOn" to
// "StartTime" (matching the real member name -- see the field's doc comment
// in models.go). columnStatTaskRuns is a persisted store.Table[
// ColumnStatisticsTaskRun], so an older snapshot's "StartedOn" key would
// silently fail to populate the renamed field on restore.
const glueSnapshotVersion = 3

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
	ResourceTags              map[string]map[string]string              `json:"resourceTags"`
	IterableFormItems         iterableFormItemsMap                      `json:"iterableFormItems"`
	GlueIdentityCenterConfig  *IdentityCenterConfig                     `json:"glueIdentityCenterConfig,omitempty"`
	DataCatalogExportConfig   *DataCatalogExportConfiguration           `json:"dataCatalogExportConfig,omitempty"`
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
		ResourceTags:              b.resourceTagsSnapshot(),
		GlueIdentityCenterConfig:  b.glueIdentityCenterConfig,
		DataCatalogExportConfig:   b.dataCatalogExportConfig,
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
	b.restoreDeterministicARNs()
	b.restoreResourceTags(snap.ResourceTags)
	b.foldRestoredNames()

	return nil
}

// restoreDeterministicARNs recomputes DevEndpoint.ARN and
// UserDefinedFunction.FunctionARN after a restore. Both fields are json:"-":
// real Glue's types.DevEndpoint and types.UserDefinedFunction expose no ARN
// on the wire (aws-sdk-go-v2 service/glue/types/types.go), so neither is
// persisted, and both read back as "" after RestoreAll otherwise. Recomputed
// the same deterministic way CreateDevEndpoint/CreateUserDefinedFunction set
// them (devEndpointARN/udfARN), matching restoreResourceTags's use of the
// same helpers for the same reason (tags.go).
func (b *InMemoryBackend) restoreDeterministicARNs() {
	for _, dep := range b.devEndpoints.All() {
		dep.ARN = b.devEndpointARN(dep.EndpointName)
	}

	for _, u := range b.udfs.All() {
		u.FunctionARN = b.udfARN(u.DatabaseName, u.FunctionName)
	}
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
	if snap.ResourceTags == nil {
		snap.ResourceTags = make(map[string]map[string]string)
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
	interruptRestoredStatements(b.sessionStatements)
	b.crawlHistory = snap.CrawlHistory
	b.schemaVersionMetadata = snap.SchemaVersionMetadata
	b.glueIdentityCenterConfig = snap.GlueIdentityCenterConfig
	b.dataCatalogExportConfig = snap.DataCatalogExportConfig
	b.iterableFormItems = snap.IterableFormItems
}

// Snapshot implements Snapshottable; other regions ride in an additive "regions" key.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	s, ok := h.Backend.(Snapshottable)
	if !ok {
		return nil
	}

	return h.peers.Snapshot(s.Snapshot(ctx), func(p *Handler) []byte { return p.Snapshot(ctx) })
}

// Restore implements Snapshottable.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	s, ok := h.Backend.(Snapshottable)
	if !ok {
		return nil
	}

	if err := s.Restore(ctx, data); err != nil {
		return err
	}

	return h.peers.Restore(
		data,
		func(p *Handler, d []byte) error { return p.Restore(ctx, d) },
		func(p *Handler) { p.Backend.StopReconciler() },
	)
}

// foldRestoredNames lowercases database/table names (and the map keys built from them) in snapshots
// written before Glue name folding, so older mixed-case catalogs stay reachable.
func (b *InMemoryBackend) foldRestoredNames() {
	for _, db := range b.databases.All() {
		db.Name = strs.Fold(db.Name)
		db.ARN = b.databaseARN(db.Name)
	}

	for _, t := range b.tables.All() {
		t.DatabaseName, t.Name = strs.Fold(t.DatabaseName), strs.Fold(t.Name)
	}

	for _, p := range b.partitions.All() {
		p.DatabaseName, p.TableName = strs.Fold(p.DatabaseName), strs.Fold(p.TableName)
	}

	for _, tv := range b.tableVersions.All() {
		if tv.Table != nil {
			tv.Table.DatabaseName, tv.Table.Name = strs.Fold(tv.Table.DatabaseName), strs.Fold(tv.Table.Name)
		}
	}

	for _, u := range b.udfs.All() {
		u.DatabaseName = strs.Fold(u.DatabaseName)
		u.FunctionARN = b.udfARN(u.DatabaseName, u.FunctionName)
	}

	for _, r := range b.tableOptimizers.All() {
		r.DatabaseName, r.TableName = strs.Fold(r.DatabaseName), strs.Fold(r.TableName)
	}

	for _, s := range b.columnStatTaskSettings.All() {
		s.DatabaseName, s.TableName = strs.Fold(s.DatabaseName), strs.Fold(s.TableName)
	}

	b.partitionIndexes = foldKeyHeads(b.partitionIndexes)
	b.tableColumnStats = foldKeyHeads(b.tableColumnStats)
	b.partitionColumnStats = foldKeyHeads(b.partitionColumnStats)
}

// foldKeyHeads lowercases the leading "db|table" segments of every "db|table|rest" key.
func foldKeyHeads[V any](m map[string]V) map[string]V {
	out := make(map[string]V, len(m))

	for k, v := range m {
		parts := strings.SplitN(k, "|", keyHeadParts+1)
		for i := 0; i < len(parts) && i < keyHeadParts; i++ {
			parts[i] = strs.Fold(parts[i])
		}

		out[strings.Join(parts, "|")] = v
	}

	return out
}

const keyHeadParts = 2

// interruptRestoredStatements fails statements that were queued or running at
// snapshot time: their interpreter did not survive the restart.
func interruptRestoredStatements(all map[string][]*Statement) {
	for _, stmts := range all {
		for _, st := range stmts {
			switch st.State {
			case stmtWaiting, stmtRunning:
				st.State = stmtError
				st.Output = &StatementOutput{
					Status: stmtError, ErrorName: "SessionInterrupted",
					ErrorValue: "the session interpreter did not survive a restart",
				}
			case stmtCancelling:
				st.State = stmtCancelled
			}
		}
	}
}
