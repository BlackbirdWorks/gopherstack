package ssm

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// ssmSnapshotVersion identifies the shape of backendSnapshot's Tables blob
// (i.e. the set of resource tables registered on b.registry -- see
// store_setup.go). It must be bumped whenever a change there would make an
// older snapshot unsafe to decode as the current shape. Restore compares this
// against the persisted value and discards (rather than attempts to
// partially decode) any mismatch -- see Restore below. This mirrors the
// services/sqs pilot (commit 0f09d77c) and the services/ec2 conversion
// (commit 12e611a4).
const ssmSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the SSM backend.
//
// Tables holds one JSON-encoded, key-sorted array per registered
// *store.Table[V], produced by [github.com/blackbirdworks/gopherstack/pkgs/store.Registry.SnapshotAll].
// Each table is registered under "<resourceName>/<region>" (see
// store_setup.go's getOrCreateTable) rather than a single flat name per
// resource, because a single ssm InMemoryBackend holds every region's state
// (unlike ec2/sqs, which are one region per backend) -- Restore must
// pre-register every (resource, region) pair found in Tables before calling
// registry.RestoreAll, see below.
//
// Every other field below is a resource collection deliberately left as a
// plain (possibly nested) map rather than a *store.Table -- see
// store_setup.go's package doc for why each one doesn't fit Table's
// func(*V) string keying requirement.
type backendSnapshot struct {
	Tables                     map[string]json.RawMessage                         `json:"tables"`
	History                    map[string]map[string][]ParameterHistory           `json:"history"`
	Tags                       map[string]map[string]*tags.Tags                   `json:"tags"`
	DocumentVersions           map[string]map[string][]DocumentVersion            `json:"document_versions"`
	DocumentPermissions        map[string]map[string][]string                     `json:"document_permissions"`
	CommandInvocations         map[string]map[string][]CommandInvocation          `json:"command_invocations"`
	PatchGroupToBaseline       map[string]map[string]string                       `json:"patch_group_to_baseline"`
	OpsItemRelatedItems        map[string]map[string][]OpsItemRelatedItem         `json:"ops_item_related_items"`
	Inventory                  map[string]map[string][]InventoryItem              `json:"inventory"`
	Compliance                 map[string]map[string][]ComplianceItem             `json:"compliance"`
	ParameterLabels            map[string]map[string]map[int64][]string           `json:"parameter_labels"`
	ResourcePolicies           map[string]map[string][]*ResourcePolicy            `json:"resource_policies"`
	MiscResourceTags           map[string]map[string]map[string]string            `json:"misc_resource_tags"`
	ResourceIDToOpsMetadataArn map[string]map[string]string                       `json:"resource_id_to_ops_metadata_arn"`
	OpsItemEvents              map[string][]OpsItemEventSummary                   `json:"ops_item_events"`
	AssociationExecutions      map[string]map[string][]AssociationExecution       `json:"association_executions"`
	AssociationExecTargets     map[string]map[string][]AssociationExecutionTarget `json:"association_exec_targets"`
	InventoryDeletions         map[string][]InventoryDeletion                     `json:"inventory_deletions"`
	InstancePatches            map[string]map[string][]PatchComplianceData        `json:"instance_patches"`
	AvailablePatches           map[string][]Patch                                 `json:"available_patches"`
	NotifiedParameterPolicies  map[string]map[string]map[string]struct{}          `json:"notified_parameter_policies"`
	Version                    int                                                `json:"version"`
}

// initSnapshotDefaults initializes nil maps in the snapshot for core fields.
func initSnapshotDefaults(snap *backendSnapshot) {
	if snap.History == nil {
		snap.History = make(map[string]map[string][]ParameterHistory)
	}

	if snap.Tags == nil {
		snap.Tags = make(map[string]map[string]*tags.Tags)
	}

	if snap.DocumentVersions == nil {
		snap.DocumentVersions = make(map[string]map[string][]DocumentVersion)
	}

	if snap.DocumentPermissions == nil {
		snap.DocumentPermissions = make(map[string]map[string][]string)
	}

	if snap.CommandInvocations == nil {
		snap.CommandInvocations = make(map[string]map[string][]CommandInvocation)
	}
}

// initSnapshotNewFields initializes nil maps for newer resource types.
func initSnapshotNewFields(snap *backendSnapshot) {
	if snap.PatchGroupToBaseline == nil {
		snap.PatchGroupToBaseline = make(map[string]map[string]string)
	}

	if snap.OpsItemRelatedItems == nil {
		snap.OpsItemRelatedItems = make(map[string]map[string][]OpsItemRelatedItem)
	}

	if snap.Inventory == nil {
		snap.Inventory = make(map[string]map[string][]InventoryItem)
	}

	if snap.Compliance == nil {
		snap.Compliance = make(map[string]map[string][]ComplianceItem)
	}

	if snap.ParameterLabels == nil {
		snap.ParameterLabels = make(map[string]map[string]map[int64][]string)
	}

	if snap.ResourcePolicies == nil {
		snap.ResourcePolicies = make(map[string]map[string][]*ResourcePolicy)
	}

	if snap.MiscResourceTags == nil {
		snap.MiscResourceTags = make(map[string]map[string]map[string]string)
	}

	if snap.ResourceIDToOpsMetadataArn == nil {
		snap.ResourceIDToOpsMetadataArn = make(map[string]map[string]string)
	}

	if snap.OpsItemEvents == nil {
		snap.OpsItemEvents = make(map[string][]OpsItemEventSummary)
	}

	if snap.AssociationExecutions == nil {
		snap.AssociationExecutions = make(map[string]map[string][]AssociationExecution)
	}

	if snap.AssociationExecTargets == nil {
		snap.AssociationExecTargets = make(map[string]map[string][]AssociationExecutionTarget)
	}

	initSnapshotPatchOpsFields(snap)
}

// initSnapshotPatchOpsFields initializes nil maps for inventory-deletion
// records and the patch-operation state (available-patches catalogue and
// per-instance compliance data) written by SendCommand's
// AWS-RunPatchBaseline handling and DescribeAvailablePatches. Split out of
// initSnapshotNewFields to keep it under the function-length limit.
func initSnapshotPatchOpsFields(snap *backendSnapshot) {
	if snap.InventoryDeletions == nil {
		snap.InventoryDeletions = make(map[string][]InventoryDeletion)
	}

	if snap.InstancePatches == nil {
		snap.InstancePatches = make(map[string]map[string][]PatchComplianceData)
	}

	if snap.AvailablePatches == nil {
		snap.AvailablePatches = make(map[string][]Patch)
	}

	if snap.NotifiedParameterPolicies == nil {
		snap.NotifiedParameterPolicies = make(map[string]map[string]map[string]struct{})
	}
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
		logger.Load(ctx).WarnContext(ctx, "ssm: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:                    ssmSnapshotVersion,
		Tables:                     tables,
		History:                    b.history,
		Tags:                       b.tags,
		DocumentVersions:           b.documentVersions,
		DocumentPermissions:        b.documentPermissions,
		CommandInvocations:         b.commandInvocations,
		PatchGroupToBaseline:       b.patchGroupToBaseline,
		OpsItemRelatedItems:        b.opsItemRelatedItems,
		Inventory:                  b.inventory,
		Compliance:                 b.compliance,
		ParameterLabels:            b.parameterLabels,
		ResourcePolicies:           b.resourcePolicies,
		MiscResourceTags:           b.miscResourceTags,
		ResourceIDToOpsMetadataArn: b.resourceIDToOpsMetadataArn,
		OpsItemEvents:              b.opsItemEvents,
		AssociationExecutions:      b.associationExecutions,
		AssociationExecTargets:     b.associationExecTargets,
		InventoryDeletions:         b.inventoryDeletions,
		InstancePatches:            b.instancePatches,
		AvailablePatches:           b.availablePatches,
		NotifiedParameterPolicies:  b.notifiedParameterPolicies,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "ssm: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// preRegisterSnapshotTables ensures every (resource, region) pair present in
// tables is registered on b.registry before registry.RestoreAll is called.
// RestoreAll only restores tables already registered (see
// [github.com/blackbirdworks/gopherstack/pkgs/store.Registry.RestoreAll]);
// a region a fresh backend has never touched would otherwise not yet have a
// table registered under that name, and its persisted data would be
// silently dropped instead of restored. Each table name has the shape
// "<resourceName>/<region>" (see store_setup.go's getOrCreateTable); regions
// never contain "/", so splitting on the first one recovers both parts.
func (b *InMemoryBackend) preRegisterSnapshotTables(tables map[string]json.RawMessage) {
	for key := range tables {
		name, region, found := strings.Cut(key, "/")
		if !found {
			continue
		}

		if register, ok := tableAccessorsByPrefix[name]; ok {
			register(b, region)
		}
	}
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "ssm", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != ssmSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption. Mirrors the services/sqs pilot (commit 0f09d77c) and
		// the services/ec2 conversion (commit 12e611a4).
		logger.Load(ctx).WarnContext(ctx,
			"ssm: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", ssmSnapshotVersion)

		b.registry.ResetAll()

		// Match NewInMemoryBackend's construction-time state: a fresh backend
		// is never truly empty of documents, so "starting empty" here means
		// starting exactly as fresh as a new backend would, not one that
		// lacks even the built-in AWS-* documents every backend is seeded
		// with. This preserves pre-existing Restore behavior for legacy
		// snapshots taken before this backend supported documents at all
		// (they have no "documents" data to discard, only a version
		// mismatch), which relied on the default documents always being
		// present after Restore.
		b.registerDefaultDocuments(defaultRegion)

		return nil
	}

	initSnapshotDefaults(&snap)
	initSnapshotNewFields(&snap)

	for _, regionTags := range b.tags {
		for _, t := range regionTags {
			t.Close()
		}
	}

	b.preRegisterSnapshotTables(snap.Tables)

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("ssm: restore snapshot tables: %w", err)
	}

	b.history = snap.History
	b.tags = snap.Tags
	b.documentVersions = snap.DocumentVersions
	b.documentPermissions = snap.DocumentPermissions
	b.commandInvocations = snap.CommandInvocations
	b.patchGroupToBaseline = snap.PatchGroupToBaseline
	b.opsItemRelatedItems = snap.OpsItemRelatedItems
	b.inventory = snap.Inventory
	b.compliance = snap.Compliance
	b.parameterLabels = snap.ParameterLabels
	b.resourcePolicies = snap.ResourcePolicies
	b.miscResourceTags = snap.MiscResourceTags
	b.resourceIDToOpsMetadataArn = snap.ResourceIDToOpsMetadataArn
	b.opsItemEvents = snap.OpsItemEvents
	b.associationExecutions = snap.AssociationExecutions
	b.associationExecTargets = snap.AssociationExecTargets
	b.inventoryDeletions = snap.InventoryDeletions
	b.instancePatches = snap.InstancePatches
	b.availablePatches = snap.AvailablePatches
	b.notifiedParameterPolicies = snap.NotifiedParameterPolicies

	// Re-seed built-in documents if they are absent from the snapshot
	// (e.g. snapshots taken before document support was added).
	for _, region := range []string{defaultRegion} {
		for _, name := range []string{"AWS-RunShellScript", "AWS-RunPowerShellScript"} {
			if !b.documentsStore(region).Has(name) {
				b.registerDefaultDocuments(region)

				break
			}
		}
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	type snapshotter interface {
		Snapshot(ctx context.Context) []byte
	}
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	type restorer interface {
		Restore(context.Context, []byte) error
	}
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(ctx, data)
	}

	return nil
}
