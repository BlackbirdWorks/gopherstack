package ssm

// Code in this file supports Phase 3.3 of the datalayer refactor: resource
// maps with a pure per-value identity (a Name/ID field of their own) are
// backed by *store.Table[T] instead of a hand-rolled map[string]T /
// map[string]*T. See pkgs/store's package doc and the services/ec2 (commit
// 12e611a4) and services/sqs (commit 0f09d77c) conversions this follows.
//
// # Why this looks different from ec2/sqs
//
// ec2 and sqs each model ONE region per InMemoryBackend instance (a single
// b.Region field), so a flat map[string]*store.Table[T] statically
// registered once at construction time is enough. ssm's InMemoryBackend is
// unusual: it holds EVERY region's state in one instance, via a second map
// layer keyed by region (region -> name -> value). store.Registry has no
// concept of "region" and requires every name registered on it to be known
// and unique up front — it cannot itself express "one table per region,
// regions unknown until first use".
//
// This file bridges the two: each convertible resource keeps its existing
// map[string]*store.Table[T] (region -> Table), created lazily by
// getOrCreateTable on first access to that region — exactly mirroring the
// lazy per-region map creation the hand-rolled code did before conversion
// (e.g. the old `if b.parameters[region] == nil { b.parameters[region] =
// make(map[string]Parameter) }` guards, now folded into the accessor). The
// first time a region's table is created it is ALSO registered on
// b.registry under "<resourceName>/<region>" so that persistence.go can
// still drive Snapshot/Restore through one *store.Registry
// (registry.SnapshotAll/RestoreAll), regardless of how many regions exist —
// see tableAccessorsByPrefix and persistence.go's Restore.
//
// # What is NOT converted here, and why
//
// The following resource fields are deliberately left as plain (possibly
// nested) maps, matching the ec2/sqs precedent of leaving non-pure-key-fn or
// one-to-many collections raw:
//   - history, documentVersions, commandInvocations, opsItemRelatedItems,
//     inventory, compliance, resourcePolicies, instancePatches,
//     associationExecutions, associationExecTargets: one-to-many
//     (map[string][]V) — there is no single V to key a Table by; these stay
//     slice-valued maps, same as ec2's spotFleetHistory/subnetCIDRAssociations
//     pattern.
//   - documentPermissions ([]string), patchGroupToBaseline (string),
//     parameterLabels (map[int64][]string), miscResourceTags
//     (map[string]string), resourceIDToOpsMetadataArn (string): the stored
//     value carries no identity field of its own (a bare string/slice/map),
//     so there is no pure func(*V) string to write — same rationale as ec2's
//     instanceIMDSOptions/verifiedAccessEndpointPolicies exclusions.
//   - opsItemEvents, availablePatches, inventoryDeletions: single-level
//     region -> []V logs/catalogues, not a keyed collection at all.
//   - tags (map[string]*tags.Tags): the map key IS the external resource ID,
//     but tags.Tags itself carries no identity field naming that resource,
//     and instances require an explicit Close() on Restore (see
//     persistence.go) — not a fit for Table's generic JSON round-trip.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func parameterKeyFn(v *Parameter) string                 { return v.Name }
func documentKeyFn(v *Document) string                   { return v.Name }
func commandKeyFn(v *Command) string                     { return v.CommandID }
func activationKeyFn(v *Activation) string               { return v.ActivationID }
func associationKeyFn(v *Association) string             { return v.AssociationID }
func maintenanceWindowKeyFn(v *MaintenanceWindow) string { return v.WindowID }
func maintenanceWindowTargetKeyFn(v *MaintenanceWindowTarget) string {
	return v.WindowTargetID
}
func maintenanceWindowTaskKeyFn(v *MaintenanceWindowTask) string { return v.WindowTaskID }
func sessionKeyFn(v *Session) string                             { return v.SessionID }
func opsItemKeyFn(v *OpsItem) string                             { return v.OpsItemID }
func opsMetadataKeyFn(v *OpsMetadata) string                     { return v.OpsMetadataArn }
func patchBaselineKeyFn(v *PatchBaseline) string                 { return v.BaselineID }
func resourceDataSyncKeyFn(v *ResourceDataSync) string           { return v.SyncName }
func automationExecutionKeyFn(v *AutomationExecution) string {
	return v.AutomationExecutionID
}
func serviceSettingKeyFn(v *ServiceSetting) string         { return v.SettingID }
func executionPreviewKeyFn(v *ExecutionPreview) string     { return v.ExecutionPreviewID }
func instancePatchStateKeyFn(v *InstancePatchState) string { return v.InstanceID }
func instancePropertyKeyFn(v *InstanceProperty) string     { return v.InstanceID }

// getOrCreateTable returns the *store.Table[V] for region from m, the
// backend's region->Table field for one resource type, creating and
// registering it on b.registry (under name+"/"+region) the first time that
// region is touched. m is a Go map (reference type), so inserting into it
// here mutates the same map the caller's field refers to.
func getOrCreateTable[V any](
	b *InMemoryBackend,
	m map[string]*store.Table[V],
	name, region string,
	keyFn func(*V) string,
) *store.Table[V] {
	t, ok := m[region]
	if !ok {
		t = store.Register(b.registry, name+"/"+region, store.New(keyFn))
		m[region] = t
	}

	return t
}

// tableAccessorsByPrefix maps each convertible resource's registry-name
// prefix to a no-op-return accessor call that forces getOrCreateTable to
// register that resource's table for a given region. persistence.go's
// Restore uses this to pre-register every (resource, region) pair present in
// an incoming snapshot's Tables blob BEFORE calling b.registry.RestoreAll —
// RestoreAll only restores tables already registered on b.registry, so a
// region a fresh backend has never seen must be registered first or its
// data would be silently dropped.
//
//nolint:gochecknoglobals // registration table, analogous to errCodeLookup-style lookup tables elsewhere
var tableAccessorsByPrefix = map[string]func(b *InMemoryBackend, region string){
	"parameters":         func(b *InMemoryBackend, region string) { b.parametersStore(region) },
	"documents":          func(b *InMemoryBackend, region string) { b.documentsStore(region) },
	"commands":           func(b *InMemoryBackend, region string) { b.commandsStore(region) },
	"activations":        func(b *InMemoryBackend, region string) { b.activationsStore(region) },
	"associations":       func(b *InMemoryBackend, region string) { b.associationsStore(region) },
	"maintenanceWindows": func(b *InMemoryBackend, region string) { b.maintenanceWindowsStore(region) },
	"maintenanceWindowTargets": func(b *InMemoryBackend, region string) {
		b.maintenanceWindowTargetsStore(region)
	},
	"maintenanceWindowTasks": func(b *InMemoryBackend, region string) {
		b.maintenanceWindowTasksStore(region)
	},
	"sessions":          func(b *InMemoryBackend, region string) { b.sessionsStore(region) },
	"opsItems":          func(b *InMemoryBackend, region string) { b.opsItemsStore(region) },
	"opsMetadata":       func(b *InMemoryBackend, region string) { b.opsMetadataStore(region) },
	"patchBaselines":    func(b *InMemoryBackend, region string) { b.patchBaselinesStore(region) },
	"resourceDataSyncs": func(b *InMemoryBackend, region string) { b.resourceDataSyncsStore(region) },
	"automationExecutions": func(b *InMemoryBackend, region string) {
		b.automationExecutionsStore(region)
	},
	"serviceSettings":   func(b *InMemoryBackend, region string) { b.serviceSettingsStore(region) },
	"executionPreviews": func(b *InMemoryBackend, region string) { b.executionPreviewsStore(region) },
	"instancePatchStates": func(b *InMemoryBackend, region string) {
		b.instancePatchStatesStore(region)
	},
	"instanceProperties": func(b *InMemoryBackend, region string) { b.instancePropertiesStore(region) },
}
