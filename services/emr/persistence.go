package emr

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// emrSnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to the DTOs/backendSnapshot itself would make an
// older snapshot unsafe to decode as the current shape (e.g. a field's
// meaning or type changes). Restore compares this against the persisted value
// and discards (rather than attempts to partially decode) any mismatch -- see
// Restore below. This is the first version: earlier (pre-Phase-3.3)
// snapshots carried no version field at all, so they also fail this check
// and are discarded rather than misread, which is the safe behaviour across
// any snapshot-format change.
const emrSnapshotVersion = 1

// clusterDTO wraps a Cluster for JSON round-tripping through store.Registry.
// store.Table[Cluster].Snapshot's plain json.Marshal(Cluster) cannot see any
// of Cluster's unexported fields -- region (used only for the live table's
// composite key, see store_setup.go) plus the handful of fields already
// hidden from Cluster's own JSON shape pre-refactor (autoTerminationPolicy,
// managedScalingPolicy, instanceGroups, bootstrapActions, steps,
// instanceFleets, formerly carried via a separate clusterExtra side-table) --
// so all of them are carried alongside Value here instead. ID mirrors the
// cluster's own ID so the DTO table has a stable composite key
// ("Region|ID") independent of Value's own (unmarshaled) fields.
type clusterDTO struct {
	Value                 *Cluster                `json:"value"`
	Region                string                  `json:"region"`
	ID                    string                  `json:"id"`
	ManagedScalingPolicy  *ManagedScalingPolicy   `json:"managedScalingPolicy,omitempty"`
	AutoTerminationPolicy *AutoTerminationPolicy  `json:"autoTerminationPolicy,omitempty"`
	InstanceGroups        []InstanceGroup         `json:"instanceGroups,omitempty"`
	InstanceFleets        []InstanceFleet         `json:"instanceFleets,omitempty"`
	Steps                 []Step                  `json:"steps,omitempty"`
	BootstrapActions      []BootstrapActionConfig `json:"bootstrapActions,omitempty"`
}

func clusterDTOKeyFn(d *clusterDTO) string { return regionKey(d.Region, d.ID) }

// regionalDTO wraps a region-nested resource for JSON round-tripping through
// store.Registry. It is shared by every converted resource whose only hidden
// field is region (SecurityConfiguration, Studio, StudioSessionMapping,
// PersistentAppUI, NotebookExecution) -- Cluster needs the dedicated
// clusterDTO above because it hides several other fields too. ID mirrors the
// resource's own composite-key component (its own identifier, or the
// studioSessionKey composite for StudioSessionMapping) so the DTO table
// itself has a stable composite key ("Region|ID").
type regionalDTO[V any] struct {
	Value  *V     `json:"value"`
	Region string `json:"region"`
	ID     string `json:"id"`
}

// regionalDTOKeyFn is the shared [store.Table] key function for every
// regionalDTO[V] table below; it mirrors the "region|id" composite key each
// live table uses (see regionKey in backend.go).
func regionalDTOKeyFn[V any](d *regionalDTO[V]) string { return regionKey(d.Region, d.ID) }

// regionOnlyDTO wraps a resource that is keyed directly by region (one value
// per region, not per-resource) -- BlockPublicAccessConfiguration and
// blockPublicAccessMeta. Unlike regionalDTO there is no separate ID: region
// itself is the whole primary key.
type regionOnlyDTO[V any] struct {
	Value  *V     `json:"value"`
	Region string `json:"region"`
}

func regionOnlyDTOKeyFn[V any](d *regionOnlyDTO[V]) string { return d.Region }

// backendSnapshot is the top-level on-disk shape for the EMR backend.
//
// Tables holds one JSON-encoded array per registered DTO table, produced by
// [store.Registry.SnapshotAll] -- the eight converted resource collections,
// each wrapped in a DTO to carry its hidden field(s) through. arnIndex is
// still a region-nested plain map (see store_setup.go for why it was not
// converted to a store.Table) and is persisted directly, as before. Version
// guards against decoding a snapshot from an incompatible (older or newer)
// build of this backend as though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage   `json:"tables"`
	ArnIndex  map[string]map[string]string `json:"arnIndex"`
	AccountID string                       `json:"accountID"`
	Region    string                       `json:"region"`
	Version   int                          `json:"version"`
}

// persistenceDTOTables groups every ephemeral DTO table buildPersistenceDTORegistry
// constructs, so Snapshot/Restore can pass them around as one value instead of
// nine separate return values.
type persistenceDTOTables struct {
	registry              *store.Registry
	clusters              *store.Table[clusterDTO]
	securityConfigs       *store.Table[regionalDTO[SecurityConfiguration]]
	studios               *store.Table[regionalDTO[Studio]]
	studioSessionMappings *store.Table[regionalDTO[StudioSessionMapping]]
	persistentAppUIs      *store.Table[regionalDTO[PersistentAppUI]]
	notebookExecutions    *store.Table[regionalDTO[NotebookExecution]]
	blockPublicAccess     *store.Table[regionOnlyDTO[BlockPublicAccessConfiguration]]
	blockPublicAccessMeta *store.Table[regionOnlyDTO[blockPublicAccessMeta]]
}

// buildPersistenceDTORegistry constructs the ephemeral DTO registry used by
// both Snapshot and Restore. It is built fresh on every call (rather than
// reusing b.registry) because the DTO value types differ from the live table
// value types (e.g. regionalDTO[V] vs V).
func buildPersistenceDTORegistry() persistenceDTOTables {
	dtoReg := store.NewRegistry()

	return persistenceDTOTables{
		registry: dtoReg,
		clusters: store.Register(dtoReg, "clusters", store.New(clusterDTOKeyFn)),
		securityConfigs: store.Register(
			dtoReg, "securityConfigs", store.New(regionalDTOKeyFn[SecurityConfiguration]),
		),
		studios: store.Register(dtoReg, "studios", store.New(regionalDTOKeyFn[Studio])),
		studioSessionMappings: store.Register(
			dtoReg, "studioSessionMappings", store.New(regionalDTOKeyFn[StudioSessionMapping]),
		),
		persistentAppUIs: store.Register(
			dtoReg, "persistentAppUIs", store.New(regionalDTOKeyFn[PersistentAppUI]),
		),
		notebookExecutions: store.Register(
			dtoReg, "notebookExecutions", store.New(regionalDTOKeyFn[NotebookExecution]),
		),
		blockPublicAccess: store.Register(
			dtoReg, "blockPublicAccess", store.New(regionOnlyDTOKeyFn[BlockPublicAccessConfiguration]),
		),
		blockPublicAccessMeta: store.Register(
			dtoReg, "blockPublicAccessMeta", store.New(regionOnlyDTOKeyFn[blockPublicAccessMeta]),
		),
	}
}

// Snapshot serialises the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	dtos := buildPersistenceDTORegistry()

	for _, v := range b.clusters.Snapshot() {
		dtos.clusters.Put(&clusterDTO{
			Value:                 v,
			Region:                v.region,
			ID:                    v.ID,
			ManagedScalingPolicy:  clonePtr(v.managedScalingPolicy),
			AutoTerminationPolicy: clonePtr(v.autoTerminationPolicy),
			InstanceGroups:        slices.Clone(v.instanceGroups),
			InstanceFleets:        slices.Clone(v.instanceFleets),
			Steps:                 slices.Clone(v.steps),
			BootstrapActions:      cloneBootstrapActions(v.bootstrapActions),
		})
	}

	for _, v := range b.securityConfigs.Snapshot() {
		dtos.securityConfigs.Put(&regionalDTO[SecurityConfiguration]{Region: v.region, ID: v.Name, Value: v})
	}

	for _, v := range b.studios.Snapshot() {
		dtos.studios.Put(&regionalDTO[Studio]{Region: v.region, ID: v.StudioID, Value: v})
	}

	for _, v := range b.studioSessionMappings.Snapshot() {
		id := studioSessionKey(v.StudioID, v.IdentityType, v.IdentityID, v.IdentityName)
		dtos.studioSessionMappings.Put(&regionalDTO[StudioSessionMapping]{Region: v.region, ID: id, Value: v})
	}

	for _, v := range b.persistentAppUIs.Snapshot() {
		dtos.persistentAppUIs.Put(&regionalDTO[PersistentAppUI]{Region: v.region, ID: v.ID, Value: v})
	}

	for _, v := range b.notebookExecutions.Snapshot() {
		dtos.notebookExecutions.Put(&regionalDTO[NotebookExecution]{
			Region: v.region, ID: v.NotebookExecutionID, Value: v,
		})
	}

	for _, v := range b.blockPublicAccess.Snapshot() {
		dtos.blockPublicAccess.Put(&regionOnlyDTO[BlockPublicAccessConfiguration]{Region: v.region, Value: v})
	}

	for _, v := range b.blockPublicAccessMeta.Snapshot() {
		dtos.blockPublicAccessMeta.Put(&regionOnlyDTO[blockPublicAccessMeta]{Region: v.region, Value: v})
	}

	tables, err := dtos.registry.SnapshotAll()
	if err != nil {
		// The DTOs above are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the
		// Manager).
		logger.Load(ctx).WarnContext(ctx, "emr: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:   emrSnapshotVersion,
		Tables:    tables,
		ArnIndex:  b.arnIndex,
		AccountID: b.accountID,
		Region:    b.region,
	}

	return persistence.MarshalSnapshot(ctx, "emr", snap)
}

// clonePtr returns a shallow copy of p, or nil if p is nil.
func clonePtr[V any](p *V) *V {
	if p == nil {
		return nil
	}

	cp := *p

	return &cp
}

// Restore loads backend state from a JSON snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "emr", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != emrSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never
		// be partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"emr: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", emrSnapshotVersion)

		b.registry.ResetAll()
		b.arnIndex = make(map[string]map[string]string)
		b.accountID = snap.AccountID
		b.region = snap.Region

		return nil
	}

	if err := b.restoreResourceTables(snap.Tables); err != nil {
		return err
	}

	if snap.ArnIndex == nil {
		snap.ArnIndex = make(map[string]map[string]string)
	}
	b.arnIndex = snap.ArnIndex

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// unwrapRegionalDTOs converts every regionalDTO[V] in dtos into its live *V,
// restoring the unexported region field each carries via setRegion (a plain
// generic type parameter V has no field access, so the region assignment is
// supplied by the caller, one per concrete V; see restoreResourceTables). A
// DTO whose Value is nil -- not producible by Snapshot, but a defensive
// guard against a hand-edited or corrupted snapshot -- is skipped rather
// than dereferenced.
func unwrapRegionalDTOs[V any](dtos *store.Table[regionalDTO[V]], setRegion func(*V, string)) []*V {
	items := make([]*V, 0, dtos.Len())

	for _, d := range dtos.All() {
		if d.Value == nil {
			continue
		}

		setRegion(d.Value, d.Region)
		items = append(items, d.Value)
	}

	return items
}

// unwrapRegionOnlyDTOs is unwrapRegionalDTOs' counterpart for the two
// region-keyed-directly tables (BlockPublicAccessConfiguration,
// blockPublicAccessMeta).
func unwrapRegionOnlyDTOs[V any](dtos *store.Table[regionOnlyDTO[V]], setRegion func(*V, string)) []*V {
	items := make([]*V, 0, dtos.Len())

	for _, d := range dtos.All() {
		if d.Value == nil {
			continue
		}

		setRegion(d.Value, d.Region)
		items = append(items, d.Value)
	}

	return items
}

// restoreResourceTables rebuilds every store.Table on b from snap's
// per-table JSON, factored out of Restore to keep Restore's own cognitive
// complexity low. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restoreResourceTables(tables map[string]json.RawMessage) error {
	dtos := buildPersistenceDTORegistry()

	if err := dtos.registry.RestoreAll(tables); err != nil {
		return fmt.Errorf("emr: restore snapshot tables: %w", err)
	}

	b.clusters.Restore(unwrapClusterDTOs(dtos.clusters))
	b.securityConfigs.Restore(unwrapRegionalDTOs(
		dtos.securityConfigs, func(v *SecurityConfiguration, r string) { v.region = r },
	))
	b.studios.Restore(unwrapRegionalDTOs(dtos.studios, func(v *Studio, r string) { v.region = r }))
	b.studioSessionMappings.Restore(unwrapRegionalDTOs(
		dtos.studioSessionMappings, func(v *StudioSessionMapping, r string) { v.region = r },
	))
	b.persistentAppUIs.Restore(unwrapRegionalDTOs(
		dtos.persistentAppUIs, func(v *PersistentAppUI, r string) { v.region = r },
	))
	b.notebookExecutions.Restore(unwrapRegionalDTOs(
		dtos.notebookExecutions, func(v *NotebookExecution, r string) { v.region = r },
	))
	b.blockPublicAccess.Restore(unwrapRegionOnlyDTOs(
		dtos.blockPublicAccess, func(v *BlockPublicAccessConfiguration, r string) { v.region = r },
	))
	b.blockPublicAccessMeta.Restore(unwrapRegionOnlyDTOs(
		dtos.blockPublicAccessMeta, func(v *blockPublicAccessMeta, r string) { v.region = r },
	))

	return nil
}

// unwrapClusterDTOs converts every clusterDTO in dtos into its live *Cluster,
// re-hydrating both the region field and the unexported extra fields
// (formerly carried via clusterExtra) each DTO wraps.
func unwrapClusterDTOs(dtos *store.Table[clusterDTO]) []*Cluster {
	items := make([]*Cluster, 0, dtos.Len())

	for _, d := range dtos.All() {
		if d.Value == nil {
			continue
		}

		c := d.Value
		c.region = d.Region
		c.managedScalingPolicy = d.ManagedScalingPolicy
		c.autoTerminationPolicy = d.AutoTerminationPolicy
		c.instanceGroups = d.InstanceGroups
		c.instanceFleets = d.InstanceFleets
		c.steps = d.Steps
		c.bootstrapActions = d.BootstrapActions
		items = append(items, c)
	}

	return items
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
