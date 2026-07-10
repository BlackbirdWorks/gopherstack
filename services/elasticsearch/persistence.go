package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// elasticsearchSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to a DTO type, a registered table's value
// type, or backendSnapshot itself would make an older snapshot unsafe to
// decode as the current shape. Restore compares this against the persisted
// value and discards (resetLocked-equivalent, not a partial decode) any
// mismatch -- see Restore below. This is the first version: the pre-Phase-3.3
// snapshot shape carried no version field at all, so it also fails this check
// (decodes as Version == 0) and is discarded the same way any other
// incompatible snapshot is.
const elasticsearchSnapshotVersion = 1

// regionalDTO wraps a region-nested resource for JSON round-tripping through
// store.Registry. It is shared by every converted resource collection here
// (Domain, Package, InboundConnection, OutboundConnection, VpcEndpoint,
// ReservedInstance) since each hides only its region field from JSON (see
// store_setup.go and the field comments in backend.go) -- the same technique
// services/emr's regionalDTO[V] uses. ID mirrors the resource's own
// composite-key component (its own identifier) so the DTO table itself has a
// stable composite key ("Region|ID") independent of Value's own (unmarshaled)
// fields.
type regionalDTO[V any] struct {
	Value  *V     `json:"value"`
	Region string `json:"region"`
	ID     string `json:"id"`
}

// regionalDTOKeyFn is the shared [store.Table] key function for every
// regionalDTO[V] table below; it mirrors the "region|id" composite key each
// live table uses (see regionKey in backend.go).
func regionalDTOKeyFn[V any](d *regionalDTO[V]) string { return regionKey(d.Region, d.ID) }

// persistenceDTOTables groups every ephemeral DTO table
// buildPersistenceDTORegistry constructs, so Snapshot/Restore can pass them
// around as one value instead of six separate return values.
type persistenceDTOTables struct {
	registry            *store.Registry
	domains             *store.Table[regionalDTO[Domain]]
	packages            *store.Table[regionalDTO[Package]]
	inboundConnections  *store.Table[regionalDTO[InboundConnection]]
	outboundConnections *store.Table[regionalDTO[OutboundConnection]]
	vpcEndpoints        *store.Table[regionalDTO[VpcEndpoint]]
	reservedInstances   *store.Table[regionalDTO[ReservedInstance]]
}

// buildPersistenceDTORegistry constructs the ephemeral DTO registry used by
// both Snapshot and Restore. It is built fresh on every call (rather than
// reusing b.registry) because the DTO value types differ from the live table
// value types (regionalDTO[V] vs V).
func buildPersistenceDTORegistry() persistenceDTOTables {
	dtoReg := store.NewRegistry()

	return persistenceDTOTables{
		registry: dtoReg,
		domains:  store.Register(dtoReg, "domains", store.New(regionalDTOKeyFn[Domain])),
		packages: store.Register(dtoReg, "packages", store.New(regionalDTOKeyFn[Package])),
		inboundConnections: store.Register(
			dtoReg, "inboundConnections", store.New(regionalDTOKeyFn[InboundConnection]),
		),
		outboundConnections: store.Register(
			dtoReg, "outboundConnections", store.New(regionalDTOKeyFn[OutboundConnection]),
		),
		vpcEndpoints: store.Register(dtoReg, "vpcEndpoints", store.New(regionalDTOKeyFn[VpcEndpoint])),
		reservedInstances: store.Register(
			dtoReg, "reservedInstances", store.New(regionalDTOKeyFn[ReservedInstance]),
		),
	}
}

// backendSnapshot is the top-level on-disk shape for the Elasticsearch
// backend.
//
// Tables holds one JSON-encoded array per DTO-wrapped table name, produced by
// [store.Registry.SnapshotAll] against the ephemeral DTO registry above -- the
// six converted resource collections, each carrying its hidden region field
// through the wrapper. PackagesByName, PackageAssociations, and VpcAccess are
// still plain region-nested maps (see store_setup.go for why they were not
// converted to store.Table) and are persisted directly, as before. ArnIndex
// is deliberately NOT persisted -- as before this refactor, it is pure
// derived data rebuilt from the restored domains table (see
// rebuildArnIndex), never serialized. Version guards against decoding a
// snapshot from an incompatible (older or newer) build of this backend as
// though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables              map[string]json.RawMessage     `json:"tables"`
	PackagesByName      map[string]map[string]string   `json:"packagesByName"`
	PackageAssociations map[string]map[string][]string `json:"packageAssociations"`
	VpcAccess           map[string]map[string][]string `json:"vpcAccess"`
	AccountID           string                         `json:"accountID"`
	Region              string                         `json:"region"`
	NextID              int                            `json:"nextID"`
	Version             int                            `json:"version"`
}

// Snapshot serialises the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	dtos := buildPersistenceDTORegistry()
	b.fillPersistenceDTOs(dtos)

	tables, err := dtos.registry.SnapshotAll()
	if err != nil {
		// The DTOs above are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the
		// Manager).
		logger.Load(ctx).WarnContext(ctx, "elasticsearch: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:             elasticsearchSnapshotVersion,
		Tables:              tables,
		PackagesByName:      b.packagesByName,
		PackageAssociations: b.packageAssociations,
		VpcAccess:           b.vpcAccess,
		AccountID:           b.accountID,
		Region:              b.region,
		NextID:              b.nextID,
	}

	return persistence.MarshalSnapshot(ctx, "elasticsearch", snap)
}

// fillPersistenceDTOs populates dtos from every live region-qualified table,
// factored out of Snapshot to keep Snapshot's own cognitive complexity low.
// Callers must hold at least b.mu.RLock.
func (b *InMemoryBackend) fillPersistenceDTOs(dtos persistenceDTOTables) {
	for _, v := range b.domains.Snapshot() {
		dtos.domains.Put(&regionalDTO[Domain]{Region: v.region, ID: v.Name, Value: v})
	}

	for _, v := range b.packages.Snapshot() {
		dtos.packages.Put(&regionalDTO[Package]{Region: v.region, ID: v.ID, Value: v})
	}

	for _, v := range b.inboundConnections.Snapshot() {
		dtos.inboundConnections.Put(&regionalDTO[InboundConnection]{Region: v.region, ID: v.ConnectionID, Value: v})
	}

	for _, v := range b.outboundConnections.Snapshot() {
		dtos.outboundConnections.Put(&regionalDTO[OutboundConnection]{Region: v.region, ID: v.ConnectionID, Value: v})
	}

	for _, v := range b.vpcEndpoints.Snapshot() {
		dtos.vpcEndpoints.Put(&regionalDTO[VpcEndpoint]{Region: v.region, ID: v.ID, Value: v})
	}

	for _, v := range b.reservedInstances.Snapshot() {
		dtos.reservedInstances.Put(&regionalDTO[ReservedInstance]{Region: v.region, ID: v.ReservationID, Value: v})
	}
}

// Restore loads backend state from a JSON snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "elasticsearch", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	// Close existing Tags to release Prometheus metrics before replacing state.
	for _, d := range b.domains.Snapshot() {
		d.Tags.Close()
	}

	if snap.Version != elasticsearchSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"elasticsearch: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", elasticsearchSnapshotVersion)

		b.registry.ResetAll()
		b.arnIndex = make(map[string]map[string]string)
		b.packagesByName = make(map[string]map[string]string)
		b.packageAssociations = make(map[string]map[string][]string)
		b.vpcAccess = make(map[string]map[string][]string)
		b.nextID = 0

		return nil
	}

	if err := b.restoreResourceTables(snap.Tables); err != nil {
		return err
	}

	b.restoreRawMaps(&snap)
	b.rebuildArnIndex()

	b.accountID = snap.AccountID
	b.region = snap.Region
	b.nextID = snap.NextID

	return nil
}

// rebuildArnIndex recomputes the region-nested ARN index from the
// just-restored domains table. arnIndex is pure derived data (never
// serialized -- see backendSnapshot's doc comment), so Restore must rebuild
// it explicitly, exactly as this backend did before the Phase 3.3 conversion.
// Callers must hold b.mu.Lock.
func (b *InMemoryBackend) rebuildArnIndex() {
	b.arnIndex = make(map[string]map[string]string)

	for _, d := range b.domains.Snapshot() {
		if b.arnIndex[d.region] == nil {
			b.arnIndex[d.region] = make(map[string]string)
		}

		b.arnIndex[d.region][d.ARN] = d.Name
	}
}

// restoreResourceTables rebuilds every store.Table on b from snap's
// per-table JSON, factored out of Restore to keep Restore's own cognitive
// complexity low. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restoreResourceTables(tables map[string]json.RawMessage) error {
	dtos := buildPersistenceDTORegistry()

	if err := dtos.registry.RestoreAll(tables); err != nil {
		return fmt.Errorf("elasticsearch: restore snapshot tables: %w", err)
	}

	b.domains.Restore(unwrapRegionalDTOs(dtos.domains, func(v *Domain, r string) { v.region = r }))
	b.packages.Restore(unwrapRegionalDTOs(dtos.packages, func(v *Package, r string) { v.region = r }))
	b.inboundConnections.Restore(
		unwrapRegionalDTOs(dtos.inboundConnections, func(v *InboundConnection, r string) { v.region = r }),
	)
	b.outboundConnections.Restore(
		unwrapRegionalDTOs(dtos.outboundConnections, func(v *OutboundConnection, r string) { v.region = r }),
	)
	b.vpcEndpoints.Restore(unwrapRegionalDTOs(dtos.vpcEndpoints, func(v *VpcEndpoint, r string) { v.region = r }))
	b.reservedInstances.Restore(
		unwrapRegionalDTOs(dtos.reservedInstances, func(v *ReservedInstance, r string) { v.region = r }),
	)

	return nil
}

// unwrapRegionalDTOs converts every regionalDTO[V] in dtos into its live *V,
// restoring the unexported region field each carries via setRegion (a plain
// generic type parameter V has no field access, so the region assignment is
// supplied by the caller, one per concrete V; see restoreResourceTables). A
// DTO whose Value is nil -- not producible by Snapshot, but a defensive
// guard against a hand-edited or corrupted snapshot -- is skipped rather than
// dereferenced.
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

// restoreRawMaps restores every plain (non-store.Table) persisted map from
// snap, defaulting each to an empty map when absent from the snapshot.
// arnIndex is handled separately by rebuildArnIndex since it is derived data,
// never persisted. Factored out of Restore to keep Restore's own cognitive
// complexity low. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restoreRawMaps(snap *backendSnapshot) {
	b.packagesByName = snap.PackagesByName
	if b.packagesByName == nil {
		b.packagesByName = make(map[string]map[string]string)
	}

	b.packageAssociations = snap.PackageAssociations
	if b.packageAssociations == nil {
		b.packageAssociations = make(map[string]map[string][]string)
	}

	b.vpcAccess = snap.VpcAccess
	if b.vpcAccess == nil {
		b.vpcAccess = make(map[string]map[string][]string)
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

// Reset implements service.Resettable by delegating to the backend.
func (h *Handler) Reset() {
	h.Backend.Reset()
}
