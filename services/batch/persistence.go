package batch

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// batchSnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to the DTOs/backendSnapshot itself would make an
// older snapshot unsafe to decode as the current shape (e.g. a field's
// meaning or type changes). Restore compares this against the persisted value
// and discards (rather than attempts to partially decode) any mismatch -- see
// Restore below. This is the first version: earlier (pre-Phase-3.3) snapshots
// carried no version field at all, so they also fail this check and are
// discarded rather than misread, which is the safe behaviour across any
// snapshot-format change.
const batchSnapshotVersion = 1

// regionalDTO wraps a region-nested resource for JSON round-tripping through
// store.Registry. It is shared by every converted resource here (all eight
// hide only a single "region" field -- see the InMemoryBackend doc comment in
// backend.go), so unlike services/emr this refactor needs no per-type DTO.
// ID mirrors the resource's own primary-key component (its name, or its ARN
// for the two resources keyed by ARN) so the DTO table itself has a stable
// composite key ("region|id") matching the live table.
type regionalDTO[V any] struct {
	Value  *V     `json:"value"`
	Region string `json:"region"`
	ID     string `json:"id"`
}

// regionalDTOKeyFn is the shared [store.Table] key function for every
// regionalDTO[V] table below; it mirrors the "region|id" composite key each
// live table uses (see regionKey in backend.go).
func regionalDTOKeyFn[V any](d *regionalDTO[V]) string { return regionKey(d.Region, d.ID) }

// backendSnapshot is the top-level on-disk shape for the Batch backend.
//
// Tables holds one JSON-encoded array per registered DTO table, produced by
// [store.Registry.SnapshotAll] -- the eight converted resource collections,
// each wrapped in a regionalDTO to carry its hidden region field through.
// JobDefRevisions is still a plain region-nested map (see store_setup.go for
// why it was not converted to a store.Table) and is persisted directly, as
// before. cesByARN, jqsByARN, crsByARN and ceToQueues were never persisted
// pre-refactor and remain unpersisted (see the InMemoryBackend doc comment in
// backend.go). Version guards against decoding a snapshot from an
// incompatible (older or newer) build of this backend as though it were the
// current shape; see Restore.
type backendSnapshot struct {
	Tables          map[string]json.RawMessage  `json:"tables"`
	JobDefRevisions map[string]map[string]int32 `json:"jobDefRevisions"`
	AccountID       string                      `json:"accountID"`
	Region          string                      `json:"region"`
	Version         int                         `json:"version"`
}

// persistenceDTOTables groups every ephemeral DTO table
// buildPersistenceDTORegistry constructs, so Snapshot/Restore can pass them
// around as one value instead of eight separate return values.
type persistenceDTOTables struct {
	registry            *store.Registry
	computeEnvironments *store.Table[regionalDTO[ComputeEnvironment]]
	jobQueues           *store.Table[regionalDTO[JobQueue]]
	jobDefinitions      *store.Table[regionalDTO[JobDefinition]]
	jobs                *store.Table[regionalDTO[Job]]
	consumableResources *store.Table[regionalDTO[ConsumableResource]]
	schedulingPolicies  *store.Table[regionalDTO[SchedulingPolicy]]
	serviceEnvironments *store.Table[regionalDTO[ServiceEnvironment]]
	serviceJobs         *store.Table[regionalDTO[ServiceJob]]
}

// buildPersistenceDTORegistry constructs the ephemeral DTO registry used by
// both Snapshot and Restore. It is built fresh on every call (rather than
// reusing b.registry) because the DTO value types (regionalDTO[V]) differ
// from the live table value types (V).
func buildPersistenceDTORegistry() persistenceDTOTables {
	dtoReg := store.NewRegistry()

	return persistenceDTOTables{
		registry: dtoReg,
		computeEnvironments: store.Register(
			dtoReg, "computeEnvironments", store.New(regionalDTOKeyFn[ComputeEnvironment]),
		),
		jobQueues: store.Register(dtoReg, "jobQueues", store.New(regionalDTOKeyFn[JobQueue])),
		jobDefinitions: store.Register(
			dtoReg, "jobDefinitions", store.New(regionalDTOKeyFn[JobDefinition]),
		),
		jobs: store.Register(dtoReg, "jobs", store.New(regionalDTOKeyFn[Job])),
		consumableResources: store.Register(
			dtoReg, "consumableResources", store.New(regionalDTOKeyFn[ConsumableResource]),
		),
		schedulingPolicies: store.Register(
			dtoReg, "schedulingPolicies", store.New(regionalDTOKeyFn[SchedulingPolicy]),
		),
		serviceEnvironments: store.Register(
			dtoReg, "serviceEnvironments", store.New(regionalDTOKeyFn[ServiceEnvironment]),
		),
		serviceJobs: store.Register(dtoReg, "serviceJobs", store.New(regionalDTOKeyFn[ServiceJob])),
	}
}

// Snapshot serialises the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	dtos := buildPersistenceDTORegistry()

	for _, v := range b.computeEnvironments.Snapshot() {
		dtos.computeEnvironments.Put(&regionalDTO[ComputeEnvironment]{
			Region: v.region, ID: v.ComputeEnvironmentName, Value: v,
		})
	}

	for _, v := range b.jobQueues.Snapshot() {
		dtos.jobQueues.Put(&regionalDTO[JobQueue]{Region: v.region, ID: v.JobQueueName, Value: v})
	}

	for _, v := range b.jobDefinitions.Snapshot() {
		dtos.jobDefinitions.Put(&regionalDTO[JobDefinition]{Region: v.region, ID: v.JobDefinitionArn, Value: v})
	}

	for _, v := range b.jobs.Snapshot() {
		dtos.jobs.Put(&regionalDTO[Job]{Region: v.region, ID: v.JobID, Value: v})
	}

	for _, v := range b.consumableResources.Snapshot() {
		dtos.consumableResources.Put(&regionalDTO[ConsumableResource]{
			Region: v.region, ID: v.ConsumableResourceName, Value: v,
		})
	}

	for _, v := range b.schedulingPolicies.Snapshot() {
		dtos.schedulingPolicies.Put(&regionalDTO[SchedulingPolicy]{Region: v.region, ID: v.Arn, Value: v})
	}

	for _, v := range b.serviceEnvironments.Snapshot() {
		dtos.serviceEnvironments.Put(&regionalDTO[ServiceEnvironment]{
			Region: v.region, ID: v.ServiceEnvironmentName, Value: v,
		})
	}

	for _, v := range b.serviceJobs.Snapshot() {
		dtos.serviceJobs.Put(&regionalDTO[ServiceJob]{Region: v.region, ID: v.JobID, Value: v})
	}

	tables, err := dtos.registry.SnapshotAll()
	if err != nil {
		// The DTOs above are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the
		// Manager).
		logger.Load(ctx).WarnContext(ctx, "batch: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:         batchSnapshotVersion,
		Tables:          tables,
		JobDefRevisions: b.jobDefRevisions,
		AccountID:       b.accountID,
		Region:          b.region,
	}

	return persistence.MarshalSnapshot(ctx, "batch", snap)
}

// Restore loads backend state from a JSON snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "batch", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != batchSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"batch: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", batchSnapshotVersion)

		b.registry.ResetAll()
		b.jobDefRevisions = make(map[string]map[string]int32)
		b.cesByARN = make(map[string]string)
		b.jqsByARN = make(map[string]string)
		b.crsByARN = make(map[string]string)
		b.ceToQueues = make(map[string]map[string]struct{})
		b.accountID = snap.AccountID
		b.region = snap.Region

		return nil
	}

	if err := b.restoreResourceTables(snap.Tables); err != nil {
		return err
	}

	if snap.JobDefRevisions != nil {
		b.jobDefRevisions = snap.JobDefRevisions
	} else {
		b.jobDefRevisions = make(map[string]map[string]int32)
	}

	// cesByARN, jqsByARN, crsByARN and ceToQueues were never persisted
	// pre-refactor (see the InMemoryBackend doc comment in backend.go) and
	// are reset empty here exactly as the old initMaps()-based Restore did --
	// preserved as-is, not fixed, per the mechanical-conversion scope.
	b.cesByARN = make(map[string]string)
	b.jqsByARN = make(map[string]string)
	b.crsByARN = make(map[string]string)
	b.ceToQueues = make(map[string]map[string]struct{})

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// unwrapRegionalDTOs converts every regionalDTO[V] in dtos into its live *V,
// restoring the unexported region field each carries via setRegion (a plain
// generic type parameter V has no field access, so the region assignment is
// supplied by the caller, one per concrete V; see restoreResourceTables). A
// DTO whose Value is nil -- not producible by Snapshot, but a defensive guard
// against a hand-edited or corrupted snapshot -- is skipped rather than
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

// restoreResourceTables rebuilds every store.Table on b from snap's
// per-table JSON, factored out of Restore to keep Restore's own cognitive
// complexity low. Table.Restore also rebuilds every index registered on each
// table (byRegion, byARN, byQueue, byName -- see store_setup.go), so unlike
// the pre-refactor Restore this needs no separate index-rebuild step.
// Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restoreResourceTables(tables map[string]json.RawMessage) error {
	dtos := buildPersistenceDTORegistry()

	if err := dtos.registry.RestoreAll(tables); err != nil {
		return fmt.Errorf("batch: restore snapshot tables: %w", err)
	}

	b.computeEnvironments.Restore(unwrapRegionalDTOs(
		dtos.computeEnvironments, func(v *ComputeEnvironment, r string) { v.region = r },
	))
	b.jobQueues.Restore(unwrapRegionalDTOs(dtos.jobQueues, func(v *JobQueue, r string) { v.region = r }))
	b.jobDefinitions.Restore(unwrapRegionalDTOs(
		dtos.jobDefinitions, func(v *JobDefinition, r string) { v.region = r },
	))
	b.jobs.Restore(unwrapRegionalDTOs(dtos.jobs, func(v *Job, r string) { v.region = r }))
	b.consumableResources.Restore(unwrapRegionalDTOs(
		dtos.consumableResources, func(v *ConsumableResource, r string) { v.region = r },
	))
	b.schedulingPolicies.Restore(unwrapRegionalDTOs(
		dtos.schedulingPolicies, func(v *SchedulingPolicy, r string) { v.region = r },
	))
	b.serviceEnvironments.Restore(unwrapRegionalDTOs(
		dtos.serviceEnvironments, func(v *ServiceEnvironment, r string) { v.region = r },
	))
	b.serviceJobs.Restore(unwrapRegionalDTOs(dtos.serviceJobs, func(v *ServiceJob, r string) { v.region = r }))

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
