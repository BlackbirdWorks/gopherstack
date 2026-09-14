package dms

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// dmsSnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to a registered table's value type or
// backendSnapshot itself would make an older snapshot unsafe to decode as
// the current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll, not a partial decode) any mismatch -- see
// Restore. The pre-Phase-3.3 snapshot format had no version field at all, so
// an old snapshot decodes with Version == 0, which is guaranteed to mismatch
// dmsSnapshotVersion and is discarded the same way any other incompatible
// snapshot is.
//
// Phase 3.3 also widens what survives a Snapshot/Restore round trip: every
// table registered on b.registry (see store_setup.go) is now included,
// whereas the pre-3.3 backendSnapshot only carried 8 of the 16 resource
// collections (certificates, replicationSubnetGroups, migrationProjects,
// replicationConfigs, connections, assessmentRuns, fleetAdvisorDatabases,
// and metadataModelRequests were previously dropped on every restart). This
// is a deliberate persistence-completeness fix that falls naturally out of
// routing every table through one registry.SnapshotAll()/RestoreAll() call
// rather than hand-listing which fields to persist; it does not change any
// AWS wire response shape.
const dmsSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the DMS backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// [store.Registry.SnapshotAll]. Every resource type here is directly
// JSON-serializable (each carries proper json tags on its identity fields;
// only the backend-owned Tags field is `json:"-"`), so no DTO layer is
// needed the way services/sqs and services/ses required for types with
// non-serializable live fields. ResourceTags is the side table that
// survives what Tables can't carry -- see reinitTagsLocked.
type backendSnapshot struct {
	Tables       map[string]json.RawMessage   `json:"tables"`
	ResourceTags map[string]map[string]string `json:"resourceTags,omitempty"`
	AccountID    string                       `json:"accountID"`
	Region       string                       `json:"region"`
	Version      int                          `json:"version"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "dms: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:      dmsSnapshotVersion,
		Tables:       tables,
		ResourceTags: b.resourceTagsSnapshot(),
		AccountID:    b.accountID,
		Region:       b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "dms: Snapshot marshal failure", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot produced by Snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "dms", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != dmsSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"dms: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", dmsSnapshotVersion)

		b.registry.ResetAll()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("dms: restore snapshot tables: %w", err)
	}

	b.reinitTagsLocked(snap.ResourceTags)

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// resourceTagsSnapshot returns the ARN -> tag-map side table for every
// taggable DMS resource reachable through AddTagsToResource/
// ListTagsForResource/RemoveTagsFromResource (see findResourceTags in
// tags.go) -- the 9 of the 11 Tags-carrying structs that have an ARN at all.
// EventSubscription and FleetAdvisorCollector are excluded: neither carries
// an ARN field, matching the real AWS types (types.go:1161-1204 has no ARN
// member; CreateFleetAdvisorCollectorInput in api_op_CreateFleetAdvisorCollector.go
// has no Tags member at all), so they are not addressable through the
// generic tagging API in this backend or in AWS. Caller must hold at least
// b.mu's read lock.
func (b *InMemoryBackend) resourceTagsSnapshot() map[string]map[string]string {
	out := make(map[string]map[string]string)

	addTags := func(arn string, t *tags.Tags) {
		if t == nil || t.Len() == 0 {
			return
		}

		out[arn] = t.Clone()
	}

	for _, ri := range b.replicationInstances.All() {
		addTags(ri.ReplicationInstanceArn, ri.Tags)
	}

	for _, ep := range b.endpoints.All() {
		addTags(ep.EndpointArn, ep.Tags)
	}

	for _, rt := range b.replicationTasks.All() {
		addTags(rt.ReplicationTaskArn, rt.Tags)
	}

	for _, dm := range b.dataMigrations.All() {
		addTags(dm.DataMigrationArn, dm.Tags)
	}

	for _, dp := range b.dataProviders.All() {
		addTags(dp.DataProviderArn, dp.Tags)
	}

	for _, ip := range b.instanceProfiles.All() {
		addTags(ip.InstanceProfileArn, ip.Tags)
	}

	for _, mp := range b.migrationProjects.All() {
		addTags(mp.MigrationProjectArn, mp.Tags)
	}

	for _, sg := range b.replicationSubnetGroups.All() {
		addTags(sg.ReplicationSubnetGroupArn, sg.Tags)
	}

	for _, rc := range b.replicationConfigs.All() {
		addTags(rc.ReplicationConfigArn, rc.Tags)
	}

	return out
}

// reinitTags re-creates the Tags registry (via tagsOf, a pointer to the
// value's Tags field) on every value in t whose Tags is currently nil, named
// prefix+name(v)+".tags". When arnOf is non-nil, the freshly created
// container is merged with resourceTags[arnOf(v)] so persisted tag values
// survive the restore; arnOf is nil for the two resource kinds with no ARN
// (EventSubscription, FleetAdvisorCollector), which get an empty container
// same as before. It is the generic building block reinitTagsLocked calls
// once per Tags-carrying table.
func reinitTags[V any](
	t *store.Table[V], tagsOf func(*V) **tags.Tags, name func(*V) string, prefix string,
	resourceTags map[string]map[string]string, arnOf func(*V) string,
) {
	for _, v := range t.All() {
		tp := tagsOf(v)
		if *tp != nil {
			continue
		}

		*tp = tags.New(prefix + name(v) + ".tags")

		if arnOf != nil {
			(*tp).Merge(resourceTags[arnOf(v)])
		}
	}
}

// reinitTagsLocked re-creates the Tags registry on every restored value
// across every table whose value type carries one, merging in resourceTags
// (the side table produced by resourceTagsSnapshot) for the 9 kinds that
// have an ARN. Tags is `json:"-"` on every resource struct (it is
// backend-owned, concurrency-safe, live state -- see the doc comments on
// ReplicationInstance etc. -- not on-disk state), so [store.Table.Restore]
// always leaves it nil; reinitTags recreates the container and this is what
// repopulates it, instead of leaving every resource's tags empty after a
// restart. This mirrors the pre-Phase-3.3 rebuildRI/rebuildEP/... helpers,
// minus their ARN-index rebuilding, which store.Table.Restore now handles
// automatically via every registered store.Index. The caller MUST hold b.mu
// for writing.
func (b *InMemoryBackend) reinitTagsLocked(resourceTags map[string]map[string]string) {
	reinitTags(b.replicationInstances, func(v *ReplicationInstance) **tags.Tags { return &v.Tags },
		func(v *ReplicationInstance) string { return v.ReplicationInstanceIdentifier }, "dms.replication-instance.",
		resourceTags, func(v *ReplicationInstance) string { return v.ReplicationInstanceArn })
	reinitTags(b.endpoints, func(v *Endpoint) **tags.Tags { return &v.Tags },
		func(v *Endpoint) string { return v.EndpointIdentifier }, "dms.endpoint.",
		resourceTags, func(v *Endpoint) string { return v.EndpointArn })
	reinitTags(b.replicationTasks, func(v *ReplicationTask) **tags.Tags { return &v.Tags },
		func(v *ReplicationTask) string { return v.ReplicationTaskIdentifier }, "dms.task.",
		resourceTags, func(v *ReplicationTask) string { return v.ReplicationTaskArn })
	reinitTags(b.dataMigrations, func(v *DataMigration) **tags.Tags { return &v.Tags },
		func(v *DataMigration) string { return v.DataMigrationName }, "dms.data-migration.",
		resourceTags, func(v *DataMigration) string { return v.DataMigrationArn })
	reinitTags(b.dataProviders, func(v *DataProvider) **tags.Tags { return &v.Tags },
		func(v *DataProvider) string { return v.DataProviderName }, "dms.data-provider.",
		resourceTags, func(v *DataProvider) string { return v.DataProviderArn })
	reinitTags(b.eventSubscriptions, func(v *EventSubscription) **tags.Tags { return &v.Tags },
		func(v *EventSubscription) string { return v.SubscriptionName }, "dms.event-subscription.", nil, nil)
	reinitTags(b.fleetAdvisorCollectors, func(v *FleetAdvisorCollector) **tags.Tags { return &v.Tags },
		func(v *FleetAdvisorCollector) string { return v.CollectorName }, "dms.fleet-advisor-collector.", nil, nil)
	reinitTags(b.instanceProfiles, func(v *InstanceProfile) **tags.Tags { return &v.Tags },
		func(v *InstanceProfile) string { return v.InstanceProfileName }, "dms.instance-profile.",
		resourceTags, func(v *InstanceProfile) string { return v.InstanceProfileArn })
	reinitTags(b.replicationSubnetGroups, func(v *ReplicationSubnetGroup) **tags.Tags { return &v.Tags },
		func(v *ReplicationSubnetGroup) string { return v.ReplicationSubnetGroupIdentifier },
		"dms.replication-subnet-group.",
		resourceTags, func(v *ReplicationSubnetGroup) string { return v.ReplicationSubnetGroupArn })
	reinitTags(b.migrationProjects, func(v *MigrationProject) **tags.Tags { return &v.Tags },
		func(v *MigrationProject) string { return v.MigrationProjectName }, "dms.migration-project.",
		resourceTags, func(v *MigrationProject) string { return v.MigrationProjectArn })
	reinitTags(b.replicationConfigs, func(v *ReplicationConfig) **tags.Tags { return &v.Tags },
		func(v *ReplicationConfig) string { return v.ReplicationConfigIdentifier }, "dms.replication-config.",
		resourceTags, func(v *ReplicationConfig) string { return v.ReplicationConfigArn })
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
