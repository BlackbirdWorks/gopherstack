package ecs

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// Snapshottable is an optional interface that a Backend may implement to
// support snapshot/restore for persistence or test isolation.
type Snapshottable interface {
	Snapshot(ctx context.Context) []byte
	Restore(context.Context, []byte) error
}

// ecsSnapshotVersion identifies the shape of backendSnapshot's Tables blob
// (i.e. the set/shape of resources registered on b.registry -- see
// registerAllTables in store_setup.go). It must be bumped whenever a change
// there would make an older snapshot unsafe to decode as the current shape.
// Restore compares this against the persisted value and discards (rather than
// attempts to partially decode) any mismatch -- see Restore below. This
// mirrors the ec2 (commit 12e611a4) and sqs (commit 0f09d77c) Phase 3.3
// conversions.
const ecsSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the ECS backend.
//
// Tables holds one JSON-encoded array per registry-registered store.Table
// (clusters, services, tasks, containerInstances, taskSets, capacityProviders,
// accountSettings, taskProtections, serviceDeployments, expressGatewayServices,
// daemons, daemonRevisions, daemonDeployments -- see registerAllTables),
// produced by store.Registry.SnapshotAll(). The nested cluster/service-scoped
// resources (services, tasks, containerInstances, taskSets, daemons) are
// stored flatly, keyed by their store.Table composite primary key, rather than
// as nested JSON objects the way the pre-conversion map[string]map[string]*T
// fields were -- Version guards against decoding an older snapshot (with the
// old nested shape) as though it were this shape.
//
// The remaining fields are resources deliberately left as plain maps by the
// conversion (see the exclusion list in registerAllTables' doc comment) and
// so are still serialised directly, exactly as before.
type backendSnapshot struct {
	Tables                map[string]json.RawMessage         `json:"tables"`
	TaskDefinitions       map[string][]*TaskDefinition       `json:"taskDefinitions"`
	Attributes            map[string]map[string]*Attribute   `json:"attributes"`
	DaemonTaskDefinitions map[string][]*DaemonTaskDefinition `json:"daemonTaskDefinitions"`
	// ResourceTags holds tags applied via TagResource/UntagResource, keyed by
	// resourceTagKey(resourceArn). Clusters and Services carry their tags inline
	// on their own struct, but task definitions and daemon task definitions are
	// tagged only through this side map (see TagResource/ListTagsForResource in
	// tags.go), so it must be persisted like any other resource
	// state or tags silently vanish across a snapshot/restore cycle.
	ResourceTags map[string][]Tag `json:"resourceTags"`
	Version      int              `json:"version"`
}

// snapshotResourceTags deep-copies the TagResource side map (see the
// backendSnapshot.ResourceTags doc comment for why this must be persisted
// alongside the primary resource maps).
func snapshotResourceTags(src map[string][]Tag) map[string][]Tag {
	dst := make(map[string][]Tag, len(src))
	for k, v := range src {
		dst[k] = copyTags(v)
	}

	return dst
}

func snapshotTaskDefinitions(src map[string][]*TaskDefinition) map[string][]*TaskDefinition {
	dst := make(map[string][]*TaskDefinition, len(src))
	for family, revs := range src {
		revsCp := make([]*TaskDefinition, len(revs))
		for i, td := range revs {
			cp := *td
			revsCp[i] = &cp
		}
		dst[family] = revsCp
	}

	return dst
}

func snapshotDaemonTaskDefinitions(src map[string][]*DaemonTaskDefinition) map[string][]*DaemonTaskDefinition {
	dst := make(map[string][]*DaemonTaskDefinition, len(src))
	for family, revs := range src {
		revsCp := make([]*DaemonTaskDefinition, len(revs))
		for i, td := range revs {
			cp := *td
			revsCp[i] = &cp
		}
		dst[family] = revsCp
	}

	return dst
}

func snapshotAttributes(src map[string]map[string]*Attribute) map[string]map[string]*Attribute {
	dst := make(map[string]map[string]*Attribute, len(src))
	for cluster, attrMap := range src {
		cp := make(map[string]*Attribute, len(attrMap))
		for key, attr := range attrMap {
			a := *attr
			cp[key] = &a
		}
		dst[cluster] = cp
	}

	return dst
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
		logger.Load(ctx).WarnContext(ctx, "ecs: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:               ecsSnapshotVersion,
		Tables:                tables,
		TaskDefinitions:       snapshotTaskDefinitions(b.taskDefinitions),
		Attributes:            snapshotAttributes(b.attributes),
		DaemonTaskDefinitions: snapshotDaemonTaskDefinitions(b.daemonTaskDefinitions),
		ResourceTags:          snapshotResourceTags(b.resourceTags),
	}

	return persistence.MarshalSnapshot(ctx, "ecs", snap)
}

// initSnapshotDefaults ensures all maps in the snapshot are non-nil so callers
// can safely assign them to the backend without nil-map panics.
func initSnapshotDefaults(snap *backendSnapshot) {
	if snap.TaskDefinitions == nil {
		snap.TaskDefinitions = make(map[string][]*TaskDefinition)
	}

	if snap.Attributes == nil {
		snap.Attributes = make(map[string]map[string]*Attribute)
	}

	if snap.DaemonTaskDefinitions == nil {
		snap.DaemonTaskDefinitions = make(map[string][]*DaemonTaskDefinition)
	}

	if snap.ResourceTags == nil {
		snap.ResourceTags = make(map[string][]Tag)
	}
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "ecs", data, &snap); err != nil {
		return err
	}

	initSnapshotDefaults(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != ecsSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields (e.g. the pre-conversion nested
		// map[string]map[string]*T shape for services/tasks/containerInstances/
		// taskSets/daemons vs. the flat store.Table composite-key shape).
		// Discard cleanly and start empty instead of erroring, since this is an
		// expected, recoverable condition (e.g. upgrading gopherstack across a
		// snapshot-format change), not data corruption. Mirrors the ec2/sqs
		// Phase 3.3 conversions.
		logger.Load(ctx).WarnContext(ctx,
			"ecs: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", ecsSnapshotVersion)

		b.registry.ResetAll()
		b.taskDefByArn.Reset()
		b.daemonTaskDefByArn.Reset()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("ecs: restore snapshot tables: %w", err)
	}

	b.taskDefinitions = snap.TaskDefinitions
	b.attributes = snap.Attributes
	b.daemonTaskDefinitions = snap.DaemonTaskDefinitions
	b.resourceTags = snap.ResourceTags

	// Rebuild the ARN→TaskDefinition cache from restored task definitions.
	// Not part of the registry-driven Tables blob: it is a derived cache, not
	// independently persisted state (see taskDefByArnKeyFn's doc comment).
	b.taskDefByArn.Reset()
	for _, revs := range b.taskDefinitions {
		for _, td := range revs {
			b.taskDefByArn.Put(td)
		}
	}

	// Rebuild the ARN→DaemonTaskDefinition cache from restored daemon task definitions.
	b.daemonTaskDefByArn.Reset()
	for _, revs := range b.daemonTaskDefinitions {
		for _, td := range revs {
			b.daemonTaskDefByArn.Put(td)
		}
	}

	// Rebuild the flat serviceIndex from the restored services table. Unlike
	// tasksByInstance (which enrichContainerInstance falls back to a linear scan
	// for), getServicesForReconciler reads serviceIndex with no fallback: leaving
	// it empty after a restore would silently stop the deployment reconciler
	// from ever seeing pre-existing services again (deployments/scaling would
	// freeze forever post-restart).
	allServices := b.services.All()
	b.serviceIndex = make(map[svcRef]bool, len(allServices))

	for _, svc := range allServices {
		b.serviceIndex[svcRef{cluster: clusterKey(svc.ClusterArn), name: svc.ServiceName}] = true
	}

	// Rebuild the containerInstance→task reverse index from the restored tasks
	// table. enrichContainerInstance already falls back to a linear scan when
	// this index is empty, but rebuilding it here keeps post-restore behavior on
	// the fast O(k) path instead of silently degrading to O(n) for every describe.
	b.tasksByInstance = make(map[string]map[string]map[string]bool, len(allServices))
	for _, task := range b.tasks.All() {
		b.indexTaskOnInstance(clusterKey(task.ClusterArn), task.ContainerInstanceArn, task.TaskArn)
	}

	return nil
}

// Snapshot implements Snapshottable by delegating to the backend when it
// supports snapshotting. Returns nil for non-snapshottable backends.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Snapshot(ctx)
	}

	return nil
}

// Restore implements Snapshottable by delegating to the backend when it
// supports snapshotting. Non-snapshottable backends are skipped.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Restore(ctx, data)
	}

	return nil
}
