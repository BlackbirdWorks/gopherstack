package resourcegroups

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// resourcegroupsSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to a DTO type, a registered table's value
// type, or backendSnapshot itself would make an older snapshot unsafe to
// decode as the current shape. Restore compares this against the persisted
// value and discards (rather than partially decodes) any mismatch -- see
// Restore below. This is the first version: earlier (pre-Phase-3.3) snapshots
// carried no version field at all, so they decode as 0 and are discarded
// rather than misread, which is the safe behaviour across any
// snapshot-format change.
const resourcegroupsSnapshotVersion = 1

// groupSnapshot is the serialisation-friendly representation of Group. It
// replaces the live *tags.Tags pointer with a plain map so JSON round-trips
// work without carrying Prometheus metric state. It doubles as the
// store.Table DTO value type for the "groups" snapshot table (see
// buildPersistenceDTORegistry); its ARN carries the live table's composite-key
// region through the round-trip (see groupRegionOf in backend.go).
type groupSnapshot struct {
	Tags           map[string]string `json:"tags,omitempty"`
	ResourceQuery  *ResourceQuery    `json:"resourceQuery,omitempty"`
	ApplicationTag map[string]string `json:"applicationTag,omitempty"`
	Name           string            `json:"name"`
	ARN            string            `json:"arn"`
	Description    string            `json:"description,omitempty"`
	OwnerID        string            `json:"ownerId,omitempty"`
	DisplayName    string            `json:"displayName,omitempty"`
	Criticality    int               `json:"criticality,omitempty"`
}

// groupSnapshotKeyFn keys the groups DTO table by ARN, which uniquely
// identifies a group (its region and name are both encoded in it).
func groupSnapshotKeyFn(v *groupSnapshot) string { return v.ARN }

// toGroupSnapshot builds the plain-map DTO for a live group.
func toGroupSnapshot(g *Group) *groupSnapshot {
	var tagMap map[string]string
	if g.Tags != nil {
		tagMap = g.Tags.Clone()
	}

	return &groupSnapshot{
		Name:           g.Name,
		ARN:            g.ARN,
		Description:    g.Description,
		OwnerID:        g.Owner,
		DisplayName:    g.DisplayName,
		Criticality:    g.Criticality,
		ResourceQuery:  g.ResourceQuery,
		ApplicationTag: g.ApplicationTag,
		Tags:           tagMap,
	}
}

// groupFromSnapshot rebuilds a live Group from its persisted representation,
// giving it a freshly (and correctly) named Tags instance -- see the Group
// field comment in backend.go for why the Prometheus metric name must match
// "rg.<name>.tags" exactly.
func groupFromSnapshot(s *groupSnapshot) *Group {
	return &Group{
		Name:           s.Name,
		ARN:            s.ARN,
		Description:    s.Description,
		Owner:          s.OwnerID,
		DisplayName:    s.DisplayName,
		Criticality:    s.Criticality,
		ResourceQuery:  s.ResourceQuery,
		ApplicationTag: s.ApplicationTag,
		Tags:           tags.FromMap("rg."+s.Name+".tags", s.Tags),
	}
}

// buildPersistenceDTORegistry constructs the ephemeral DTO registry used by
// both Snapshot and Restore for the "dirty" groups table (see store_setup.go's
// registerAllTables doc). It is built fresh on every call because the DTO
// value type (groupSnapshot) differs from the live table value type (Group).
func buildPersistenceDTORegistry() (*store.Registry, *store.Table[groupSnapshot]) {
	dtoReg := store.NewRegistry()
	groupDTOs := store.Register(dtoReg, "groups", store.New(groupSnapshotKeyFn))

	return dtoReg, groupDTOs
}

// backendSnapshot is the top-level on-disk shape for the Resource Groups
// backend.
//
// Tables holds one JSON-encoded array per table name: "tagSyncTasks" (the one
// clean table on b.registry) plus "groups" (the DTO table built above for the
// dirty groups table) -- see store_setup.go's registerAllTables doc for the
// clean/dirty split.
//
// GroupConfigurations, GroupResources, and GroupingStatuses are the plain
// region-nested maps left unconverted by Phase 3.3 (see store_setup.go):
// each holds a value with no identity of its own (a slice of items, ARN
// strings, or status records), so there is nothing for store.Table to key on,
// and each is persisted here directly instead.
type backendSnapshot struct {
	Tables              map[string]json.RawMessage                     `json:"tables"`
	GroupConfigurations map[string]map[string][]GroupConfigurationItem `json:"groupConfigurations"`
	GroupResources      map[string]map[string][]string                 `json:"groupResources"`
	GroupingStatuses    map[string]map[string][]GroupingStatusItem     `json:"groupingStatuses"`
	AccountSettings     AccountSettings                                `json:"accountSettings"`
	AccountID           string                                         `json:"accountID"`
	Region              string                                         `json:"region"`
	Version             int                                            `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "resourcegroups: failed to snapshot backend", "error", err)

		return nil
	}

	dtoReg, groupDTOs := buildPersistenceDTORegistry()

	for _, g := range b.groups.Snapshot() {
		groupDTOs.Put(toGroupSnapshot(g))
	}

	dtoTables, err := dtoReg.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "resourcegroups: failed to snapshot groups table", "error", err)

		return nil
	}

	maps.Copy(tables, dtoTables)

	snap := backendSnapshot{
		Version:             resourcegroupsSnapshotVersion,
		Tables:              tables,
		GroupConfigurations: b.groupConfigurations,
		GroupResources:      b.groupResources,
		GroupingStatuses:    b.groupingStatuses,
		AccountSettings:     b.accountSettings,
		AccountID:           b.accountID,
		Region:              b.region,
	}

	return persistence.MarshalSnapshot(ctx, "resourcegroups", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "resourcegroups", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.closeAllGroupTags()

	if snap.Version != resourcegroupsSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"resourcegroups: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", resourcegroupsSnapshotVersion)

		b.registry.ResetAll()
		b.groups.Reset()
		b.groupConfigurations = make(map[string]map[string][]GroupConfigurationItem)
		b.groupResources = make(map[string]map[string][]string)
		b.groupingStatuses = make(map[string]map[string][]GroupingStatusItem)
		b.accountSettings = AccountSettings{}
		b.accountID = snap.AccountID
		b.region = snap.Region

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("resourcegroups: restore snapshot tables: %w", err)
	}

	if err := b.restoreGroups(snap.Tables); err != nil {
		return err
	}

	b.restorePlainMaps(&snap)

	b.accountSettings = snap.AccountSettings
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// restoreGroups rebuilds the "dirty" groups store.Table from snap's DTO
// table, factored out of Restore to keep Restore's own cognitive complexity
// low. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restoreGroups(tables map[string]json.RawMessage) error {
	dtoReg, groupDTOs := buildPersistenceDTORegistry()
	if err := dtoReg.RestoreAll(tables); err != nil {
		return fmt.Errorf("resourcegroups: restore snapshot groups table: %w", err)
	}

	groups := make([]*Group, 0, groupDTOs.Len())
	for _, s := range groupDTOs.All() {
		groups = append(groups, groupFromSnapshot(s))
	}

	b.groups.Restore(groups)

	return nil
}

// restorePlainMaps restores every plain (non-store.Table) persisted map from
// snap, defaulting each to an empty map when absent from the snapshot.
// Factored out of Restore to keep Restore's own cognitive complexity low.
// Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restorePlainMaps(snap *backendSnapshot) {
	b.groupConfigurations = snap.GroupConfigurations
	if b.groupConfigurations == nil {
		b.groupConfigurations = make(map[string]map[string][]GroupConfigurationItem)
	}

	b.groupResources = snap.GroupResources
	if b.groupResources == nil {
		b.groupResources = make(map[string]map[string][]string)
	}

	b.groupingStatuses = snap.GroupingStatuses
	if b.groupingStatuses == nil {
		b.groupingStatuses = make(map[string]map[string][]GroupingStatusItem)
	}
}

// closeAllGroupTags releases Prometheus metrics for every group before replacing state.
// Must be called under b.mu (write lock).
func (b *InMemoryBackend) closeAllGroupTags() {
	for _, g := range b.groups.All() {
		if g.Tags != nil {
			g.Tags.Close()
		}
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
