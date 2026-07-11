package route53

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// route53SnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to zoneDataSnapshot, backendSnapshot, or any table
// registered below would make an older snapshot unsafe to decode as the
// current shape. Restore compares this against the persisted value and
// discards (rather than attempts to partially decode) any mismatch — see
// Restore.
const route53SnapshotVersion = 1

// zoneDataSnapshot captures the serialisable fields of a zoneData. It exists
// as a separate DTO (rather than JSON tags directly on zoneData) because
// zoneData's fields are unexported -- encoding/json cannot marshal them at
// all, let alone round-trip them -- making zones the one "dirty" table in
// this backend that needs its own DTO-registry (see store_setup.go's doc
// comment for the clean/dirty split across all converted tables).
type zoneDataSnapshot struct {
	Records       map[string]*ResourceRecordSet `json:"records"`
	Zone          HostedZone                    `json:"zone"`
	DNSSECEnabled bool                          `json:"dnssecEnabled,omitempty"`
}

// zoneDataSnapshotKey is the [store.Table] key function used for the
// ephemeral DTO table built inside Snapshot/Restore. It mirrors zoneKeyFn so
// the on-disk table is keyed identically to the live b.zones table.
func zoneDataSnapshotKey(zs *zoneDataSnapshot) string { return zs.Zone.ID }

// backendSnapshot is the top-level on-disk shape for the Route 53 backend.
//
// Tables holds one JSON-encoded array per registered table, produced by
// [store.Registry.SnapshotAll]: "zones" ([]*zoneDataSnapshot, via a throwaway
// DTO registry because zoneData is "dirty"), plus the seven "clean" tables
// registered directly on b.registry ("healthChecks", "keySigningKeys",
// "cidrCollections", "queryLoggingConfigs", "reusableDelegationSets",
// "trafficPolicyInstances", "changes") whose live struct types are already
// JSON-safe. TrafficPolicies, VPCAssociations, VPCAssocAuthorizations, and
// Tags are the fields store_setup.go documents as left as plain maps, so they
// are marshaled the same way they always were, outside the Tables map.
// Version guards against decoding a snapshot from an incompatible (older or
// newer) build of this backend as though it were the current shape; see
// Restore.
type backendSnapshot struct {
	Tables                 map[string]json.RawMessage               `json:"tables"`
	TrafficPolicies        map[string][]*TrafficPolicy              `json:"trafficPolicies,omitempty"`
	VPCAssociations        map[string][]vpcAssociation              `json:"vpcAssociations,omitempty"`
	VPCAssocAuthorizations map[string][]VPCAssociationAuthorization `json:"vpcAssocAuthorizations,omitempty"`
	Tags                   map[string]*svcTags.Tags                 `json:"tags,omitempty"`
	Version                int                                      `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	// The seven "clean" tables can be snapshotted straight off b.registry:
	// their live struct types (HealthCheck, KeySigningKey, CidrCollection,
	// QueryLoggingConfig, ReusableDelegationSet, TrafficPolicyInstance,
	// ChangeInfo) are already JSON-safe, so no DTO mapping is needed.
	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "route53: snapshot table marshal failed", "error", err)

		return nil
	}

	// zones is "dirty" (zoneData's fields are all unexported) so it needs a
	// throwaway DTO registry purely to reuse store's deterministic, type-erased
	// JSON encoding instead of hand-rolling the marshal step.
	dtoReg := store.NewRegistry()
	zoneDTOs := store.Register(dtoReg, "zones", store.New(zoneDataSnapshotKey))

	for _, zd := range b.zones.Snapshot() {
		zoneDTOs.Put(&zoneDataSnapshot{
			Zone:          zd.zone,
			Records:       zd.records,
			DNSSECEnabled: zd.dnssecEnabled,
		})
	}

	zoneTables, err := dtoReg.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "route53: snapshot zones marshal failed", "error", err)

		return nil
	}

	tables["zones"] = zoneTables["zones"]

	snap := backendSnapshot{
		Version:                route53SnapshotVersion,
		Tables:                 tables,
		TrafficPolicies:        b.trafficPolicies,
		VPCAssociations:        b.vpcAssociations,
		VPCAssocAuthorizations: b.vpcAssocAuthorizations,
		Tags:                   b.tags,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "route53 Snapshot failed", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
// The DNS registrar is not restored — it must be re-wired by the caller after restore.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "route53", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != route53SnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape — that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"route53: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", route53SnapshotVersion)

		b.registry.ResetAll()
		b.trafficPolicies = make(map[string][]*TrafficPolicy)
		b.vpcAssociations = make(map[string][]vpcAssociation)
		b.vpcAssocAuthorizations = make(map[string][]VPCAssociationAuthorization)
		b.tags = make(map[string]*svcTags.Tags)

		return nil
	}

	// b.registry.RestoreAll restores the seven "clean" tables directly, but
	// must never see "zones": zoneData's fields are all unexported, so
	// unmarshaling snap.Tables["zones"] into the live Table[zoneData] would
	// silently produce zero-valued garbage (every entry keying to the same
	// empty zone ID). zones is restored separately below via its own DTO
	// registry, so it is excluded here.
	cleanTables := make(map[string]json.RawMessage, len(snap.Tables))

	for name, raw := range snap.Tables {
		if name == "zones" {
			continue
		}

		cleanTables[name] = raw
	}

	if err := b.registry.RestoreAll(cleanTables); err != nil {
		return fmt.Errorf("route53: restore snapshot tables: %w", err)
	}

	dtoReg := store.NewRegistry()
	zoneDTOs := store.Register(dtoReg, "zones", store.New(zoneDataSnapshotKey))

	if err := dtoReg.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("route53: restore snapshot zones: %w", err)
	}

	liveZones := make([]*zoneData, 0, zoneDTOs.Len())

	for _, zs := range zoneDTOs.All() {
		records := zs.Records
		if records == nil {
			records = make(map[string]*ResourceRecordSet)
		}

		liveZones = append(liveZones, &zoneData{
			zone:          zs.Zone,
			records:       records,
			dnssecEnabled: zs.DNSSECEnabled,
		})
	}

	b.zones.Restore(liveZones)

	b.trafficPolicies = snap.TrafficPolicies
	if b.trafficPolicies == nil {
		b.trafficPolicies = make(map[string][]*TrafficPolicy)
	}

	b.vpcAssociations = snap.VPCAssociations
	if b.vpcAssociations == nil {
		b.vpcAssociations = make(map[string][]vpcAssociation)
	}

	b.vpcAssocAuthorizations = snap.VPCAssocAuthorizations
	if b.vpcAssocAuthorizations == nil {
		b.vpcAssocAuthorizations = make(map[string][]VPCAssociationAuthorization)
	}

	b.tags = snap.Tags
	if b.tags == nil {
		b.tags = make(map[string]*svcTags.Tags)
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
