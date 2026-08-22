package guardduty

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// guarddutySnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to a DTO type, a registered table's value
// type, or backendSnapshot itself would make an older snapshot unsafe to
// decode as the current shape. Restore compares this against the persisted
// value and discards (registry.ResetAll plus the dirty tables' own Reset,
// not a partial decode) any mismatch -- see Restore below. Version 1 was the
// first version: the pre-Phase-3.3 snapshot had a different top-level shape
// (one field per resource map, no version field at all), so an old snapshot
// decodes with Version == 0, which is guaranteed to mismatch
// guarddutySnapshotVersion and is discarded the same way any other
// incompatible snapshot is. Version 2 (gopherstack-tp8x item 6) dropped
// MalwareScan.TriggerDetails, a registered table's value type, so a v1
// snapshot with that field present must not be decoded as v2.
const guarddutySnapshotVersion = 2

// detectorDTO wraps a detector-nested resource whose only hidden field is
// DetectorID (Filter, IPSet, ThreatIntelSet, PublishingDestination,
// ThreatEntitySet, TrustedEntitySet) for JSON round-tripping through the
// ephemeral persistence registry below -- the same technique
// services/codeartifact's regionalDTO[V] uses, qualified by detector instead
// of region. ID mirrors the resource's own composite-key component (see
// store_setup.go's *TableKeyFn) so the DTO table itself has a stable
// composite key ("DetectorID|ID").
type detectorDTO[V any] struct {
	Value      *V     `json:"value"`
	DetectorID string `json:"detectorId"`
	ID         string `json:"id"`
}

func detectorDTOKeyFn[V any](d *detectorDTO[V]) string { return detectorKey(d.DetectorID, d.ID) }

// byDetectorDTO wraps a resource that is keyed directly by detectorID (one
// value per detector, not per-resource) -- OrgConfig, AdminAccount,
// MalwareScanSettings. Unlike detectorDTO there is no separate ID: DetectorID
// itself is the whole primary key.
type byDetectorDTO[V any] struct {
	Value      *V     `json:"value"`
	DetectorID string `json:"detectorId"`
}

func byDetectorDTOKeyFn[V any](d *byDetectorDTO[V]) string { return d.DetectorID }

// persistenceDTOTables groups every ephemeral DTO table
// buildPersistenceDTORegistry constructs, so Snapshot/Restore can pass them
// around as one value instead of ten separate return values.
type persistenceDTOTables struct {
	registry               *store.Registry
	filters                *store.Table[detectorDTO[Filter]]
	ipSets                 *store.Table[detectorDTO[IPSet]]
	threatIntelSets        *store.Table[detectorDTO[ThreatIntelSet]]
	publishingDestinations *store.Table[detectorDTO[PublishingDestination]]
	threatEntitySets       *store.Table[detectorDTO[ThreatEntitySet]]
	trustedEntitySets      *store.Table[detectorDTO[TrustedEntitySet]]
	investigations         *store.Table[detectorDTO[Investigation]]
	orgConfigs             *store.Table[byDetectorDTO[OrgConfig]]
	adminAccounts          *store.Table[byDetectorDTO[AdminAccount]]
	malwareScanSettings    *store.Table[byDetectorDTO[MalwareScanSettings]]
}

// buildPersistenceDTORegistry constructs the ephemeral DTO registry used by
// both Snapshot and Restore for the ten "dirty" tables (see
// store_setup.go's registerAllTables doc). It is built fresh on every call
// (rather than reusing b.registry) because the DTO value types differ from
// the live table value types (e.g. detectorDTO[V] vs V).
func buildPersistenceDTORegistry() persistenceDTOTables {
	dtoReg := store.NewRegistry()

	return persistenceDTOTables{
		registry:        dtoReg,
		filters:         store.Register(dtoReg, "filters", store.New(detectorDTOKeyFn[Filter])),
		ipSets:          store.Register(dtoReg, "ipSets", store.New(detectorDTOKeyFn[IPSet])),
		threatIntelSets: store.Register(dtoReg, "threatIntelSets", store.New(detectorDTOKeyFn[ThreatIntelSet])),
		publishingDestinations: store.Register(
			dtoReg, "publishingDestinations", store.New(detectorDTOKeyFn[PublishingDestination]),
		),
		threatEntitySets:  store.Register(dtoReg, "threatEntitySets", store.New(detectorDTOKeyFn[ThreatEntitySet])),
		trustedEntitySets: store.Register(dtoReg, "trustedEntitySets", store.New(detectorDTOKeyFn[TrustedEntitySet])),
		investigations:    store.Register(dtoReg, "investigations", store.New(detectorDTOKeyFn[Investigation])),
		orgConfigs:        store.Register(dtoReg, "orgConfigs", store.New(byDetectorDTOKeyFn[OrgConfig])),
		adminAccounts:     store.Register(dtoReg, "adminAccounts", store.New(byDetectorDTOKeyFn[AdminAccount])),
		malwareScanSettings: store.Register(
			dtoReg, "malwareScanSettings", store.New(byDetectorDTOKeyFn[MalwareScanSettings]),
		),
	}
}

// backendSnapshot is the top-level on-disk shape for the GuardDuty backend.
//
// Tables holds one JSON-encoded array per registered table name: the seven
// "clean" tables on b.registry (detectors, findings, members, invitations,
// orgAdminAccounts, malwareScans, malwareProtectionPlans) plus the ten DTO
// tables built above for the "dirty" ones (filters, ipSets,
// threatIntelSets, publishingDestinations, threatEntitySets,
// trustedEntitySets, investigations, orgConfigs, adminAccounts,
// malwareScanSettings). Tags is
// a plain map left unconverted (its values, map[string]string, have no
// identity of their own -- see store_setup.go) and is persisted here
// directly, as before. MemberSeq is a scalar counter, also carried directly.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage   `json:"tables"`
	Tags      map[string]map[string]string `json:"tags"`
	AccountID string                       `json:"accountId"`
	Region    string                       `json:"region"`
	MemberSeq int64                        `json:"memberSeq"`
	Version   int                          `json:"version"`
}

// Snapshot serializes backend state to JSON. It implements
// persistence.Persistable via Handler.Snapshot (see below).
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "guardduty: snapshot table marshal failed", "error", err)

		return nil
	}

	dtos := buildPersistenceDTORegistry()

	for _, v := range b.filters.Snapshot() {
		dtos.filters.Put(&detectorDTO[Filter]{Value: v, DetectorID: v.DetectorID, ID: v.Name})
	}

	for _, v := range b.ipSets.Snapshot() {
		dtos.ipSets.Put(&detectorDTO[IPSet]{Value: v, DetectorID: v.DetectorID, ID: v.IPSetID})
	}

	for _, v := range b.threatIntelSets.Snapshot() {
		dtos.threatIntelSets.Put(&detectorDTO[ThreatIntelSet]{
			Value: v, DetectorID: v.DetectorID, ID: v.ThreatIntelSetID,
		})
	}

	for _, v := range b.publishingDestinations.Snapshot() {
		dtos.publishingDestinations.Put(&detectorDTO[PublishingDestination]{
			Value: v, DetectorID: v.DetectorID, ID: v.DestinationID,
		})
	}

	for _, v := range b.threatEntitySets.Snapshot() {
		dtos.threatEntitySets.Put(&detectorDTO[ThreatEntitySet]{
			Value: v, DetectorID: v.DetectorID, ID: v.ThreatEntitySetID,
		})
	}

	for _, v := range b.trustedEntitySets.Snapshot() {
		dtos.trustedEntitySets.Put(&detectorDTO[TrustedEntitySet]{
			Value: v, DetectorID: v.DetectorID, ID: v.TrustedEntitySetID,
		})
	}

	for _, v := range b.investigations.Snapshot() {
		dtos.investigations.Put(&detectorDTO[Investigation]{
			Value: v, DetectorID: v.DetectorID, ID: v.InvestigationID,
		})
	}

	for _, v := range b.orgConfigs.Snapshot() {
		dtos.orgConfigs.Put(&byDetectorDTO[OrgConfig]{Value: v, DetectorID: v.detectorID})
	}

	for _, v := range b.adminAccounts.Snapshot() {
		dtos.adminAccounts.Put(&byDetectorDTO[AdminAccount]{Value: v, DetectorID: v.detectorID})
	}

	for _, v := range b.malwareScanSettings.Snapshot() {
		dtos.malwareScanSettings.Put(&byDetectorDTO[MalwareScanSettings]{Value: v, DetectorID: v.detectorID})
	}

	dtoTables, err := dtos.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "guardduty: snapshot DTO table marshal failed", "error", err)

		return nil
	}
	maps.Copy(tables, dtoTables)

	snap := backendSnapshot{
		Version:   guarddutySnapshotVersion,
		Tables:    tables,
		Tags:      b.tags,
		AccountID: b.accountID,
		Region:    b.region,
		MemberSeq: b.memberSeq,
	}

	return persistence.MarshalSnapshot(ctx, "guardduty", snap)
}

// Restore deserializes backend state from JSON. It implements
// persistence.Persistable via Handler.Restore (see below).
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "guardduty", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != guarddutySnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"guardduty: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", guarddutySnapshotVersion)

		b.registry.ResetAll()
		b.filters.Reset()
		b.ipSets.Reset()
		b.threatIntelSets.Reset()
		b.publishingDestinations.Reset()
		b.threatEntitySets.Reset()
		b.trustedEntitySets.Reset()
		b.investigations.Reset()
		b.orgConfigs.Reset()
		b.adminAccounts.Reset()
		b.malwareScanSettings.Reset()
		b.tags = make(map[string]map[string]string)
		b.memberSeq = 0
		b.accountID = snap.AccountID
		b.region = snap.Region

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("guardduty: restore snapshot tables: %w", err)
	}

	if err := b.restoreDirtyTables(snap.Tables); err != nil {
		return err
	}

	if snap.Tags == nil {
		snap.Tags = make(map[string]map[string]string)
	}
	b.tags = snap.Tags

	b.accountID = snap.AccountID
	b.region = snap.Region
	b.memberSeq = snap.MemberSeq

	return nil
}

// unwrapDetectorDTOs converts every detectorDTO[V] in dtos into its live *V,
// restoring the hidden DetectorID field each carries via setDetectorID (a
// plain generic type parameter V has no field access, so the assignment is
// supplied by the caller, one per concrete V; see restoreDirtyTables). A DTO
// whose Value is nil -- not producible by Snapshot, but a defensive guard
// against a hand-edited or corrupted snapshot -- is skipped rather than
// dereferenced.
func unwrapDetectorDTOs[V any](dtos *store.Table[detectorDTO[V]], setDetectorID func(*V, string)) []*V {
	items := make([]*V, 0, dtos.Len())

	for _, d := range dtos.All() {
		if d.Value == nil {
			continue
		}

		setDetectorID(d.Value, d.DetectorID)
		items = append(items, d.Value)
	}

	return items
}

// unwrapByDetectorDTOs is unwrapDetectorDTOs' counterpart for the three
// keyed-directly-by-detector tables (OrgConfig, AdminAccount,
// MalwareScanSettings).
func unwrapByDetectorDTOs[V any](dtos *store.Table[byDetectorDTO[V]], setDetectorID func(*V, string)) []*V {
	items := make([]*V, 0, dtos.Len())

	for _, d := range dtos.All() {
		if d.Value == nil {
			continue
		}

		setDetectorID(d.Value, d.DetectorID)
		items = append(items, d.Value)
	}

	return items
}

// restoreDirtyTables rebuilds every "dirty" store.Table on b from tables via
// the ephemeral DTO registry, factored out of Restore to keep Restore's own
// cognitive complexity low. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restoreDirtyTables(tables map[string]json.RawMessage) error {
	dtos := buildPersistenceDTORegistry()
	if err := dtos.registry.RestoreAll(tables); err != nil {
		return fmt.Errorf("guardduty: restore snapshot DTO tables: %w", err)
	}

	b.filters.Restore(unwrapDetectorDTOs(dtos.filters, func(v *Filter, id string) { v.DetectorID = id }))
	b.ipSets.Restore(unwrapDetectorDTOs(dtos.ipSets, func(v *IPSet, id string) { v.DetectorID = id }))
	b.threatIntelSets.Restore(
		unwrapDetectorDTOs(dtos.threatIntelSets, func(v *ThreatIntelSet, id string) { v.DetectorID = id }),
	)
	b.publishingDestinations.Restore(
		unwrapDetectorDTOs(dtos.publishingDestinations, func(v *PublishingDestination, id string) {
			v.DetectorID = id
		}),
	)
	b.threatEntitySets.Restore(
		unwrapDetectorDTOs(dtos.threatEntitySets, func(v *ThreatEntitySet, id string) { v.DetectorID = id }),
	)
	b.trustedEntitySets.Restore(
		unwrapDetectorDTOs(dtos.trustedEntitySets, func(v *TrustedEntitySet, id string) { v.DetectorID = id }),
	)
	b.investigations.Restore(
		unwrapDetectorDTOs(dtos.investigations, func(v *Investigation, id string) { v.DetectorID = id }),
	)

	b.orgConfigs.Restore(unwrapByDetectorDTOs(dtos.orgConfigs, func(v *OrgConfig, id string) { v.detectorID = id }))
	b.adminAccounts.Restore(
		unwrapByDetectorDTOs(dtos.adminAccounts, func(v *AdminAccount, id string) { v.detectorID = id }),
	)
	b.malwareScanSettings.Restore(
		unwrapByDetectorDTOs(dtos.malwareScanSettings, func(v *MalwareScanSettings, id string) { v.detectorID = id }),
	)

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
// Without this, cli.go's generic setupPersistence (which type-asserts the
// registered service.Registerable, i.e. *Handler, for Snapshot/Restore)
// never finds a persistable GuardDuty service even though InMemoryBackend
// itself has always implemented Snapshot/Restore -- the backend methods
// were dead wiring until Handler delegated to them.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
