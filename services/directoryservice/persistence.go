package directoryservice

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// directoryserviceSnapshotVersion identifies the shape of [backendSnapshot].
// It must be bumped whenever a change to regionalDTO/backendSnapshot itself
// would make an older snapshot unsafe to decode as the current shape (e.g. a
// field's meaning or type changes). Restore compares this against the
// persisted value and discards (rather than attempts to partially decode)
// any mismatch -- see Restore below. This is the first version: earlier
// (pre-Phase-3.3) snapshots carried no version field at all, so they also
// fail this check and are discarded rather than misread, which is the safe
// behaviour across any snapshot-format change.
const directoryserviceSnapshotVersion = 2

// regionalDTO wraps a region-nested resource for JSON round-tripping through
// store.Registry. Table[V].Snapshot's plain json.Marshal(V) cannot see the
// unexported `region` field each of the sixteen converted stored*/storedADAssessment
// types carries (used only for the live table's composite key -- see
// store_setup.go), so it is carried alongside Value here instead. ID mirrors
// the resource's own identifier (or composite sub-key, e.g. "directoryId:extra")
// so the DTO table itself has a stable composite key ("Region|ID") independent
// of which concrete V it wraps.
type regionalDTO[V any] struct {
	Value  *V     `json:"value"`
	Region string `json:"region"`
	ID     string `json:"id"`
}

// regionalDTOKeyFn is the shared [store.Table] key function for every
// regionalDTO[V] table below; it mirrors the "region|id" composite key each
// live table uses (see regionKey in backend.go).
func regionalDTOKeyFn[V any](d *regionalDTO[V]) string { return regionKey(d.Region, d.ID) }

// backendSnapshot is the top-level on-disk shape for the DirectoryService backend.
//
// Tables holds one JSON-encoded array per registered DTO table, produced by
// [store.Registry.SnapshotAll] -- the sixteen region-qualified resource
// tables, each wrapped in a regionalDTO to carry its region field through.
// Aliases, IPRoutes, DirDataAccess, CAEnrollment, DirSettings and
// UpdateInfoEntries are still region-nested plain maps (see store_setup.go
// for why they were not converted to store.Table) and are persisted
// directly, as before. Version guards against decoding a snapshot from an
// incompatible (older or newer) build of this backend as though it were the
// current shape; see Restore.
type backendSnapshot struct {
	Tables            map[string]json.RawMessage                      `json:"tables"`
	Aliases           map[string]map[string]string                    `json:"aliases"`
	IPRoutes          map[string]map[string][]storedIpRoute           `json:"ipRoutes"`
	DirDataAccess     map[string]map[string]bool                      `json:"dirDataAccess"`
	CAEnrollment      map[string]map[string]*CAEnrollmentPolicy       `json:"caEnrollment"`
	DirSettings       map[string]map[string][]*storedDirectorySetting `json:"dirSettings"`
	UpdateInfoEntries map[string]map[string][]*storedUpdateInfo       `json:"updateInfoEntries"`
	AccountID         string                                          `json:"accountID"`
	Region            string                                          `json:"region"`
	Version           int                                             `json:"version"`
}

// persistenceDTOTables holds the ephemeral DTO registry and its sixteen typed
// tables used by both BackendSnapshot and Restore. A struct (rather than
// neptune's positional multi-return) keeps buildPersistenceDTOTables callable
// and readable at this table count.
type persistenceDTOTables struct {
	registry              *store.Registry
	directories           *store.Table[regionalDTO[storedDirectory]]
	snapshots             *store.Table[regionalDTO[storedSnapshot]]
	dsRegions             *store.Table[regionalDTO[storedRegion]]
	schemaExtensions      *store.Table[regionalDTO[storedSchemaExtension]]
	conditionalForwarders *store.Table[regionalDTO[storedConditionalForwarder]]
	logSubscriptions      *store.Table[regionalDTO[storedLogSubscription]]
	eventTopics           *store.Table[regionalDTO[storedEventTopic]]
	domainControllers     *store.Table[regionalDTO[storedDomainController]]
	trusts                *store.Table[regionalDTO[storedTrust]]
	sharedDirectories     *store.Table[regionalDTO[storedSharedDirectory]]
	certificates          *store.Table[regionalDTO[storedCertificate]]
	ldapsSettings         *store.Table[regionalDTO[storedLDAPSSetting]]
	clientAuthSettings    *store.Table[regionalDTO[storedClientAuthSetting]]
	radiusSettings        *store.Table[regionalDTO[storedRadiusSettings]]
	adAssessments         *store.Table[regionalDTO[storedADAssessment]]
	hybridADUpdates       *store.Table[regionalDTO[storedHybridADUpdate]]
}

// buildPersistenceDTOTables constructs the ephemeral DTO registry used by
// both BackendSnapshot and Restore. It is built fresh on every call (rather
// than reusing b.registry) because the DTO value types differ from the live
// table value types (regionalDTO[V] vs V).
func buildPersistenceDTOTables() *persistenceDTOTables {
	reg := store.NewRegistry()

	return &persistenceDTOTables{
		registry: reg,
		directories: store.Register(
			reg, "directories", store.New(regionalDTOKeyFn[storedDirectory]),
		),
		snapshots: store.Register(
			reg, "snapshots", store.New(regionalDTOKeyFn[storedSnapshot]),
		),
		dsRegions: store.Register(
			reg, "dsRegions", store.New(regionalDTOKeyFn[storedRegion]),
		),
		schemaExtensions: store.Register(
			reg, "schemaExtensions", store.New(regionalDTOKeyFn[storedSchemaExtension]),
		),
		conditionalForwarders: store.Register(
			reg, "conditionalForwarders", store.New(regionalDTOKeyFn[storedConditionalForwarder]),
		),
		logSubscriptions: store.Register(
			reg, "logSubscriptions", store.New(regionalDTOKeyFn[storedLogSubscription]),
		),
		eventTopics: store.Register(
			reg, "eventTopics", store.New(regionalDTOKeyFn[storedEventTopic]),
		),
		domainControllers: store.Register(
			reg, "domainControllers", store.New(regionalDTOKeyFn[storedDomainController]),
		),
		trusts: store.Register(
			reg, "trusts", store.New(regionalDTOKeyFn[storedTrust]),
		),
		sharedDirectories: store.Register(
			reg, "sharedDirectories", store.New(regionalDTOKeyFn[storedSharedDirectory]),
		),
		certificates: store.Register(
			reg, "certificates", store.New(regionalDTOKeyFn[storedCertificate]),
		),
		ldapsSettings: store.Register(
			reg, "ldapsSettings", store.New(regionalDTOKeyFn[storedLDAPSSetting]),
		),
		clientAuthSettings: store.Register(
			reg, "clientAuthSettings", store.New(regionalDTOKeyFn[storedClientAuthSetting]),
		),
		radiusSettings: store.Register(
			reg, "radiusSettings", store.New(regionalDTOKeyFn[storedRadiusSettings]),
		),
		adAssessments: store.Register(
			reg, "adAssessments", store.New(regionalDTOKeyFn[storedADAssessment]),
		),
		hybridADUpdates: store.Register(
			reg, "hybridADUpdates", store.New(regionalDTOKeyFn[storedHybridADUpdate]),
		),
	}
}

// populateDTOTables copies every live store.Table's current contents into
// dto, wrapping each value in a regionalDTO so its unexported region field
// survives the JSON round trip. Split into four sub-functions (rather than
// one long function) to keep each function's cognitive complexity low.
func (b *InMemoryBackend) populateDTOTables(dto *persistenceDTOTables) {
	b.populateCoreDTOTables(dto)
	b.populateForwardingDTOTables(dto)
	b.populateTrustDTOTables(dto)
	b.populateSettingsDTOTables(dto)
}

func (b *InMemoryBackend) populateCoreDTOTables(dto *persistenceDTOTables) {
	for _, v := range b.directories.Snapshot() {
		dto.directories.Put(&regionalDTO[storedDirectory]{Region: v.region, ID: v.DirectoryID, Value: v})
	}
	for _, v := range b.snapshots.Snapshot() {
		dto.snapshots.Put(&regionalDTO[storedSnapshot]{Region: v.region, ID: v.SnapshotID, Value: v})
	}
	for _, v := range b.dsRegions.Snapshot() {
		dto.dsRegions.Put(&regionalDTO[storedRegion]{
			Region: v.region, ID: dsRegionID(v.DirectoryID, v.RegionName), Value: v,
		})
	}
	for _, v := range b.schemaExtensions.Snapshot() {
		dto.schemaExtensions.Put(&regionalDTO[storedSchemaExtension]{Region: v.region, ID: v.ExtensionID, Value: v})
	}
}

func (b *InMemoryBackend) populateForwardingDTOTables(dto *persistenceDTOTables) {
	for _, v := range b.conditionalForwarders.Snapshot() {
		dto.conditionalForwarders.Put(&regionalDTO[storedConditionalForwarder]{
			Region: v.region, ID: conditionalForwarderID(v.DirectoryID, v.RemoteDomainName), Value: v,
		})
	}
	for _, v := range b.logSubscriptions.Snapshot() {
		dto.logSubscriptions.Put(&regionalDTO[storedLogSubscription]{
			Region: v.region, ID: logSubscriptionID(v.DirectoryID, v.LogGroupName), Value: v,
		})
	}
	for _, v := range b.eventTopics.Snapshot() {
		dto.eventTopics.Put(&regionalDTO[storedEventTopic]{
			Region: v.region, ID: eventTopicID(v.DirectoryID, v.TopicName), Value: v,
		})
	}
	for _, v := range b.domainControllers.Snapshot() {
		dto.domainControllers.Put(&regionalDTO[storedDomainController]{Region: v.region, ID: v.ControllerID, Value: v})
	}
}

func (b *InMemoryBackend) populateTrustDTOTables(dto *persistenceDTOTables) {
	for _, v := range b.trusts.Snapshot() {
		dto.trusts.Put(&regionalDTO[storedTrust]{Region: v.region, ID: v.TrustID, Value: v})
	}
	for _, v := range b.sharedDirectories.Snapshot() {
		dto.sharedDirectories.Put(&regionalDTO[storedSharedDirectory]{
			Region: v.region, ID: v.SharedDirectoryID, Value: v,
		})
	}
	for _, v := range b.certificates.Snapshot() {
		dto.certificates.Put(&regionalDTO[storedCertificate]{Region: v.region, ID: v.CertificateID, Value: v})
	}
	for _, v := range b.ldapsSettings.Snapshot() {
		dto.ldapsSettings.Put(&regionalDTO[storedLDAPSSetting]{
			Region: v.region, ID: ldapsSettingID(v.DirectoryID, v.LDAPSType), Value: v,
		})
	}
}

func (b *InMemoryBackend) populateSettingsDTOTables(dto *persistenceDTOTables) {
	for _, v := range b.clientAuthSettings.Snapshot() {
		dto.clientAuthSettings.Put(&regionalDTO[storedClientAuthSetting]{
			Region: v.region, ID: clientAuthSettingID(v.DirectoryID, v.AuthType), Value: v,
		})
	}
	for _, v := range b.radiusSettings.Snapshot() {
		dto.radiusSettings.Put(&regionalDTO[storedRadiusSettings]{Region: v.region, ID: v.DirectoryID, Value: v})
	}
	for _, v := range b.adAssessments.Snapshot() {
		dto.adAssessments.Put(&regionalDTO[storedADAssessment]{Region: v.Region, ID: v.AssessmentID, Value: v})
	}
	for _, v := range b.hybridADUpdates.Snapshot() {
		dto.hybridADUpdates.Put(&regionalDTO[storedHybridADUpdate]{Region: v.region, ID: v.RequestID, Value: v})
	}
}

// unwrapRegionalDTOs converts every regionalDTO[V] in dtos into its live *V,
// restoring the unexported region field each carries via setRegion (a plain
// generic type parameter V has no field access, so the region assignment is
// supplied by the caller, one per concrete V). A DTO whose Value is nil --
// not producible by BackendSnapshot, but a defensive guard against a
// hand-edited or corrupted snapshot -- is skipped rather than dereferenced.
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
// complexity low. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restoreResourceTables(tables map[string]json.RawMessage) error {
	dto := buildPersistenceDTOTables()

	if err := dto.registry.RestoreAll(tables); err != nil {
		return fmt.Errorf("directoryservice: restore snapshot tables: %w", err)
	}

	b.restoreCoreTables(dto)
	b.restoreForwardingTables(dto)
	b.restoreTrustTables(dto)
	b.restoreSettingsTables(dto)

	return nil
}

func (b *InMemoryBackend) restoreCoreTables(dto *persistenceDTOTables) {
	b.directories.Restore(unwrapRegionalDTOs(dto.directories, func(v *storedDirectory, r string) { v.region = r }))
	b.snapshots.Restore(unwrapRegionalDTOs(dto.snapshots, func(v *storedSnapshot, r string) { v.region = r }))
	b.dsRegions.Restore(unwrapRegionalDTOs(dto.dsRegions, func(v *storedRegion, r string) { v.region = r }))
	b.schemaExtensions.Restore(
		unwrapRegionalDTOs(dto.schemaExtensions, func(v *storedSchemaExtension, r string) { v.region = r }),
	)
}

func (b *InMemoryBackend) restoreForwardingTables(dto *persistenceDTOTables) {
	b.conditionalForwarders.Restore(
		unwrapRegionalDTOs(dto.conditionalForwarders, func(v *storedConditionalForwarder, r string) { v.region = r }),
	)
	b.logSubscriptions.Restore(
		unwrapRegionalDTOs(dto.logSubscriptions, func(v *storedLogSubscription, r string) { v.region = r }),
	)
	b.eventTopics.Restore(unwrapRegionalDTOs(dto.eventTopics, func(v *storedEventTopic, r string) { v.region = r }))
	b.domainControllers.Restore(
		unwrapRegionalDTOs(dto.domainControllers, func(v *storedDomainController, r string) { v.region = r }),
	)
}

func (b *InMemoryBackend) restoreTrustTables(dto *persistenceDTOTables) {
	b.trusts.Restore(unwrapRegionalDTOs(dto.trusts, func(v *storedTrust, r string) { v.region = r }))
	b.sharedDirectories.Restore(
		unwrapRegionalDTOs(dto.sharedDirectories, func(v *storedSharedDirectory, r string) { v.region = r }),
	)
	b.certificates.Restore(unwrapRegionalDTOs(dto.certificates, func(v *storedCertificate, r string) { v.region = r }))
	b.ldapsSettings.Restore(
		unwrapRegionalDTOs(dto.ldapsSettings, func(v *storedLDAPSSetting, r string) { v.region = r }),
	)
}

func (b *InMemoryBackend) restoreSettingsTables(dto *persistenceDTOTables) {
	b.clientAuthSettings.Restore(
		unwrapRegionalDTOs(dto.clientAuthSettings, func(v *storedClientAuthSetting, r string) { v.region = r }),
	)
	b.radiusSettings.Restore(
		unwrapRegionalDTOs(dto.radiusSettings, func(v *storedRadiusSettings, r string) { v.region = r }),
	)
	b.adAssessments.Restore(
		unwrapRegionalDTOs(dto.adAssessments, func(v *storedADAssessment, r string) { v.Region = r }),
	)
	b.hybridADUpdates.Restore(
		unwrapRegionalDTOs(dto.hybridADUpdates, func(v *storedHybridADUpdate, r string) { v.region = r }),
	)
}

// orEmptyRegionMap returns m unchanged when non-nil, or a fresh empty map
// otherwise. Used to normalize the six raw region-nested maps on Restore, the
// same way a fresh NewInMemoryBackend would leave them non-nil.
func orEmptyRegionMap[V any](m map[string]map[string]V) map[string]map[string]V {
	if m == nil {
		return make(map[string]map[string]V)
	}

	return m
}

// BackendSnapshot serializes the backend state to JSON, nested by region.
func (b *InMemoryBackend) BackendSnapshot() []byte {
	b.mu.RLock("BackendSnapshot")
	defer b.mu.RUnlock()

	dto := buildPersistenceDTOTables()
	b.populateDTOTables(dto)

	tables, err := dto.registry.SnapshotAll()
	if err != nil {
		// The DTOs above are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data; matches the original implementation's silent
		// best-effort json.Marshal(...) (data, _ := json.Marshal(...)).
		return nil
	}

	snap := backendSnapshot{
		Version:           directoryserviceSnapshotVersion,
		Tables:            tables,
		Aliases:           b.aliases,
		IPRoutes:          b.ipRoutes,
		DirDataAccess:     b.dirDataAccess,
		CAEnrollment:      b.caEnrollment,
		DirSettings:       b.dirSettings,
		UpdateInfoEntries: b.updateInfoEntries,
		AccountID:         b.accountID,
		Region:            b.region,
	}

	data, _ := json.Marshal(snap)

	return data
}

// Restore deserializes backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "directoryservice", data, &snap); err != nil {
		return err
	}

	if snap.Version != directoryserviceSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"directoryservice: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", directoryserviceSnapshotVersion)

		b.resetAllState()
		b.accountID = snap.AccountID
		b.region = snap.Region

		return nil
	}

	if err := b.restoreResourceTables(snap.Tables); err != nil {
		return err
	}

	b.aliases = orEmptyRegionMap(snap.Aliases)
	b.ipRoutes = orEmptyRegionMap(snap.IPRoutes)
	b.dirDataAccess = orEmptyRegionMap(snap.DirDataAccess)
	b.caEnrollment = orEmptyRegionMap(snap.CAEnrollment)
	b.dirSettings = orEmptyRegionMap(snap.DirSettings)
	b.updateInfoEntries = orEmptyRegionMap(snap.UpdateInfoEntries)
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}
