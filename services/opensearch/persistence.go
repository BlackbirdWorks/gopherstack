package opensearch

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

// opensearchSnapshotVersion identifies the shape of backendSnapshot's Tables
// blob (the set of "clean" tables on b.registry plus the DTO tables built
// below for the "dirty" ones -- see store_setup.go's registerAllTables doc
// for the clean/dirty split). Bump whenever a change to a DTO type, a
// registered table's value type, or backendSnapshot itself would make an
// older snapshot unsafe to decode as the current shape. Restore compares this
// against the persisted value and discards (rather than attempts to
// partially decode) any mismatch -- see Restore below. Mirrors the
// services/sqs pilot (commit 0f09d77c) and services/apigateway (commit
// 6da0334e).
// Left at 3 (not bumped) despite this pass registering three new "clean"
// tables (dataSourceAttachments, capabilities, migrations, see
// store_setup.go): [store.Registry.RestoreAll] resets any registered table
// whose name is absent from an older snapshot's Tables map rather than
// erroring, so a pre-existing snapshot restores exactly as before with these
// three tables simply empty -- no existing struct shape changed, so nothing
// about decoding an older snapshot became unsafe. Only bump this when a
// registered/DTO table's value *shape* changes (as the 1->2 and 2->3 bumps
// above did), not for pure table additions.
const opensearchSnapshotVersion = 3

// dryRunSnapshot, autoTuneSnapshot, dataSourceSnapshot, and
// domainIndexSnapshot are DTOs used ONLY for Snapshot/Restore. Each mirrors
// its live type field for field, except the identity field (DomainName) is
// given a real JSON tag here instead of the live type's `json:"-"` (see
// models.go / advanced.go) -- marshaling the live type directly
// would silently drop that field, and unmarshaling into it would leave it
// permanently empty, corrupting the table's key on restore. This is the same
// DTO-registry technique services/sqs uses for its Queue/moveTaskState types
// (commit 0f09d77c) and services/apigateway uses for its nested-resource
// families (commit 6da0334e), applied here for the same reason (a json:"-"
// identity field needed only to key the pkgs/store table).
type dryRunSnapshot struct {
	DryRunID           string           `json:"dryRunId"`
	DryRunStatus       string           `json:"dryRunStatus"`
	CreationDate       string           `json:"creationDate"`
	UpdateDate         string           `json:"updateDate"`
	DomainName         string           `json:"domainName"`
	ValidationFailures []map[string]any `json:"validationFailures"`
}

func dryRunSnapshotKey(v *dryRunSnapshot) string { return v.DomainName }

func toDryRunSnapshot(v *DryRunStatus) *dryRunSnapshot {
	return &dryRunSnapshot{
		DryRunID:           v.DryRunID,
		DryRunStatus:       v.DryRunStatus,
		CreationDate:       v.CreationDate,
		UpdateDate:         v.UpdateDate,
		DomainName:         v.DomainName,
		ValidationFailures: v.ValidationFailures,
	}
}

func fromDryRunSnapshot(v *dryRunSnapshot) *DryRunStatus {
	return &DryRunStatus{
		DryRunID:           v.DryRunID,
		DryRunStatus:       v.DryRunStatus,
		CreationDate:       v.CreationDate,
		UpdateDate:         v.UpdateDate,
		DomainName:         v.DomainName,
		ValidationFailures: v.ValidationFailures,
	}
}

type autoTuneSnapshot struct {
	DesiredState         string                        `json:"desiredState"`
	DomainName           string                        `json:"domainName"`
	MaintenanceSchedules []AutoTuneMaintenanceSchedule `json:"maintenanceSchedules,omitempty"`
}

func autoTuneSnapshotKey(v *autoTuneSnapshot) string { return autoTuneKey(v.DomainName) }

func toAutoTuneSnapshot(v *AutoTuneConfig) *autoTuneSnapshot {
	return &autoTuneSnapshot{
		DesiredState:         v.DesiredState,
		MaintenanceSchedules: v.MaintenanceSchedules,
		DomainName:           v.DomainName,
	}
}

func fromAutoTuneSnapshot(v *autoTuneSnapshot) *AutoTuneConfig {
	return &AutoTuneConfig{
		DesiredState:         v.DesiredState,
		MaintenanceSchedules: v.MaintenanceSchedules,
		DomainName:           v.DomainName,
	}
}

type dataSourceSnapshot struct {
	Name           string          `json:"name"`
	Description    string          `json:"description"`
	Status         string          `json:"status,omitempty"`
	DomainName     string          `json:"domainName"`
	DataSourceType json.RawMessage `json:"dataSourceType,omitempty"`
}

func dataSourceSnapshotKey(v *dataSourceSnapshot) string { return dataSourceKey(v.DomainName, v.Name) }

func toDataSourceSnapshot(v *DataSource) *dataSourceSnapshot {
	return &dataSourceSnapshot{
		Name:           v.Name,
		Description:    v.Description,
		DataSourceType: v.DataSourceType,
		Status:         v.Status,
		DomainName:     v.DomainName,
	}
}

func fromDataSourceSnapshot(v *dataSourceSnapshot) *DataSource {
	return &DataSource{
		Name:           v.Name,
		Description:    v.Description,
		DataSourceType: v.DataSourceType,
		Status:         v.Status,
		DomainName:     v.DomainName,
	}
}

type domainIndexSnapshot struct {
	Mappings      map[string]any            `json:"mappings,omitempty"`
	Settings      map[string]any            `json:"settings,omitempty"`
	Aliases       map[string]any            `json:"aliases,omitempty"`
	Documents     map[string]map[string]any `json:"documents,omitempty"`
	IndexName     string                    `json:"indexName"`
	IndexStatus   string                    `json:"indexStatus"`
	DomainName    string                    `json:"domainName"`
	DocumentCount int                       `json:"documentCount"`
}

func domainIndexSnapshotKey(v *domainIndexSnapshot) string {
	return domainIndexKey(v.DomainName, v.IndexName)
}

func toDomainIndexSnapshot(v *DomainIndex) *domainIndexSnapshot {
	return &domainIndexSnapshot{
		Mappings:      v.Mappings,
		Settings:      v.Settings,
		Aliases:       v.Aliases,
		Documents:     v.Documents,
		IndexName:     v.IndexName,
		IndexStatus:   v.IndexStatus,
		DomainName:    v.DomainName,
		DocumentCount: v.DocumentCount,
	}
}

func fromDomainIndexSnapshot(v *domainIndexSnapshot) *DomainIndex {
	return &DomainIndex{
		Mappings:      v.Mappings,
		Settings:      v.Settings,
		Aliases:       v.Aliases,
		Documents:     v.Documents,
		IndexName:     v.IndexName,
		IndexStatus:   v.IndexStatus,
		DomainName:    v.DomainName,
		DocumentCount: v.DocumentCount,
	}
}

// dirtyTableNames lists the "dirty" table names shared by Snapshot and
// Restore (see store_setup.go's registerAllTables doc). Both build an
// ephemeral DTO [store.Registry] under these exact names, so a snapshot
// written by one version of Snapshot always lines up with the same version
// of Restore.
//
//nolint:gochecknoglobals // fixed lookup table, mirrors errCodeLookup-style tables elsewhere
var dirtyTableNames = struct {
	dryRuns, autoTunes, domainDataSources, domainIndexes string
}{
	dryRuns:           "dryRuns",
	autoTunes:         "autoTunes",
	domainDataSources: "domainDataSources",
	domainIndexes:     "domainIndexes",
}

// backendSnapshot is the top-level on-disk shape for the OpenSearch backend.
//
// Tables holds one JSON-encoded array per table -- both the "clean" tables
// registered on b.registry (produced by [store.Registry.SnapshotAll]) and the
// "dirty" DTO tables built inline in Snapshot (see store_setup.go's
// registerAllTables doc for the clean/dirty split), merged into one map so a
// single Tables blob round-trips the whole backend.
//
// The remaining fields are the plain maps left unconverted (see
// store_setup.go's registerAllTables doc for why) plus the account/region and
// monotonic ID counters.
//
// domainPackages (the reverse packageID<-domainName index used by
// ListPackagesForDomain) is deliberately NOT included here: the pre-existing
// backend, before this pkgs/store conversion, never persisted or restored it
// either (only its forward twin, PackageAssociations, round-tripped). That
// asymmetry is preserved byte-for-byte rather than fixed as part of this
// mechanical conversion -- see the per-map persistence audit in the Phase 3.3
// conversion notes.
type backendSnapshot struct {
	Tables                map[string]json.RawMessage       `json:"tables"`
	VpcAuthorizations     map[string][]AuthorizedPrincipal `json:"vpcAuthorizations"`
	ScheduledActions      map[string][]*ScheduledAction    `json:"scheduledActions"`
	PackageAssociations   map[string]map[string]bool       `json:"packageAssociations"`
	DomainMaintenances    map[string][]*DomainMaintenance  `json:"domainMaintenances"`
	UpgradeHistory        map[string][]*UpgradeHistory     `json:"upgradeHistory"`
	AccountID             string                           `json:"accountID"`
	Region                string                           `json:"region"`
	DefaultApplicationArn string                           `json:"defaultApplicationArn"`
	AppIDCounter          int                              `json:"appIDCounter"`
	ConnCounter           int                              `json:"connCounter"`
	VpcEndpointCounter    int                              `json:"vpcEndpointCounter"`
	PackageCounter        int                              `json:"packageCounter"`
	MaintenanceCounter    int                              `json:"maintenanceCounter"`
	ReservedCounter       int                              `json:"reservedCounter"`
	SlCollCounter         int                              `json:"slCollCounter"`
	SlSecConfigCounter    int                              `json:"slSecConfigCounter"`
	Version               int                              `json:"version"`
}

// newDirtyDTORegistry builds the ephemeral DTO [store.Registry] shared by
// Snapshot and Restore for the "dirty" tables (see store_setup.go's
// registerAllTables doc).
func newDirtyDTORegistry() (
	*store.Registry,
	*store.Table[dryRunSnapshot],
	*store.Table[autoTuneSnapshot],
	*store.Table[dataSourceSnapshot],
	*store.Table[domainIndexSnapshot],
) {
	dtoReg := store.NewRegistry()
	dryRunDTOs := store.Register(dtoReg, dirtyTableNames.dryRuns, store.New(dryRunSnapshotKey))
	autoTuneDTOs := store.Register(dtoReg, dirtyTableNames.autoTunes, store.New(autoTuneSnapshotKey))
	dataSourceDTOs := store.Register(dtoReg, dirtyTableNames.domainDataSources, store.New(dataSourceSnapshotKey))
	domainIndexDTOs := store.Register(dtoReg, dirtyTableNames.domainIndexes, store.New(domainIndexSnapshotKey))

	return dtoReg, dryRunDTOs, autoTuneDTOs, dataSourceDTOs, domainIndexDTOs
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		// The registered tables are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the Manager).
		logger.Load(ctx).WarnContext(ctx, "opensearch: snapshot table marshal failed", "error", err)

		return nil
	}

	dtoReg, dryRunDTOs, autoTuneDTOs, dataSourceDTOs, domainIndexDTOs := newDirtyDTORegistry()

	for _, v := range b.dryRuns.Snapshot() {
		dryRunDTOs.Put(toDryRunSnapshot(v))
	}

	for _, v := range b.autoTunes.Snapshot() {
		autoTuneDTOs.Put(toAutoTuneSnapshot(v))
	}

	for _, v := range b.domainDataSources.Snapshot() {
		dataSourceDTOs.Put(toDataSourceSnapshot(v))
	}

	for _, v := range b.domainIndexes.Snapshot() {
		domainIndexDTOs.Put(toDomainIndexSnapshot(v))
	}

	dirtyTables, err := dtoReg.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "opensearch: snapshot DTO table marshal failed", "error", err)

		return nil
	}

	maps.Copy(tables, dirtyTables)

	snap := backendSnapshot{
		Version:               opensearchSnapshotVersion,
		Tables:                tables,
		VpcAuthorizations:     b.vpcAuthorizations,
		ScheduledActions:      b.scheduledActions,
		PackageAssociations:   b.packageAssociations,
		DomainMaintenances:    b.domainMaintenances,
		UpgradeHistory:        b.upgradeHistory,
		AccountID:             b.accountID,
		Region:                b.region,
		DefaultApplicationArn: b.defaultApplicationArn,
		AppIDCounter:          b.appIDCounter,
		ConnCounter:           b.connCounter,
		VpcEndpointCounter:    b.vpcEndpointCounter,
		PackageCounter:        b.packageCounter,
		MaintenanceCounter:    b.maintenanceCounter,
		ReservedCounter:       b.reservedCounter,
		SlCollCounter:         b.slCollCounter,
		SlSecConfigCounter:    b.slSecConfigCounter,
	}

	return persistence.MarshalSnapshot(ctx, "opensearch", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "opensearch", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	// Release tags resources from the domains being replaced.
	for _, d := range b.domains.All() {
		d.Tags.Close()
	}

	if snap.Version != opensearchSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"opensearch: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", opensearchSnapshotVersion)

		b.registry.ResetAll()
		b.dryRuns.Reset()
		b.autoTunes.Reset()
		b.domainDataSources.Reset()
		b.domainIndexes.Reset()
		b.vpcAuthorizations = make(map[string][]AuthorizedPrincipal)
		b.scheduledActions = make(map[string][]*ScheduledAction)
		b.packageAssociations = make(map[string]map[string]bool)
		b.domainMaintenances = make(map[string][]*DomainMaintenance)
		b.upgradeHistory = make(map[string][]*UpgradeHistory)
		b.domainPackages = make(map[string]map[string]bool)
		b.defaultApplicationArn = ""

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("opensearch: restore snapshot tables: %w", err)
	}

	dtoReg, dryRunDTOs, autoTuneDTOs, dataSourceDTOs, domainIndexDTOs := newDirtyDTORegistry()

	if err := dtoReg.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("opensearch: restore snapshot DTO tables: %w", err)
	}

	restoreDirtyTables(b, dryRunDTOs, autoTuneDTOs, dataSourceDTOs, domainIndexDTOs)

	restoreRawMaps(b, &snap)

	b.accountID = snap.AccountID
	b.region = snap.Region
	b.appIDCounter = snap.AppIDCounter
	b.connCounter = snap.ConnCounter
	b.vpcEndpointCounter = snap.VpcEndpointCounter
	b.packageCounter = snap.PackageCounter
	b.maintenanceCounter = snap.MaintenanceCounter
	b.reservedCounter = snap.ReservedCounter
	b.slCollCounter = snap.SlCollCounter
	b.slSecConfigCounter = snap.SlSecConfigCounter

	fixNilDomainTags(b)

	return nil
}

// restoreDirtyTables converts each DTO table's contents back to its live
// value type and loads the corresponding live b.<table> via Table.Restore,
// split out from Restore to keep its growth in check.
func restoreDirtyTables(
	b *InMemoryBackend,
	dryRunDTOs *store.Table[dryRunSnapshot],
	autoTuneDTOs *store.Table[autoTuneSnapshot],
	dataSourceDTOs *store.Table[dataSourceSnapshot],
	domainIndexDTOs *store.Table[domainIndexSnapshot],
) {
	dryRuns := make([]*DryRunStatus, 0, dryRunDTOs.Len())
	for _, v := range dryRunDTOs.All() {
		dryRuns = append(dryRuns, fromDryRunSnapshot(v))
	}
	b.dryRuns.Restore(dryRuns)

	autoTunes := make([]*AutoTuneConfig, 0, autoTuneDTOs.Len())
	for _, v := range autoTuneDTOs.All() {
		autoTunes = append(autoTunes, fromAutoTuneSnapshot(v))
	}
	b.autoTunes.Restore(autoTunes)

	dataSources := make([]*DataSource, 0, dataSourceDTOs.Len())
	for _, v := range dataSourceDTOs.All() {
		dataSources = append(dataSources, fromDataSourceSnapshot(v))
	}
	b.domainDataSources.Restore(dataSources)

	domainIndexes := make([]*DomainIndex, 0, domainIndexDTOs.Len())
	for _, v := range domainIndexDTOs.All() {
		domainIndexes = append(domainIndexes, fromDomainIndexSnapshot(v))
	}
	b.domainIndexes.Restore(domainIndexes)
}

// restoreRawMaps restores the plain-map fields left unconverted by the
// pkgs/store migration (see store_setup.go's registerAllTables doc),
// defaulting each to an empty map when absent from the snapshot.
func restoreRawMaps(b *InMemoryBackend, snap *backendSnapshot) {
	if snap.VpcAuthorizations != nil {
		b.vpcAuthorizations = snap.VpcAuthorizations
	} else {
		b.vpcAuthorizations = make(map[string][]AuthorizedPrincipal)
	}

	if snap.ScheduledActions != nil {
		b.scheduledActions = snap.ScheduledActions
	} else {
		b.scheduledActions = make(map[string][]*ScheduledAction)
	}

	if snap.PackageAssociations != nil {
		b.packageAssociations = snap.PackageAssociations
	} else {
		b.packageAssociations = make(map[string]map[string]bool)
	}

	if snap.DomainMaintenances != nil {
		b.domainMaintenances = snap.DomainMaintenances
	} else {
		b.domainMaintenances = make(map[string][]*DomainMaintenance)
	}

	if snap.UpgradeHistory != nil {
		b.upgradeHistory = snap.UpgradeHistory
	} else {
		b.upgradeHistory = make(map[string][]*UpgradeHistory)
	}

	// domainPackages is deliberately left untouched here -- see backendSnapshot's
	// doc comment: the pre-existing backend never persisted or restored this
	// reverse index either, and that asymmetry is preserved byte-for-byte.

	b.defaultApplicationArn = snap.DefaultApplicationArn
}

// fixNilDomainTags ensures that every restored domain has a valid Tags
// instance. When a domain is round-tripped through JSON, tags.Tags's
// UnmarshalJSON creates a new underlying safemap, but if the JSON value was
// null we still need a usable Tags.
func fixNilDomainTags(b *InMemoryBackend) {
	for _, d := range b.domains.All() {
		if d != nil && d.Tags == nil {
			d.Tags = tags.New("opensearch." + d.Name + ".tags")
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
