package kinesisanalyticsv2

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// kinesisanalyticsv2SnapshotVersion identifies the shape of [backendSnapshot].
// It must be bumped whenever a change to a DTO type or backendSnapshot
// itself would make an older snapshot unsafe to decode as the current shape.
// Restore compares this against the persisted value and discards
// (b.registry.ResetAll, not a partial decode) any mismatch -- see Restore.
// The pre-Phase-3.3 snapshot format had no version field at all, so an old
// snapshot decodes with Version == 0, which is guaranteed to mismatch
// kinesisanalyticsv2SnapshotVersion and is discarded the same way any other
// incompatible snapshot is.
//
// v2 added persistedApplication.CodeConfig/FlinkConfig/
// EnvironmentPropertyGroups/SnapshotsEnabled/RollbackEnabled/
// EncryptionConfig/RunConfig/LastUpdateTimestamp -- the
// ApplicationCodeConfiguration/FlinkApplicationConfiguration/
// EnvironmentProperties/ApplicationSnapshotConfiguration/
// ApplicationSystemRollbackConfiguration/ApplicationEncryptionConfiguration/
// RunConfiguration state that was previously accepted on the wire but never
// modeled at all (see PARITY.md). A v1 snapshot has none of these fields, so
// it must not be decoded as v2 (they'd silently come back nil/zero, which
// happens to be a safe default here, but the version-mismatch discard
// convention is simpler to reason about than special-casing this one field
// set as "safe to leave zero").
const kinesisanalyticsv2SnapshotVersion = 2

// persistedApplication is a persistence-safe representation of Application,
// including fields that use json:"-" on the live struct (CreatedAt, Region,
// Tags, and every *Descriptions slice) -- see store_setup.go's file doc
// comment for why applications can't be registered on b.registry for
// Snapshot/Restore directly.
type persistedApplication struct {
	LastUpdateTimestamp               time.Time                        `json:"last_update_timestamp"`
	ApplicationVersionCreateTimestamp time.Time                        `json:"application_version_create_timestamp"`
	CreatedAt                         time.Time                        `json:"created_at"`
	CodeConfig                        *ApplicationCodeConfigDesc       `json:"CodeConfig,omitempty"`
	ApplicationVersionRolledBackTo    *int64                           `json:"ApplicationVersionRolledBackTo,omitempty"`
	ApplicationVersionRolledBackFrom  *int64                           `json:"ApplicationVersionRolledBackFrom,omitempty"`
	ApplicationVersionUpdatedFrom     *int64                           `json:"ApplicationVersionUpdatedFrom,omitempty"`
	RunConfig                         *RunConfigDesc                   `json:"RunConfig,omitempty"`
	EncryptionConfig                  *ApplicationEncryptionConfigDesc `json:"EncryptionConfig,omitempty"`
	RollbackEnabled                   *bool                            `json:"RollbackEnabled,omitempty"`
	SnapshotsEnabled                  *bool                            `json:"SnapshotsEnabled,omitempty"`
	FlinkConfig                       *FlinkApplicationConfigDesc      `json:"FlinkConfig,omitempty"`
	RuntimeEnvironment                string                           `json:"RuntimeEnvironment"`
	ApplicationName                   string                           `json:"ApplicationName"`
	Region                            string                           `json:"region"`
	ApplicationARN                    string                           `json:"ApplicationARN"`
	ApplicationStatus                 string                           `json:"ApplicationStatus"`
	ServiceExecutionRole              string                           `json:"ServiceExecutionRole,omitempty"`
	ApplicationDescription            string                           `json:"ApplicationDescription,omitempty"`
	ApplicationMode                   string                           `json:"ApplicationMode,omitempty"`
	MaintenanceWindowStartTime        string                           `json:"MaintenanceWindowStartTime,omitempty"`
	Tags                              []Tag                            `json:"Tags"`
	EnvironmentPropertyGroups         []PropertyGroup                  `json:"EnvironmentPropertyGroups,omitempty"`
	VpcConfigurationDescriptions      []VpcConfigurationDescription    `json:"VpcConfigurationDescriptions"`
	ReferenceDataSourceDescriptions   []ReferenceDataSourceDescription `json:"ReferenceDataSourceDescriptions"`
	CloudWatchLoggingOptionDescs      []CloudWatchLoggingOptionDesc    `json:"CloudWatchLoggingOptionDescs"`
	OutputDescriptions                []OutputDescription              `json:"OutputDescriptions"`
	InputDescriptions                 []InputDescription               `json:"InputDescriptions"`
	ApplicationVersionID              int64                            `json:"ApplicationVersionId"`
}

// persistedApplicationKey is the [store.Table] key function used for the
// ephemeral DTO table built inside Snapshot/Restore. It mirrors
// applicationKeyFn so the on-disk table is keyed identically to the live
// b.applications table.
func persistedApplicationKey(p *persistedApplication) string {
	return applicationKey(p.Region, p.ApplicationName)
}

// persistedSnapshot is a persistence-safe representation of Snapshot,
// including fields that use json:"-" on the live struct (SnapshotCreation,
// Region, AppName).
type persistedSnapshot struct {
	SnapshotCreation   time.Time `json:"snapshot_creation"`
	Region             string    `json:"region"`
	AppName            string    `json:"appName"`
	ApplicationARN     string    `json:"ApplicationARN"`
	SnapshotName       string    `json:"SnapshotName"`
	SnapshotStatus     string    `json:"SnapshotStatus"`
	ApplicationVersion int64     `json:"ApplicationVersionId"`
}

// persistedSnapshotKey is the [store.Table] key function used for the
// ephemeral DTO table built inside Snapshot/Restore. It mirrors
// snapshotKeyFn so the on-disk table is keyed identically to the live
// b.snapshots table.
func persistedSnapshotKey(p *persistedSnapshot) string {
	return snapshotKey(p.Region, p.AppName, p.SnapshotName)
}

// newDTORegistry builds the ephemeral DTO [store.Registry] shared by
// Snapshot and Restore for the tables that can't be registered on
// b.registry directly for encoding purposes (see store_setup.go).
func newDTORegistry() (*store.Registry, *store.Table[persistedApplication], *store.Table[persistedSnapshot]) {
	dtoReg := store.NewRegistry()
	appDTOs := store.Register(dtoReg, "applications", store.New(persistedApplicationKey))
	snapDTOs := store.Register(dtoReg, "snapshots", store.New(persistedSnapshotKey))

	return dtoReg, appDTOs, snapDTOs
}

func toPersistedApp(app *Application) *persistedApplication {
	return &persistedApplication{
		CreatedAt:                         app.CreatedAt,
		LastUpdateTimestamp:               app.LastUpdateTimestamp,
		ApplicationVersionCreateTimestamp: app.ApplicationVersionCreateTimestamp,
		Region:                            app.Region,
		ApplicationARN:                    app.ApplicationARN,
		ApplicationName:                   app.ApplicationName,
		ApplicationStatus:                 app.ApplicationStatus,
		RuntimeEnvironment:                app.RuntimeEnvironment,
		ServiceExecutionRole:              app.ServiceExecutionRole,
		ApplicationDescription:            app.ApplicationDescription,
		ApplicationMode:                   app.ApplicationMode,
		// MaintenanceWindowStartTime was previously declared on
		// persistedApplication but never assigned here or restored below --
		// UpdateApplicationMaintenanceConfiguration state silently didn't
		// survive Snapshot/Restore. Fixed alongside the v2 field additions.
		MaintenanceWindowStartTime:       app.MaintenanceWindowStartTime,
		Tags:                             cloneTags(app.Tags),
		CloudWatchLoggingOptionDescs:     copyCWLOptions(app.CloudWatchLoggingOptionDescs),
		InputDescriptions:                copyInputDescs(app.InputDescriptions),
		OutputDescriptions:               copyOutputDescs(app.OutputDescriptions),
		ReferenceDataSourceDescriptions:  copyRefDataSources(app.ReferenceDataSourceDescriptions),
		VpcConfigurationDescriptions:     copyVpcConfigs(app.VpcConfigurationDescriptions),
		EnvironmentPropertyGroups:        copyPropertyGroups(app.EnvironmentPropertyGroups),
		CodeConfig:                       app.CodeConfig,
		FlinkConfig:                      app.FlinkConfig,
		SnapshotsEnabled:                 app.SnapshotsEnabled,
		RollbackEnabled:                  app.RollbackEnabled,
		EncryptionConfig:                 app.EncryptionConfig,
		RunConfig:                        app.RunConfig,
		ApplicationVersionUpdatedFrom:    app.ApplicationVersionUpdatedFrom,
		ApplicationVersionRolledBackFrom: app.ApplicationVersionRolledBackFrom,
		ApplicationVersionRolledBackTo:   app.ApplicationVersionRolledBackTo,
		ApplicationVersionID:             app.ApplicationVersionID,
	}
}

// ensureNonNil returns the slice if non-nil, otherwise returns an empty slice of the same type.
func ensureNonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}

	return s
}

func fromPersistedApp(p *persistedApplication) *Application {
	return &Application{
		CreatedAt:                         p.CreatedAt,
		LastUpdateTimestamp:               p.LastUpdateTimestamp,
		ApplicationVersionCreateTimestamp: p.ApplicationVersionCreateTimestamp,
		Region:                            p.Region,
		ApplicationARN:                    p.ApplicationARN,
		ApplicationName:                   p.ApplicationName,
		ApplicationStatus:                 p.ApplicationStatus,
		RuntimeEnvironment:                p.RuntimeEnvironment,
		ServiceExecutionRole:              p.ServiceExecutionRole,
		ApplicationDescription:            p.ApplicationDescription,
		ApplicationMode:                   p.ApplicationMode,
		MaintenanceWindowStartTime:        p.MaintenanceWindowStartTime,
		ApplicationVersionID:              p.ApplicationVersionID,
		Tags:                              ensureNonNil(p.Tags),
		CloudWatchLoggingOptionDescs:      ensureNonNil(p.CloudWatchLoggingOptionDescs),
		InputDescriptions:                 ensureNonNil(p.InputDescriptions),
		OutputDescriptions:                ensureNonNil(p.OutputDescriptions),
		ReferenceDataSourceDescriptions:   ensureNonNil(p.ReferenceDataSourceDescriptions),
		VpcConfigurationDescriptions:      ensureNonNil(p.VpcConfigurationDescriptions),
		EnvironmentPropertyGroups:         ensureNonNil(p.EnvironmentPropertyGroups),
		CodeConfig:                        p.CodeConfig,
		FlinkConfig:                       p.FlinkConfig,
		SnapshotsEnabled:                  p.SnapshotsEnabled,
		RollbackEnabled:                   p.RollbackEnabled,
		EncryptionConfig:                  p.EncryptionConfig,
		RunConfig:                         p.RunConfig,
		ApplicationVersionUpdatedFrom:     p.ApplicationVersionUpdatedFrom,
		ApplicationVersionRolledBackFrom:  p.ApplicationVersionRolledBackFrom,
		ApplicationVersionRolledBackTo:    p.ApplicationVersionRolledBackTo,
	}
}

func toPersistedSnap(s *Snapshot) *persistedSnapshot {
	return &persistedSnapshot{
		SnapshotCreation:   s.SnapshotCreation,
		Region:             s.Region,
		AppName:            s.AppName,
		ApplicationARN:     s.ApplicationARN,
		SnapshotName:       s.SnapshotName,
		SnapshotStatus:     s.SnapshotStatus,
		ApplicationVersion: s.ApplicationVersion,
	}
}

func fromPersistedSnap(p *persistedSnapshot) *Snapshot {
	return &Snapshot{
		SnapshotCreation:   p.SnapshotCreation,
		Region:             p.Region,
		AppName:            p.AppName,
		ApplicationARN:     p.ApplicationARN,
		SnapshotName:       p.SnapshotName,
		SnapshotStatus:     p.SnapshotStatus,
		ApplicationVersion: p.ApplicationVersion,
	}
}

// backendSnapshot is the top-level on-disk shape for the Kinesis Data
// Analytics v2 backend.
//
// Tables holds one JSON-encoded array per registered DTO table, produced by
// [store.Registry.SnapshotAll] on the ephemeral DTO registry -- "applications"
// ([]*persistedApplication) and "snapshots" ([]*persistedSnapshot). operations
// and versions are not persisted, matching pre-Phase-3.3 behavior (see
// InMemoryBackend's doc comment in backend.go). Version guards against
// decoding a snapshot from an incompatible (older or newer) build of this
// backend as though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables  map[string]json.RawMessage `json:"tables"`
	NextID  int64                      `json:"next_id"`
	Version int                        `json:"version"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	dtoReg, appDTOs, snapDTOs := newDTORegistry()

	for _, app := range b.applications.Snapshot() {
		appDTOs.Put(toPersistedApp(app))
	}

	for _, s := range b.snapshots.Snapshot() {
		snapDTOs.Put(toPersistedSnap(s))
	}

	tables, err := dtoReg.SnapshotAll()
	if err != nil {
		// The DTOs above are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the
		// Manager).
		logger.Load(ctx).WarnContext(ctx, "kinesisanalyticsv2: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version: kinesisanalyticsv2SnapshotVersion,
		Tables:  tables,
		NextID:  b.nextID,
	}

	return persistence.MarshalSnapshot(ctx, "kinesisanalyticsv2", &snap)
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "kinesisanalyticsv2", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != kinesisanalyticsv2SnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"kinesisanalyticsv2: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", kinesisanalyticsv2SnapshotVersion)

		b.registry.ResetAll()
		b.operations = make(map[string]map[string][]*ApplicationOperation)
		b.versions = make(map[string]map[string][]*Application)
		b.nextID = 0

		return nil
	}

	dtoReg, appDTOs, snapDTOs := newDTORegistry()

	if err := dtoReg.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("kinesisanalyticsv2: restore snapshot tables: %w", err)
	}

	liveApps := make([]*Application, 0, appDTOs.Len())
	for _, p := range appDTOs.All() {
		liveApps = append(liveApps, fromPersistedApp(p))
	}

	b.applications.Restore(liveApps)

	liveSnaps := make([]*Snapshot, 0, snapDTOs.Len())
	for _, p := range snapDTOs.All() {
		liveSnaps = append(liveSnaps, fromPersistedSnap(p))
	}

	b.snapshots.Restore(liveSnaps)

	// operations and versions are deliberately left untouched here, exactly
	// matching pre-Phase-3.3 behavior: neither was ever part of
	// backendSnapshot, so a Restore call never reset (or populated) them --
	// only Reset() and the version-mismatch branch above do that. See
	// InMemoryBackend's doc comment in backend.go.
	b.nextID = snap.NextID

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	if sb, ok := h.Backend.(*InMemoryBackend); ok {
		return sb.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if sb, ok := h.Backend.(*InMemoryBackend); ok {
		return sb.Restore(ctx, data)
	}

	return nil
}
