package lightsail

// This file backs family N (9 ops: CreateRelationalDatabase,
// CreateRelationalDatabaseFromSnapshot, DeleteRelationalDatabase,
// GetRelationalDatabase, GetRelationalDatabases, StartRelationalDatabase,
// StopRelationalDatabase, RebootRelationalDatabase, UpdateRelationalDatabase),
// family O (7 ops: GetRelationalDatabaseEvents, GetRelationalDatabaseLogEvents,
// GetRelationalDatabaseLogStreams, GetRelationalDatabaseMasterUserPassword,
// GetRelationalDatabaseMetricData, GetRelationalDatabaseParameters,
// UpdateRelationalDatabaseParameters), and family P (4 ops:
// CreateRelationalDatabaseSnapshot, DeleteRelationalDatabaseSnapshot,
// GetRelationalDatabaseSnapshot, GetRelationalDatabaseSnapshots) -- the
// largest self-contained sub-product (20 ops, PARITY.md's suggested
// implementation ordering step 8).

import (
	"cmp"
	"slices"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// defaultEventDurationMinutes is GetRelationalDatabaseEventsInput.DurationInMinutes's
// documented default ("to get all events from the past 2 hours, enter 120... Default: 60").
const defaultEventDurationMinutes = 60

const (
	opTypeCreateRelationalDatabase           = "CreateRelationalDatabase"
	opTypeDeleteRelationalDatabase           = "DeleteRelationalDatabase"
	opTypeStartRelationalDatabase            = "StartRelationalDatabase"
	opTypeStopRelationalDatabase             = "StopRelationalDatabase"
	opTypeRebootRelationalDatabase           = "RebootRelationalDatabase"
	opTypeUpdateRelationalDatabase           = "UpdateRelationalDatabase"
	opTypeUpdateRelationalDatabaseParameters = "UpdateRelationalDatabaseParameters"
	opTypeCreateRelationalDatabaseSnapshot   = "CreateRelationalDatabaseSnapshot"
	opTypeDeleteRelationalDatabaseSnapshot   = "DeleteRelationalDatabaseSnapshot"

	// defaultRDSLogStreams is a defensible, SDK-unconfirmed stand-in log
	// stream catalog by AWS RDS-family convention (PARITY.md 4.3) -- this
	// SDK module does not enumerate real Lightsail log stream names
	// anywhere.
	rdsLogStreamError = "error/mysqld.log"
	rdsLogStreamSlow  = "slow-query/mysql-slow.log"
)

//nolint:gochecknoglobals // static reference table, read-only
var seedRDSLogStreams = []string{rdsLogStreamError, rdsLogStreamSlow}

// defaultRDSParameters is a small, defensible, CLEARLY SYNTHETIC seed
// parameter set for the mysql engine (PARITY.md 4.3: no real Lightsail
// parameter catalog is published in this SDK module).
func defaultRDSParameters() map[string]RelationalDatabaseParameter {
	seed := []RelationalDatabaseParameter{
		{
			ParameterName: "max_connections", ParameterValue: "100", DataType: "integer",
			ApplyType: "dynamic", ApplyMethod: "immediate", IsModifiable: true,
			Description: "The maximum permitted number of simultaneous client connections",
		},
		{
			ParameterName: "character_set_server", ParameterValue: "utf8mb4", DataType: "string",
			ApplyType: "dynamic", ApplyMethod: "immediate", IsModifiable: true,
			Description: "The server's default character set",
		},
	}
	out := make(map[string]RelationalDatabaseParameter, len(seed))

	for _, p := range seed {
		out[p.ParameterName] = p
	}

	return out
}

// CreateRelationalDatabase creates a new managed database. The Engine
// value is NOT restricted to "mysql" -- this SDK module's typed enum
// documents itself as open/expandable (PARITY.md 4.3), so a caller-supplied
// non-mysql engine string is accepted rather than rejected on the strength
// of Values() listing only one value.
func (b *InMemoryBackend) CreateRelationalDatabase(
	name, masterDatabaseName, masterUsername, masterUserPassword,
	blueprintID, bundleID, availabilityZone, preferredBackupWindow, preferredMaintenanceWindow string,
	publiclyAccessible bool,
	userTags map[string]string,
) ([]Operation, error) {
	rdsBd, ok := findRDSBundle(bundleID)
	if !ok {
		return nil, validationError("unknown RelationalDatabaseBundleId: " + bundleID)
	}

	bp, bpOK := findRDSBlueprint(blueprintID)
	if !bpOK {
		return nil, validationError("unknown RelationalDatabaseBlueprintId: " + blueprintID)
	}

	if masterUserPassword == "" {
		masterUserPassword = newSupportCode()
	} else if err := validateMasterPassword(bp.Engine, masterUserPassword); err != nil {
		return nil, err
	}

	backupWindow := cmp.Or(preferredBackupWindow, defaultPreferredBackupWindow)
	maintenanceWindow := cmp.Or(preferredMaintenanceWindow, defaultPreferredMaintenanceWindow)

	if err := validateDatabaseWindows(backupWindow, maintenanceWindow); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateRelationalDatabase")
	defer b.mu.Unlock()

	if err := b.registerNameLocked(ResourceTypeRelationalDatabase, name); err != nil {
		return nil, err
	}

	az := availabilityZone
	if az == "" {
		az = availabilityZoneA(b.region)
	}

	now := nowUTC()
	db := &RelationalDatabase{
		Name:                       name,
		Arn:                        b.regionalARN(ResourceTypeRelationalDatabase, newUUID()),
		SupportCode:                newSupportCode(),
		State:                      RelationalDatabaseStateCreating,
		MasterUserPasswordSetAt:    now,
		Engine:                     bp.Engine,
		EngineVersion:              bp.EngineVersion,
		MasterDatabaseName:         masterDatabaseName,
		MasterUsername:             masterUsername,
		MasterUserPassword:         masterUserPassword,
		BlueprintID:                blueprintID,
		BundleID:                   bundleID,
		CPUCount:                   rdsBd.CPUCount,
		DiskSizeInGb:               rdsBd.DiskSizeInGb,
		RAMSizeInGb:                rdsBd.RAMSizeInGb,
		PubliclyAccessible:         publiclyAccessible,
		BackupRetentionEnabled:     true,
		PreferredBackupWindow:      backupWindow,
		PreferredMaintenanceWindow: maintenanceWindow,
		CreatedAt:                  now,
		LatestRestorableTime:       now,
		Location:                   ResourceLocation{RegionName: b.region, AvailabilityZone: az},
		Parameters:                 defaultRDSParameters(),
		Tags:                       tags.New("lightsail.database." + name + ".tags"),
	}
	db.Tags.Merge(userTags)
	b.databases.Put(db)

	b.scheduleRDSAvailableLocked(name)

	return b.newOperationsLocked(opTypeCreateRelationalDatabase, ResourceTypeRelationalDatabase, []string{name}), nil
}

func findRDSBundle(id string) (*RelationalDatabaseBundle, bool) {
	for _, bd := range seedRDSBundles {
		if bd.BundleID == id {
			return &bd, true
		}
	}

	return nil, false
}

func findRDSBlueprint(id string) (*RelationalDatabaseBlueprint, bool) {
	for _, bp := range seedRDSBlueprints {
		if bp.BlueprintID == id {
			return &bp, true
		}
	}

	return nil, false
}

func (b *InMemoryBackend) scheduleRDSAvailableLocked(name string) {
	b.work.After("RelationalDatabaseAvailable", asyncTransitionDelay, func() {
		b.mu.Lock("RelationalDatabase-async-available")
		defer b.mu.Unlock()

		if db, found := b.databases.Get(name); found && db.State == RelationalDatabaseStateCreating {
			db.State = RelationalDatabaseStateAvailable
		}
	})
}

// RestoreDatabaseRequest is CreateRelationalDatabaseFromSnapshot's input: a
// snapshot, or a source database plus RestoreTime/UseLatestRestorableTime.
type RestoreDatabaseRequest struct {
	RestoreTime             *time.Time
	Tags                    map[string]string
	Name                    string
	SnapshotName            string
	SourceName              string
	AvailabilityZone        string
	BundleID                string
	PubliclyAccessible      bool
	UseLatestRestorableTime bool
}

// restoreOrigin is what a restored database inherits from its snapshot or source.
type restoreOrigin struct {
	passwordSetAt      time.Time
	engine             string
	engineVersion      string
	blueprintID        string
	bundleID           string
	masterDatabaseName string
	masterUsername     string
	password           string
}

// resolveRestoreOriginLocked validates the restore request against its snapshot
// or source database. Caller must hold b.mu.
func (b *InMemoryBackend) resolveRestoreOriginLocked(req *RestoreDatabaseRequest) (restoreOrigin, error) {
	if req.SnapshotName != "" {
		snap, ok := b.dbSnapshots.Get(req.SnapshotName)
		if !ok {
			return restoreOrigin{}, notFoundError("RelationalDatabaseSnapshot", req.SnapshotName)
		}

		return restoreOrigin{
			engine: snap.Engine, engineVersion: snap.EngineVersion,
			blueprintID: snap.FromRelationalDatabaseBlueprintID, bundleID: snap.FromRelationalDatabaseBundleID,
		}, nil
	}

	if req.SourceName == "" {
		return restoreOrigin{}, validationError(
			"either relationalDatabaseSnapshotName or sourceRelationalDatabaseName is required",
		)
	}

	if req.RestoreTime != nil && req.UseLatestRestorableTime {
		return restoreOrigin{}, validationError("restoreTime cannot be specified with useLatestRestorableTime")
	}

	if req.RestoreTime == nil && !req.UseLatestRestorableTime {
		return restoreOrigin{}, validationError(
			"restoreTime or useLatestRestorableTime is required with sourceRelationalDatabaseName",
		)
	}

	src, ok := b.databases.Get(req.SourceName)
	if !ok {
		return restoreOrigin{}, notFoundError("RelationalDatabase", req.SourceName)
	}

	view := src.clone()
	if !view.BackupRetentionEnabled {
		return restoreOrigin{}, validationError("automated backups are not enabled for " + req.SourceName)
	}

	if req.RestoreTime != nil &&
		(req.RestoreTime.Before(view.CreatedAt) || !req.RestoreTime.Before(view.LatestRestorableTime)) {
		return restoreOrigin{}, validationError(
			"restoreTime must be after the database was created and before the latest restorable time",
		)
	}

	return restoreOrigin{
		engine: view.Engine, engineVersion: view.EngineVersion, blueprintID: view.BlueprintID,
		bundleID: view.BundleID, masterDatabaseName: view.MasterDatabaseName,
		masterUsername: view.MasterUsername, password: view.MasterUserPassword,
		passwordSetAt: view.MasterUserPasswordSetAt,
	}, nil
}

// CreateRelationalDatabaseFromSnapshot restores a new database from a
// RelationalDatabaseSnapshot, or from a source database's automated backups.
func (b *InMemoryBackend) CreateRelationalDatabaseFromSnapshot(req *RestoreDatabaseRequest) ([]Operation, error) {
	b.mu.Lock("CreateRelationalDatabaseFromSnapshot")
	defer b.mu.Unlock()

	origin, err := b.resolveRestoreOriginLocked(req)
	if err != nil {
		return nil, err
	}

	bundle := cmp.Or(req.BundleID, origin.bundleID)

	rdsBd, ok := findRDSBundle(bundle)
	if !ok {
		return nil, validationError("unknown RelationalDatabaseBundleId: " + bundle)
	}

	if orig, found := findRDSBundle(origin.bundleID); found &&
		(rdsBd.RAMSizeInGb < orig.RAMSizeInGb || rdsBd.DiskSizeInGb < orig.DiskSizeInGb) {
		return nil, validationError("the bundle cannot be smaller than the source database's bundle")
	}

	if err = b.registerNameLocked(ResourceTypeRelationalDatabase, req.Name); err != nil {
		return nil, err
	}

	now := nowUTC()
	db := &RelationalDatabase{
		Name:                       req.Name,
		Arn:                        b.regionalARN(ResourceTypeRelationalDatabase, newUUID()),
		SupportCode:                newSupportCode(),
		State:                      RelationalDatabaseStateCreating,
		Engine:                     origin.engine,
		EngineVersion:              origin.engineVersion,
		MasterDatabaseName:         origin.masterDatabaseName,
		MasterUsername:             origin.masterUsername,
		MasterUserPassword:         origin.password,
		MasterUserPasswordSetAt:    cmp.Or(origin.passwordSetAt, now),
		BlueprintID:                origin.blueprintID,
		BundleID:                   bundle,
		CPUCount:                   rdsBd.CPUCount,
		DiskSizeInGb:               rdsBd.DiskSizeInGb,
		RAMSizeInGb:                rdsBd.RAMSizeInGb,
		PubliclyAccessible:         req.PubliclyAccessible,
		BackupRetentionEnabled:     true,
		PreferredBackupWindow:      defaultPreferredBackupWindow,
		PreferredMaintenanceWindow: defaultPreferredMaintenanceWindow,
		CreatedAt:                  now,
		LatestRestorableTime:       now,
		Location: ResourceLocation{
			RegionName: b.region, AvailabilityZone: cmp.Or(req.AvailabilityZone, availabilityZoneA(b.region)),
		},
		Parameters: defaultRDSParameters(),
		Tags:       tags.New("lightsail.database." + req.Name + ".tags"),
	}
	db.Tags.Merge(req.Tags)
	b.databases.Put(db)

	b.scheduleRDSAvailableLocked(req.Name)

	return b.newOperationsLocked(
		opTypeCreateRelationalDatabase,
		ResourceTypeRelationalDatabase,
		[]string{req.Name},
	), nil
}

// DeleteRelationalDatabase deletes the named database, optionally taking a
// final snapshot first.
func (b *InMemoryBackend) DeleteRelationalDatabase(
	name, finalSnapshotName string,
	skipFinalSnapshot bool,
) ([]Operation, error) {
	b.mu.Lock("DeleteRelationalDatabase")
	defer b.mu.Unlock()

	db, ok := b.databases.Get(name)
	if !ok {
		return nil, notFoundError("RelationalDatabase", name)
	}

	if !skipFinalSnapshot {
		if finalSnapshotName == "" {
			return nil, validationError(
				"FinalRelationalDatabaseSnapshotName is required unless SkipFinalSnapshot is true",
			)
		}

		if err := b.createDBSnapshotLocked(db, finalSnapshotName, nil); err != nil {
			return nil, err
		}
	}

	if db.Tags != nil {
		db.Tags.Close()
	}

	b.databases.Delete(name)
	b.unregisterNameLocked(name)

	return b.newOperationsLocked(opTypeDeleteRelationalDatabase, ResourceTypeRelationalDatabase, []string{name}), nil
}

// GetRelationalDatabase returns the named database.
func (b *InMemoryBackend) GetRelationalDatabase(name string) (*RelationalDatabase, error) {
	b.mu.RLock("GetRelationalDatabase")
	defer b.mu.RUnlock()

	db, ok := b.databases.Get(name)
	if !ok {
		return nil, notFoundError("RelationalDatabase", name)
	}

	return db.clone(), nil
}

// GetRelationalDatabases returns every database, paginated.
func (b *InMemoryBackend) GetRelationalDatabases(token string) (page.Page[*RelationalDatabase], error) {
	b.mu.RLock("GetRelationalDatabases")
	defer b.mu.RUnlock()

	all := b.databases.All()
	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	out := make([]*RelationalDatabase, len(all))
	for i, v := range all {
		out[i] = v.clone()
	}

	return paginateGeneric(out, token)
}

// StartRelationalDatabase transitions the named database from stopped to
// available.
func (b *InMemoryBackend) StartRelationalDatabase(name string) ([]Operation, error) {
	b.mu.Lock("StartRelationalDatabase")
	defer b.mu.Unlock()

	db, ok := b.databases.Get(name)
	if !ok {
		return nil, notFoundError("RelationalDatabase", name)
	}

	db.State = RelationalDatabaseStateStarting
	b.scheduleRDSStateLocked(name, RelationalDatabaseStateStarting, RelationalDatabaseStateAvailable)

	return b.newOperationsLocked(opTypeStartRelationalDatabase, ResourceTypeRelationalDatabase, []string{name}), nil
}

// StopRelationalDatabase transitions the named database from available to
// stopped, optionally taking a snapshot first.
func (b *InMemoryBackend) StopRelationalDatabase(name, snapshotName string) ([]Operation, error) {
	b.mu.Lock("StopRelationalDatabase")
	defer b.mu.Unlock()

	db, ok := b.databases.Get(name)
	if !ok {
		return nil, notFoundError("RelationalDatabase", name)
	}

	if snapshotName != "" {
		if err := b.createDBSnapshotLocked(db, snapshotName, nil); err != nil {
			return nil, err
		}
	}

	db.State = RelationalDatabaseStateStopping
	b.scheduleRDSStateLocked(name, RelationalDatabaseStateStopping, RelationalDatabaseStateStopped)

	return b.newOperationsLocked(opTypeStopRelationalDatabase, ResourceTypeRelationalDatabase, []string{name}), nil
}

// RebootRelationalDatabase reboots the named database.
func (b *InMemoryBackend) RebootRelationalDatabase(name string) ([]Operation, error) {
	b.mu.Lock("RebootRelationalDatabase")
	defer b.mu.Unlock()

	db, ok := b.databases.Get(name)
	if !ok {
		return nil, notFoundError("RelationalDatabase", name)
	}

	db.State = RelationalDatabaseStateRebooting
	b.scheduleRDSStateLocked(name, RelationalDatabaseStateRebooting, RelationalDatabaseStateAvailable)
	b.addRDSEventLocked(db, "Database instance rebooted", "notification")

	return b.newOperationsLocked(opTypeRebootRelationalDatabase, ResourceTypeRelationalDatabase, []string{name}), nil
}

func (b *InMemoryBackend) scheduleRDSStateLocked(name, fromState, toState string) {
	b.work.After("RelationalDatabaseTransition", asyncTransitionDelay, func() {
		b.mu.Lock("RelationalDatabase-async-transition")
		defer b.mu.Unlock()

		if db, found := b.databases.Get(name); found && db.State == fromState {
			db.State = toState
		}
	})
}

func (b *InMemoryBackend) addRDSEventLocked(db *RelationalDatabase, message, category string) {
	db.Events = append(db.Events, RelationalDatabaseEvent{
		Message: message, Resource: db.Name, CreatedAt: nowUTC(), EventCategories: []string{category},
	})
}

// UpdateDatabaseRequest is UpdateRelationalDatabase's input; nil pointers mean
// "leave unchanged".
type UpdateDatabaseRequest struct {
	EnableBackupRetention    *bool
	DisableBackupRetention   *bool
	PubliclyAccessible       *bool
	Name                     string
	MasterUserPassword       string
	PreferredBackupWindow    string
	PreferredMaintenance     string
	CaCertificateIdentifier  string
	BlueprintID              string
	ApplyImmediately         bool
	RotateMasterUserPassword bool
}

// UpdateRelationalDatabase applies caller-supplied updates to the named
// database. Password and engine-version changes wait for the next maintenance
// window unless ApplyImmediately; backup-retention changes always wait
// (api_op_UpdateRelationalDatabase.go).
func (b *InMemoryBackend) UpdateRelationalDatabase(req *UpdateDatabaseRequest) ([]Operation, error) {
	b.mu.Lock("UpdateRelationalDatabase")
	defer b.mu.Unlock()

	db, ok := b.databases.Get(req.Name)
	if !ok {
		return nil, notFoundError("RelationalDatabase", req.Name)
	}

	now := nowUTC()
	db.applyDue(now)

	newBP, err := validateDatabaseUpdate(db, req)
	if err != nil {
		return nil, err
	}

	if req.PreferredBackupWindow != "" {
		db.PreferredBackupWindow = req.PreferredBackupWindow
	}

	if req.PreferredMaintenance != "" {
		db.PreferredMaintenanceWindow = req.PreferredMaintenance
	}

	if req.CaCertificateIdentifier != "" {
		db.CaCertificateIdentifier = req.CaCertificateIdentifier
	}

	if req.PubliclyAccessible != nil {
		db.PubliclyAccessible = *req.PubliclyAccessible
	}

	stageDatabaseChanges(db, req, newBP, now)

	return b.newOperationsLocked(
		opTypeUpdateRelationalDatabase,
		ResourceTypeRelationalDatabase,
		[]string{req.Name},
	), nil
}

func validateDatabaseUpdate(db *RelationalDatabase, req *UpdateDatabaseRequest) (*RelationalDatabaseBlueprint, error) {
	backup := cmp.Or(req.PreferredBackupWindow, db.PreferredBackupWindow)
	maintenance := cmp.Or(req.PreferredMaintenance, db.PreferredMaintenanceWindow)

	if req.PreferredBackupWindow != "" || req.PreferredMaintenance != "" {
		if err := validateDatabaseWindows(backup, maintenance); err != nil {
			return nil, err
		}
	}

	if req.MasterUserPassword != "" {
		if err := validateMasterPassword(db.Engine, req.MasterUserPassword); err != nil {
			return nil, err
		}
	}

	if req.BlueprintID == "" {
		return nil, nil //nolint:nilnil // no blueprint change requested
	}

	bp, ok := findRDSBlueprint(req.BlueprintID)
	if !ok {
		return nil, validationError("unknown RelationalDatabaseBlueprintId: " + req.BlueprintID)
	}

	if bp.Engine != db.Engine {
		return nil, validationError("relationalDatabaseBlueprintId must use the database's engine " + db.Engine)
	}

	return bp, nil
}

// stageDatabaseChanges applies the password, engine-version and backup-retention
// changes now or queues them for the next maintenance window.
func stageDatabaseChanges(
	db *RelationalDatabase,
	req *UpdateDatabaseRequest,
	bp *RelationalDatabaseBlueprint,
	now time.Time,
) {
	password := req.MasterUserPassword
	if password == "" && req.RotateMasterUserPassword {
		password = newSupportCode()
	}

	applyAt := nextWindowStart(now, db.PreferredMaintenanceWindow)

	if password != "" {
		if req.ApplyImmediately {
			db.setMasterPassword(password, now)
		} else {
			p := db.pending()
			p.MasterUserPassword, p.MasterUserPasswordSetAt, p.ApplyAt = password, now, applyAt
		}
	}

	if bp != nil && bp.EngineVersion != db.EngineVersion {
		if req.ApplyImmediately {
			db.EngineVersion, db.BlueprintID = bp.EngineVersion, bp.BlueprintID
		} else {
			p := db.pending()
			p.EngineVersion, p.BlueprintID, p.ApplyAt = bp.EngineVersion, bp.BlueprintID, applyAt
		}
	}

	var retention *bool

	switch {
	case req.EnableBackupRetention != nil && *req.EnableBackupRetention:
		retention = req.EnableBackupRetention
	case req.DisableBackupRetention != nil && *req.DisableBackupRetention:
		off := false
		retention = &off
	default:
	}

	if retention != nil && *retention != db.BackupRetentionEnabled {
		p := db.pending()
		p.BackupRetentionEnabled, p.ApplyAt = retention, applyAt
	}
}

// GetRelationalDatabaseEvents returns the named database's recorded events
// from the last durationInMinutes (default 60, per
// GetRelationalDatabaseEventsInput's documented default), paginated.
func (b *InMemoryBackend) GetRelationalDatabaseEvents(
	name, token string, durationInMinutes int32,
) (page.Page[RelationalDatabaseEvent], error) {
	b.mu.RLock("GetRelationalDatabaseEvents")
	defer b.mu.RUnlock()

	db, ok := b.databases.Get(name)
	if !ok {
		return page.Page[RelationalDatabaseEvent]{}, notFoundError("RelationalDatabase", name)
	}

	duration := durationInMinutes
	if duration <= 0 {
		duration = defaultEventDurationMinutes
	}

	cutoff := nowUTC().Add(-time.Duration(duration) * time.Minute)

	events := make([]RelationalDatabaseEvent, 0, len(db.Events))

	for _, e := range db.Events {
		if !e.CreatedAt.Before(cutoff) {
			events = append(events, e)
		}
	}

	sort.Slice(events, func(i, j int) bool { return events[i].CreatedAt.Before(events[j].CreatedAt) })

	return paginateGeneric(events, token)
}

// GetRelationalDatabaseLogEvents returns a real, well-formed, EMPTY log
// event page for the named database/log stream -- this emulator runs no
// real MySQL server to produce genuine log lines from, so returning
// plausible-looking fabricated log text would violate parity-principles.md
// exactly like the metric-data ops (PARITY.md 4.10's sibling risk).
func (b *InMemoryBackend) GetRelationalDatabaseLogEvents(name, logStreamName string) error {
	b.mu.RLock("GetRelationalDatabaseLogEvents")
	defer b.mu.RUnlock()

	if _, ok := b.databases.Get(name); !ok {
		return notFoundError("RelationalDatabase", name)
	}

	if logStreamName != "" && !slices.Contains(seedRDSLogStreams, logStreamName) {
		return notFoundError("log stream", logStreamName)
	}

	return nil
}

// GetRelationalDatabaseLogStreams returns the seed log-stream-name catalog
// (PARITY.md 4.3: no real catalog is published in this SDK module).
func (b *InMemoryBackend) GetRelationalDatabaseLogStreams(name string) ([]string, error) {
	b.mu.RLock("GetRelationalDatabaseLogStreams")
	defer b.mu.RUnlock()

	if _, ok := b.databases.Get(name); !ok {
		return nil, notFoundError("RelationalDatabase", name)
	}

	return append([]string(nil), seedRDSLogStreams...), nil
}

// GetRelationalDatabaseMasterUserPassword returns the password for the
// requested version and when it was set: CURRENT, PREVIOUS (before the most
// recent rotation), or PENDING (queued for the next maintenance window; absent
// once promoted, per api_op_GetRelationalDatabaseMasterUserPassword.go).
func (b *InMemoryBackend) GetRelationalDatabaseMasterUserPassword(
	name, passwordVersion string,
) (string, time.Time, error) {
	b.mu.RLock("GetRelationalDatabaseMasterUserPassword")
	defer b.mu.RUnlock()

	stored, ok := b.databases.Get(name)
	if !ok {
		return "", time.Time{}, notFoundError("RelationalDatabase", name)
	}

	db := stored.clone()

	switch passwordVersion {
	case PasswordVersionPrevious:
		return db.PreviousMasterUserPassword, db.PreviousMasterUserPasswordSetAt, nil
	case PasswordVersionPending:
		if db.Pending == nil || db.Pending.MasterUserPassword == "" {
			return "", time.Time{}, validationError("no pending master user password for " + name)
		}

		return db.Pending.MasterUserPassword, db.Pending.MasterUserPasswordSetAt, nil
	default:
		return db.MasterUserPassword, db.MasterUserPasswordSetAt, nil
	}
}

// GetRelationalDatabaseMetricData returns a real, well-formed, EMPTY
// MetricData response -- one of the six honestly-unfakeable telemetry ops
// (PARITY.md 4.10).
func (b *InMemoryBackend) GetRelationalDatabaseMetricData(name string) error {
	b.mu.RLock("GetRelationalDatabaseMetricData")
	defer b.mu.RUnlock()

	if _, ok := b.databases.Get(name); !ok {
		return notFoundError("RelationalDatabase", name)
	}

	return nil
}

// GetRelationalDatabaseParameters returns the named database's parameter
// list, paginated.
func (b *InMemoryBackend) GetRelationalDatabaseParameters(
	name, token string,
) (page.Page[RelationalDatabaseParameter], error) {
	b.mu.RLock("GetRelationalDatabaseParameters")
	defer b.mu.RUnlock()

	db, ok := b.databases.Get(name)
	if !ok {
		return page.Page[RelationalDatabaseParameter]{}, notFoundError("RelationalDatabase", name)
	}

	out := make([]RelationalDatabaseParameter, 0, len(db.Parameters))
	for _, p := range db.Parameters {
		out = append(out, p)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ParameterName < out[j].ParameterName })

	return paginateGeneric(out, token)
}

// UpdateRelationalDatabaseParameters updates one or more of the named
// database's modifiable parameters.
func (b *InMemoryBackend) UpdateRelationalDatabaseParameters(
	name string,
	params []RelationalDatabaseParameter,
) ([]Operation, error) {
	b.mu.Lock("UpdateRelationalDatabaseParameters")
	defer b.mu.Unlock()

	db, ok := b.databases.Get(name)
	if !ok {
		return nil, notFoundError("RelationalDatabase", name)
	}

	for _, p := range params {
		existing, found := db.Parameters[p.ParameterName]
		if found && !existing.IsModifiable {
			return nil, validationError("parameter " + p.ParameterName + " is not modifiable")
		}

		if found {
			existing.ParameterValue = p.ParameterValue
			db.Parameters[p.ParameterName] = existing
		} else {
			p.IsModifiable = true
			db.Parameters[p.ParameterName] = p
		}
	}

	db.ParameterApplyStatus = "pending-reboot"

	return b.newOperationsLocked(
		opTypeUpdateRelationalDatabaseParameters,
		ResourceTypeRelationalDatabase,
		[]string{name},
	), nil
}

// createDBSnapshotLocked creates a RelationalDatabaseSnapshot of db.
// Callers must hold b.mu.
func (b *InMemoryBackend) createDBSnapshotLocked(
	db *RelationalDatabase,
	snapshotName string,
	userTags map[string]string,
) error {
	if err := b.registerNameLocked(ResourceTypeRelationalDatabaseSnapshot, snapshotName); err != nil {
		return err
	}

	snap := &RelationalDatabaseSnapshot{
		Name:                              snapshotName,
		Arn:                               b.regionalARN(ResourceTypeRelationalDatabaseSnapshot, newUUID()),
		SupportCode:                       newSupportCode(),
		State:                             SnapshotStateAvailable,
		Engine:                            db.Engine,
		EngineVersion:                     db.EngineVersion,
		FromRelationalDatabaseName:        db.Name,
		FromRelationalDatabaseArn:         db.Arn,
		FromRelationalDatabaseBlueprintID: db.BlueprintID,
		FromRelationalDatabaseBundleID:    db.BundleID,
		SizeInGb:                          db.DiskSizeInGb,
		CreatedAt:                         nowUTC(),
		Location:                          db.Location,
		Tags:                              tags.New("lightsail.dbsnapshot." + snapshotName + ".tags"),
	}
	snap.Tags.Merge(userTags)
	b.dbSnapshots.Put(snap)

	return nil
}

// CreateRelationalDatabaseSnapshot creates a snapshot of the named
// database.
func (b *InMemoryBackend) CreateRelationalDatabaseSnapshot(
	dbName, snapshotName string,
	userTags map[string]string,
) ([]Operation, error) {
	b.mu.Lock("CreateRelationalDatabaseSnapshot")
	defer b.mu.Unlock()

	db, ok := b.databases.Get(dbName)
	if !ok {
		return nil, notFoundError("RelationalDatabase", dbName)
	}

	if err := b.createDBSnapshotLocked(db, snapshotName, userTags); err != nil {
		return nil, err
	}

	return b.newOperationsLocked(
		opTypeCreateRelationalDatabaseSnapshot,
		ResourceTypeRelationalDatabaseSnapshot,
		[]string{snapshotName},
	), nil
}

// DeleteRelationalDatabaseSnapshot deletes the named database snapshot.
func (b *InMemoryBackend) DeleteRelationalDatabaseSnapshot(name string) ([]Operation, error) {
	b.mu.Lock("DeleteRelationalDatabaseSnapshot")
	defer b.mu.Unlock()

	snap, ok := b.dbSnapshots.Get(name)
	if !ok {
		return nil, notFoundError("RelationalDatabaseSnapshot", name)
	}

	if snap.Tags != nil {
		snap.Tags.Close()
	}

	b.dbSnapshots.Delete(name)
	b.unregisterNameLocked(name)

	return b.newOperationsLocked(
		opTypeDeleteRelationalDatabaseSnapshot,
		ResourceTypeRelationalDatabaseSnapshot,
		[]string{name},
	), nil
}

// GetRelationalDatabaseSnapshot returns the named database snapshot.
func (b *InMemoryBackend) GetRelationalDatabaseSnapshot(name string) (*RelationalDatabaseSnapshot, error) {
	b.mu.RLock("GetRelationalDatabaseSnapshot")
	defer b.mu.RUnlock()

	snap, ok := b.dbSnapshots.Get(name)
	if !ok {
		return nil, notFoundError("RelationalDatabaseSnapshot", name)
	}

	return snap.clone(), nil
}

// GetRelationalDatabaseSnapshots returns every database snapshot, paginated.
func (b *InMemoryBackend) GetRelationalDatabaseSnapshots(token string) (page.Page[*RelationalDatabaseSnapshot], error) {
	b.mu.RLock("GetRelationalDatabaseSnapshots")
	defer b.mu.RUnlock()

	all := b.dbSnapshots.All()
	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	out := make([]*RelationalDatabaseSnapshot, len(all))
	for i, v := range all {
		out[i] = v.clone()
	}

	return paginateGeneric(out, token)
}
