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
	"slices"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

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
	blueprintID, bundleID, availabilityZone string, publiclyAccessible bool,
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
	//nolint:gosec // G101 false positive: MasterUserPassword is assigned from a caller-supplied
	// variable (validated/defaulted above), not a hardcoded credential literal.
	db := &RelationalDatabase{
		Name:                       name,
		Arn:                        b.regionalARN(ResourceTypeRelationalDatabase, newUUID()),
		SupportCode:                newSupportCode(),
		State:                      RelationalDatabaseStateCreating,
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
		PreferredBackupWindow:      "07:00-07:30",
		PreferredMaintenanceWindow: "sun:08:00-sun:08:30",
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

// CreateRelationalDatabaseFromSnapshot restores a new database from an
// existing RelationalDatabaseSnapshot.
func (b *InMemoryBackend) CreateRelationalDatabaseFromSnapshot(
	name, snapshotName, availabilityZone, bundleID string, publiclyAccessible bool, userTags map[string]string,
) ([]Operation, error) {
	b.mu.Lock("CreateRelationalDatabaseFromSnapshot")
	defer b.mu.Unlock()

	snap, ok := b.dbSnapshots.Get(snapshotName)
	if !ok {
		return nil, notFoundError("RelationalDatabaseSnapshot", snapshotName)
	}

	bundle := bundleID
	if bundle == "" {
		bundle = snap.FromRelationalDatabaseBundleID
	}

	rdsBd, ok := findRDSBundle(bundle)
	if !ok {
		return nil, validationError("unknown RelationalDatabaseBundleId: " + bundle)
	}

	if err := b.registerNameLocked(ResourceTypeRelationalDatabase, name); err != nil {
		return nil, err
	}

	az := availabilityZone
	if az == "" {
		az = availabilityZoneA(b.region)
	}

	now := nowUTC()
	db := &RelationalDatabase{
		Name:                   name,
		Arn:                    b.regionalARN(ResourceTypeRelationalDatabase, newUUID()),
		SupportCode:            newSupportCode(),
		State:                  RelationalDatabaseStateCreating,
		Engine:                 snap.Engine,
		EngineVersion:          snap.EngineVersion,
		BlueprintID:            snap.FromRelationalDatabaseBlueprintID,
		BundleID:               bundle,
		CPUCount:               rdsBd.CPUCount,
		DiskSizeInGb:           rdsBd.DiskSizeInGb,
		RAMSizeInGb:            rdsBd.RAMSizeInGb,
		PubliclyAccessible:     publiclyAccessible,
		BackupRetentionEnabled: true,
		CreatedAt:              now,
		LatestRestorableTime:   now,
		Location:               ResourceLocation{RegionName: b.region, AvailabilityZone: az},
		Parameters:             defaultRDSParameters(),
		Tags:                   tags.New("lightsail.database." + name + ".tags"),
	}
	db.Tags.Merge(userTags)
	b.databases.Put(db)

	b.scheduleRDSAvailableLocked(name)

	return b.newOperationsLocked(opTypeCreateRelationalDatabase, ResourceTypeRelationalDatabase, []string{name}), nil
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

// UpdateRelationalDatabase applies caller-supplied updates to the named
// database.
func (b *InMemoryBackend) UpdateRelationalDatabase(
	name string, masterUserPassword, preferredBackupWindow, preferredMaintenanceWindow, caCertificateIdentifier string,
	enableBackupRetention, disableBackupRetention, publiclyAccessible *bool,
) ([]Operation, error) {
	b.mu.Lock("UpdateRelationalDatabase")
	defer b.mu.Unlock()

	db, ok := b.databases.Get(name)
	if !ok {
		return nil, notFoundError("RelationalDatabase", name)
	}

	if masterUserPassword != "" {
		db.PreviousMasterUserPassword = db.MasterUserPassword
		db.MasterUserPassword = masterUserPassword
	}

	if preferredBackupWindow != "" {
		db.PreferredBackupWindow = preferredBackupWindow
	}

	if preferredMaintenanceWindow != "" {
		db.PreferredMaintenanceWindow = preferredMaintenanceWindow
	}

	if caCertificateIdentifier != "" {
		db.CaCertificateIdentifier = caCertificateIdentifier
	}

	if enableBackupRetention != nil && *enableBackupRetention {
		db.BackupRetentionEnabled = true
	}

	if disableBackupRetention != nil && *disableBackupRetention {
		db.BackupRetentionEnabled = false
	}

	if publiclyAccessible != nil {
		db.PubliclyAccessible = *publiclyAccessible
	}

	return b.newOperationsLocked(opTypeUpdateRelationalDatabase, ResourceTypeRelationalDatabase, []string{name}), nil
}

// GetRelationalDatabaseEvents returns the named database's recorded events,
// paginated.
func (b *InMemoryBackend) GetRelationalDatabaseEvents(name, token string) (page.Page[RelationalDatabaseEvent], error) {
	b.mu.RLock("GetRelationalDatabaseEvents")
	defer b.mu.RUnlock()

	db, ok := b.databases.Get(name)
	if !ok {
		return page.Page[RelationalDatabaseEvent]{}, notFoundError("RelationalDatabase", name)
	}

	events := append([]RelationalDatabaseEvent(nil), db.Events...)
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

// GetRelationalDatabaseMasterUserPassword genuinely decrypts (returns) the
// real master password material this backend stores, by PasswordVersion
// (PARITY.md 4.3): CURRENT is db.MasterUserPassword, PREVIOUS is the value
// before the most recent UpdateRelationalDatabase rotation (empty if none
// occurred), PENDING is not modeled (this backend applies password changes
// immediately, never queues one) and falls back to CURRENT.
func (b *InMemoryBackend) GetRelationalDatabaseMasterUserPassword(name, passwordVersion string) (string, error) {
	b.mu.RLock("GetRelationalDatabaseMasterUserPassword")
	defer b.mu.RUnlock()

	db, ok := b.databases.Get(name)
	if !ok {
		return "", notFoundError("RelationalDatabase", name)
	}

	if passwordVersion == PasswordVersionPrevious {
		return db.PreviousMasterUserPassword, nil
	}

	return db.MasterUserPassword, nil
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
