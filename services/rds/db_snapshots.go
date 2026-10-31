package rds

import (
	"cmp"
	"fmt"
	"net/url"
	"slices"
	"time"
)

// CreateDBSnapshot creates a snapshot of the given DB instance.
func (b *InMemoryBackend) CreateDBSnapshot(snapshotID, instanceID string) (*DBSnapshot, error) {
	return b.CreateDBSnapshotAs("", snapshotID, instanceID)
}

// CreateDBSnapshotAs is CreateDBSnapshot owned by caller's account.
func (b *InMemoryBackend) CreateDBSnapshotAs(caller, snapshotID, instanceID string) (*DBSnapshot, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBSnapshotIdentifier is required", ErrInvalidParameter)
	}

	if instanceID == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier is required", ErrInvalidParameter)
	}

	data, err := b.captureInstanceData(instanceID)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateDBSnapshot")
	defer b.mu.Unlock()

	if _, exists := b.snapshots.Get(normalizeID(snapshotID)); exists {
		return nil, fmt.Errorf("%w: snapshot %s already exists", ErrSnapshotAlreadyExists, snapshotID)
	}

	inst, exists := b.instances.Get(normalizeID(instanceID))
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, instanceID)
	}

	snap := b.newManualSnapshotLocked(snapshotID, inst)
	snap.OwnerAccountID = caller
	snap.DBSnapshotArn = b.ownedSnapshotARN(caller, "snapshot", snapshotID)
	b.snapshots.Put(snap)
	b.storeSnapDataLocked(snapKindInstance, snapshotID, data)
	b.copyTenantDatabasesToSnapshotLocked(snap.DBSnapshotIdentifier, inst)

	cp := *snap

	return &cp, nil
}

// copyTenantDatabasesToSnapshotLocked records a DBSnapshotTenantDatabase
// entry for every tenant database on inst, mirroring how AWS snapshots a
// multi-tenant (CDB) instance's PDBs along with the instance itself. Callers
// must already hold b.mu for writing.
func (b *InMemoryBackend) copyTenantDatabasesToSnapshotLocked(snapshotID string, inst *DBInstance) {
	for _, tdb := range b.tenantDatabases.All() {
		if !idEqual(tdb.DBInstanceIdentifier, inst.DBInstanceIdentifier) {
			continue
		}
		b.addDBSnapshotTenantDatabaseLocked(snapshotID, inst.DBInstanceIdentifier, tdb.TenantDBName, inst.Engine)
	}
}

// newManualSnapshotLocked builds a manual DB snapshot record for inst. It does
// not check for an existing snapshot with the same ID or insert into
// b.snapshots — callers must do both under b.mu.
func (b *InMemoryBackend) newManualSnapshotLocked(snapshotID string, inst *DBInstance) *DBSnapshot {
	snap := &DBSnapshot{
		SnapshotCreateTime:   time.Now().UTC(),
		DBSnapshotIdentifier: snapshotID,
		DBSnapshotArn:        b.rdsARN("snapshot", snapshotID),
		DBInstanceIdentifier: inst.DBInstanceIdentifier,
		DbiResourceID:        inst.DbiResourceID,
		Engine:               inst.Engine,
		EngineVersion:        inst.EngineVersion,
		Status:               instanceStatusAvailable,
		AllocatedStorage:     inst.AllocatedStorage,
		Port:                 inst.Port,
		StorageType:          inst.StorageType,
		StorageEncrypted:     inst.StorageEncrypted,
		SnapshotType:         snapshotTypeManual,
		MasterUsername:       inst.MasterUsername,
		DBInstanceClass:      inst.DBInstanceClass,
		OptionGroupName:      inst.OptionGroupName,
		PercentProgress:      percentProgressComplete,
	}
	if inst.StorageEncrypted {
		snap.KmsKeyID = inst.KmsKeyID
	}

	return snap
}

// DescribeDBSnapshots returns snapshots. If snapshotID is non-empty, returns only that snapshot.
// If instanceID is non-empty, returns all snapshots for that instance.
func (b *InMemoryBackend) DescribeDBSnapshots(snapshotID, instanceID string) ([]DBSnapshot, error) {
	return b.DescribeDBSnapshotsAs(SnapshotQuery{SnapshotID: snapshotID, ParentID: instanceID})
}

// DescribeDBSnapshotsAs is DescribeDBSnapshots from the caller's point of view.
func (b *InMemoryBackend) DescribeDBSnapshotsAs(q SnapshotQuery) ([]DBSnapshot, error) {
	b.mu.RLock("DescribeDBSnapshots")
	defer b.mu.RUnlock()

	if q.SnapshotID != "" {
		id, byARN := snapshotRef(q.SnapshotID)
		snap, exists := b.snapshots.Get(normalizeID(id))
		if !exists || !b.snapshotReachableLocked(
			q.CallerAccount, snap.OwnerAccountID, snapKindInstance, snap.DBSnapshotIdentifier, byARN,
		) {
			return nil, fmt.Errorf("%w: snapshot %s not found", ErrSnapshotNotFound, q.SnapshotID)
		}

		return []DBSnapshot{*snap}, nil
	}

	snaps := make([]DBSnapshot, 0, b.snapshots.Len())
	for _, snap := range b.snapshots.All() {
		if q.ParentID != "" && !idEqual(snap.DBInstanceIdentifier, q.ParentID) {
			continue
		}
		if !b.snapshotListedLocked(
			q,
			snapKindInstance,
			snap.DBSnapshotIdentifier,
			snap.OwnerAccountID,
			snap.SnapshotType,
		) {
			continue
		}
		snaps = append(snaps, *snap)
	}
	slices.SortFunc(snaps, func(a, b DBSnapshot) int {
		if a.DBSnapshotIdentifier < b.DBSnapshotIdentifier {
			return -1
		}
		if a.DBSnapshotIdentifier > b.DBSnapshotIdentifier {
			return 1
		}

		return 0
	})

	return snaps, nil
}

// isKnownDBSnapshotFilterName reports whether name is a Filters.Filter.N.Name
// value AWS recognizes for DescribeDBSnapshots. "dbi-resource-id" is accepted
// (to avoid rejecting otherwise-valid client requests) and is matched against
// the snapshot's DbiResourceID, mirroring the DescribeDBInstances filter of
// the same name.
func isKnownDBSnapshotFilterName(name string) bool {
	switch name {
	case filterNameDBInstanceID, "db-snapshot-id", filterNameDbiResourceID, filterNameSnapshotType, filterNameEngine:
		return true
	default:
		return false
	}
}

// applyDBSnapshotFilters narrows snaps per the AWS DescribeDBSnapshots
// Filters contract: each filter ANDs together, and a filter's Values list is
// OR-matched against the corresponding snapshot field. An unrecognized
// filter name returns InvalidParameterValue, matching real AWS.
func applyDBSnapshotFilters(vals url.Values, snaps []DBSnapshot) ([]DBSnapshot, error) {
	filters := parseDescribeFilters(vals)
	if len(filters) == 0 {
		return snaps, nil
	}

	for name := range filters {
		if !isKnownDBSnapshotFilterName(name) {
			return nil, fmt.Errorf("%w: Unrecognized filter name: %s", ErrInvalidParameter, name)
		}
	}

	filtered := make([]DBSnapshot, 0, len(snaps))
	for _, s := range snaps {
		if matchesAllDBSnapshotFilters(s, filters) {
			filtered = append(filtered, s)
		}
	}

	return filtered, nil
}

func matchesAllDBSnapshotFilters(s DBSnapshot, filters map[string][]string) bool {
	for name, values := range filters {
		switch name {
		case filterNameDBInstanceID:
			if !containsFoldIDOrARN(values, s.DBInstanceIdentifier) {
				return false
			}
		case "db-snapshot-id":
			if !containsFold(values, s.DBSnapshotIdentifier) {
				return false
			}
		case filterNameDbiResourceID:
			if !slices.Contains(values, s.DbiResourceID) {
				return false
			}
		case filterNameSnapshotType:
			if !slices.Contains(values, s.SnapshotType) {
				return false
			}
		case filterNameEngine:
			if !slices.Contains(values, s.Engine) {
				return false
			}
		}
	}

	return true
}

// DeleteDBSnapshot removes the given snapshot.
func (b *InMemoryBackend) DeleteDBSnapshot(snapshotID string) (*DBSnapshot, error) {
	return b.DeleteDBSnapshotAs("", snapshotID)
}

// DeleteDBSnapshotAs deletes a snapshot the caller owns.
func (b *InMemoryBackend) DeleteDBSnapshotAs(caller, snapshotID string) (*DBSnapshot, error) {
	b.mu.Lock("DeleteDBSnapshot")
	defer b.mu.Unlock()

	snap, exists := b.snapshots.Get(normalizeID(snapshotID))
	if !exists || !ownsSnapshot(caller, snap.OwnerAccountID) {
		return nil, fmt.Errorf("%w: snapshot %s not found", ErrSnapshotNotFound, snapshotID)
	}

	cp := *snap
	b.snapshots.Delete(normalizeID(snapshotID))
	b.dropSnapDataLocked(snapKindInstance, snapshotID)
	// Use snap.DBSnapshotIdentifier (the stored, creation-time casing) rather
	// than the raw snapshotID argument: they can differ purely in case (see
	// normalizeID), and the tags map is keyed by ARN built from whichever
	// casing was live when tags were added.
	delete(b.tags, cmp.Or(snap.DBSnapshotArn, b.rdsARN("snapshot", snap.DBSnapshotIdentifier)))

	return &cp, nil
}

// CopyDBSnapshot creates a copy of the given snapshot with a new identifier.
func (b *InMemoryBackend) CopyDBSnapshot(
	sourceSnapshotID, targetSnapshotID string,
	opts CopyDBSnapshotOptions,
) (*DBSnapshot, error) {
	if sourceSnapshotID == "" {
		return nil, fmt.Errorf("%w: SourceDBSnapshotIdentifier must not be empty", ErrInvalidParameter)
	}
	if targetSnapshotID == "" {
		return nil, fmt.Errorf("%w: TargetDBSnapshotIdentifier must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("CopyDBSnapshot")
	defer b.mu.Unlock()

	srcID, byARN := snapshotRef(sourceSnapshotID)
	src, exists := b.snapshots.Get(normalizeID(srcID))
	if !exists || !b.snapshotReachableLocked(
		opts.CallerAccount, src.OwnerAccountID, snapKindInstance, src.DBSnapshotIdentifier, byARN,
	) {
		return nil, fmt.Errorf("%w: snapshot %s not found", ErrSnapshotNotFound, sourceSnapshotID)
	}
	if _, alreadyExists := b.snapshots.Get(normalizeID(targetSnapshotID)); alreadyExists {
		return nil, fmt.Errorf("%w: snapshot %s already exists", ErrSnapshotAlreadyExists, targetSnapshotID)
	}

	kmsKeyID := opts.KmsKeyID
	if kmsKeyID == "" && src.StorageEncrypted {
		kmsKeyID = src.KmsKeyID
	}

	snap := &DBSnapshot{
		SnapshotCreateTime:   time.Now().UTC(),
		DBSnapshotIdentifier: targetSnapshotID,
		DBSnapshotArn:        b.ownedSnapshotARN(opts.CallerAccount, "snapshot", targetSnapshotID),
		OwnerAccountID:       opts.CallerAccount,
		DBInstanceIdentifier: src.DBInstanceIdentifier,
		DbiResourceID:        src.DbiResourceID,
		Engine:               src.Engine,
		EngineVersion:        src.EngineVersion,
		Status:               instanceStatusAvailable,
		AllocatedStorage:     src.AllocatedStorage,
		Port:                 src.Port,
		StorageType:          src.StorageType,
		StorageEncrypted:     src.StorageEncrypted,
		SnapshotType:         snapshotTypeManual,
		KmsKeyID:             kmsKeyID,
		SourceRegion:         opts.SourceRegion,
		MasterUsername:       src.MasterUsername,
		DBInstanceClass:      src.DBInstanceClass,
		OptionGroupName:      src.OptionGroupName,
		PercentProgress:      percentProgressComplete,
	}
	b.snapshots.Put(snap)
	b.copySnapDataLocked(snapKindInstance, src.DBSnapshotIdentifier, targetSnapshotID)
	cp := *snap

	return &cp, nil
}

// RestoreDBInstanceFromDBSnapshot creates a new DB instance from the given
// snapshot. DBParameterGroupName defaults to "default.<engine>" when
// omitted, matching the documented "default DBParameterGroup for the
// specified DB engine" (rds@v1.124.1
// api_op_RestoreDBInstanceFromDBSnapshot.go:241) and the same convention
// CreateDBCluster already uses. OptionGroupName has no documented default
// and is honored only when supplied.
func (b *InMemoryBackend) RestoreDBInstanceFromDBSnapshot(
	id, snapshotID string,
	opts DBInstanceOptions,
) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier is required", ErrInvalidParameter)
	}
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBSnapshotIdentifier is required", ErrInvalidParameter)
	}

	var (
		result   *DBInstance
		endpoint string
		err      error
	)

	func() {
		b.mu.Lock("RestoreDBInstanceFromDBSnapshot")
		defer b.mu.Unlock()

		b.reconcileInstancesLocked()

		if _, exists := b.instances.Get(normalizeID(id)); exists {
			err = fmt.Errorf("%w: instance %s already exists", ErrInstanceAlreadyExists, id)

			return
		}

		snapID, byARN := snapshotRef(snapshotID)
		snap, exists := b.snapshots.Get(normalizeID(snapID))
		if !exists || !b.snapshotReachableLocked(
			opts.CallerAccount, snap.OwnerAccountID, snapKindInstance, snap.DBSnapshotIdentifier, byARN,
		) {
			err = fmt.Errorf("%w: snapshot %s not found", ErrSnapshotNotFound, snapshotID)

			return
		}

		if opts.StorageType == "" {
			opts.StorageType = snap.StorageType
		}
		if opts.AvailabilityZone == "" {
			opts.AvailabilityZone = b.region + "a"
		}
		if opts.DBParameterGroupName == "" {
			opts.DBParameterGroupName = "default." + snap.Engine
		}

		var secret MasterSecret
		if secret, err = b.createMasterSecret("db", "", opts.MasterSecretRequest, ""); err != nil {
			return
		}

		endpoint = fmt.Sprintf("%s.%s.%s.rds.amazonaws.com", id, b.accountID, b.region)
		port := cmp.Or(opts.DBPortNumber, snap.Port, enginePort(snap.Engine))

		inst := restoredInstanceFromSnapshot(id, b.rdsARN("db", id), snap, opts, secret, endpoint, port)
		applyVpcSecurityGroups(inst, opts.VpcSecurityGroupIDs)
		b.instances.Put(inst)
		seed := b.snapSeedLocked(snapKindInstance, snap.DBSnapshotIdentifier)
		inst.DBName = cmp.Or(inst.DBName, seed.dbName)
		if !b.provisionSeededInstanceLocked(inst, "", seed) {
			b.instanceReadyAt[id] = time.Now().Add(instanceTransitionDelay)
			b.scheduleReconcilerLocked()
		}
		b.publishInstanceEventLocked(id, "DB instance restored from snapshot")
		cp := *inst
		result = &cp
	}()

	if err != nil {
		return nil, err
	}

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(endpoint)
	}

	return result, nil
}

// restoredInstanceFromSnapshot builds the creating DBInstance a snapshot restore produces.
func restoredInstanceFromSnapshot(
	id, arn string, snap *DBSnapshot, opts DBInstanceOptions, secret MasterSecret, endpoint string, port int,
) *DBInstance {
	return &DBInstance{
		MasterSecret:                     secret,
		DBInstanceIdentifier:             id,
		DBInstanceArn:                    arn,
		DbiResourceID:                    id,
		Engine:                           snap.Engine,
		EngineVersion:                    snap.EngineVersion,
		DBInstanceStatus:                 instanceStatusCreating,
		InstanceCreateTime:               time.Now().UTC(),
		DBInstanceClass:                  cmp.Or(opts.DBInstanceClass, snap.DBInstanceClass),
		MasterUsername:                   snap.MasterUsername,
		DBSubnetGroupName:                opts.DBSubnetGroupName,
		PubliclyAccessible:               opts.PubliclyAccessible,
		AutoMinorVersionUpgrade:          opts.AutoMinorVersionUpgrade,
		BackupRetentionPeriod:            opts.BackupRetentionPeriod,
		CopyTagsToSnapshot:               opts.CopyTagsToSnapshot,
		NetworkType:                      opts.NetworkType,
		Iops:                             opts.Iops,
		StorageThroughput:                opts.StorageThroughput,
		Endpoint:                         endpoint,
		Port:                             port,
		AllocatedStorage:                 snap.AllocatedStorage,
		StorageType:                      opts.StorageType,
		StorageEncrypted:                 snap.StorageEncrypted,
		AvailabilityZone:                 opts.AvailabilityZone,
		MultiAZ:                          opts.MultiAZ,
		DeletionProtection:               opts.DeletionProtection,
		DBParameterGroupName:             opts.DBParameterGroupName,
		OptionGroupName:                  opts.OptionGroupName,
		IAMDatabaseAuthenticationEnabled: opts.IAMDatabaseAuthenticationEnabled,
		UseDefaultProcessorFeatures:      opts.UseDefaultProcessorFeatures,
		BackupTarget:                     opts.BackupTarget,
		EnabledCloudwatchLogsExports:     opts.EnabledCloudwatchLogsExports,
	}
}

// DescribeDBSnapshotAttributes returns attributes for a DB snapshot.
func (b *InMemoryBackend) DescribeDBSnapshotAttributes(snapshotID string) (*DBSnapshotAttributesResult, error) {
	return b.DescribeDBSnapshotAttributesAs("", snapshotID)
}

// DescribeDBSnapshotAttributesAs describes the attributes of a snapshot the caller owns.
func (b *InMemoryBackend) DescribeDBSnapshotAttributesAs(
	caller, snapshotID string,
) (*DBSnapshotAttributesResult, error) {
	b.mu.RLock("DescribeDBSnapshotAttributes")
	defer b.mu.RUnlock()
	if snap, ok := b.snapshots.Get(normalizeID(snapshotID)); !ok || !ownsSnapshot(caller, snap.OwnerAccountID) {
		return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapshotID)
	}
	if attrs, ok := b.snapshotAttributes.Get(normalizeID(snapshotID)); ok {
		cp := *attrs

		return &cp, nil
	}
	result := &DBSnapshotAttributesResult{
		DBSnapshotIdentifier: snapshotID,
		DBSnapshotAttributes: []DBSnapshotAttribute{},
	}
	cp := *result

	return &cp, nil
}

// ModifyDBSnapshot modifies settings of a DB snapshot.
func (b *InMemoryBackend) ModifyDBSnapshot(snapshotID, optionGroupName, engineVersion string) (*DBSnapshot, error) {
	b.mu.Lock("ModifyDBSnapshot")
	defer b.mu.Unlock()
	snap, ok := b.snapshots.Get(normalizeID(snapshotID))
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapshotID)
	}
	if optionGroupName != "" {
		snap.OptionGroupName = optionGroupName
	}
	if engineVersion != "" {
		snap.EngineVersion = engineVersion
	}
	cp := *snap

	return &cp, nil
}

// ModifyDBSnapshotAttribute adds or removes attribute values for a DB snapshot.
func (b *InMemoryBackend) ModifyDBSnapshotAttribute(
	snapshotID, attributeName string,
	valuesToAdd, valuesToRemove []string,
) (*DBSnapshotAttributesResult, error) {
	return b.ModifyDBSnapshotAttributeAs("", snapshotID, attributeName, valuesToAdd, valuesToRemove)
}

// ModifyDBSnapshotAttributeAs modifies the attributes of a snapshot the caller owns.
func (b *InMemoryBackend) ModifyDBSnapshotAttributeAs(
	caller, snapshotID, attributeName string,
	valuesToAdd, valuesToRemove []string,
) (*DBSnapshotAttributesResult, error) {
	b.mu.Lock("ModifyDBSnapshotAttribute")
	defer b.mu.Unlock()
	if snap, ok := b.snapshots.Get(normalizeID(snapshotID)); !ok || !ownsSnapshot(caller, snap.OwnerAccountID) {
		return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapshotID)
	}
	result, ok := b.snapshotAttributes.Get(normalizeID(snapshotID))
	if !ok {
		result = &DBSnapshotAttributesResult{
			DBSnapshotIdentifier: snapshotID,
			DBSnapshotAttributes: []DBSnapshotAttribute{},
		}
		b.snapshotAttributes.Put(result)
	}
	applySnapshotAttributeChange(&result.DBSnapshotAttributes, attributeName, valuesToAdd, valuesToRemove)
	cp := *result

	return &cp, nil
}
