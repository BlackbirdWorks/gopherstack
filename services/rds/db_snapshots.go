package rds

import (
	"fmt"
	"slices"
	"time"
)

// CreateDBSnapshot creates a snapshot of the given DB instance.
func (b *InMemoryBackend) CreateDBSnapshot(snapshotID, instanceID string) (*DBSnapshot, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBSnapshotIdentifier is required", ErrInvalidParameter)
	}

	if instanceID == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateDBSnapshot")
	defer b.mu.Unlock()

	if _, exists := b.snapshots.Get(snapshotID); exists {
		return nil, fmt.Errorf("%w: snapshot %s already exists", ErrSnapshotAlreadyExists, snapshotID)
	}

	inst, exists := b.instances.Get(instanceID)
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, instanceID)
	}

	snap := b.newManualSnapshotLocked(snapshotID, inst)
	b.snapshots.Put(snap)

	cp := *snap

	return &cp, nil
}

// newManualSnapshotLocked builds a manual DB snapshot record for inst. It does
// not check for an existing snapshot with the same ID or insert into
// b.snapshots — callers must do both under b.mu.
func (b *InMemoryBackend) newManualSnapshotLocked(snapshotID string, inst *DBInstance) *DBSnapshot {
	snap := &DBSnapshot{
		SnapshotCreateTime:   time.Now().UTC(),
		DBSnapshotIdentifier: snapshotID,
		DBInstanceIdentifier: inst.DBInstanceIdentifier,
		Engine:               inst.Engine,
		EngineVersion:        inst.EngineVersion,
		Status:               instanceStatusAvailable,
		AllocatedStorage:     inst.AllocatedStorage,
		Port:                 inst.Port,
		StorageType:          inst.StorageType,
		StorageEncrypted:     inst.StorageEncrypted,
		SnapshotType:         "manual",
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
	b.mu.RLock("DescribeDBSnapshots")
	defer b.mu.RUnlock()

	if snapshotID != "" {
		snap, exists := b.snapshots.Get(snapshotID)
		if !exists {
			return nil, fmt.Errorf("%w: snapshot %s not found", ErrSnapshotNotFound, snapshotID)
		}

		return []DBSnapshot{*snap}, nil
	}

	snaps := make([]DBSnapshot, 0, b.snapshots.Len())
	for _, snap := range b.snapshots.All() {
		if instanceID != "" && snap.DBInstanceIdentifier != instanceID {
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

// DeleteDBSnapshot removes the given snapshot.
func (b *InMemoryBackend) DeleteDBSnapshot(snapshotID string) (*DBSnapshot, error) {
	b.mu.Lock("DeleteDBSnapshot")
	defer b.mu.Unlock()

	snap, exists := b.snapshots.Get(snapshotID)
	if !exists {
		return nil, fmt.Errorf("%w: snapshot %s not found", ErrSnapshotNotFound, snapshotID)
	}

	cp := *snap
	b.snapshots.Delete(snapshotID)
	delete(b.tags, b.rdsARN("snapshot", snapshotID))

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

	src, exists := b.snapshots.Get(sourceSnapshotID)
	if !exists {
		return nil, fmt.Errorf("%w: snapshot %s not found", ErrSnapshotNotFound, sourceSnapshotID)
	}
	if _, alreadyExists := b.snapshots.Get(targetSnapshotID); alreadyExists {
		return nil, fmt.Errorf("%w: snapshot %s already exists", ErrSnapshotAlreadyExists, targetSnapshotID)
	}

	kmsKeyID := opts.KmsKeyID
	if kmsKeyID == "" && src.StorageEncrypted {
		kmsKeyID = src.KmsKeyID
	}

	snap := &DBSnapshot{
		SnapshotCreateTime:   time.Now().UTC(),
		DBSnapshotIdentifier: targetSnapshotID,
		DBInstanceIdentifier: src.DBInstanceIdentifier,
		Engine:               src.Engine,
		EngineVersion:        src.EngineVersion,
		Status:               instanceStatusAvailable,
		AllocatedStorage:     src.AllocatedStorage,
		Port:                 src.Port,
		StorageType:          src.StorageType,
		StorageEncrypted:     src.StorageEncrypted,
		KmsKeyID:             kmsKeyID,
		SourceRegion:         opts.SourceRegion,
		OptionGroupName:      src.OptionGroupName,
		PercentProgress:      percentProgressComplete,
	}
	b.snapshots.Put(snap)
	cp := *snap

	return &cp, nil
}

// RestoreDBInstanceFromDBSnapshot creates a new DB instance from the given snapshot.
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

	b.mu.Lock("RestoreDBInstanceFromDBSnapshot")
	b.reconcileInstancesLocked()

	if _, exists := b.instances.Get(id); exists {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: instance %s already exists", ErrInstanceAlreadyExists, id)
	}

	snap, exists := b.snapshots.Get(snapshotID)
	if !exists {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: snapshot %s not found", ErrSnapshotNotFound, snapshotID)
	}

	if opts.StorageType == "" {
		opts.StorageType = snap.StorageType
	}
	if opts.AvailabilityZone == "" {
		opts.AvailabilityZone = b.region + "a"
	}

	endpoint := fmt.Sprintf("%s.%s.%s.rds.amazonaws.com", id, b.accountID, b.region)
	port := snap.Port
	if port == 0 {
		port = enginePort(snap.Engine)
	}

	inst := &DBInstance{
		DBInstanceIdentifier: id,
		DbiResourceID:        id,
		Engine:               snap.Engine,
		EngineVersion:        snap.EngineVersion,
		DBInstanceStatus:     instanceStatusAvailable,
		Endpoint:             endpoint,
		Port:                 port,
		AllocatedStorage:     snap.AllocatedStorage,
		StorageType:          opts.StorageType,
		StorageEncrypted:     snap.StorageEncrypted,
		AvailabilityZone:     opts.AvailabilityZone,
		MultiAZ:              opts.MultiAZ,
		DeletionProtection:   opts.DeletionProtection,
	}
	b.instances.Put(inst)
	b.publishInstanceEventLocked(id, "DB instance restored from snapshot")
	cp := *inst

	b.mu.Unlock()

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(endpoint)
	}

	return &cp, nil
}

// DescribeDBSnapshotAttributes returns attributes for a DB snapshot.
func (b *InMemoryBackend) DescribeDBSnapshotAttributes(snapshotID string) (*DBSnapshotAttributesResult, error) {
	b.mu.RLock("DescribeDBSnapshotAttributes")
	defer b.mu.RUnlock()
	if _, ok := b.snapshots.Get(snapshotID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapshotID)
	}
	if attrs, ok := b.snapshotAttributes.Get(snapshotID); ok {
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
	snap, ok := b.snapshots.Get(snapshotID)
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
	b.mu.Lock("ModifyDBSnapshotAttribute")
	defer b.mu.Unlock()
	if _, ok := b.snapshots.Get(snapshotID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapshotID)
	}
	result, ok := b.snapshotAttributes.Get(snapshotID)
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
