package docdb

import (
	"context"
	"fmt"
	"sort"
)

func (b *InMemoryBackend) CreateDBInstance(
	ctx context.Context,
	id, clusterID, instanceClass, engine string,
	promotionTier int,
	tags map[string]string,
	opts *CreateDBInstanceOptions,
) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier is required", ErrInvalidParameter)
	}
	if promotionTier < 0 || promotionTier > maxPromotionTier {
		return nil, fmt.Errorf(
			"%w: PromotionTier must be between 0 and %d",
			ErrInvalidParameter, maxPromotionTier,
		)
	}
	if err := validateTags(tags); err != nil {
		return nil, err
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CreateDBInstance")
	defer b.mu.Unlock()
	if b.instanceHas(region, id) {
		return nil, fmt.Errorf("%w: instance %s already exists", ErrInstanceAlreadyExists, id)
	}
	if clusterID != "" {
		if !b.clusterHas(region, clusterID) {
			return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
		}
	}
	if engine == "" {
		engine = docDBEngine
	}
	if instanceClass == "" {
		instanceClass = defaultInstanceClass
	}
	var clusterEngineVersion string
	var clusterStorageEncrypted bool
	var clusterAZ string
	var clusterSubnetGroupName string
	if clusterID != "" {
		if parentCluster, exists := b.clusterGet(region, clusterID); exists {
			clusterEngineVersion = parentCluster.EngineVersion
			clusterStorageEncrypted = parentCluster.StorageEncrypted
			clusterAZ = firstAZ(parentCluster.AvailabilityZones)
			clusterSubnetGroupName = parentCluster.DBSubnetGroupName
		}
	}
	instanceArn := b.instanceARN(region, id)
	endpoint := fmt.Sprintf("%s.docdb.%s.amazonaws.com", id, region)

	var (
		caCertID           string
		copyTagsToSnapshot bool
	)
	if opts != nil {
		caCertID = opts.CACertificateIdentifier
		copyTagsToSnapshot = opts.CopyTagsToSnapshot
	}

	inst := &DBInstance{
		region:                  region,
		DBInstanceIdentifier:    id,
		DBClusterIdentifier:     clusterID,
		DBInstanceClass:         instanceClass,
		Engine:                  engine,
		DBInstanceStatus:        statusAvailable,
		Endpoint:                endpoint,
		Port:                    defaultDocDBPort,
		DBInstanceArn:           instanceArn,
		EngineVersion:           valueOrDefault(clusterEngineVersion, defaultEngineVersion),
		StorageEncrypted:        clusterStorageEncrypted,
		AvailabilityZone:        clusterAZ,
		DBSubnetGroupName:       clusterSubnetGroupName,
		PromotionTier:           promotionTier,
		Tags:                    copyTags(tags),
		CACertificateIdentifier: caCertID,
		CopyTagsToSnapshot:      copyTagsToSnapshot,
	}
	b.instancePut(inst)
	if len(tags) > 0 {
		b.tagsStore(region)[instanceArn] = tagsFromMap(tags)
	}
	b.recordEvent(region, id, sourceTypeDBInstance, instanceArn, "DB instance created", eventCatCreate)

	return copyInstance(inst), nil
}

func (b *InMemoryBackend) DescribeDBInstances(ctx context.Context, id, clusterID string) ([]DBInstance, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDBInstances")
	defer b.mu.RUnlock()
	if id != "" {
		inst, exists := b.instanceGet(region, id)
		if !exists {
			return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
		}

		return []DBInstance{*copyInstance(inst)}, nil
	}
	instances := b.instancesInRegion(region)
	result := make([]DBInstance, 0, len(instances))
	for _, inst := range instances {
		if clusterID != "" && inst.DBClusterIdentifier != clusterID {
			continue
		}
		result = append(result, *copyInstance(inst))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].DBInstanceIdentifier < result[j].DBInstanceIdentifier
	})

	return result, nil
}

// GetClusterMembers returns the instances that belong to a given cluster, ordered by identifier.
func (b *InMemoryBackend) GetClusterMembers(ctx context.Context, clusterID string) []DBClusterMemberEntry {
	region := getRegion(ctx, b.region)
	b.mu.RLock("GetClusterMembers")
	defer b.mu.RUnlock()
	var members []DBClusterMemberEntry
	for _, inst := range b.instancesInRegion(region) {
		if inst.DBClusterIdentifier == clusterID {
			members = append(members, DBClusterMemberEntry{
				DBInstanceIdentifier: inst.DBInstanceIdentifier,
				PromotionTier:        inst.PromotionTier,
			})
		}
	}
	sort.Slice(members, func(i, j int) bool {
		return members[i].DBInstanceIdentifier < members[j].DBInstanceIdentifier
	})
	if len(members) == 0 {
		return members
	}
	// Default writer is the alphabetically first member; FailoverDBCluster
	// can override this via DBCluster.WriterInstanceID.
	writerIdx := 0
	if c, exists := b.clusterGet(region, clusterID); exists && c.WriterInstanceID != "" {
		for i, m := range members {
			if m.DBInstanceIdentifier == c.WriterInstanceID {
				writerIdx = i

				break
			}
		}
	}
	members[writerIdx].IsClusterWriter = true

	return members
}

func (b *InMemoryBackend) DeleteDBInstance(ctx context.Context, id string) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteDBInstance")
	defer b.mu.Unlock()
	inst, exists := b.instanceGet(region, id)
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	cp := copyInstance(inst)
	b.instanceDelete(region, id)
	delete(b.tagsStore(region), b.instanceARN(region, id))
	b.recordEvent(region, id, sourceTypeDBInstance, cp.DBInstanceArn, "DB instance deleted", eventCatDelete)

	return cp, nil
}

func (b *InMemoryBackend) ModifyDBInstance(
	ctx context.Context,
	id, instanceClass string,
	autoMinorVersionUpgrade *bool,
	preferredMaintenanceWindow string,
	opts *ModifyDBInstanceOptions,
) (*DBInstance, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("ModifyDBInstance")
	defer b.mu.Unlock()
	inst, exists := b.instanceGet(region, id)
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	if instanceClass != "" {
		inst.DBInstanceClass = instanceClass
	}
	if autoMinorVersionUpgrade != nil {
		inst.AutoMinorVersionUpgrade = *autoMinorVersionUpgrade
	}
	if preferredMaintenanceWindow != "" {
		inst.PreferredMaintenanceWindow = preferredMaintenanceWindow
	}
	if opts == nil {
		return copyInstance(inst), nil
	}
	if opts.CACertificateIdentifier != "" {
		inst.CACertificateIdentifier = opts.CACertificateIdentifier
	}
	if opts.CopyTagsToSnapshot != nil {
		inst.CopyTagsToSnapshot = *opts.CopyTagsToSnapshot
	}
	if opts.PromotionTier != nil {
		if *opts.PromotionTier < 0 || *opts.PromotionTier > maxPromotionTier {
			return nil, fmt.Errorf(
				"%w: PromotionTier must be between 0 and %d",
				ErrInvalidParameter, maxPromotionTier,
			)
		}
		inst.PromotionTier = *opts.PromotionTier
	}

	return copyInstance(inst), nil
}

func (b *InMemoryBackend) RebootDBInstance(ctx context.Context, id string) (*DBInstance, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("RebootDBInstance")
	defer b.mu.Unlock()
	inst, exists := b.instanceGet(region, id)
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}

	return copyInstance(inst), nil
}
