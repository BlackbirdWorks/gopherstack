package neptune

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) instanceGet(region, id string) (*DBInstance, bool) {
	return b.instances.Get(regionKey(region, id))
}

func (b *InMemoryBackend) instanceHas(region, id string) bool {
	return b.instances.Has(regionKey(region, id))
}

func (b *InMemoryBackend) instancePut(v *DBInstance) { b.instances.Put(v) }

func (b *InMemoryBackend) instanceDelete(region, id string) {
	b.instances.Delete(regionKey(region, id))
}

func (b *InMemoryBackend) instancesInRegion(region string) []*DBInstance {
	return b.instancesByRegion.Get(region)
}

// instanceARN returns the region-scoped ARN for a Neptune DB instance.
func (b *InMemoryBackend) instanceARN(region, id string) string {
	return arn.Build("neptune", region, b.accountID, "db:"+id)
}

// CreateDBInstance creates a new Neptune DB instance.
func (b *InMemoryBackend) CreateDBInstance(
	ctx context.Context,
	id, clusterID, instanceClass string,
	opts DBInstanceCreateOptions,
) (*DBInstance, error) {
	if err := validateNeptuneIdentifier(id, "DBInstanceIdentifier"); err != nil {
		return nil, err
	}
	if opts.PromotionTier < 0 || opts.PromotionTier > maxPromotionTier {
		return nil, fmt.Errorf(
			"%w: PromotionTier %d is not valid; must be between 0 and %d",
			ErrInvalidParameter, opts.PromotionTier, maxPromotionTier,
		)
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
	if instanceClass == "" {
		instanceClass = defaultInstanceClass
	}
	maintenanceWindow := defaultMaintenanceWindow
	if opts.PreferredMaintenanceWindow != "" {
		maintenanceWindow = opts.PreferredMaintenanceWindow
	}
	endpoint := fmt.Sprintf("%s.neptune.%s.amazonaws.com", id, region)
	engineVersion := defaultEngineVersion
	dbSubnetGroupName := ""
	if clusterID != "" {
		if cl, ok := b.clusterGet(region, clusterID); ok {
			engineVersion = cl.EngineVersion
			dbSubnetGroupName = cl.DBSubnetGroupName
		}
	}
	inst := &DBInstance{
		region:                          region,
		DBInstanceIdentifier:            id,
		DBInstanceArn:                   b.instanceARN(region, id),
		DBClusterIdentifier:             clusterID,
		DBInstanceClass:                 instanceClass,
		Engine:                          neptuneEngine,
		EngineVersion:                   engineVersion,
		DBInstanceStatus:                clusterStatusAvailable,
		InstanceCreateTime:              nowISO8601(),
		Endpoint:                        endpoint,
		Port:                            defaultNeptunePort,
		AutoMinorVersionUpgrade:         true,
		PreferredMaintenanceWindow:      maintenanceWindow,
		DBParameterGroupName:            opts.DBParameterGroupName,
		DBSubnetGroupName:               dbSubnetGroupName,
		PreferredBackupWindow:           opts.PreferredBackupWindow,
		AvailabilityZone:                opts.AvailabilityZone,
		CopyTagsToSnapshot:              opts.CopyTagsToSnapshot,
		EnableIAMDatabaseAuthentication: opts.EnableIAMDatabaseAuthentication,
		PromotionTier:                   opts.PromotionTier,
		StorageEncrypted:                opts.StorageEncrypted,
	}
	if opts.AutoMinorVersionUpgrade {
		inst.AutoMinorVersionUpgrade = opts.AutoMinorVersionUpgrade
	}
	b.instancePut(inst)
	if clusterID != "" {
		if cl, ok := b.clusterGet(region, clusterID); ok {
			isWriter := len(cl.DBClusterMembers) == 0
			cl.DBClusterMembers = append(cl.DBClusterMembers, DBClusterMember{
				DBInstanceIdentifier: id,
				IsClusterWriter:      isWriter,
			})
		}
	}
	cp := *inst

	return &cp, nil
}

// DescribeDBInstances returns all Neptune DB instances or a specific one by ID.
// The clusterFilter (when non-empty) restricts results to instances of that cluster.
func (b *InMemoryBackend) DescribeDBInstances(
	ctx context.Context,
	id, clusterFilter string,
) ([]DBInstance, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDBInstances")
	defer b.mu.RUnlock()
	if id != "" {
		inst, exists := b.instanceGet(region, id)
		if !exists {
			return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
		}
		cp := *inst

		return []DBInstance{cp}, nil
	}
	instances := b.instancesInRegion(region)
	result := make([]DBInstance, 0, len(instances))
	for _, inst := range instances {
		if clusterFilter != "" && inst.DBClusterIdentifier != clusterFilter {
			continue
		}
		result = append(result, *inst)
	}
	slices.SortFunc(result, func(a, b DBInstance) int {
		return strings.Compare(a.DBInstanceIdentifier, b.DBInstanceIdentifier)
	})

	return result, nil
}

// DeleteDBInstance deletes a Neptune DB instance.
func (b *InMemoryBackend) DeleteDBInstance(ctx context.Context, id string) (*DBInstance, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteDBInstance")
	defer b.mu.Unlock()
	inst, exists := b.instanceGet(region, id)
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	cp := *inst
	b.instanceDelete(region, id)
	delete(b.tagsStore(region), b.instanceARN(region, id))
	if cp.DBClusterIdentifier != "" {
		if cl, ok := b.clusterGet(region, cp.DBClusterIdentifier); ok {
			members := make([]DBClusterMember, 0, len(cl.DBClusterMembers))
			for _, m := range cl.DBClusterMembers {
				if m.DBInstanceIdentifier != id {
					members = append(members, m)
				}
			}
			cl.DBClusterMembers = members
		}
	}

	return &cp, nil
}

// ModifyDBInstance modifies a Neptune DB instance.
func (b *InMemoryBackend) ModifyDBInstance(
	ctx context.Context,
	id, instanceClass string,
	opts DBInstanceModifyOptions,
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
	if opts.DBParameterGroupName != "" {
		inst.DBParameterGroupName = opts.DBParameterGroupName
	}
	if opts.PreferredMaintenanceWindow != "" {
		inst.PreferredMaintenanceWindow = opts.PreferredMaintenanceWindow
	}
	if opts.PreferredBackupWindow != "" {
		inst.PreferredBackupWindow = opts.PreferredBackupWindow
	}
	if opts.AutoMinorVersionUpgradeSet {
		inst.AutoMinorVersionUpgrade = opts.AutoMinorVersionUpgrade
	}
	if opts.CopyTagsToSnapshotSet {
		inst.CopyTagsToSnapshot = opts.CopyTagsToSnapshot
	}
	if opts.IamAuthSet {
		inst.EnableIAMDatabaseAuthentication = opts.EnableIAMDatabaseAuthentication
	}
	if opts.PromotionTierSet {
		inst.PromotionTier = opts.PromotionTier
	}
	cp := *inst

	return &cp, nil
}

// RebootDBInstance reboots a Neptune DB instance.
func (b *InMemoryBackend) RebootDBInstance(ctx context.Context, id string) (*DBInstance, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("RebootDBInstance")
	defer b.mu.Unlock()
	inst, exists := b.instanceGet(region, id)
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	cp := *inst

	return &cp, nil
}
