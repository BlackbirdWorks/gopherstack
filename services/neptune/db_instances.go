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

// clusterInheritedInstanceFields holds the DB cluster-derived values a new
// instance inherits at create time.
type clusterInheritedInstanceFields struct {
	EngineVersion         string
	DBSubnetGroupName     string
	NetworkType           string
	VpcSecurityGroupIDs   []string
	BackupRetentionPeriod int
}

// instanceClusterInherited resolves EngineVersion, DBSubnetGroupName,
// NetworkType, BackupRetentionPeriod, and VpcSecurityGroupIDs for a new
// instance from its parent cluster. An explicit DBSubnetGroupName wins;
// omitted defaults to the cluster's (api_op_CreateDBInstance.go's
// DBSubnetGroupName doc does not spell this default out, but every Neptune
// instance belongs to a cluster, so this mirrors the pre-existing inherit
// behavior for the omitted case). BackupRetentionPeriod/VpcSecurityGroupIDs
// mirror NetworkType's inheritance: CreateDBInstanceInput's own members of
// these names are documented "Not applicable...managed by the DB cluster".
func (b *InMemoryBackend) instanceClusterInherited(
	region, clusterID, explicitSubnetGroup string,
) clusterInheritedInstanceFields {
	dbSubnetGroupName := explicitSubnetGroup
	if clusterID == "" {
		return clusterInheritedInstanceFields{
			EngineVersion:     defaultEngineVersion,
			DBSubnetGroupName: dbSubnetGroupName,
		}
	}
	cl, ok := b.clusterGet(region, clusterID)
	if !ok {
		return clusterInheritedInstanceFields{
			EngineVersion:     defaultEngineVersion,
			DBSubnetGroupName: dbSubnetGroupName,
		}
	}
	if dbSubnetGroupName == "" {
		dbSubnetGroupName = cl.DBSubnetGroupName
	}
	vpcSGs := make([]string, len(cl.VpcSecurityGroupIDs))
	copy(vpcSGs, cl.VpcSecurityGroupIDs)

	return clusterInheritedInstanceFields{
		EngineVersion:         cl.EngineVersion,
		DBSubnetGroupName:     dbSubnetGroupName,
		NetworkType:           cl.NetworkType,
		BackupRetentionPeriod: cl.BackupRetentionPeriod,
		VpcSecurityGroupIDs:   vpcSGs,
	}
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
	if err := validateMonitoringInterval(opts.MonitoringInterval); err != nil {
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
	if opts.DBSubnetGroupName != "" && !b.subnetGroupHas(region, opts.DBSubnetGroupName) {
		return nil, fmt.Errorf(
			"%w: subnet group %s not found",
			ErrSubnetGroupNotFound, opts.DBSubnetGroupName,
		)
	}
	if instanceClass == "" {
		instanceClass = defaultInstanceClass
	}
	maintenanceWindow := defaultMaintenanceWindow
	if opts.PreferredMaintenanceWindow != "" {
		maintenanceWindow = opts.PreferredMaintenanceWindow
	}
	endpoint := fmt.Sprintf("%s.neptune.%s.amazonaws.com", id, region)
	inherited := b.instanceClusterInherited(region, clusterID, opts.DBSubnetGroupName)
	dbSecurityGroups := make([]string, len(opts.DBSecurityGroups))
	copy(dbSecurityGroups, opts.DBSecurityGroups)
	inst := &DBInstance{
		region:                          region,
		DBInstanceIdentifier:            id,
		DBInstanceArn:                   b.instanceARN(region, id),
		DBClusterIdentifier:             clusterID,
		DBInstanceClass:                 instanceClass,
		Engine:                          neptuneEngine,
		EngineVersion:                   inherited.EngineVersion,
		DBInstanceStatus:                clusterStatusAvailable,
		InstanceCreateTime:              nowISO8601(),
		Endpoint:                        endpoint,
		Port:                            defaultNeptunePort,
		AutoMinorVersionUpgrade:         true,
		PreferredMaintenanceWindow:      maintenanceWindow,
		DBParameterGroupName:            opts.DBParameterGroupName,
		DBSubnetGroupName:               inherited.DBSubnetGroupName,
		NetworkType:                     inherited.NetworkType,
		BackupRetentionPeriod:           inherited.BackupRetentionPeriod,
		VpcSecurityGroupIDs:             inherited.VpcSecurityGroupIDs,
		DBSecurityGroups:                dbSecurityGroups,
		MonitoringInterval:              opts.MonitoringInterval,
		MonitoringRoleArn:               opts.MonitoringRoleArn,
		Iops:                            opts.Iops,
		PreferredBackupWindow:           opts.PreferredBackupWindow,
		AvailabilityZone:                opts.AvailabilityZone,
		CopyTagsToSnapshot:              opts.CopyTagsToSnapshot,
		EnableIAMDatabaseAuthentication: opts.EnableIAMDatabaseAuthentication,
		PromotionTier:                   opts.PromotionTier,
		StorageEncrypted:                opts.StorageEncrypted,
		DeletionProtection:              opts.DeletionProtection,
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
	b.recordEvent(region, id, sourceTypeDBInstance, "DB instance created", "creation")
	cp := *inst

	return &cp, nil
}

// DescribeDBInstances returns all Neptune DB instances or a specific one by ID.
// The clusterFilter (when non-empty) restricts results to instances of that cluster.
func (b *InMemoryBackend) DescribeDBInstances(
	ctx context.Context,
	id string,
	clusterFilter []string,
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
		if len(clusterFilter) > 0 && !slices.Contains(clusterFilter, inst.DBClusterIdentifier) {
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
//
// Neptune has no DB-instance-level snapshot resource at all (verified: no
// CreateDBSnapshot/DBSnapshot type exists anywhere in the pinned SDK, unlike
// DeleteDBCluster's real cluster-snapshot feature), so unlike DeleteDBCluster
// this does not enforce "FinalDBSnapshotIdentifier required when
// SkipFinalSnapshot is false" -- there is no snapshot resource to create.
// The one real, structurally-checkable rule (specifying both is a request
// error) is enforced.
func (b *InMemoryBackend) DeleteDBInstance(
	ctx context.Context, id string, opts DBInstanceDeleteOptions,
) (*DBInstance, error) {
	if opts.SkipFinalSnapshot && opts.FinalDBSnapshotIdentifier != "" {
		return nil, fmt.Errorf(
			"%w: FinalDBSnapshotIdentifier cannot be specified when SkipFinalSnapshot is true",
			ErrSnapshotRequired,
		)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteDBInstance")
	defer b.mu.Unlock()
	inst, exists := b.instanceGet(region, id)
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	if inst.DBClusterIdentifier != "" {
		if cl, ok := b.clusterGet(region, inst.DBClusterIdentifier); ok && len(cl.DBClusterMembers) == 1 {
			return nil, fmt.Errorf(
				"%w: instance %s is the only instance in cluster %s",
				ErrInvalidDBInstanceStateFault, id, cl.DBClusterIdentifier,
			)
		}
	}
	if inst.DeletionProtection {
		return nil, fmt.Errorf(
			"%w: instance %s cannot be deleted because deletion protection is enabled",
			ErrInvalidDBInstanceStateFault, id,
		)
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
	b.recordEvent(region, id, sourceTypeDBInstance, "DB instance deleted", "deletion")

	return &cp, nil
}

// ModifyDBInstance modifies a Neptune DB instance.
func (b *InMemoryBackend) ModifyDBInstance(
	ctx context.Context,
	id, instanceClass string,
	opts DBInstanceModifyOptions,
) (*DBInstance, error) {
	if err := validateModifyDBInstanceOptions(opts); err != nil {
		return nil, err
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("ModifyDBInstance")
	defer b.mu.Unlock()
	inst, exists := b.instanceGet(region, id)
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	applyDBInstanceModifications(inst, instanceClass, opts)
	cp := *inst

	return &cp, nil
}

// validateModifyDBInstanceOptions validates the request-shape rules for
// ModifyDBInstance's optional fields.
func validateModifyDBInstanceOptions(opts DBInstanceModifyOptions) error {
	if opts.MonitoringIntervalSet {
		if err := validateMonitoringInterval(opts.MonitoringInterval); err != nil {
			return err
		}
	}
	if opts.PortSet && (opts.Port < minNeptunePort || opts.Port > maxNeptunePort) {
		return fmt.Errorf(
			"%w: DBPortNumber %d is not valid; must be between %d and %d",
			ErrInvalidParameter, opts.Port, minNeptunePort, maxNeptunePort,
		)
	}

	return nil
}

// applyDBInstanceModifications applies opts' fields onto inst. Callers must
// hold the write lock and have already validated opts.
func applyDBInstanceModifications(inst *DBInstance, instanceClass string, opts DBInstanceModifyOptions) {
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
	if opts.DeletionProtectionSet {
		inst.DeletionProtection = opts.DeletionProtection
	}
	applyDBInstanceMonitoringAndNetwork(inst, opts)
}

// applyDBInstanceMonitoringAndNetwork applies the Enhanced Monitoring,
// Iops, port, and DBSecurityGroups fields of opts onto inst.
func applyDBInstanceMonitoringAndNetwork(inst *DBInstance, opts DBInstanceModifyOptions) {
	if opts.MonitoringIntervalSet {
		inst.MonitoringInterval = opts.MonitoringInterval
	}
	if opts.MonitoringRoleArn != "" {
		inst.MonitoringRoleArn = opts.MonitoringRoleArn
	}
	if opts.IopsSet {
		inst.Iops = opts.Iops
	}
	if opts.PortSet {
		inst.Port = opts.Port
	}
	if len(opts.DBSecurityGroups) > 0 {
		dbSecurityGroups := make([]string, len(opts.DBSecurityGroups))
		copy(dbSecurityGroups, opts.DBSecurityGroups)
		inst.DBSecurityGroups = dbSecurityGroups
	}
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
