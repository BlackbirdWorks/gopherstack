package rds

import (
	"fmt"
	"net/url"
	"slices"
	"time"
)

// normalizeDBInstanceDefaults fills in empty/zero values with AWS defaults.
func normalizeDBInstanceDefaults(
	engine, instanceClass string,
	allocatedStorage int,
	masterUser, region string,
	opts *DBInstanceOptions,
) (string, string, int, string) {
	if engine == "" {
		engine = "postgres"
	}
	if instanceClass == "" {
		instanceClass = defaultInstanceClass
	}
	if allocatedStorage <= 0 {
		allocatedStorage = defaultAllocatedStorage
	}
	if masterUser == "" {
		masterUser = "admin"
	}
	if opts.StorageType == "" {
		opts.StorageType = "gp2"
	}
	if opts.AvailabilityZone == "" {
		opts.AvailabilityZone = region + "a"
	}

	return engine, instanceClass, allocatedStorage, masterUser
}

func (b *InMemoryBackend) CreateDBInstance(
	id, engine, instanceClass, dbName, masterUser, paramGroupName string,
	allocatedStorage int,
	opts DBInstanceOptions,
) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier is required", ErrInvalidParameter)
	}
	if !dbInstanceIDRegex.MatchString(id) {
		return nil, fmt.Errorf(
			"%w: DBInstanceIdentifier %q is invalid (must start with a letter, 1–63 alphanumeric/hyphen chars)",
			ErrInvalidParameter, id,
		)
	}

	var (
		result   *DBInstance
		endpoint string
		err      error
	)

	func() {
		b.mu.Lock("CreateDBInstance")
		defer b.mu.Unlock()

		b.reconcileInstancesLocked()

		if _, exists := b.instances.Get(id); exists {
			err = fmt.Errorf("%w: instance %s already exists", ErrInstanceAlreadyExists, id)

			return
		}

		engine, instanceClass, allocatedStorage, masterUser = normalizeDBInstanceDefaults(
			engine, instanceClass, allocatedStorage, masterUser, b.region, &opts,
		)

		port := enginePort(engine)
		endpoint = fmt.Sprintf("%s.%s.%s.rds.amazonaws.com", id, b.accountID, b.region)

		vpcSGs := make([]VpcSecurityGroupMembership, 0, len(opts.VpcSecurityGroupIDs))
		for _, sgID := range opts.VpcSecurityGroupIDs {
			vpcSGs = append(vpcSGs, VpcSecurityGroupMembership{VpcSecurityGroupID: sgID, Status: subscriptionStatusActive})
		}

		inst := &DBInstance{
			InstanceCreateTime:               time.Now().UTC(),
			DBInstanceIdentifier:             id,
			DbiResourceID:                    id,
			DBInstanceClass:                  instanceClass,
			DBClusterIdentifier:              opts.DBClusterIdentifier,
			Engine:                           engine,
			EngineVersion:                    opts.EngineVersion,
			DBInstanceStatus:                 instanceStatusCreating,
			MasterUsername:                   masterUser,
			DBName:                           dbName,
			Endpoint:                         endpoint,
			Port:                             port,
			AllocatedStorage:                 allocatedStorage,
			DBParameterGroupName:             paramGroupName,
			OptionGroupName:                  opts.OptionGroupName,
			MultiAZ:                          opts.MultiAZ,
			StorageType:                      opts.StorageType,
			StorageEncrypted:                 opts.StorageEncrypted,
			AvailabilityZone:                 opts.AvailabilityZone,
			BackupRetentionPeriod:            opts.BackupRetentionPeriod,
			IAMDatabaseAuthenticationEnabled: opts.IAMDatabaseAuthenticationEnabled,
			DeletionProtection:               opts.DeletionProtection,
			Iops:                             opts.Iops,
			StorageThroughput:                opts.StorageThroughput,
			LicenseModel:                     opts.LicenseModel,
			MonitoringInterval:               opts.MonitoringInterval,
			MonitoringRoleArn:                opts.MonitoringRoleArn,
			PreferredMaintenanceWindow:       opts.PreferredMaintenanceWindow,
			PreferredBackupWindow:            opts.PreferredBackupWindow,
			KmsKeyID:                         opts.KmsKeyID,
			CopyTagsToSnapshot:               opts.CopyTagsToSnapshot,
			EnabledCloudwatchLogsExports:     opts.EnabledCloudwatchLogsExports,
			VpcSecurityGroups:                vpcSGs,
			ReadReplicaIdentifiers:           []string{},
			PubliclyAccessible:               opts.PubliclyAccessible,
			PerformanceInsightsEnabled:       opts.PerformanceInsightsEnabled,
			StorageOptimized:                 opts.StorageOptimized,
			OptimizedWrites:                  opts.OptimizedWrites,
			EngineLifecycleSupport:           opts.EngineLifecycleSupport,
		}
		b.instances.Put(inst)
		b.publishInstanceEventLocked(id, "DB instance created")

		// If joining a cluster, add this instance to the cluster's member list.
		if opts.DBClusterIdentifier != "" {
			if cluster, exists := b.clusters.Get(opts.DBClusterIdentifier); exists {
				cluster.DBClusterMembers = append(cluster.DBClusterMembers, DBClusterMember{
					DBInstanceIdentifier: id,
					IsClusterWriter:      len(cluster.DBClusterMembers) == 0,
				})
			}
		}
		b.maybeRegisterAutomatedBackup(id, engine, port, allocatedStorage, opts)
		b.instanceReadyAt[id] = time.Now().Add(instanceTransitionDelay)
		b.scheduleReconcilerLocked()
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

// DeleteDBInstance removes the DB instance with the given identifier, skipping
// the AWS final-snapshot contract (SkipFinalSnapshot=true). It exists for
// existing callers (e.g. CloudFormation resource cleanup) that pre-date the
// SkipFinalSnapshot/FinalDBSnapshotIdentifier parameters and manage their own
// snapshot semantics. New callers that need AWS-accurate DeleteDBInstance
// behavior (final snapshot, DeleteAutomatedBackups) should use
// DeleteDBInstanceWithOptions.
func (b *InMemoryBackend) DeleteDBInstance(id string) (*DBInstance, error) {
	return b.DeleteDBInstanceWithOptions(id, true, "", true)
}

// DeleteDBInstanceWithOptions removes the DB instance with the given identifier,
// honoring the AWS DeleteDBInstance parameter contract:
//   - SkipFinalSnapshot=false (the AWS default) requires a non-empty
//     finalSnapshotID; a manual snapshot of the instance is taken before it is
//     removed.
//   - SkipFinalSnapshot=true is mutually exclusive with a non-empty
//     finalSnapshotID (AWS: InvalidParameterCombination either way).
//   - deleteAutomatedBackups (AWS default true) controls whether the
//     instance's automated backup record is removed along with the instance.
func (b *InMemoryBackend) DeleteDBInstanceWithOptions(
	id string,
	skipFinalSnapshot bool,
	finalSnapshotID string,
	deleteAutomatedBackups bool,
) (*DBInstance, error) {
	var (
		result *DBInstance
		err    error
	)

	func() {
		b.mu.Lock("DeleteDBInstance")
		defer b.mu.Unlock()

		b.reconcileInstancesLocked()

		// AWS resolves the target instance before validating the
		// SkipFinalSnapshot/FinalDBSnapshotIdentifier combination: deleting a
		// nonexistent instance returns DBInstanceNotFoundFault even when the
		// snapshot parameters are also missing/invalid.
		inst, exists := b.instances.Get(id)
		if !exists {
			err = fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)

			return
		}

		if !skipFinalSnapshot && finalSnapshotID == "" {
			err = fmt.Errorf(
				"%w: FinalDBSnapshotIdentifier is required unless SkipFinalSnapshot is specified",
				ErrInvalidParameterCombination,
			)

			return
		}
		if skipFinalSnapshot && finalSnapshotID != "" {
			err = fmt.Errorf(
				"%w: the FinalDBSnapshotIdentifier parameter cannot be specified when SkipFinalSnapshot is enabled",
				ErrInvalidParameterCombination,
			)

			return
		}

		if inst.DeletionProtection {
			err = fmt.Errorf(
				"%w: cannot delete protected DB Instance %s, disable deletion protection first",
				ErrInvalidDBInstanceState, id,
			)

			return
		}

		if inst.DBInstanceStatus == instanceStatusDeleting {
			err = fmt.Errorf("%w: instance %s is already being deleted", ErrInvalidDBInstanceState, id)

			return
		}

		if !skipFinalSnapshot {
			if _, snapExists := b.snapshots.Get(finalSnapshotID); snapExists {
				err = fmt.Errorf("%w: snapshot %s already exists", ErrSnapshotAlreadyExists, finalSnapshotID)

				return
			}
			b.snapshots.Put(b.newManualSnapshotLocked(finalSnapshotID, inst))
		}

		inst.DBInstanceStatus = instanceStatusDeleting
		b.publishInstanceEventLocked(id, "DB instance deletion started")
		cp := *inst
		b.publishInstanceEventLocked(id, "DB instance deleted")

		// Remove this instance from its source's ReadReplicaIdentifiers.
		if inst.ReplicaSourceDBInstanceIdentifier != "" {
			if src, srcExists := b.instances.Get(inst.ReplicaSourceDBInstanceIdentifier); srcExists {
				src.ReadReplicaIdentifiers = slices.DeleteFunc(src.ReadReplicaIdentifiers, func(s string) bool {
					return s == id
				})
			}
		}

		b.instances.Delete(id)
		delete(b.tags, b.rdsARN("db", id))
		delete(b.instanceRoles, id)
		delete(b.instanceReadyAt, id)
		if deleteAutomatedBackups {
			delete(b.automatedBackups, id)
		}

		result = &cp
	}()

	if err != nil {
		return nil, err
	}

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Deregister(result.Endpoint)
	}

	return result, nil
}

// DescribeDBInstances returns instances. If id is non-empty, returns only that instance.
func (b *InMemoryBackend) DescribeDBInstances(id string) ([]DBInstance, error) {
	var (
		result []DBInstance
		err    error
	)

	func() {
		b.mu.RLock("DescribeDBInstances")
		defer b.mu.RUnlock()

		if id != "" {
			inst, exists := b.instances.Get(id)
			if !exists {
				err = fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)

				return
			}
			result = []DBInstance{*inst}

			return
		}

		instances := make([]DBInstance, 0, b.instances.Len())
		for _, inst := range b.instances.All() {
			instances = append(instances, *inst)
		}
		result = instances
	}()

	if err != nil {
		return nil, err
	}

	if id == "" {
		slices.SortFunc(result, func(a, b DBInstance) int {
			if a.DBInstanceIdentifier < b.DBInstanceIdentifier {
				return -1
			}
			if a.DBInstanceIdentifier > b.DBInstanceIdentifier {
				return 1
			}

			return 0
		})
	}

	return result, nil
}

// applyParamGroupUpdate validates and applies a parameter group change.
func (b *InMemoryBackend) applyParamGroupUpdate(inst *DBInstance, paramGroupName string) error {
	if paramGroupName == "" {
		return nil
	}

	if _, pgExists := b.parameterGroups.Get(paramGroupName); !pgExists {
		return fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, paramGroupName)
	}

	inst.DBParameterGroupName = paramGroupName

	return nil
}

// applyVpcSecurityGroups updates VPC security group memberships on an instance.
func applyVpcSecurityGroups(inst *DBInstance, sgIDs []string) {
	if len(sgIDs) == 0 {
		return
	}

	vpcSGs := make([]VpcSecurityGroupMembership, 0, len(sgIDs))
	for _, sgID := range sgIDs {
		vpcSGs = append(vpcSGs, VpcSecurityGroupMembership{VpcSecurityGroupID: sgID, Status: subscriptionStatusActive})
	}

	inst.VpcSecurityGroups = vpcSGs
}

// applyDBInstanceSchedulingOpts applies monitoring/maintenance window fields.
func applyDBInstanceSchedulingOpts(inst *DBInstance, opts DBInstanceOptions) {
	if opts.MonitoringInterval >= 0 && opts.MonitoringInterval != inst.MonitoringInterval {
		inst.MonitoringInterval = opts.MonitoringInterval
	}
	if opts.MonitoringRoleArn != "" {
		inst.MonitoringRoleArn = opts.MonitoringRoleArn
	}
	if opts.PreferredMaintenanceWindow != "" {
		inst.PreferredMaintenanceWindow = opts.PreferredMaintenanceWindow
	}
	if opts.PreferredBackupWindow != "" {
		inst.PreferredBackupWindow = opts.PreferredBackupWindow
	}
}

func (b *InMemoryBackend) applyDBInstanceModifications(
	inst *DBInstance,
	instanceClass string,
	allocatedStorage int,
	opts DBInstanceOptions,
	applyImmediately bool,
) error {
	if applyImmediately {
		applyDeferrableFields(inst, instanceClass, allocatedStorage, opts)
	} else {
		if pv := buildPendingModifiedValues(inst, instanceClass, allocatedStorage, opts); pv != nil {
			inst.PendingModifiedValues = pv
		}
	}

	return b.applyImmediateFields(inst, opts)
}

// applyDeferrableFields applies fields that can be deferred to a maintenance window.
// Called only when ApplyImmediately=true — clears any previously pending values.
func applyDeferrableFields(inst *DBInstance, instanceClass string, allocatedStorage int, opts DBInstanceOptions) {
	inst.PendingModifiedValues = nil
	if instanceClass != "" {
		inst.DBInstanceClass = instanceClass
	}
	if allocatedStorage > 0 {
		inst.AllocatedStorage = allocatedStorage
	}
	if opts.StorageType != "" {
		inst.StorageType = opts.StorageType
	}
	if opts.Iops > 0 {
		inst.Iops = opts.Iops
	}
	if opts.StorageThroughput > 0 {
		inst.StorageThroughput = opts.StorageThroughput
	}
	if opts.EngineVersion != "" {
		inst.EngineVersion = opts.EngineVersion
	}
	if opts.MultiAZSet || opts.MultiAZ {
		inst.MultiAZ = opts.MultiAZ
	}
}

// applyImmediateFields applies fields that always take effect right away regardless of ApplyImmediately.
func (b *InMemoryBackend) applyImmediateFields(inst *DBInstance, opts DBInstanceOptions) error {
	if opts.BackupRetentionPeriod >= 0 {
		inst.BackupRetentionPeriod = opts.BackupRetentionPeriod
	}
	applyDBInstanceFlagsImmediate(inst, opts)
	if err := b.applyParamGroupUpdate(inst, opts.DBParameterGroupName); err != nil {
		return err
	}
	if opts.OptionGroupName != "" {
		inst.OptionGroupName = opts.OptionGroupName
	}
	if opts.LicenseModel != "" {
		inst.LicenseModel = opts.LicenseModel
	}
	applyDBInstanceSchedulingOpts(inst, opts)
	applyVpcSecurityGroups(inst, opts.VpcSecurityGroupIDs)
	if len(opts.EnabledCloudwatchLogsExports) > 0 {
		inst.EnabledCloudwatchLogsExports = opts.EnabledCloudwatchLogsExports
	}

	return nil
}

// buildPendingModifiedValues returns a PendingModifiedValues if any deferrable field
// differs from the instance's current value, or nil if nothing would change.
func buildPendingModifiedValues(
	inst *DBInstance,
	instanceClass string,
	allocatedStorage int,
	opts DBInstanceOptions,
) *PendingModifiedValues {
	pv := &PendingModifiedValues{}
	changed := false

	if instanceClass != "" && instanceClass != inst.DBInstanceClass {
		pv.DBInstanceClass = instanceClass
		changed = true
	}
	if allocatedStorage > 0 && allocatedStorage != inst.AllocatedStorage {
		pv.AllocatedStorage = allocatedStorage
		changed = true
	}
	if opts.StorageType != "" && opts.StorageType != inst.StorageType {
		pv.StorageType = opts.StorageType
		changed = true
	}
	if opts.Iops > 0 && opts.Iops != inst.Iops {
		pv.Iops = opts.Iops
		changed = true
	}
	if opts.EngineVersion != "" && opts.EngineVersion != inst.EngineVersion {
		pv.EngineVersion = opts.EngineVersion
		changed = true
	}
	if (opts.MultiAZSet || opts.MultiAZ) && opts.MultiAZ != inst.MultiAZ {
		b := opts.MultiAZ
		pv.MultiAZChange = &b
		changed = true
	}

	if !changed {
		return nil
	}

	return pv
}

// applyDBInstanceFlagsImmediate applies boolean flags that always take effect immediately.
// It excludes MultiAZ which is deferred when ApplyImmediately=false.
func applyDBInstanceFlagsImmediate(inst *DBInstance, opts DBInstanceOptions) {
	if opts.IAMDatabaseAuthSet {
		inst.IAMDatabaseAuthenticationEnabled = opts.IAMDatabaseAuthenticationEnabled
	} else if opts.IAMDatabaseAuthenticationEnabled {
		inst.IAMDatabaseAuthenticationEnabled = opts.IAMDatabaseAuthenticationEnabled
	}
	if opts.DeletionProtectionSet {
		inst.DeletionProtection = opts.DeletionProtection
	} else if opts.DeletionProtection {
		inst.DeletionProtection = opts.DeletionProtection
	}
	if opts.CopyTagsToSnapshot {
		inst.CopyTagsToSnapshot = opts.CopyTagsToSnapshot
	}
	if opts.PubliclyAccessible {
		inst.PubliclyAccessible = opts.PubliclyAccessible
	}
	if opts.PerformanceInsightsEnabled {
		inst.PerformanceInsightsEnabled = opts.PerformanceInsightsEnabled
	}
	if opts.StorageOptimized {
		inst.StorageOptimized = true
	}
	if opts.OptimizedWrites {
		inst.OptimizedWrites = true
	}
	if opts.EngineLifecycleSupport != "" {
		inst.EngineLifecycleSupport = opts.EngineLifecycleSupport
	}
}

// applyPendingModifications applies deferred changes stored in inst.PendingModifiedValues
// and clears the pending values. Called by the reconciler when the instance becomes available.
func applyPendingModifications(inst *DBInstance) {
	pv := inst.PendingModifiedValues
	if pv == nil {
		return
	}
	if pv.DBInstanceClass != "" {
		inst.DBInstanceClass = pv.DBInstanceClass
	}
	if pv.AllocatedStorage > 0 {
		inst.AllocatedStorage = pv.AllocatedStorage
	}
	if pv.StorageType != "" {
		inst.StorageType = pv.StorageType
	}
	if pv.Iops > 0 {
		inst.Iops = pv.Iops
	}
	if pv.EngineVersion != "" {
		inst.EngineVersion = pv.EngineVersion
	}
	if pv.MultiAZChange != nil {
		inst.MultiAZ = *pv.MultiAZChange
	}
	inst.PendingModifiedValues = nil
}

func (b *InMemoryBackend) ModifyDBInstance(
	id, instanceClass string,
	allocatedStorage int,
	opts DBInstanceOptions,
) (*DBInstance, error) {
	b.mu.Lock("ModifyDBInstance")
	b.reconcileInstancesLocked()
	defer b.mu.Unlock()

	inst, exists := b.instances.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}

	if err := b.applyDBInstanceModifications(
		inst, instanceClass, allocatedStorage, opts, opts.ApplyImmediately,
	); err != nil {
		return nil, err
	}
	inst.DBInstanceStatus = instanceStatusModifying
	b.instanceReadyAt[id] = time.Now().Add(instanceTransitionDelay)
	b.scheduleReconcilerLocked()
	b.publishInstanceEventLocked(id, "DB instance modification started")
	cp := *inst

	return &cp, nil
}

// RestoreDBInstanceToPointInTime creates a new DB instance as a point-in-time restore of the source.
func (b *InMemoryBackend) RestoreDBInstanceToPointInTime(
	id, sourceID string,
	opts DBInstanceOptions,
) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: TargetDBInstanceIdentifier is required", ErrInvalidParameter)
	}
	if sourceID == "" {
		return nil, fmt.Errorf("%w: SourceDBInstanceIdentifier is required", ErrInvalidParameter)
	}

	var (
		result   *DBInstance
		endpoint string
		err      error
	)

	func() {
		b.mu.Lock("RestoreDBInstanceToPointInTime")
		defer b.mu.Unlock()

		b.reconcileInstancesLocked()

		if _, exists := b.instances.Get(id); exists {
			err = fmt.Errorf("%w: instance %s already exists", ErrInstanceAlreadyExists, id)

			return
		}

		source, exists := b.instances.Get(sourceID)
		if !exists {
			err = fmt.Errorf("%w: source instance %s not found", ErrInstanceNotFound, sourceID)

			return
		}

		if opts.StorageType == "" {
			opts.StorageType = source.StorageType
		}
		if opts.AvailabilityZone == "" {
			opts.AvailabilityZone = source.AvailabilityZone
		}

		endpoint = fmt.Sprintf("%s.%s.%s.rds.amazonaws.com", id, b.accountID, b.region)
		inst := &DBInstance{
			DBInstanceIdentifier: id,
			DbiResourceID:        id,
			DBInstanceClass:      source.DBInstanceClass,
			Engine:               source.Engine,
			EngineVersion:        source.EngineVersion,
			DBInstanceStatus:     instanceStatusAvailable,
			MasterUsername:       source.MasterUsername,
			DBName:               source.DBName,
			Endpoint:             endpoint,
			Port:                 source.Port,
			AllocatedStorage:     source.AllocatedStorage,
			DBParameterGroupName: source.DBParameterGroupName,
			StorageType:          opts.StorageType,
			StorageEncrypted:     source.StorageEncrypted,
			AvailabilityZone:     opts.AvailabilityZone,
			MultiAZ:              opts.MultiAZ,
			DeletionProtection:   opts.DeletionProtection,
		}
		b.instances.Put(inst)
		b.publishInstanceEventLocked(id, "DB instance restored to point in time")
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

// StartDBInstance starts a stopped DB instance.
func (b *InMemoryBackend) StartDBInstance(id string) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("StartDBInstance")
	b.reconcileInstancesLocked()
	defer b.mu.Unlock()

	inst, exists := b.instances.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	if inst.DBInstanceStatus != instanceStatusStopped {
		return nil, fmt.Errorf("%w: instance %s is not in stopped state", ErrInvalidDBInstanceState, id)
	}

	inst.DBInstanceStatus = instanceStatusAvailable
	cp := *inst

	return &cp, nil
}

// StopDBInstance stops a running DB instance.
func (b *InMemoryBackend) StopDBInstance(id string) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("StopDBInstance")
	b.reconcileInstancesLocked()
	defer b.mu.Unlock()

	inst, exists := b.instances.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	if inst.DBInstanceStatus != instanceStatusAvailable {
		return nil, fmt.Errorf("%w: instance %s is not in available state", ErrInvalidDBInstanceState, id)
	}

	inst.DBInstanceStatus = instanceStatusStopped
	cp := *inst

	return &cp, nil
}

// CreateDBInstanceReadReplica creates a read replica of the given source instance.
// sourceRegion is optional; when non-empty it indicates a cross-region replica.
func (b *InMemoryBackend) CreateDBInstanceReadReplica(id, sourceID, sourceRegion string) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBInstanceReadReplica")
	b.reconcileInstancesLocked()
	defer b.mu.Unlock()
	if _, exists := b.instances.Get(id); exists {
		return nil, fmt.Errorf("%w: instance %s already exists", ErrInstanceAlreadyExists, id)
	}

	var (
		instanceClass    string
		engine           string
		engineVersion    string
		masterUser       string
		port             int
		allocatedStorage int
	)

	source, sourceExists := b.instances.Get(sourceID)
	switch {
	case sourceExists:
		instanceClass = source.DBInstanceClass
		engine = source.Engine
		engineVersion = source.EngineVersion
		masterUser = source.MasterUsername
		port = source.Port
		allocatedStorage = source.AllocatedStorage
	case sourceRegion != "":
		// Cross-region replica: source instance lives in another region.
		// Use defaults; the caller should supply a valid ARN as sourceID.
		engine = enginePostgres
		port = defaultPort
		allocatedStorage = defaultAllocatedStorage
	default:
		return nil, fmt.Errorf("%w: source instance %s not found", ErrInstanceNotFound, sourceID)
	}

	endpoint := fmt.Sprintf("%s.%s.%s.rds.amazonaws.com", id, b.accountID, b.region)
	replica := &DBInstance{
		DBInstanceIdentifier:              id,
		DbiResourceID:                     id,
		DBInstanceClass:                   instanceClass,
		Engine:                            engine,
		EngineVersion:                     engineVersion,
		DBInstanceStatus:                  instanceStatusAvailable,
		MasterUsername:                    masterUser,
		Endpoint:                          endpoint,
		Port:                              port,
		AllocatedStorage:                  allocatedStorage,
		ReplicaSourceDBInstanceIdentifier: sourceID,
	}
	b.instances.Put(replica)
	b.publishInstanceEventLocked(id, "DB read replica created")

	// Track reverse read replica reference on source instance.
	if source != nil {
		source.ReadReplicaIdentifiers = append(source.ReadReplicaIdentifiers, id)
	}

	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(endpoint)
	}

	cp := *replica

	return &cp, nil
}

// PromoteReadReplica promotes a read replica to a standalone instance.
func (b *InMemoryBackend) PromoteReadReplica(id string) (*DBInstance, error) {
	b.mu.Lock("PromoteReadReplica")
	b.reconcileInstancesLocked()
	defer b.mu.Unlock()
	inst, exists := b.instances.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}

	// Remove promoted instance from source's ReadReplicaIdentifiers.
	if inst.ReplicaSourceDBInstanceIdentifier != "" {
		if src, srcExists := b.instances.Get(inst.ReplicaSourceDBInstanceIdentifier); srcExists {
			src.ReadReplicaIdentifiers = slices.DeleteFunc(src.ReadReplicaIdentifiers, func(s string) bool {
				return s == id
			})
		}
	}

	inst.ReplicaSourceDBInstanceIdentifier = ""
	cp := *inst

	return &cp, nil
}

// RebootDBInstance reboots the given instance.
// AWS returns the instance in "rebooting" state; it transitions to "available" after the delay.
func (b *InMemoryBackend) RebootDBInstance(id string) (*DBInstance, error) {
	b.mu.Lock("RebootDBInstance")
	b.reconcileInstancesLocked()
	defer b.mu.Unlock()
	inst, exists := b.instances.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	inst.DBInstanceStatus = instanceStatusRebooting
	b.instanceReadyAt[id] = time.Now().Add(instanceTransitionDelay)
	b.scheduleReconcilerLocked()
	b.publishInstanceEventLocked(id, "DB instance reboot initiated")
	cp := *inst

	return &cp, nil
}

// DescribeValidDBInstanceModifications returns the valid modifications for the given instance.
// This is a stub that returns a minimal set of valid instance classes.
func (b *InMemoryBackend) DescribeValidDBInstanceModifications(id string) (*DBInstance, error) {
	b.mu.RLock("DescribeValidDBInstanceModifications")
	defer b.mu.RUnlock()
	inst, exists := b.instances.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	cp := *inst

	return &cp, nil
}

// isKnownDBInstanceFilterName reports whether name is a Filters.Filter.N.Name
// value AWS recognizes for DescribeDBInstances. "domain" is accepted by AWS
// but has no meaningful analog in this emulator (Directory Service domain
// membership), so it is not implemented as a match predicate — an instance
// passes it vacuously.
func isKnownDBInstanceFilterName(name string) bool {
	switch name {
	case "db-cluster-id", "db-instance-id", "dbi-resource-id", "domain", "engine":
		return true
	default:
		return false
	}
}

// applyDBInstanceFilters narrows instances per the AWS DescribeDBInstances
// Filters contract: each filter ANDs together, and a filter's Values list is
// OR-matched against the corresponding instance field. An unrecognized filter
// name returns InvalidParameterValue, matching real AWS.
func applyDBInstanceFilters(vals url.Values, instances []DBInstance) ([]DBInstance, error) {
	filters := parseDescribeFilters(vals)
	if len(filters) == 0 {
		return instances, nil
	}

	for name := range filters {
		if !isKnownDBInstanceFilterName(name) {
			return nil, fmt.Errorf("%w: Unrecognized filter name: %s", ErrInvalidParameter, name)
		}
	}

	filtered := make([]DBInstance, 0, len(instances))
	for _, inst := range instances {
		if matchesAllDBInstanceFilters(inst, filters) {
			filtered = append(filtered, inst)
		}
	}

	return filtered, nil
}

func matchesAllDBInstanceFilters(inst DBInstance, filters map[string][]string) bool {
	for name, values := range filters {
		switch name {
		case "db-cluster-id":
			if !slices.Contains(values, inst.DBClusterIdentifier) {
				return false
			}
		case "db-instance-id":
			if !slices.Contains(values, inst.DBInstanceIdentifier) {
				return false
			}
		case "dbi-resource-id":
			if !slices.Contains(values, inst.DbiResourceID) {
				return false
			}
		case "engine":
			if !slices.Contains(values, inst.Engine) {
				return false
			}
		case "domain":
			// No domain-membership state is modeled; accept unconditionally.
		}
	}

	return true
}

// parseTagKeyMembers parses TagKeys.member.N form values.
// parseMultiValueParam extracts indexed values from a form param prefix
// (e.g. "VpcSecurityGroupIds.VpcSecurityGroupId.N").
// validMonitoringInterval returns true if v is one of the AWS-allowed monitoring intervals.
func validMonitoringInterval(v int) bool {
	switch v {
	case 0,
		1,
		monitoringInterval5,
		monitoringInterval10,
		monitoringInterval15,
		monitoringInterval30,
		monitoringInterval60:
		return true
	}

	return false
}

// SwitchoverReadReplica converts a read replica to a standalone instance.
func (b *InMemoryBackend) SwitchoverReadReplica(instanceID string) (*DBInstance, error) {
	b.mu.Lock("SwitchoverReadReplica")
	defer b.mu.Unlock()
	inst, ok := b.instances.Get(instanceID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}
	inst.ReplicaSourceDBInstanceIdentifier = ""
	cp := *inst

	return &cp, nil
}

// RestoreDBInstanceFromS3 restores a DB instance from an S3 backup.
func (b *InMemoryBackend) RestoreDBInstanceFromS3(id, engine, dbInstanceClass, s3Bucket string) (*DBInstance, error) {
	if s3Bucket == "" {
		return nil, fmt.Errorf("%w: s3BucketName is required", ErrInvalidParameter)
	}
	if id == "" {
		return nil, fmt.Errorf("%w: dbInstanceIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("RestoreDBInstanceFromS3")
	defer b.mu.Unlock()
	if _, exists := b.instances.Get(id); exists {
		return nil, fmt.Errorf("%w: %s", ErrInstanceAlreadyExists, id)
	}
	inst := &DBInstance{
		DBInstanceIdentifier: id,
		DBInstanceClass:      dbInstanceClass,
		Engine:               engine,
		DBInstanceStatus:     "creating",
		AllocatedStorage:     defaultAllocatedStorage,
		StorageType:          "gp2",
	}
	b.instances.Put(inst)
	b.instanceReadyAt[id] = time.Now().Add(instanceReadyDelaySeconds * time.Second)
	b.scheduleReconcilerLocked()
	cp := *inst

	return &cp, nil
}

const instanceReadyDelaySeconds = 2
