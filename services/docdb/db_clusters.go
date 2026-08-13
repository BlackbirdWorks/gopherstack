package docdb

import (
	"context"
	"fmt"
	"sort"
	"time"
)

func validateCreateDBClusterParams(
	id, engineVersion, masterUserPassword string,
	backupRetentionPeriod int,
	tags map[string]string,
) error {
	if id == "" {
		return fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	if err := validateEngineVersion(engineVersion); err != nil {
		return err
	}
	if masterUserPassword != "" {
		if err := validateMasterUserPassword(masterUserPassword); err != nil {
			return err
		}
	}
	if err := validateTags(tags); err != nil {
		return err
	}
	if backupRetentionPeriod != 0 && (backupRetentionPeriod < 1 || backupRetentionPeriod > maxBackupRetentionPeriod) {
		return fmt.Errorf(
			"%w: BackupRetentionPeriod must be between 1 and %d",
			ErrInvalidParameter, maxBackupRetentionPeriod,
		)
	}

	return nil
}

// extractCreateDBClusterOpts pulls the optional CreateDBClusterOptions
// fields out into plain return values (deep-copying the two slice fields),
// factored out of CreateDBCluster to keep its own line count under funlen's
// threshold.
func extractCreateDBClusterOpts(
	opts *CreateDBClusterOptions,
) (string, []string, []string) {
	if opts == nil {
		return "", nil, nil
	}
	var vpcSecurityGroupIDs, enabledCloudwatchLogsExports []string
	if len(opts.VpcSecurityGroupIDs) > 0 {
		vpcSecurityGroupIDs = make([]string, len(opts.VpcSecurityGroupIDs))
		copy(vpcSecurityGroupIDs, opts.VpcSecurityGroupIDs)
	}
	if len(opts.EnabledCloudwatchLogsExports) > 0 {
		enabledCloudwatchLogsExports = make([]string, len(opts.EnabledCloudwatchLogsExports))
		copy(enabledCloudwatchLogsExports, opts.EnabledCloudwatchLogsExports)
	}

	return opts.KmsKeyID, vpcSecurityGroupIDs, enabledCloudwatchLogsExports
}

// CreateDBCluster creates a cluster. The unnamed string parameter between
// masterUserPassword and paramGroupName is a deliberately ignored
// database-name slot, kept only to hold this exported method's positional
// signature stable for call sites outside this package; docdb's real
// CreateDBClusterInput has no DatabaseName member at all (verified against
// docdb@v1.51.4, gopherstack-xou3), so the value is never read from the wire
// and never stored -- see handleCreateDBCluster, which no longer parses it.
func (b *InMemoryBackend) CreateDBCluster(
	ctx context.Context,
	id, engine, engineVersion, masterUser, masterUserPassword string,
	_, paramGroupName, subnetGroupName string,
	port int,
	storageEncrypted, deletionProtection bool,
	backupRetentionPeriod int,
	preferredBackupWindow, preferredMaintenanceWindow string,
	availabilityZones []string,
	tags map[string]string,
	opts *CreateDBClusterOptions,
) (*DBCluster, error) {
	if err := validateCreateDBClusterParams(
		id, engineVersion, masterUserPassword, backupRetentionPeriod, tags,
	); err != nil {
		return nil, err
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CreateDBCluster")
	defer b.mu.Unlock()
	if b.clusterHas(region, id) {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, id)
	}
	if engine == "" {
		engine = docDBEngine
	}
	if engineVersion == "" {
		engineVersion = defaultEngineVersion
	}
	if paramGroupName == "" {
		paramGroupName = defaultParamGroupName(engineVersion)
	}
	if port <= 0 {
		port = defaultDocDBPort
	}
	if backupRetentionPeriod == 0 {
		backupRetentionPeriod = 1
	}
	if preferredBackupWindow == "" {
		preferredBackupWindow = defaultBackupWindow
	}
	if preferredMaintenanceWindow == "" {
		preferredMaintenanceWindow = defaultMaintenanceWindow
	}
	clusterArn := b.clusterARN(region, id)
	endpoint := fmt.Sprintf("%s.cluster.docdb.%s.amazonaws.com", id, region)
	readerEndpoint := fmt.Sprintf("%s.cluster-ro.docdb.%s.amazonaws.com", id, region)
	azs := make([]string, len(availabilityZones))
	copy(azs, availabilityZones)

	kmsKeyID, vpcSecurityGroupIDs, enabledCloudwatchLogsExports := extractCreateDBClusterOpts(opts)

	cluster := &DBCluster{
		region:                       region,
		DBClusterIdentifier:          id,
		Engine:                       engine,
		Status:                       statusAvailable,
		MasterUsername:               masterUser,
		DBClusterParameterGroupName:  paramGroupName,
		DBSubnetGroupName:            subnetGroupName,
		Endpoint:                     endpoint,
		ReaderEndpoint:               readerEndpoint,
		Port:                         port,
		DBClusterArn:                 clusterArn,
		EngineVersion:                engineVersion,
		StorageEncrypted:             storageEncrypted,
		DeletionProtection:           deletionProtection,
		BackupRetentionPeriod:        backupRetentionPeriod,
		PreferredBackupWindow:        preferredBackupWindow,
		PreferredMaintenanceWindow:   preferredMaintenanceWindow,
		AvailabilityZones:            azs,
		ClusterCreateTime:            time.Now().UTC().Format(time.RFC3339),
		Tags:                         copyTags(tags),
		KmsKeyID:                     kmsKeyID,
		VpcSecurityGroupIDs:          vpcSecurityGroupIDs,
		EnabledCloudwatchLogsExports: enabledCloudwatchLogsExports,
	}
	b.clusterPut(cluster)
	if len(tags) > 0 {
		b.tagsStore(region)[clusterArn] = tagsFromMap(tags)
	}
	b.recordEvent(region, id, sourceTypeDBCluster, clusterArn, "DB cluster created", eventCatCreate)

	return copyCluster(cluster), nil
}

func (b *InMemoryBackend) DescribeDBClusters(ctx context.Context, id string) ([]DBCluster, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDBClusters")
	defer b.mu.RUnlock()
	if id != "" {
		c, exists := b.clusterGet(region, id)
		if !exists {
			return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
		}

		return []DBCluster{*copyCluster(c)}, nil
	}
	clusters := b.clustersInRegion(region)
	result := make([]DBCluster, 0, len(clusters))
	for _, c := range clusters {
		result = append(result, *copyCluster(c))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].DBClusterIdentifier < result[j].DBClusterIdentifier
	})

	return result, nil
}

func (b *InMemoryBackend) DeleteDBCluster(
	ctx context.Context,
	id string,
	opts *DeleteDBClusterOptions,
) (*DBCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusterGet(region, id)
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	if c.DeletionProtection {
		return nil, fmt.Errorf("%w: cluster %s has deletion protection enabled", ErrInvalidClusterState, id)
	}
	for _, inst := range b.instancesInRegion(region) {
		if inst.DBClusterIdentifier == id {
			return nil, fmt.Errorf("%w: cluster %s still has instances, delete them first", ErrInvalidClusterState, id)
		}
	}
	if opts == nil || (!opts.SkipFinalSnapshot && opts.FinalDBSnapshotIdentifier == "") {
		return nil, fmt.Errorf(
			"%w: specify SkipFinalSnapshot=true or provide FinalDBSnapshotIdentifier",
			ErrInvalidParameter,
		)
	}

	cp := copyCluster(c)

	// Create a final snapshot if requested.
	if !opts.SkipFinalSnapshot && opts.FinalDBSnapshotIdentifier != "" {
		snapID := opts.FinalDBSnapshotIdentifier
		if b.clusterSnapshotHas(region, snapID) {
			return nil, fmt.Errorf(
				"%w: cluster snapshot %s already exists",
				ErrClusterSnapshotAlreadyExists,
				snapID,
			)
		}
		snap := &DBClusterSnapshot{
			region:                      region,
			DBClusterSnapshotIdentifier: snapID,
			DBClusterIdentifier:         id,
			Engine:                      c.Engine,
			Status:                      statusAvailable,
			EngineVersion:               c.EngineVersion,
			StorageEncrypted:            c.StorageEncrypted,
			SnapshotType:                "manual",
			PercentProgress:             snapshotPercentageComplete,
			SnapshotCreateTime:          time.Now().UTC().Format(time.RFC3339),
			DBClusterArn:                b.clusterARN(region, id),
		}
		b.clusterSnapshotPut(snap)
	}

	b.clusterDelete(region, id)
	delete(b.tagsStore(region), b.clusterARN(region, id))
	b.recordEvent(region, id, sourceTypeDBCluster, cp.DBClusterArn, "DB cluster deleted", eventCatDelete)

	return cp, nil
}

func (b *InMemoryBackend) ModifyDBCluster(
	ctx context.Context,
	id, paramGroupName string,
	deletionProtection *bool,
	backupRetentionPeriod int,
	preferredBackupWindow, preferredMaintenanceWindow string,
	opts *ModifyDBClusterOptions,
) (*DBCluster, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("ModifyDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusterGet(region, id)
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	if c.Status == statusDeleting {
		return nil, fmt.Errorf("%w: cluster %s is in deleting state", ErrInvalidClusterState, id)
	}
	if paramGroupName != "" {
		c.DBClusterParameterGroupName = paramGroupName
	}
	if deletionProtection != nil {
		c.DeletionProtection = *deletionProtection
	}
	if backupRetentionPeriod > 0 {
		c.BackupRetentionPeriod = backupRetentionPeriod
	}
	if preferredBackupWindow != "" {
		c.PreferredBackupWindow = preferredBackupWindow
	}
	if preferredMaintenanceWindow != "" {
		c.PreferredMaintenanceWindow = preferredMaintenanceWindow
	}
	if opts != nil {
		if opts.MasterUserPassword != "" {
			if err := validateMasterUserPassword(opts.MasterUserPassword); err != nil {
				return nil, err
			}
		}

		applyModifyDBClusterOpts(c, opts)
		if opts.NewDBClusterIdentifier != "" {
			b.clusterDelete(region, id)
			b.clusterPut(c)
		}
	}

	return copyCluster(c), nil
}

// applyModifyDBClusterOpts applies optional ModifyDBCluster parameters to an existing cluster.
func applyModifyDBClusterOpts(c *DBCluster, opts *ModifyDBClusterOptions) {
	if opts.EngineVersion != "" {
		c.EngineVersion = opts.EngineVersion
	}
	if opts.NewDBClusterIdentifier != "" {
		c.DBClusterIdentifier = opts.NewDBClusterIdentifier
	}
	if opts.Port > 0 {
		c.Port = opts.Port
	}
	if len(opts.VpcSecurityGroupIDs) > 0 {
		vpcSGs := make([]string, len(opts.VpcSecurityGroupIDs))
		copy(vpcSGs, opts.VpcSecurityGroupIDs)
		c.VpcSecurityGroupIDs = vpcSGs
	}
	if len(opts.EnableLogsTypes) > 0 {
		existing := make(map[string]bool, len(c.EnabledCloudwatchLogsExports))
		for _, t := range c.EnabledCloudwatchLogsExports {
			existing[t] = true
		}
		for _, t := range opts.EnableLogsTypes {
			if !existing[t] {
				c.EnabledCloudwatchLogsExports = append(c.EnabledCloudwatchLogsExports, t)
				existing[t] = true
			}
		}
	}
	if len(opts.DisableLogsTypes) > 0 {
		disableSet := make(map[string]bool, len(opts.DisableLogsTypes))
		for _, t := range opts.DisableLogsTypes {
			disableSet[t] = true
		}
		kept := c.EnabledCloudwatchLogsExports[:0]
		for _, t := range c.EnabledCloudwatchLogsExports {
			if !disableSet[t] {
				kept = append(kept, t)
			}
		}
		c.EnabledCloudwatchLogsExports = kept
	}
}

func (b *InMemoryBackend) StopDBCluster(ctx context.Context, id string) (*DBCluster, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("StopDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusterGet(region, id)
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	if c.Status != statusAvailable {
		return nil, fmt.Errorf("%w: cluster %s is not in available state", ErrInvalidClusterState, id)
	}
	c.Status = statusStopped
	b.recordEvent(region, id, sourceTypeDBCluster, c.DBClusterArn, "DB cluster stopped", "availability")

	return copyCluster(c), nil
}

func (b *InMemoryBackend) StartDBCluster(ctx context.Context, id string) (*DBCluster, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("StartDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusterGet(region, id)
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	if c.Status != statusStopped {
		return nil, fmt.Errorf("%w: cluster %s is not in stopped state", ErrInvalidClusterState, id)
	}
	c.Status = statusAvailable
	b.recordEvent(region, id, sourceTypeDBCluster, c.DBClusterArn, "DB cluster started", "availability")

	return copyCluster(c), nil
}

func (b *InMemoryBackend) FailoverDBCluster(
	ctx context.Context,
	id, targetInstanceID string,
) (*DBCluster, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("FailoverDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusterGet(region, id)
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	if c.Status != statusAvailable {
		return nil, fmt.Errorf("%w: cluster %s is not in available state for failover", ErrInvalidClusterState, id)
	}
	if targetInstanceID != "" {
		member := false
		for _, inst := range b.instancesInRegion(region) {
			if inst.DBClusterIdentifier == id && inst.DBInstanceIdentifier == targetInstanceID {
				member = true

				break
			}
		}
		if !member {
			return nil, fmt.Errorf(
				"%w: instance %s is not a member of cluster %s",
				ErrInvalidInstanceState, targetInstanceID, id,
			)
		}
	}
	c.WriterInstanceID = targetInstanceID
	b.recordEvent(region, id, sourceTypeDBCluster, c.DBClusterArn, "DB cluster failover started", "failover")

	return copyCluster(c), nil
}

// RestoreDBClusterFromSnapshot restores a new cluster from a snapshot.
func (b *InMemoryBackend) RestoreDBClusterFromSnapshot(
	ctx context.Context,
	snapshotID, clusterID, engine string,
) (*DBCluster, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBClusterSnapshotIdentifier is required", ErrInvalidParameter)
	}
	if clusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("RestoreDBClusterFromSnapshot")
	defer b.mu.Unlock()
	snap, snapExists := b.clusterSnapshotGet(region, snapshotID)
	if !snapExists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
	}
	if b.clusterHas(region, clusterID) {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, clusterID)
	}
	if engine == "" {
		engine = snap.Engine
	}
	engineVersion := snap.EngineVersion
	if engineVersion == "" {
		engineVersion = defaultEngineVersion
	}
	var paramGroupName, subnetGroupName string
	if src, exists := b.clusterGet(region, snap.DBClusterIdentifier); exists {
		paramGroupName = src.DBClusterParameterGroupName
		subnetGroupName = src.DBSubnetGroupName
	}
	if paramGroupName == "" {
		paramGroupName = defaultParamGroupName(engineVersion)
	}
	clusterArn := b.clusterARN(region, clusterID)
	endpoint := fmt.Sprintf("%s.cluster.docdb.%s.amazonaws.com", clusterID, region)
	readerEndpoint := fmt.Sprintf("%s.cluster-ro.docdb.%s.amazonaws.com", clusterID, region)
	cluster := &DBCluster{
		region:                      region,
		DBClusterIdentifier:         clusterID,
		Engine:                      engine,
		Status:                      statusAvailable,
		DBClusterParameterGroupName: paramGroupName,
		DBSubnetGroupName:           subnetGroupName,
		Endpoint:                    endpoint,
		ReaderEndpoint:              readerEndpoint,
		Port:                        defaultDocDBPort,
		DBClusterArn:                clusterArn,
		EngineVersion:               engineVersion,
		StorageEncrypted:            snap.StorageEncrypted,
		ClusterCreateTime:           time.Now().UTC().Format(time.RFC3339),
	}
	b.clusterPut(cluster)

	return copyCluster(cluster), nil
}

// RestoreDBClusterToPointInTime restores a new cluster to a point in time from a source cluster.
func (b *InMemoryBackend) RestoreDBClusterToPointInTime(
	ctx context.Context,
	sourceClusterID, targetClusterID string,
) (*DBCluster, error) {
	if sourceClusterID == "" {
		return nil, fmt.Errorf("%w: SourceDBClusterIdentifier is required", ErrInvalidParameter)
	}
	if targetClusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("RestoreDBClusterToPointInTime")
	defer b.mu.Unlock()
	src, srcExists := b.clusterGet(region, sourceClusterID)
	if !srcExists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, sourceClusterID)
	}
	if b.clusterHas(region, targetClusterID) {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, targetClusterID)
	}
	clusterArn := b.clusterARN(region, targetClusterID)
	endpoint := fmt.Sprintf("%s.cluster.docdb.%s.amazonaws.com", targetClusterID, region)
	readerEndpoint := fmt.Sprintf("%s.cluster-ro.docdb.%s.amazonaws.com", targetClusterID, region)
	cluster := &DBCluster{
		region:                      region,
		DBClusterIdentifier:         targetClusterID,
		Engine:                      src.Engine,
		Status:                      statusAvailable,
		MasterUsername:              src.MasterUsername,
		DBClusterParameterGroupName: src.DBClusterParameterGroupName,
		DBSubnetGroupName:           src.DBSubnetGroupName,
		Endpoint:                    endpoint,
		ReaderEndpoint:              readerEndpoint,
		Port:                        src.Port,
		DBClusterArn:                clusterArn,
		EngineVersion:               src.EngineVersion,
		StorageEncrypted:            src.StorageEncrypted,
		PreferredBackupWindow:       src.PreferredBackupWindow,
		PreferredMaintenanceWindow:  src.PreferredMaintenanceWindow,
		ClusterCreateTime:           time.Now().UTC().Format(time.RFC3339),
	}
	b.clusterPut(cluster)

	return copyCluster(cluster), nil
}
