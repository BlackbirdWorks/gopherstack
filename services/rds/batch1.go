package rds

// batch1.go provides real stateful backend implementations for DB shard groups,
// integrations, tenant databases, cluster automated backups, instance automated
// backup replication, and snapshot tenant databases.

import (
	"fmt"
	"slices"
	"time"
)

// ---- DB Shard Group operations ----

const (
	shardGroupStatusAvailableInternal = instanceStatusAvailable
	shardGroupStatusDeletingInternal  = instanceStatusDeleting
	shardGroupStatusRebootingInternal = "rebooting"

	integrationStatusActive   = "active"
	integrationStatusDeleting = instanceStatusDeleting

	tenantStatusAvailableInternal = instanceStatusAvailable
	tenantStatusDeletingInternal  = instanceStatusDeleting

	clusterBackupStatusAvailable = instanceStatusAvailable
	clusterBackupStatusDeleted   = "deleted"

	instanceBackupStatusReplicating = "replicating"
	instanceBackupStatusRetained    = "retained"
)

// CreateDBShardGroup creates a new Aurora Limitless DB shard group.
func (b *InMemoryBackend) CreateDBShardGroup(
	id, clusterID string,
	maxACU float64,
	minACU float64,
	computeRedundancy int,
	publiclyAccessible bool,
) (*DBShardGroup, error) {
	b.mu.Lock("CreateDBShardGroup")
	defer b.mu.Unlock()

	if id == "" {
		return nil, fmt.Errorf("%w: DBShardGroupIdentifier is required", ErrInvalidParameter)
	}
	if _, exists := b.shardGroups.Get(id); exists {
		return nil, fmt.Errorf("%w: %s", ErrDBShardGroupAlreadyExists, id)
	}
	if clusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}

	sg := &DBShardGroup{
		DBShardGroupIdentifier: id,
		DBClusterIdentifier:    clusterID,
		MaxACU:                 maxACU,
		MinACU:                 minACU,
		ComputeRedundancy:      computeRedundancy,
		PubliclyAccessible:     publiclyAccessible,
		Status:                 shardGroupStatusAvailableInternal,
		Endpoint:               id + ".limitless." + clusterID + ".rds.amazonaws.com",
	}
	b.shardGroups.Put(sg)
	cp := *sg

	return &cp, nil
}

// DeleteDBShardGroup deletes a DB shard group.
func (b *InMemoryBackend) DeleteDBShardGroup(id string) (*DBShardGroup, error) {
	b.mu.Lock("DeleteDBShardGroup")
	defer b.mu.Unlock()

	sg, exists := b.shardGroups.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrDBShardGroupNotFound, id)
	}

	cp := *sg
	cp.Status = shardGroupStatusDeletingInternal
	b.shardGroups.Delete(id)

	return &cp, nil
}

// DescribeDBShardGroups returns DB shard groups, optionally filtered by ID.
func (b *InMemoryBackend) DescribeDBShardGroups(id string) ([]DBShardGroup, error) {
	b.mu.RLock("DescribeDBShardGroups")
	defer b.mu.RUnlock()

	result := make([]DBShardGroup, 0, b.shardGroups.Len())
	for _, sg := range b.shardGroups.All() {
		if id != "" && sg.DBShardGroupIdentifier != id {
			continue
		}
		result = append(result, *sg)
	}

	if id != "" && len(result) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrDBShardGroupNotFound, id)
	}

	slices.SortFunc(result, func(a, b DBShardGroup) int {
		if a.DBShardGroupIdentifier < b.DBShardGroupIdentifier {
			return -1
		}
		if a.DBShardGroupIdentifier > b.DBShardGroupIdentifier {
			return 1
		}

		return 0
	})

	return result, nil
}

// ModifyDBShardGroup modifies a DB shard group's settings.
func (b *InMemoryBackend) ModifyDBShardGroup(
	id string,
	maxACU float64,
	computeRedundancy int,
) (*DBShardGroup, error) {
	b.mu.Lock("ModifyDBShardGroup")
	defer b.mu.Unlock()

	sg, exists := b.shardGroups.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrDBShardGroupNotFound, id)
	}

	if maxACU > 0 {
		sg.MaxACU = maxACU
	}
	if computeRedundancy >= 0 {
		sg.ComputeRedundancy = computeRedundancy
	}
	cp := *sg

	return &cp, nil
}

// RebootDBShardGroup reboots a DB shard group.
func (b *InMemoryBackend) RebootDBShardGroup(id string) (*DBShardGroup, error) {
	b.mu.Lock("RebootDBShardGroup")
	defer b.mu.Unlock()

	sg, exists := b.shardGroups.Get(id)
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrDBShardGroupNotFound, id)
	}

	sg.Status = shardGroupStatusRebootingInternal
	cp := *sg
	sg.Status = shardGroupStatusAvailableInternal

	return &cp, nil
}

// ---- Integration operations ----

// CreateIntegration creates a new zero-ETL integration.
func (b *InMemoryBackend) CreateIntegration(
	name, sourceARN, targetARN, kmsKeyID, dataFilter, description string,
) (*Integration, error) {
	b.mu.Lock("CreateIntegration")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: IntegrationName is required", ErrInvalidParameter)
	}
	if _, exists := b.integrations.Get(name); exists {
		return nil, fmt.Errorf("%w: %s", ErrIntegrationAlreadyExists, name)
	}

	intg := &Integration{
		IntegrationArn: fmt.Sprintf(
			"arn:aws:rds:%s:%s:integration:%s",
			b.region,
			b.accountID,
			name,
		),
		IntegrationName:        name,
		SourceArn:              sourceARN,
		TargetArn:              targetARN,
		KmsKeyID:               kmsKeyID,
		DataFilter:             dataFilter,
		IntegrationDescription: description,
		Status:                 integrationStatusActive,
		CreatedAt:              time.Now(),
	}
	b.integrations.Put(intg)
	cp := *intg

	return &cp, nil
}

// DeleteIntegration deletes an integration by name or ARN identifier.
func (b *InMemoryBackend) DeleteIntegration(identifier string) (*Integration, error) {
	b.mu.Lock("DeleteIntegration")
	defer b.mu.Unlock()

	for _, intg := range b.integrations.All() {
		if intg.IntegrationName == identifier || intg.IntegrationArn == identifier {
			cp := *intg
			cp.Status = integrationStatusDeleting
			b.integrations.Delete(intg.IntegrationName)

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: %s", ErrIntegrationNotFound, identifier)
}

// DescribeIntegrations returns integrations, optionally filtered by identifier.
func (b *InMemoryBackend) DescribeIntegrations(identifier string) ([]Integration, error) {
	b.mu.RLock("DescribeIntegrations")
	defer b.mu.RUnlock()

	result := make([]Integration, 0, b.integrations.Len())
	for _, intg := range b.integrations.All() {
		if identifier != "" && intg.IntegrationName != identifier &&
			intg.IntegrationArn != identifier {
			continue
		}
		result = append(result, *intg)
	}

	if identifier != "" && len(result) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrIntegrationNotFound, identifier)
	}

	slices.SortFunc(result, func(a, b Integration) int {
		if a.IntegrationName < b.IntegrationName {
			return -1
		}
		if a.IntegrationName > b.IntegrationName {
			return 1
		}

		return 0
	})

	return result, nil
}

// ModifyIntegration modifies an integration's description or data filter.
func (b *InMemoryBackend) ModifyIntegration(identifier, dataFilter, description string) (*Integration, error) {
	b.mu.Lock("ModifyIntegration")
	defer b.mu.Unlock()

	for _, intg := range b.integrations.All() {
		if intg.IntegrationName == identifier || intg.IntegrationArn == identifier {
			if dataFilter != "" {
				intg.DataFilter = dataFilter
			}
			if description != "" {
				intg.IntegrationDescription = description
			}
			cp := *intg

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: %s", ErrIntegrationNotFound, identifier)
}

// ---- Tenant Database operations ----

func tenantKey(instanceID, tenantDBName string) string {
	return instanceID + "/" + tenantDBName
}

// CreateTenantDatabase creates a new tenant database within an RDS instance.
func (b *InMemoryBackend) CreateTenantDatabase(
	instanceID, tenantDBName, masterUsername string,
) (*TenantDatabase, error) {
	b.mu.Lock("CreateTenantDatabase")
	defer b.mu.Unlock()

	if instanceID == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier is required", ErrInvalidParameter)
	}
	if tenantDBName == "" {
		return nil, fmt.Errorf("%w: TenantDBName is required", ErrInvalidParameter)
	}

	key := tenantKey(instanceID, tenantDBName)
	if _, exists := b.tenantDatabases.Get(key); exists {
		return nil, fmt.Errorf(
			"%w: %s/%s",
			ErrTenantDatabaseAlreadyExists,
			instanceID,
			tenantDBName,
		)
	}

	tdb := &TenantDatabase{
		DBInstanceIdentifier: instanceID,
		TenantDBName:         tenantDBName,
		MasterUsername:       masterUsername,
		TenantDatabaseARN: fmt.Sprintf(
			"arn:aws:rds:%s:%s:tenant-database:%s/%s",
			b.region, b.accountID, instanceID, tenantDBName,
		),
		DbiResourceID: fmt.Sprintf("db-%s-%s", instanceID, tenantDBName),
		Status:        tenantStatusAvailableInternal,
		CreatedAt:     time.Now(),
	}
	b.tenantDatabases.Put(tdb)
	cp := *tdb

	return &cp, nil
}

// DeleteTenantDatabase deletes a tenant database.
func (b *InMemoryBackend) DeleteTenantDatabase(
	instanceID, tenantDBName string,
) (*TenantDatabase, error) {
	b.mu.Lock("DeleteTenantDatabase")
	defer b.mu.Unlock()

	key := tenantKey(instanceID, tenantDBName)
	tdb, exists := b.tenantDatabases.Get(key)
	if !exists {
		return nil, fmt.Errorf("%w: %s/%s", ErrTenantDatabaseNotFound, instanceID, tenantDBName)
	}

	cp := *tdb
	cp.Status = tenantStatusDeletingInternal
	b.tenantDatabases.Delete(key)

	return &cp, nil
}

// DescribeTenantDatabases returns tenant databases, optionally filtered by instance and name.
func (b *InMemoryBackend) DescribeTenantDatabases(
	instanceID, tenantDBName string,
) ([]TenantDatabase, error) {
	b.mu.RLock("DescribeTenantDatabases")
	defer b.mu.RUnlock()

	result := make([]TenantDatabase, 0, b.tenantDatabases.Len())
	for _, tdb := range b.tenantDatabases.All() {
		if instanceID != "" && tdb.DBInstanceIdentifier != instanceID {
			continue
		}
		if tenantDBName != "" && tdb.TenantDBName != tenantDBName {
			continue
		}
		result = append(result, *tdb)
	}

	slices.SortFunc(result, func(a, b TenantDatabase) int {
		keyA := a.DBInstanceIdentifier + "/" + a.TenantDBName
		keyB := b.DBInstanceIdentifier + "/" + b.TenantDBName
		if keyA < keyB {
			return -1
		}
		if keyA > keyB {
			return 1
		}

		return 0
	})

	return result, nil
}

// ModifyTenantDatabase modifies a tenant database (e.g. master password).
func (b *InMemoryBackend) ModifyTenantDatabase(
	instanceID, tenantDBName string,
) (*TenantDatabase, error) {
	b.mu.Lock("ModifyTenantDatabase")
	defer b.mu.Unlock()

	key := tenantKey(instanceID, tenantDBName)
	tdb, exists := b.tenantDatabases.Get(key)
	if !exists {
		return nil, fmt.Errorf("%w: %s/%s", ErrTenantDatabaseNotFound, instanceID, tenantDBName)
	}

	cp := *tdb

	return &cp, nil
}

// ---- DB Cluster Automated Backup operations ----

// CreateDBClusterAutomatedBackup records an automated backup for a cluster.
// Called internally when creating clusters with backup retention > 0.
func (b *InMemoryBackend) CreateDBClusterAutomatedBackup(
	clusterID string,
) *DBClusterAutomatedBackup {
	b.mu.Lock("CreateDBClusterAutomatedBackup")
	defer b.mu.Unlock()

	cluster, exists := b.clusters.Get(clusterID)
	if !exists {
		return nil
	}

	backup := &DBClusterAutomatedBackup{
		DBClusterIdentifier: clusterID,
		DBClusterResourceID: fmt.Sprintf("cluster-%s", clusterID),
		Engine:              cluster.Engine,
		EngineVersion:       cluster.EngineVersion,
		Region:              b.region,
		Status:              clusterBackupStatusAvailable,
		StorageEncrypted:    cluster.StorageEncrypted,
	}
	b.clusterAutomatedBackups.Put(backup)

	return backup
}

// DeleteDBClusterAutomatedBackup deletes a cluster automated backup by resource ID.
func (b *InMemoryBackend) DeleteDBClusterAutomatedBackup(
	resourceID string,
) (*DBClusterAutomatedBackup, error) {
	b.mu.Lock("DeleteDBClusterAutomatedBackup")
	defer b.mu.Unlock()

	for _, backup := range b.clusterAutomatedBackups.All() {
		if backup.DBClusterResourceID == resourceID || backup.DBClusterIdentifier == resourceID {
			cp := *backup
			cp.Status = clusterBackupStatusDeleted
			b.clusterAutomatedBackups.Delete(clusterAutomatedBackupsKeyFn(backup))

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: %s", ErrDBClusterAutomatedBackupNotFound, resourceID)
}

// DescribeDBClusterAutomatedBackups lists cluster automated backups, optionally filtered.
func (b *InMemoryBackend) DescribeDBClusterAutomatedBackups(
	clusterID string,
) []DBClusterAutomatedBackup {
	b.mu.RLock("DescribeDBClusterAutomatedBackups")
	defer b.mu.RUnlock()

	result := make([]DBClusterAutomatedBackup, 0, b.clusterAutomatedBackups.Len())
	for _, backup := range b.clusterAutomatedBackups.All() {
		if clusterID != "" && backup.DBClusterIdentifier != clusterID {
			continue
		}
		result = append(result, *backup)
	}

	slices.SortFunc(result, func(a, b DBClusterAutomatedBackup) int {
		if a.DBClusterIdentifier < b.DBClusterIdentifier {
			return -1
		}
		if a.DBClusterIdentifier > b.DBClusterIdentifier {
			return 1
		}

		return 0
	})

	return result
}

// ---- DB Instance Automated Backup enhanced operations ----

// DeleteDBInstanceAutomatedBackup marks an automated backup as deleted.
func (b *InMemoryBackend) DeleteDBInstanceAutomatedBackup(
	resourceID string,
) (*DBInstanceAutomatedBackup, error) {
	b.mu.Lock("DeleteDBInstanceAutomatedBackup")
	defer b.mu.Unlock()

	for key, backup := range b.automatedBackups {
		if backup.DbiResourceID == resourceID || backup.DBInstanceIdentifier == resourceID {
			cp := *backup
			cp.Status = clusterBackupStatusDeleted
			delete(b.automatedBackups, key)

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: %s", ErrDBInstanceAutomatedBackupNotFound, resourceID)
}

// StartDBInstanceAutomatedBackupsReplication starts cross-region replication for an instance backup.
func (b *InMemoryBackend) StartDBInstanceAutomatedBackupsReplication(
	sourceInstanceARN string,
	backupRetentionPeriod int,
) (*DBInstanceAutomatedBackup, error) {
	b.mu.Lock("StartDBInstanceAutomatedBackupsReplication")
	defer b.mu.Unlock()

	if sourceInstanceARN == "" {
		return nil, fmt.Errorf("%w: SourceDBInstanceArn is required", ErrInvalidParameter)
	}

	// Derive a simple ID from the ARN for keying the backup record.
	instanceID := sourceInstanceARN
	key := "repl-" + instanceID

	backup := &DBInstanceAutomatedBackup{
		DBInstanceArn:         sourceInstanceARN,
		DBInstanceIdentifier:  instanceID,
		DbiResourceID:         fmt.Sprintf("db-%s", instanceID),
		Region:                b.region,
		Status:                instanceBackupStatusReplicating,
		BackupRetentionPeriod: backupRetentionPeriod,
	}
	b.automatedBackups[key] = backup
	cp := *backup

	return &cp, nil
}

// StopDBInstanceAutomatedBackupsReplication stops cross-region replication for an instance backup.
func (b *InMemoryBackend) StopDBInstanceAutomatedBackupsReplication(
	sourceInstanceARN string,
) (*DBInstanceAutomatedBackup, error) {
	b.mu.Lock("StopDBInstanceAutomatedBackupsReplication")
	defer b.mu.Unlock()

	for key, backup := range b.automatedBackups {
		if backup.DBInstanceArn == sourceInstanceARN ||
			backup.DBInstanceIdentifier == sourceInstanceARN {
			cp := *backup
			cp.Status = instanceStatusStopped
			delete(b.automatedBackups, key)

			return &cp, nil
		}
	}

	// Return a stub record indicating replication stopped even if not found.
	return &DBInstanceAutomatedBackup{
		DBInstanceArn:        sourceInstanceARN,
		DBInstanceIdentifier: sourceInstanceARN,
		Status:               instanceStatusStopped,
	}, nil
}

// ---- DB Snapshot Tenant Database operations ----

// AddDBSnapshotTenantDatabase records a tenant database within a snapshot.
// Called internally when creating snapshots from instances with tenant databases.
func (b *InMemoryBackend) AddDBSnapshotTenantDatabase(
	snapshotID, instanceID, tenantDBName, engine string,
) {
	b.mu.Lock("AddDBSnapshotTenantDatabase")
	defer b.mu.Unlock()

	entry := &DBSnapshotTenantDatabase{
		DBSnapshotIdentifier: snapshotID,
		DBInstanceIdentifier: instanceID,
		TenantDatabaseName:   tenantDBName,
		Engine:               engine,
		Status:               instanceStatusAvailable,
	}
	b.snapshotTenantDatabases[snapshotID] = append(b.snapshotTenantDatabases[snapshotID], entry)
}

// DescribeDBSnapshotTenantDatabases lists tenant databases within snapshots.
func (b *InMemoryBackend) DescribeDBSnapshotTenantDatabases(
	snapshotID, instanceID string,
) []DBSnapshotTenantDatabase {
	b.mu.RLock("DescribeDBSnapshotTenantDatabases")
	defer b.mu.RUnlock()

	result := make([]DBSnapshotTenantDatabase, 0)
	for snapID, entries := range b.snapshotTenantDatabases {
		if snapshotID != "" && snapID != snapshotID {
			continue
		}
		for _, entry := range entries {
			if instanceID != "" && entry.DBInstanceIdentifier != instanceID {
				continue
			}
			result = append(result, *entry)
		}
	}

	slices.SortFunc(result, func(a, b DBSnapshotTenantDatabase) int {
		keyA := a.DBSnapshotIdentifier + "/" + a.TenantDatabaseName
		keyB := b.DBSnapshotIdentifier + "/" + b.TenantDatabaseName
		if keyA < keyB {
			return -1
		}
		if keyA > keyB {
			return 1
		}

		return 0
	})

	return result
}
