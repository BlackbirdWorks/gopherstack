package rds

import (
	"fmt"
	"slices"
	"time"
)

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

const (
	tenantStatusAvailableInternal = instanceStatusAvailable
	tenantStatusDeletingInternal  = instanceStatusDeleting
)
