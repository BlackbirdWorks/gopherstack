package rds

import (
	"fmt"
	"net/url"
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

// isKnownTenantDatabaseFilterName reports whether name is a
// Filters.Filter.N.Name value AWS recognizes for DescribeTenantDatabases
// (rds@v1.124.1 api_op_DescribeTenantDatabases.go:42-53).
// "tenant-database-resource-id" is accepted (to avoid rejecting an
// otherwise-valid client request) but TenantDatabase carries no such
// attribute, so it is not implemented as a match predicate, matching the
// existing DescribeDBInstances "domain" precedent (db_instances.go).
func isKnownTenantDatabaseFilterName(name string) bool {
	switch name {
	case filterNameTenantDBName, filterNameTenantDatabaseResourceID, filterNameDbiResourceID:
		return true
	default:
		return false
	}
}

// applyTenantDatabaseFilters narrows tdbs per the AWS DescribeTenantDatabases
// Filters contract: each filter ANDs together, and a filter's Values list is
// OR-matched against the corresponding tenant database field. An
// unrecognized filter name returns InvalidParameterValue, matching real AWS.
func applyTenantDatabaseFilters(vals url.Values, tdbs []TenantDatabase) ([]TenantDatabase, error) {
	filters := parseDescribeFilters(vals)
	if len(filters) == 0 {
		return tdbs, nil
	}

	for name := range filters {
		if !isKnownTenantDatabaseFilterName(name) {
			return nil, fmt.Errorf("%w: Unrecognized filter name: %s", ErrInvalidParameter, name)
		}
	}

	filtered := make([]TenantDatabase, 0, len(tdbs))
	for _, tdb := range tdbs {
		if matchesAllTenantDatabaseFilters(tdb, filters) {
			filtered = append(filtered, tdb)
		}
	}

	return filtered, nil
}

func matchesAllTenantDatabaseFilters(tdb TenantDatabase, filters map[string][]string) bool {
	for name, values := range filters {
		switch name {
		case filterNameTenantDBName:
			if !slices.Contains(values, tdb.TenantDBName) {
				return false
			}
		case filterNameDbiResourceID:
			if !slices.Contains(values, tdb.DbiResourceID) {
				return false
			}
		case filterNameTenantDatabaseResourceID:
			// Not modeled; accept unconditionally.
		}
	}

	return true
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

// addDBSnapshotTenantDatabaseLocked records a tenant database within a
// snapshot. Callers must already hold b.mu for writing.
func (b *InMemoryBackend) addDBSnapshotTenantDatabaseLocked(
	snapshotID, instanceID, tenantDBName, engine string,
) {
	entry := &DBSnapshotTenantDatabase{
		DBSnapshotIdentifier: snapshotID,
		DBInstanceIdentifier: instanceID,
		TenantDatabaseName:   tenantDBName,
		Engine:               engine,
		Status:               instanceStatusAvailable,
	}
	b.snapshotTenantDatabases[snapshotID] = append(b.snapshotTenantDatabases[snapshotID], entry)
}

// AddDBSnapshotTenantDatabase records a tenant database within a snapshot.
// CreateDBSnapshot (db_snapshots.go) now calls this itself for every tenant
// database on the snapshotted instance; exported for direct test seeding too.
func (b *InMemoryBackend) AddDBSnapshotTenantDatabase(
	snapshotID, instanceID, tenantDBName, engine string,
) {
	b.mu.Lock("AddDBSnapshotTenantDatabase")
	defer b.mu.Unlock()

	b.addDBSnapshotTenantDatabaseLocked(snapshotID, instanceID, tenantDBName, engine)
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

// isKnownDBSnapshotTenantDatabaseFilterName reports whether name is a
// Filters.Filter.N.Name value AWS recognizes for
// DescribeDBSnapshotTenantDatabases (rds@v1.124.1
// api_op_DescribeDBSnapshotTenantDatabases.go:59-79).
// "tenant-database-resource-id", "dbi-resource-id", and "snapshot-type" are
// accepted (to avoid rejecting an otherwise-valid client request) but
// DBSnapshotTenantDatabase carries none of those attributes, so they are not
// implemented as match predicates, matching the existing DescribeDBInstances
// "domain" precedent (db_instances.go).
func isKnownDBSnapshotTenantDatabaseFilterName(name string) bool {
	switch name {
	case filterNameTenantDBName, filterNameTenantDatabaseResourceID, filterNameDbiResourceID,
		filterNameDBInstanceID, filterNameDBSnapshotID, filterNameSnapshotType:
		return true
	default:
		return false
	}
}

// applyDBSnapshotTenantDatabaseFilters narrows entries per the AWS
// DescribeDBSnapshotTenantDatabases Filters contract: each filter ANDs
// together, and a filter's Values list is OR-matched against the
// corresponding entry field. db-instance-id accepts identifiers or ARNs per
// this op's own doc comment (api_op:74-75); db-snapshot-id accepts plain
// identifiers only (api_op:77). An unrecognized filter name returns
// InvalidParameterValue, matching real AWS.
func applyDBSnapshotTenantDatabaseFilters(
	vals url.Values, entries []DBSnapshotTenantDatabase,
) ([]DBSnapshotTenantDatabase, error) {
	filters := parseDescribeFilters(vals)
	if len(filters) == 0 {
		return entries, nil
	}

	for name := range filters {
		if !isKnownDBSnapshotTenantDatabaseFilterName(name) {
			return nil, fmt.Errorf("%w: Unrecognized filter name: %s", ErrInvalidParameter, name)
		}
	}

	filtered := make([]DBSnapshotTenantDatabase, 0, len(entries))
	for _, e := range entries {
		if matchesAllDBSnapshotTenantDatabaseFilters(e, filters) {
			filtered = append(filtered, e)
		}
	}

	return filtered, nil
}

func matchesAllDBSnapshotTenantDatabaseFilters(e DBSnapshotTenantDatabase, filters map[string][]string) bool {
	for name, values := range filters {
		switch name {
		case filterNameTenantDBName:
			if !slices.Contains(values, e.TenantDatabaseName) {
				return false
			}
		case filterNameDBInstanceID:
			if !containsFoldIDOrARN(values, e.DBInstanceIdentifier) {
				return false
			}
		case filterNameDBSnapshotID:
			if !containsFold(values, e.DBSnapshotIdentifier) {
				return false
			}
		case filterNameTenantDatabaseResourceID, filterNameDbiResourceID, filterNameSnapshotType:
			// Not modeled; accept unconditionally.
		}
	}

	return true
}

const (
	tenantStatusAvailableInternal = instanceStatusAvailable
	tenantStatusDeletingInternal  = instanceStatusDeleting
)
