package timestreamwrite

import (
	"fmt"
	"maps"
	"sort"
	"time"
)

// CreateDatabase creates a new Timestream database with an optional KMS key and
// initial tags. KmsKeyID is applied atomically at creation time (matching the AWS
// API, which accepts KmsKeyId directly on CreateDatabaseInput) so CreationTime and
// LastUpdatedTime stay equal on the returned Database, and no other request can
// observe the database without its KMS key in between.
func (b *InMemoryBackend) CreateDatabase(name, kmsKeyID string, tags map[string]string) (*Database, error) {
	b.mu.Lock("CreateDatabase")
	defer b.mu.Unlock()

	if b.databases.Has(name) {
		return nil, fmt.Errorf("%w: database %s already exists", ErrDatabaseAlreadyExists, name)
	}

	now := time.Now()
	db := &Database{
		DatabaseName:    name,
		ARN:             databaseARN(name),
		KmsKeyID:        kmsKeyID,
		TableCount:      0,
		CreationTime:    now,
		LastUpdatedTime: now,
	}
	b.databases.Put(db)
	b.records[name] = make(map[string]*tableRecords)

	if len(tags) > 0 {
		b.tags[db.ARN] = make(map[string]string, len(tags))
		maps.Copy(b.tags[db.ARN], tags)
	}

	cp := *db

	return &cp, nil
}

// DescribeDatabase returns information about a database.
func (b *InMemoryBackend) DescribeDatabase(name string) (*Database, error) {
	b.mu.RLock("DescribeDatabase")
	defer b.mu.RUnlock()

	db, ok := b.databases.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: database %s not found", ErrDatabaseNotFound, name)
	}

	cp := *db

	return &cp, nil
}

// ListDatabases returns all databases sorted by name.
func (b *InMemoryBackend) ListDatabases() []Database {
	b.mu.RLock("ListDatabases")
	defer b.mu.RUnlock()

	out := make([]Database, 0, b.databases.Len())
	for _, db := range b.databases.All() {
		cp := *db
		out = append(out, cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].DatabaseName < out[j].DatabaseName
	})

	return out
}

// DeleteDatabase deletes a database and all its tables.
func (b *InMemoryBackend) DeleteDatabase(name string) error {
	b.mu.Lock("DeleteDatabase")
	defer b.mu.Unlock()

	if !b.databases.Has(name) {
		return fmt.Errorf("%w: database %s not found", ErrDatabaseNotFound, name)
	}

	// Copy the index group before mutating b.tables: Index.Get's returned
	// slice is owned by the index and is invalidated by Table.Delete calls
	// made while ranging over it (see pkgs/store.Index.Get's doc comment).
	tbls := append([]*Table(nil), b.tablesByDatabase.Get(name)...)

	// Clean up tags and per-table mutexes for all tables in this database
	// before dropping the records map so lockmetrics doesn't leak handles.
	for _, tbl := range tbls {
		delete(b.tags, tableARN(name, tbl.TableName))
	}

	for _, slot := range b.records[name] {
		if slot != nil && slot.mu != nil {
			slot.mu.Close()
		}
	}

	for _, tbl := range tbls {
		b.tables.Delete(tableKey(name, tbl.TableName))
	}

	b.databases.Delete(name)
	delete(b.records, name)
	delete(b.tags, databaseARN(name))

	return nil
}

// UpdateDatabase updates the KMS key for a database.
func (b *InMemoryBackend) UpdateDatabase(name, kmsKeyID string) (*Database, error) {
	b.mu.Lock("UpdateDatabase")
	defer b.mu.Unlock()

	db, ok := b.databases.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: database %s not found", ErrDatabaseNotFound, name)
	}

	db.KmsKeyID = kmsKeyID
	db.LastUpdatedTime = time.Now()
	cp := *db

	return &cp, nil
}

// AddDatabaseInternal directly inserts a database into the backend, bypassing
// validation. Intended only for test setup.
func (b *InMemoryBackend) AddDatabaseInternal(db *Database) {
	b.mu.Lock("AddDatabaseInternal")
	defer b.mu.Unlock()

	cp := *db
	b.databases.Put(&cp)

	if b.records[db.DatabaseName] == nil {
		b.records[db.DatabaseName] = make(map[string]*tableRecords)
	}
}
