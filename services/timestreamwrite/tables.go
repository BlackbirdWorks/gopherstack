package timestreamwrite

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// tableStatusActive is the normal operational state for a table.
const tableStatusActive = "ACTIVE"

// defaultMemoryRetentionHours is the AWS default for MemoryStoreRetentionPeriodInHours
// when no retention properties are specified at table creation time.
const defaultMemoryRetentionHours = int64(6)

// defaultMagneticRetentionDays is the AWS default for MagneticStoreRetentionPeriodInDays
// when no retention properties are specified at table creation time.
const defaultMagneticRetentionDays = int64(73)

// CreateTable creates a new table in the specified database with optional initial tags.
func (b *InMemoryBackend) CreateTable(
	dbName, tblName string,
	tags map[string]string,
	inp *CreateTableInput,
) (*Table, error) {
	b.mu.Lock("CreateTable")
	defer b.mu.Unlock()

	if !b.databases.Has(dbName) {
		return nil, fmt.Errorf("%w: database %s not found", ErrDatabaseNotFound, dbName)
	}

	if b.tables.Has(tableKey(dbName, tblName)) {
		return nil, fmt.Errorf("%w: table %s already exists", ErrTableAlreadyExists, tblName)
	}

	now := time.Now()
	tbl := &Table{
		DatabaseName:    dbName,
		TableName:       tblName,
		ARN:             tableARN(dbName, tblName),
		TableStatus:     tableStatusActive,
		CreationTime:    now,
		LastUpdatedTime: now,
	}

	if inp != nil {
		tbl.RetentionProperties = inp.RetentionProperties
		tbl.MagneticStoreWriteProperties = inp.MagneticStoreWriteProperties
		tbl.Schema = inp.Schema
	}

	if tbl.RetentionProperties == nil {
		tbl.RetentionProperties = &RetentionProperties{
			MemoryStoreRetentionPeriodInHours:  defaultMemoryRetentionHours,
			MagneticStoreRetentionPeriodInDays: defaultMagneticRetentionDays,
		}
	}

	b.tables.Put(tbl)
	b.records[dbName][tblName] = &tableRecords{
		mu:          lockmetrics.New("timestreamwrite.table"),
		recordIndex: make(map[string]int),
	}

	db, _ := b.databases.Get(dbName)
	db.TableCount++

	if len(tags) > 0 {
		b.tags[tbl.ARN] = make(map[string]string, len(tags))
		maps.Copy(b.tags[tbl.ARN], tags)
	}

	cp := *tbl

	return &cp, nil
}

// DescribeTable returns information about a table.
func (b *InMemoryBackend) DescribeTable(dbName, tblName string) (*Table, error) {
	b.mu.RLock("DescribeTable")
	defer b.mu.RUnlock()

	if !b.databases.Has(dbName) {
		return nil, fmt.Errorf("%w: database %s not found", ErrDatabaseNotFound, dbName)
	}

	tbl, ok := b.tables.Get(tableKey(dbName, tblName))
	if !ok {
		return nil, fmt.Errorf("%w: table %s not found", ErrTableNotFound, tblName)
	}

	cp := *tbl

	return &cp, nil
}

// ListTables returns all tables in a database sorted by name, or every table
// across every database when dbName is empty. ListTablesInput marks no
// member required, DatabaseName included (api_op_ListTables.go,
// timestreamwrite@v1.38.4) -- gopherstack-4ly2.
func (b *InMemoryBackend) ListTables(dbName string) ([]Table, error) {
	b.mu.RLock("ListTables")
	defer b.mu.RUnlock()

	var group []*Table

	if dbName == "" {
		group = b.tables.All()
	} else {
		if !b.databases.Has(dbName) {
			return nil, fmt.Errorf("%w: database %s not found", ErrDatabaseNotFound, dbName)
		}

		group = b.tablesByDatabase.Get(dbName)
	}

	out := make([]Table, 0, len(group))

	for _, tbl := range group {
		cp := *tbl
		out = append(out, cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].TableName < out[j].TableName
	})

	return out, nil
}

// DeleteTable deletes a table from a database.
func (b *InMemoryBackend) DeleteTable(dbName, tblName string) error {
	b.mu.Lock("DeleteTable")
	defer b.mu.Unlock()

	if !b.databases.Has(dbName) {
		return fmt.Errorf("%w: database %s not found", ErrDatabaseNotFound, dbName)
	}

	key := tableKey(dbName, tblName)
	if !b.tables.Has(key) {
		return fmt.Errorf("%w: table %s not found", ErrTableNotFound, tblName)
	}

	arn := tableARN(dbName, tblName)

	if slot := b.records[dbName][tblName]; slot != nil && slot.mu != nil {
		slot.mu.Close()
	}

	b.tables.Delete(key)
	delete(b.records[dbName], tblName)
	delete(b.tags, arn)

	db, _ := b.databases.Get(dbName)
	db.TableCount--

	return nil
}

// UpdateTable updates a table's properties.
func (b *InMemoryBackend) UpdateTable(dbName, tblName string, inp *UpdateTableInput) (*Table, error) {
	b.mu.Lock("UpdateTable")
	defer b.mu.Unlock()

	if !b.databases.Has(dbName) {
		return nil, fmt.Errorf("%w: database %s not found", ErrDatabaseNotFound, dbName)
	}

	tbl, ok := b.tables.Get(tableKey(dbName, tblName))
	if !ok {
		return nil, fmt.Errorf("%w: table %s not found", ErrTableNotFound, tblName)
	}

	if inp != nil {
		if inp.RetentionProperties != nil {
			tbl.RetentionProperties = inp.RetentionProperties
		}

		if inp.MagneticStoreWriteProperties != nil {
			tbl.MagneticStoreWriteProperties = inp.MagneticStoreWriteProperties
		}

		if inp.Schema != nil {
			tbl.Schema = inp.Schema
		}
	}

	tbl.LastUpdatedTime = time.Now()
	cp := *tbl

	return &cp, nil
}

// AddTableInternal directly inserts a table into the backend, bypassing
// validation. The parent database must exist. Intended only for test setup.
func (b *InMemoryBackend) AddTableInternal(tbl *Table) {
	b.mu.Lock("AddTableInternal")
	defer b.mu.Unlock()

	if b.records[tbl.DatabaseName] == nil {
		b.records[tbl.DatabaseName] = make(map[string]*tableRecords)
	}

	cp := *tbl
	b.tables.Put(&cp)

	// If a slot already exists for this table, close its mutex before
	// overwriting so we don't leak the old lockmetrics.RWMutex.
	if existing := b.records[tbl.DatabaseName][tbl.TableName]; existing != nil && existing.mu != nil {
		existing.mu.Close()
	}

	b.records[tbl.DatabaseName][tbl.TableName] = &tableRecords{
		mu:          lockmetrics.New("timestreamwrite.table"),
		recordIndex: make(map[string]int),
	}

	if db, ok := b.databases.Get(tbl.DatabaseName); ok {
		db.TableCount++
	}
}
