package glue

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"
)

// cloneColumns returns a deep copy of a Column slice, including each column's
// Parameters map (a shallow slice copy would still alias the map headers).
func cloneColumns(cols []Column) []Column {
	if len(cols) == 0 {
		return nil
	}

	out := make([]Column, len(cols))
	for i, c := range cols {
		out[i] = c
		out[i].Parameters = maps.Clone(c.Parameters)
	}

	return out
}

// cloneStorageDescriptor returns a deep copy of a StorageDescriptor, including
// its Columns, Parameters, SerdeInfo, BucketColumns and SortColumns.
func cloneStorageDescriptor(sd StorageDescriptor) StorageDescriptor {
	cp := sd
	cp.Columns = cloneColumns(sd.Columns)
	cp.Parameters = maps.Clone(sd.Parameters)

	if len(sd.BucketColumns) > 0 {
		cp.BucketColumns = append([]string(nil), sd.BucketColumns...)
	}

	if len(sd.SortColumns) > 0 {
		cp.SortColumns = append([]Order(nil), sd.SortColumns...)
	}

	if sd.SerdeInfo != nil {
		si := *sd.SerdeInfo
		si.Parameters = maps.Clone(sd.SerdeInfo.Parameters)
		cp.SerdeInfo = &si
	}

	return cp
}

// cloneTable returns a deep copy of a Table, including nested slices.
func cloneTable(t *Table) *Table {
	cp := *t
	cp.StorageDescriptor = cloneStorageDescriptor(t.StorageDescriptor)
	cp.PartitionKeys = cloneColumns(t.PartitionKeys)
	cp.Parameters = maps.Clone(t.Parameters)

	return &cp
}

// tableKey returns a map key for a table.
func tableKey(dbName, tableName string) string {
	return fmt.Sprintf("%s|%s", dbName, tableName)
}

// tableVersionKey returns a map key for a table version.
func tableVersionKey(dbName, tableName, versionID string) string {
	return fmt.Sprintf("%s|%s|%s", dbName, tableName, versionID)
}

// CreateTable creates a new Glue table in a database.
func (b *InMemoryBackend) CreateTable(dbName string, input TableInput) (*Table, error) {
	b.mu.Lock("CreateTable")
	defer b.mu.Unlock()

	if !b.databases.Has(dbName) {
		return nil, ErrNotFound
	}

	key := tableKey(dbName, input.Name)
	if b.tables.Has(key) {
		return nil, ErrAlreadyExists
	}

	now := float64(time.Now().Unix())
	t := &Table{
		Name:              input.Name,
		DatabaseName:      dbName,
		CatalogID:         b.accountID,
		Description:       input.Description,
		Owner:             input.Owner,
		Retention:         input.Retention,
		Parameters:        maps.Clone(input.Parameters),
		StorageDescriptor: input.StorageDescriptor,
		PartitionKeys:     input.PartitionKeys,
		TableType:         input.TableType,
		CreateTime:        now,
		UpdateTime:        now,
	}
	b.tables.Put(t)

	return t, nil
}

// GetTable retrieves a Glue table.
func (b *InMemoryBackend) GetTable(dbName, tableName string) (*Table, error) {
	b.mu.RLock("GetTable")
	defer b.mu.RUnlock()

	t, ok := b.tables.Get(tableKey(dbName, tableName))
	if !ok {
		return nil, ErrNotFound
	}

	return cloneTable(t), nil
}

// GetTables returns all tables in a database sorted by name.
func (b *InMemoryBackend) GetTables(dbName string) ([]*Table, error) {
	b.mu.RLock("GetTables")
	defer b.mu.RUnlock()

	if !b.databases.Has(dbName) {
		return nil, ErrNotFound
	}

	prefix := dbName + "|"
	src := b.tables.Snapshot()
	out := make([]*Table, 0, len(src))

	for _, t := range src {
		if k := tableKey(t.DatabaseName, t.Name); len(k) > len(prefix) && k[:len(prefix)] == prefix {
			// Clone before returning: GetTable already clones, but GetTables was
			// handing out the live backend pointer, letting a caller mutate
			// (or a concurrent JSON marshal race against a writer mutating)
			// catalog state without holding b.mu.
			out = append(out, cloneTable(t))
		}
	}

	return out, nil
}

// UpdateTable updates an existing Glue table.
func (b *InMemoryBackend) UpdateTable(dbName string, input TableInput) error {
	b.mu.Lock("UpdateTable")
	defer b.mu.Unlock()

	key := tableKey(dbName, input.Name)

	t, ok := b.tables.Get(key)
	if !ok {
		return ErrNotFound
	}

	t.Description = input.Description
	t.Owner = input.Owner
	t.Retention = input.Retention
	t.Parameters = maps.Clone(input.Parameters)
	t.StorageDescriptor = input.StorageDescriptor
	t.PartitionKeys = input.PartitionKeys
	t.TableType = input.TableType
	t.UpdateTime = float64(time.Now().Unix())

	return nil
}

// DeleteTable deletes a Glue table and all its partitions.
func (b *InMemoryBackend) DeleteTable(dbName, tableName string) error {
	b.mu.Lock("DeleteTable")
	defer b.mu.Unlock()

	key := tableKey(dbName, tableName)
	if !b.tables.Has(key) {
		return ErrNotFound
	}

	b.tables.Delete(key)
	b.deleteTablePartitionsLocked(dbName, tableName)

	return nil
}

// BatchDeleteTable deletes multiple tables and cascades to partitions and versions.
func (b *InMemoryBackend) BatchDeleteTable(dbName string, tableNames []string) []TableError {
	b.mu.Lock("BatchDeleteTable")
	defer b.mu.Unlock()

	errs := make([]TableError, 0, len(tableNames))

	for _, name := range tableNames {
		key := tableKey(dbName, name)
		if !b.tables.Has(key) {
			errs = append(errs, TableError{
				TableName: name,
				ErrorDetail: ErrorDetail{
					ErrorCode:    errEntityNotFoundCode,
					ErrorMessage: "table not found",
				},
			})

			continue
		}

		b.tables.Delete(key)
		b.deleteTablePartitionsLocked(dbName, name)
	}

	return errs
}

// BatchDeleteTableVersion deletes multiple table versions.
func (b *InMemoryBackend) BatchDeleteTableVersion(
	dbName, tableName string,
	versionIDs []string,
) []TableVersionError {
	b.mu.Lock("BatchDeleteTableVersion")
	defer b.mu.Unlock()

	errs := make([]TableVersionError, 0, len(versionIDs))

	for _, vid := range versionIDs {
		key := tableVersionKey(dbName, tableName, vid)
		if !b.tableVersions.Has(key) {
			errs = append(errs, TableVersionError{
				TableName: tableName,
				VersionID: vid,
				ErrorDetail: ErrorDetail{
					ErrorCode:    errEntityNotFoundCode,
					ErrorMessage: "table version not found",
				},
			})

			continue
		}

		b.tableVersions.Delete(key)
	}

	return errs
}

// AddTableVersionInternal adds a table version directly to the backend without
// validation. dbName/tableName are stamped onto the stored copy's nested Table
// field (rather than trusted from the caller-supplied tv, which the existing
// test-seed callers often leave zero-valued) so the store.Table key -- derived
// purely from the value via tableVersionEntryKeyFn -- matches the
// dbName/tableName this entry is filed under, exactly as the previous raw map
// (keyed externally on the same two parameters) did.
func (b *InMemoryBackend) AddTableVersionInternal(dbName, tableName string, tv *TableVersion) {
	b.mu.Lock("AddTableVersionInternal")
	defer b.mu.Unlock()

	cp := *tv
	if cp.Table == nil {
		cp.Table = &Table{}
	} else {
		t := *cp.Table
		cp.Table = &t
	}

	cp.Table.DatabaseName = dbName
	cp.Table.Name = tableName

	b.tableVersions.Put(&cp)
}

// SearchTables returns tables matching searchText against the table name.
// Per SearchTablesInput.SearchText's doc (api_op_SearchTables.go: "Specifying
// a value in quotes filters based on an exact match to the value"), a value
// wrapped in double quotes requires an exact (case-insensitive) match on
// Name; otherwise it's a case-insensitive substring match. An empty
// searchText returns all tables.
func (b *InMemoryBackend) SearchTables(searchText string) []*Table {
	b.mu.RLock("SearchTables")
	defer b.mu.RUnlock()

	quoted := len(searchText) >= 2 && strings.HasPrefix(searchText, `"`) && strings.HasSuffix(searchText, `"`)

	target := searchText
	if quoted {
		target = searchText[1 : len(searchText)-1]
	}

	lower := strings.ToLower(target)
	out := make([]*Table, 0)

	for _, t := range b.tables.Snapshot() {
		name := strings.ToLower(t.Name)

		matches := lower == "" || (quoted && name == lower) || (!quoted && strings.Contains(name, lower))
		if matches {
			out = append(out, cloneTable(t))
		}
	}

	return out
}

func (b *InMemoryBackend) DeleteTableVersion(dbName, tableName, versionID string) error {
	b.mu.Lock("DeleteTableVersion")
	defer b.mu.Unlock()

	key := tableVersionKey(dbName, tableName, versionID)
	if !b.tableVersions.Has(key) {
		return fmt.Errorf(
			"table version %q not found for %s.%s: %w",
			versionID,
			dbName,
			tableName,
			ErrNotFound,
		)
	}
	b.tableVersions.Delete(key)

	return nil
}

// GetTableVersions returns all stored versions for a table, sorted by versionID.
func (b *InMemoryBackend) GetTableVersions(dbName, tableName string) []*TableVersion {
	b.mu.RLock("GetTableVersions")
	defer b.mu.RUnlock()

	prefix := tableVersionKey(dbName, tableName, "")
	src := b.tableVersions.Snapshot()
	out := make([]*TableVersion, 0, len(src))

	for _, tv := range src {
		if k := tableVersionEntryKeyFn(tv); len(k) > len(prefix) && k[:len(prefix)] == prefix {
			cp := *tv
			if tv.Table != nil {
				t := *tv.Table
				cp.Table = &t
			}

			out = append(out, &cp)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].VersionID < out[j].VersionID
	})

	return out
}

// GetTableVersion returns a specific version of a table.
func (b *InMemoryBackend) GetTableVersion(
	dbName, tableName, versionID string,
) (*TableVersion, error) {
	b.mu.RLock("GetTableVersion")
	defer b.mu.RUnlock()

	key := tableVersionKey(dbName, tableName, versionID)

	tv, ok := b.tableVersions.Get(key)
	if !ok {
		return nil, ErrNotFound
	}

	cp := *tv
	if tv.Table != nil {
		t := *tv.Table
		cp.Table = &t
	}

	return &cp, nil
}
