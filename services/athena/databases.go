package athena

import (
	"fmt"
	"maps"
	"sort"
	"strings"
)

// GetDatabase returns a database by catalog and name.
func (b *InMemoryBackend) GetDatabase(catalog, name string) (*Database, error) {
	if catalog == "" {
		return nil, fmt.Errorf("%w: CatalogName is required", ErrValidation)
	}

	if name == "" {
		return nil, fmt.Errorf("%w: DatabaseName is required", ErrValidation)
	}

	b.mu.RLock("GetDatabase")
	defer b.mu.RUnlock()

	d, ok := b.databases.Get(databaseKey(catalog, name))
	if !ok {
		return nil, fmt.Errorf("%w: database %q not found in catalog %q", ErrMetadata, name, catalog)
	}

	cp := *d
	cp.Parameters = maps.Clone(d.Parameters)

	return &cp, nil
}

// ListDatabases returns all databases for a catalog.
func (b *InMemoryBackend) ListDatabases(catalog string) ([]Database, error) {
	if catalog == "" {
		return nil, fmt.Errorf("%w: CatalogName is required", ErrValidation)
	}

	b.mu.RLock("ListDatabases")
	defer b.mu.RUnlock()

	dbs := b.databasesByCatalog.Get(catalog)
	out := make([]Database, 0, len(dbs))

	for _, d := range dbs {
		cp := *d
		cp.Parameters = maps.Clone(d.Parameters)
		out = append(out, cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}

// GetTableMetadata returns the metadata for a single table.
func (b *InMemoryBackend) GetTableMetadata(catalog, database, table string) (*TableMetadata, error) {
	if catalog == "" || database == "" || table == "" {
		return nil, fmt.Errorf("%w: CatalogName, DatabaseName, TableName are required", ErrValidation)
	}

	b.mu.RLock("GetTableMetadata")
	defer b.mu.RUnlock()

	t, ok := b.tables.Get(tableMetadataKey(catalog, database, table))
	if !ok {
		return nil, fmt.Errorf("%w: table %q not found in %s/%s", ErrMetadata, table, catalog, database)
	}

	cp := *t
	cp.Parameters = maps.Clone(t.Parameters)
	cp.Columns = append([]Column(nil), t.Columns...)
	cp.PartitionKeys = append([]Column(nil), t.PartitionKeys...)

	return &cp, nil
}

// ListTableMetadata returns all tables for a database, optionally filtered by name prefix.
func (b *InMemoryBackend) ListTableMetadata(catalog, database, expr string) ([]TableMetadata, error) {
	if catalog == "" || database == "" {
		return nil, fmt.Errorf("%w: CatalogName and DatabaseName are required", ErrValidation)
	}

	b.mu.RLock("ListTableMetadata")
	defer b.mu.RUnlock()

	tables := b.tablesByDatabase.Get(databaseKey(catalog, database))
	out := make([]TableMetadata, 0, len(tables))

	for _, t := range tables {
		if expr != "" && !strings.Contains(t.Name, expr) {
			continue
		}

		cp := *t
		cp.Parameters = maps.Clone(t.Parameters)
		cp.Columns = append([]Column(nil), t.Columns...)
		cp.PartitionKeys = append([]Column(nil), t.PartitionKeys...)
		out = append(out, cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}
