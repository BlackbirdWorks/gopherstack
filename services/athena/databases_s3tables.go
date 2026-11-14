package athena

import (
	"fmt"
	"maps"
	"regexp"
)

// s3TablesView returns the S3 Tables catalog serving catalog. Callers must hold b.mu.
func (b *InMemoryBackend) s3TablesView(catalog string) (*s3TablesCatalog, bool) {
	bucket, ok := s3TablesBucket(catalog)
	if !ok || b.s3tables == nil {
		return nil, false
	}

	get, _ := b.s3.(S3Getter)

	return &s3TablesCatalog{src: b.s3tables, region: b.region, bucket: bucket, get: get}, true
}

func s3TablesDatabase(cat *s3TablesCatalog, catalog, name string) (*Database, error) {
	dbs, err := s3TablesDatabases(cat, catalog)
	if err != nil {
		return nil, err
	}

	for _, d := range dbs {
		if d.Name == name {
			return &d, nil
		}
	}

	return nil, fmt.Errorf("%w: database %q not found in catalog %q", ErrMetadata, name, catalog)
}

func s3TablesDatabases(cat *s3TablesCatalog, catalog string) ([]Database, error) {
	names, err := cat.src.ListNamespaces(cat.region, cat.bucket)
	if err != nil {
		return nil, fmt.Errorf("%w: catalog %q not found: %w", ErrMetadata, catalog, err)
	}

	out := make([]Database, 0, len(names))
	for _, n := range names {
		out = append(out, Database{Catalog: catalog, Name: n})
	}

	return out, nil
}

func s3TablesTable(cat *s3TablesCatalog, catalog, database, table string) (*TableMetadata, error) {
	def, err := cat.GetTableDef(database, table)
	if err != nil {
		return nil, fmt.Errorf("%w: table %q not found in %s/%s", ErrMetadata, table, catalog, database)
	}

	return &TableMetadata{
		Catalog: catalog, Database: database, Name: table, TableType: tableTypeExternal,
		Parameters: maps.Clone(def.Parameters), Columns: def.Columns,
	}, nil
}

func s3TablesTables(cat *s3TablesCatalog, catalog, database string, re *regexp.Regexp) ([]TableMetadata, error) {
	names, err := cat.src.ListTables(cat.region, cat.bucket, database)
	if err != nil {
		return nil, fmt.Errorf("%w: database %q not found in catalog %q: %w", ErrMetadata, database, catalog, err)
	}

	var out []TableMetadata

	for _, n := range names {
		if re != nil && !re.MatchString(n) {
			continue
		}

		t, terr := s3TablesTable(cat, catalog, database, n)
		if terr != nil {
			continue
		}

		out = append(out, *t)
	}

	return out, nil
}
