package athena

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/iceberg"
)

const s3TablesCatalogPrefix = "s3tablescatalog/"

// ErrCatalogCommitConflict is returned when a catalog commit finds the table's
// metadata location moved since it was read.
var ErrCatalogCommitConflict = errors.New("catalog commit conflict")

var errS3TablesPartitions = errors.New(
	"NOT_SUPPORTED: S3 Tables are Iceberg tables and have no Hive partitions",
)

// S3TableInfo is an S3 Tables table as the SQL engine sees it.
type S3TableInfo struct {
	MetadataLocation string
	Location         string
}

// S3TablesSource resolves tables of the federated s3tablescatalog/<table-bucket>
// catalog (database = namespace), wired via SetS3TablesSource.
type S3TablesSource interface {
	GetTableInfo(region, tableBucket, namespace, table string) (*S3TableInfo, error)
	CommitMetadataLocation(region, tableBucket, namespace, table, expected, next string) error
	// CreateTable allocates an empty Iceberg table (no metadata.json yet) and returns its warehouse location.
	// Errors wrap ErrCatalogEntityExists or ErrCatalogEntityNotFound where they apply.
	CreateTable(region, tableBucket, namespace, table string) (*S3TableInfo, error)
	DeleteTable(region, tableBucket, namespace, table string) error
	ListNamespaces(region, tableBucket string) ([]string, error)
	ListTables(region, tableBucket, namespace string) ([]string, error)
}

// tableAllocator is implemented by catalogs that choose a new table's location.
type tableAllocator interface {
	AllocateTable(db, name string) (*S3TableInfo, error)
}

// tableCommitter is implemented by catalogs that can swap an Iceberg table's metadata location.
type tableCommitter interface {
	CommitTableMetadata(db, name, expected, next string) error
}

// SetS3TablesSource wires the S3 Tables federated catalog.
func (b *InMemoryBackend) SetS3TablesSource(src S3TablesSource) { b.s3tables = src }

func s3TablesBucket(catalog string) (string, bool) {
	if len(catalog) <= len(s3TablesCatalogPrefix) ||
		!strings.EqualFold(catalog[:len(s3TablesCatalogPrefix)], s3TablesCatalogPrefix) {
		return "", false
	}

	return catalog[len(s3TablesCatalogPrefix):], true
}

// tableCatalogFor returns the table catalog serving a catalog name: the wired
// Glue catalog for GLUE data catalogs, or an S3 Tables view for
// s3tablescatalog/<bucket>. Callers must hold b.mu.
func (b *InMemoryBackend) tableCatalogFor(catalog string) (GlueTableCatalog, bool) {
	if _, ok := s3TablesBucket(catalog); ok {
		if b.s3tables == nil {
			return nil, false
		}

		cat, _ := b.s3TablesView(catalog)

		return cat, true
	}

	if !b.isGlueBacked(catalog) {
		return nil, false
	}

	cat, ok := b.glueSource.(GlueTableCatalog)

	return cat, ok
}

type s3TablesCatalog struct {
	src    S3TablesSource
	get    S3Getter
	region string
	bucket string
}

func (c *s3TablesCatalog) GetTableDef(db, name string) (*GlueTableDef, error) {
	info, err := c.src.GetTableInfo(c.region, c.bucket, db, name)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCatalogEntityNotFound, err)
	}

	return &GlueTableDef{
		Name: name, TableType: tableTypeExternal, Location: info.Location,
		Columns:    c.columnsOf(info),
		Parameters: map[string]string{paramTableType: tableTypeIceberg, paramMetadataLoc: info.MetadataLocation},
	}, nil
}

// columnsOf reads the current schema from metadata.json; unreadable metadata yields no columns.
func (c *s3TablesCatalog) columnsOf(info *S3TableInfo) []Column {
	if c.get == nil || info.MetadataLocation == "" {
		return nil
	}

	l := &tableLoader{get: c.get, limits: defaultEngineLimits()}

	data, err := l.readURI(context.Background(), info.MetadataLocation)
	if err != nil {
		return nil
	}

	meta, err := iceberg.ParseMetadata(data)
	if err != nil {
		return nil
	}

	cols, err := icebergColumns(meta.CurrentSchema(), "")
	if err != nil {
		return nil
	}

	return cols
}

func (c *s3TablesCatalog) GetPartitionDefs(string, string) ([]GluePartitionDef, error) {
	return nil, nil
}

// CreateTableDef registers a table whose metadata.json the caller already wrote at def.Parameters[metadata_location].
func (c *s3TablesCatalog) CreateTableDef(db string, def GlueTableDef) error {
	info, err := c.src.GetTableInfo(c.region, c.bucket, db, def.Name)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrCatalogEntityNotFound, err)
	}

	return c.src.CommitMetadataLocation(
		c.region, c.bucket, db, def.Name, info.MetadataLocation, lowerParam(def.Parameters, paramMetadataLoc),
	)
}

func (c *s3TablesCatalog) AllocateTable(db, name string) (*S3TableInfo, error) {
	return c.src.CreateTable(c.region, c.bucket, db, name)
}

func (c *s3TablesCatalog) DeleteTable(db, name string) error {
	return c.src.DeleteTable(c.region, c.bucket, db, name)
}

func (c *s3TablesCatalog) CreatePartitionDefs(string, string, []GluePartitionDef) error {
	return errS3TablesPartitions
}

func (c *s3TablesCatalog) CommitTableMetadata(db, name, expected, next string) error {
	return c.src.CommitMetadataLocation(c.region, c.bucket, db, name, expected, next)
}
