package athena

import (
	"errors"
	"fmt"
	"strings"
)

const s3TablesCatalogPrefix = "s3tablescatalog/"

// ErrCatalogCommitConflict is returned when a catalog commit finds the table's
// metadata location moved since it was read.
var ErrCatalogCommitConflict = errors.New("catalog commit conflict")

var errS3TablesDDL = errors.New(
	"NOT_SUPPORTED: schema changes in S3 Tables catalogs are not supported; use the S3 Tables API",
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
	if bucket, ok := s3TablesBucket(catalog); ok {
		if b.s3tables == nil {
			return nil, false
		}

		return &s3TablesCatalog{src: b.s3tables, region: b.region, bucket: bucket}, true
	}

	if !b.isGlueBacked(catalog) {
		return nil, false
	}

	cat, ok := b.glueSource.(GlueTableCatalog)

	return cat, ok
}

type s3TablesCatalog struct {
	src    S3TablesSource
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
		Parameters: map[string]string{paramTableType: tableTypeIceberg, paramMetadataLoc: info.MetadataLocation},
	}, nil
}

func (c *s3TablesCatalog) GetPartitionDefs(string, string) ([]GluePartitionDef, error) {
	return nil, nil
}

func (c *s3TablesCatalog) CreateTableDef(string, GlueTableDef) error { return errS3TablesDDL }

func (c *s3TablesCatalog) DeleteTable(string, string) error { return errS3TablesDDL }

func (c *s3TablesCatalog) CreatePartitionDefs(string, string, []GluePartitionDef) error {
	return errS3TablesDDL
}

func (c *s3TablesCatalog) CommitTableMetadata(db, name, expected, next string) error {
	return c.src.CommitMetadataLocation(c.region, c.bucket, db, name, expected, next)
}
