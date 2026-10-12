package athena

import (
	"context"
	"errors"
	"time"

	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// ErrCatalogEntityNotFound and ErrCatalogEntityExists are returned by a
// GlueTableCatalog for missing and duplicate tables or partitions.
var (
	ErrCatalogEntityNotFound = errors.New("catalog entity not found")
	ErrCatalogEntityExists   = errors.New("catalog entity already exists")
)

// GlueTableDef is the storage-level view of a Glue table the SQL engine reads.
type GlueTableDef struct {
	Parameters      map[string]string
	SerdeParameters map[string]string
	Name            string
	TableType       string
	Location        string
	SerdeLibrary    string
	InputFormat     string
	OutputFormat    string
	Columns         []Column
	PartitionKeys   []Column
}

// GluePartitionDef is a registered partition's key values and S3 location.
type GluePartitionDef struct {
	Location string
	Values   []string
}

// GlueTableCatalog is the optional table-level side of the wired
// GlueMetadataSource; without it the SQL engine stays inactive.
type GlueTableCatalog interface {
	GetTableDef(db, name string) (*GlueTableDef, error)
	GetPartitionDefs(db, name string) ([]GluePartitionDef, error)
	CreateTableDef(db string, def GlueTableDef) error
	DeleteTable(db, name string) error
	CreatePartitionDefs(db, table string, parts []GluePartitionDef) error
}

// S3Lister is the listing side of the wired S3 backend.
type S3Lister interface {
	ListObjectsV2(ctx context.Context, input *sdk_s3.ListObjectsV2Input) (*sdk_s3.ListObjectsV2Output, error)
}

// EngineLimits bounds a query's resource use; zero fields keep the defaults.
type EngineLimits struct {
	MaxLoadedRows  int
	MaxResultRows  int
	MaxResultBytes int64
	MaxScanBytes   int64
	QueryTimeout   time.Duration
}

const (
	defaultMaxLoadedRows  = 1_000_000
	defaultMaxResultRows  = 1_000_000
	defaultMaxResultBytes = 256 << 20
	defaultMaxScanBytes   = 1 << 30
	defaultQueryTimeout   = 30 * time.Minute
)

func defaultEngineLimits() EngineLimits {
	return EngineLimits{
		MaxLoadedRows:  defaultMaxLoadedRows,
		MaxResultRows:  defaultMaxResultRows,
		MaxResultBytes: defaultMaxResultBytes,
		MaxScanBytes:   defaultMaxScanBytes,
		QueryTimeout:   defaultQueryTimeout,
	}
}

// SetEngineLimits overrides the SQL engine's resource caps and query timeout.
func (b *InMemoryBackend) SetEngineLimits(l EngineLimits) {
	b.mu.Lock("SetEngineLimits")
	defer b.mu.Unlock()

	if l.MaxLoadedRows > 0 {
		b.limits.MaxLoadedRows = l.MaxLoadedRows
	}

	if l.MaxResultRows > 0 {
		b.limits.MaxResultRows = l.MaxResultRows
	}

	if l.MaxResultBytes > 0 {
		b.limits.MaxResultBytes = l.MaxResultBytes
	}

	if l.MaxScanBytes > 0 {
		b.limits.MaxScanBytes = l.MaxScanBytes
	}

	if l.QueryTimeout > 0 {
		b.limits.QueryTimeout = l.QueryTimeout
	}
}

// engineDeps returns the wired Glue table catalog and S3 reader/lister, or
// ok=false when any is missing. Callers must hold b.mu.
func (b *InMemoryBackend) engineDeps() (GlueTableCatalog, S3Getter, S3Lister, bool) {
	cat, ok := b.glueSource.(GlueTableCatalog)
	if !ok || b.s3 == nil {
		return nil, nil, nil, false
	}

	get, gok := b.s3.(S3Getter)
	list, lok := b.s3.(S3Lister)

	if !gok || !lok {
		return nil, nil, nil, false
	}

	return cat, get, list, true
}
