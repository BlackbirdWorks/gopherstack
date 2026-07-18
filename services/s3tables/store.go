package s3tables

import (
	"maps"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	statusEnabled                                 = "enabled"
	keySettings                                   = "settings"
	storageClassStandard                          = "STANDARD"
	maintenanceTypeIcebergCompaction              = "icebergCompaction"
	maintenanceTypeIcebergSnapshotManagement      = "icebergSnapshotManagement"
	maintenanceTypeIcebergUnreferencedFileRemoval = "icebergUnreferencedFileRemoval"
	metadataJSONSuffix                            = ".metadata.json"
	metadataJSONGzipSuffix                        = ".metadata.json.gz"
	defaultSSEAlgorithm                           = "AES256"

	// Default page sizes for List operations, applied when the caller does
	// not specify (or specifies a non-positive) maxBuckets/maxNamespaces/
	// maxTables -- real S3 Tables likewise caps unlimited requests to a
	// server-side default rather than returning every resource in one page.
	s3tablesDefaultMaxBuckets    = 1000
	s3tablesDefaultMaxNamespaces = 1000
	s3tablesDefaultMaxTables     = 1000
)

// InMemoryBackend is an in-memory store for S3 Tables resources.
type InMemoryBackend struct {
	// registry holds every store.Table below that has no field hidden from
	// plain JSON marshaling -- see store_setup.go's file doc comment.
	registry *store.Registry

	tableBuckets *store.Table[TableBucket] // keyed by ARN

	namespaces         *store.Table[Namespace] // keyed by tableBucketARN + "::" + namespace
	namespacesByBucket *store.Index[Namespace] // secondary: tableBucketARN -> namespaces

	tables            *store.Table[Table] // keyed by ARN
	tablesByComposite *store.Index[Table] // secondary: bucketARN::ns::name -> table (replaces the old tableIndex map)
	tablesByBucket    *store.Index[Table] // secondary: tableBucketARN -> tables

	// bucketReplication and tableRecordExpiry are off-registry: their value
	// types hide their primary-key field (json:"-"), so registry.SnapshotAll
	// would silently drop it -- persistence.go builds a separate ephemeral
	// DTO registry for them instead (see that file's doc comment).
	bucketReplication *store.Table[BucketReplicationConfig] // keyed by bucket ARN
	tableRecordExpiry *store.Table[TableRecordExpiryConfig] // keyed by table ARN

	// tableReplication (a bare bool with no identity of its own), tags (a
	// non-*T value map), and tableReplicationConfigs (a non-*T value map)
	// do not fit store.Table's keyed-*T shape, so they remain plain maps --
	// see persistence.go for how they round-trip alongside the tables above.
	tableReplication        map[string]bool              // keyed by table ARN
	tags                    map[string]map[string]string // keyed by ARN
	tableReplicationConfigs map[string]map[string]any    // keyed by table ARN

	// Lock ordering: muBuckets → muNamespaces → muTables → muState
	muBuckets    *lockmetrics.RWMutex // covers tableBuckets
	muNamespaces *lockmetrics.RWMutex // covers namespaces
	muTables     *lockmetrics.RWMutex // covers tables + tablesByComposite + tablesByBucket
	muState      *lockmetrics.RWMutex // covers replication, expiry, tags, tableReplicationConfigs
	accountID    string
	region       string
}

// NewInMemoryBackend creates a new in-memory S3 Tables backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:                store.NewRegistry(),
		tableReplication:        make(map[string]bool),
		tags:                    make(map[string]map[string]string),
		tableReplicationConfigs: make(map[string]map[string]any),
		accountID:               accountID,
		region:                  region,
		muBuckets:               lockmetrics.New("s3tables.buckets"),
		muNamespaces:            lockmetrics.New("s3tables.namespaces"),
		muTables:                lockmetrics.New("s3tables.tables"),
		muState:                 lockmetrics.New("s3tables.state"),
	}

	registerAllTables(b)

	return b
}

// tableCompositeKey returns the composite index key for a table.
func tableCompositeKey(tableBucketARN, nsStr, name string) string {
	return tableBucketARN + "::" + nsStr + "::" + name
}

// TableBucketARN builds an ARN for a TableBucket.
func (b *InMemoryBackend) TableBucketARN(name string) string {
	return arn.Build("s3tables", b.region, b.accountID, "bucket/"+name)
}

// TableARN builds an ARN for a Table.
func (b *InMemoryBackend) TableARN(bucketName, namespaceName, tableName string) string {
	return arn.Build("s3tables", b.region, b.accountID,
		"bucket/"+bucketName+"/table/"+namespaceName+"/"+tableName)
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the account ID for this backend.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset clears all stored state.
func (b *InMemoryBackend) Reset() {
	b.muBuckets.Lock("Reset")
	b.muNamespaces.Lock("Reset")
	b.muTables.Lock("Reset")
	b.muState.Lock("Reset")
	defer b.muBuckets.Unlock()
	defer b.muNamespaces.Unlock()
	defer b.muTables.Unlock()
	defer b.muState.Unlock()

	b.registry.ResetAll()
	b.bucketReplication.Reset()
	b.tableRecordExpiry.Reset()

	b.tableReplication = make(map[string]bool)
	b.tags = make(map[string]map[string]string)
	b.tableReplicationConfigs = make(map[string]map[string]any)
}

func namespaceKey(tableBucketARN, namespace string) string {
	return tableBucketARN + "::" + namespace
}

// TagResource adds or updates tags on a resource (bucket or table ARN).
func (b *InMemoryBackend) TagResource(resourceArn string, newTags map[string]string) error {
	b.muState.Lock("TagResource")
	defer b.muState.Unlock()

	existing := b.tags[resourceArn]
	if existing == nil {
		existing = make(map[string]string)
	}

	maps.Copy(existing, newTags)

	b.tags[resourceArn] = existing

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	b.muState.Lock("UntagResource")
	defer b.muState.Unlock()

	existing := b.tags[resourceArn]
	if existing == nil {
		return nil
	}

	for _, k := range tagKeys {
		delete(existing, k)
	}

	return nil
}

// ListTagsForResource returns all tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	b.muState.RLock("ListTagsForResource")
	defer b.muState.RUnlock()

	existing := b.tags[resourceArn]
	if existing == nil {
		return map[string]string{}, nil
	}

	result := make(map[string]string, len(existing))
	maps.Copy(result, existing)

	return result, nil
}

// tableByComposite looks up a table by its bucket/namespace/name composite
// key via the byComposite secondary index. The index enforces at most one
// match per composite key (CreateTable/RenameTable reject collisions), so
// the first (only) match is returned.
func (b *InMemoryBackend) tableByComposite(tableBucketARN, nsStr, name string) (*Table, bool) {
	matches := b.tablesByComposite.Get(tableCompositeKey(tableBucketARN, nsStr, name))
	if len(matches) == 0 {
		return nil, false
	}

	return matches[0], true
}

func cloneStringSlice(s []string) []string {
	if s == nil {
		return []string{}
	}

	out := make([]string, len(s))
	copy(out, s)

	return out
}

func cloneAnyMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}

	return maps.Clone(m)
}

// joinNamespace joins a namespace slice with "." for use as a map key.
func joinNamespace(ns []string) string {
	return strings.Join(ns, ".")
}

// splitNamespace splits a dot-separated namespace string back into a slice.
func splitNamespace(ns string) []string {
	if ns == "" {
		return []string{}
	}

	return strings.Split(ns, ".")
}
