package s3tables

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
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

var (
	// ErrTableBucketNotFound is returned when a TableBucket does not exist.
	ErrTableBucketNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrTableBucketAlreadyExists is returned when a TableBucket already exists.
	ErrTableBucketAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrNamespaceNotFound is returned when a Namespace does not exist.
	ErrNamespaceNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrNamespaceAlreadyExists is returned when a Namespace already exists.
	ErrNamespaceAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrTableNotFound is returned when a Table does not exist.
	ErrTableNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrTableAlreadyExists is returned when a Table already exists.
	ErrTableAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrTableVersionConflict is returned when an optimistic-lock token is stale.
	ErrTableVersionConflict = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrInvalidTableMetadataLocation is returned when an Iceberg metadata URI is invalid.
	ErrInvalidTableMetadataLocation = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
	// ErrNilAppContext is returned when a nil AppContext is passed to Init.
	ErrNilAppContext = awserr.New("InvalidParameter", awserr.ErrInvalidParameter)
	// ErrInvalidContinuationToken is returned when a list operation's
	// continuation token is malformed.
	ErrInvalidContinuationToken = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
)

// TableBucket represents an S3 Tables table bucket.
type TableBucket struct {
	CreatedAt                time.Time      `json:"createdAt"`
	MaintenanceConfiguration map[string]any `json:"maintenanceConfiguration"`
	Encryption               map[string]any `json:"encryption"`
	ARN                      string         `json:"arn"`
	Name                     string         `json:"name"`
	OwnerAccountID           string         `json:"ownerAccountID"`
	Policy                   string         `json:"policy"`
	StorageClass             string         `json:"storageClass"`
	// MetricsConfigurationID is the unique identifier AWS assigns to a table
	// bucket's metrics configuration once PutTableBucketMetricsConfiguration
	// has been called. It is cleared by DeleteTableBucketMetricsConfiguration.
	MetricsConfigurationID string `json:"metricsConfigurationID,omitempty"`
	MetricsEnabled         bool   `json:"metricsEnabled"`
}

// Namespace represents an S3 Tables namespace.
type Namespace struct {
	CreatedAt      time.Time `json:"createdAt"`
	TableBucketARN string    `json:"tableBucketARN"`
	OwnerAccountID string    `json:"ownerAccountID"`
	CreatedBy      string    `json:"createdBy"`
	Policy         string    `json:"policy"`
	NamespaceID    string    `json:"namespaceID"`
	Namespace      []string  `json:"namespace"`
}

// BucketReplicationConfig holds replication configuration for a table bucket.
type BucketReplicationConfig struct {
	// TableBucketARN is the store.Table primary-key qualifier for the
	// bucketReplication table (see store_setup.go). BucketReplicationConfig
	// carries no identity of its own -- it was previously keyed externally
	// by the map key alone -- so this field was added to give the live
	// store.Table a key to derive. It is tagged json:"-" because it is
	// never part of the S3 Tables wire API (handleGetTableBucketReplication
	// builds its own response map from Destinations only); persistence.go's
	// arnDTO wrapper carries the ARN explicitly for Snapshot/Restore since a
	// plain json.Marshal of this type drops any json:"-" field.
	TableBucketARN string                   `json:"-"`
	Destinations   []ReplicationDestination `json:"destinations"`
}

// ReplicationDestination is a single replication destination.
type ReplicationDestination struct {
	DestinationBucketARN string `json:"destinationBucketARN"`
}

// TableRecordExpiryConfig holds record expiration configuration for a table.
type TableRecordExpiryConfig struct {
	Status string `json:"status"`
	// TableARN is the store.Table primary-key qualifier for the
	// tableRecordExpiry table (see store_setup.go) -- see
	// BucketReplicationConfig.TableBucketARN's doc comment for why this
	// field exists and is tagged json:"-".
	TableARN string `json:"-"`
}

// Table represents an S3 Tables table.
type Table struct {
	CreatedAt                time.Time      `json:"createdAt"`
	ModifiedAt               time.Time      `json:"modifiedAt"`
	MaintenanceConfiguration map[string]any `json:"maintenanceConfiguration"`
	// Encryption is the table-level encryption override set at CreateTable
	// time. When nil, the table has no override and GetTableEncryption
	// falls back to the owning bucket's configuration, then to the AWS
	// default (SSE-S3/AES256) -- see GetTableEncryption. There is no
	// separate PutTableEncryption SDK operation; this can only be set at
	// creation.
	Encryption        map[string]any `json:"encryption"`
	TableBucketARN    string         `json:"tableBucketARN"`
	Format            string         `json:"format"`
	VersionToken      string         `json:"versionToken"`
	MetadataLocation  string         `json:"metadataLocation"`
	WarehouseLocation string         `json:"warehouseLocation"`
	ARN               string         `json:"arn"`
	OwnerAccountID    string         `json:"ownerAccountID"`
	Policy            string         `json:"policy"`
	Name              string         `json:"name"`
	StorageClass      string         `json:"storageClass"`
	Namespace         []string       `json:"namespace"`
}

// CreateTableBucketOptions holds the optional settings a caller may supply
// at CreateTableBucket time (encryptionConfiguration, storageClassConfiguration,
// tags on the wire request) in addition to the required name.
type CreateTableBucketOptions struct {
	Encryption   map[string]any
	Tags         map[string]string
	StorageClass string
}

// CreateTableOptions holds the optional settings a caller may supply at
// CreateTable time (encryptionConfiguration, storageClassConfiguration, tags
// on the wire request) in addition to the required name/format.
type CreateTableOptions struct {
	Encryption   map[string]any
	Tags         map[string]string
	StorageClass string
}

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

// PutTableBucketReplication sets replication config for a table bucket.
func (b *InMemoryBackend) PutTableBucketReplication(
	bucketARN string,
	cfg *BucketReplicationConfig,
) error {
	b.muBuckets.RLock("PutTableBucketReplication")
	defer b.muBuckets.RUnlock()

	b.muState.Lock("PutTableBucketReplication")
	defer b.muState.Unlock()

	if !b.tableBuckets.Has(bucketARN) {
		return fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	cfg.TableBucketARN = bucketARN
	b.bucketReplication.Put(cfg)

	return nil
}

// GetTableBucketReplication returns the replication config for a table bucket.
func (b *InMemoryBackend) GetTableBucketReplication(
	bucketARN string,
) (*BucketReplicationConfig, error) {
	b.muBuckets.RLock("GetTableBucketReplication")
	defer b.muBuckets.RUnlock()

	b.muState.RLock("GetTableBucketReplication")
	defer b.muState.RUnlock()

	if !b.tableBuckets.Has(bucketARN) {
		return nil, fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	cfg, ok := b.bucketReplication.Get(bucketARN)
	if !ok {
		return nil, fmt.Errorf(
			"%w: no replication configuration for table bucket %q",
			ErrTableBucketNotFound,
			bucketARN,
		)
	}

	return cfg, nil
}

// DeleteTableBucketReplication removes the replication config for a table bucket.
func (b *InMemoryBackend) DeleteTableBucketReplication(bucketARN string) error {
	b.muBuckets.RLock("DeleteTableBucketReplication")
	defer b.muBuckets.RUnlock()

	b.muState.Lock("DeleteTableBucketReplication")
	defer b.muState.Unlock()

	if !b.tableBuckets.Has(bucketARN) {
		return fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	b.bucketReplication.Delete(bucketARN)

	return nil
}

// PutTableReplication marks a table as having replication enabled (legacy boolean form).
func (b *InMemoryBackend) PutTableReplication(tableArn string) error {
	b.muTables.RLock("PutTableReplication")
	defer b.muTables.RUnlock()

	b.muState.Lock("PutTableReplication")
	defer b.muState.Unlock()

	if !b.tables.Has(tableArn) {
		return fmt.Errorf("%w: table %q not found", ErrTableNotFound, tableArn)
	}

	b.tableReplication[tableArn] = true

	return nil
}

// DeleteTableReplication removes replication for a table.
func (b *InMemoryBackend) DeleteTableReplication(tableArn string) error {
	b.muTables.RLock("DeleteTableReplication")
	defer b.muTables.RUnlock()

	b.muState.Lock("DeleteTableReplication")
	defer b.muState.Unlock()

	if !b.tables.Has(tableArn) {
		return fmt.Errorf("%w: table %q not found", ErrTableNotFound, tableArn)
	}

	delete(b.tableReplication, tableArn)
	delete(b.tableReplicationConfigs, tableArn)

	return nil
}

// PutTableRecordExpirationConfiguration sets record expiration config for a table.
func (b *InMemoryBackend) PutTableRecordExpirationConfiguration(
	tableArn string,
	cfg *TableRecordExpiryConfig,
) error {
	b.muTables.RLock("PutTableRecordExpirationConfiguration")
	defer b.muTables.RUnlock()

	b.muState.Lock("PutTableRecordExpirationConfiguration")
	defer b.muState.Unlock()

	if !b.tables.Has(tableArn) {
		return fmt.Errorf("%w: table %q not found", ErrTableNotFound, tableArn)
	}

	cfg.TableARN = tableArn
	b.tableRecordExpiry.Put(cfg)

	return nil
}

// GetTableRecordExpirationConfiguration returns record expiry config for a table, defaulting to DISABLED.
func (b *InMemoryBackend) GetTableRecordExpirationConfiguration(
	tableArn string,
) (*TableRecordExpiryConfig, error) {
	b.muTables.RLock("GetTableRecordExpirationConfiguration")
	defer b.muTables.RUnlock()

	b.muState.RLock("GetTableRecordExpirationConfiguration")
	defer b.muState.RUnlock()

	if !b.tables.Has(tableArn) {
		return nil, fmt.Errorf("%w: table %q not found", ErrTableNotFound, tableArn)
	}

	cfg, ok := b.tableRecordExpiry.Get(tableArn)
	if !ok {
		return &TableRecordExpiryConfig{Status: "DISABLED"}, nil
	}

	return cfg, nil
}

func namespaceKey(tableBucketARN, namespace string) string {
	return tableBucketARN + "::" + namespace
}

// CreateTableBucket creates a new TableBucket.
func (b *InMemoryBackend) CreateTableBucket(
	name string,
	opts CreateTableBucketOptions,
) (*TableBucket, error) {
	b.muBuckets.Lock("CreateTableBucket")
	defer b.muBuckets.Unlock()

	bucketARN := b.TableBucketARN(name)
	if b.tableBuckets.Has(bucketARN) {
		return nil, fmt.Errorf(
			"%w: table bucket %q already exists",
			ErrTableBucketAlreadyExists,
			name,
		)
	}

	storageClass := storageClassStandard
	if opts.StorageClass != "" {
		storageClass = opts.StorageClass
	}

	tb := &TableBucket{
		ARN:            bucketARN,
		Name:           name,
		OwnerAccountID: b.accountID,
		CreatedAt:      time.Now().UTC(),
		StorageClass:   storageClass,
		Encryption:     cloneAnyMap(opts.Encryption),
		MaintenanceConfiguration: map[string]any{
			maintenanceTypeIcebergUnreferencedFileRemoval: map[string]any{
				keyStatusField: statusEnabled,
				keySettings: map[string]any{
					maintenanceTypeIcebergUnreferencedFileRemoval: map[string]any{
						"nonCurrentDays":   float64(1),
						"unreferencedDays": float64(3), //nolint:mnd // AWS default: 3 days for unreferenced files
					},
				},
			},
		},
	}
	b.tableBuckets.Put(tb)

	// TagResource only takes muState (see its own lock comment), which sits
	// after muBuckets in the documented lock order, so acquiring it here
	// while muBuckets is already held is safe.
	if len(opts.Tags) > 0 {
		_ = b.TagResource(bucketARN, opts.Tags)
	}

	return cloneTableBucket(tb), nil
}

// GetTableBucket returns a TableBucket by ARN.
func (b *InMemoryBackend) GetTableBucket(bucketARN string) (*TableBucket, error) {
	b.muBuckets.RLock("GetTableBucket")
	defer b.muBuckets.RUnlock()

	tb, ok := b.tableBuckets.Get(bucketARN)
	if !ok {
		return nil, fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	return cloneTableBucket(tb), nil
}

// DeleteTableBucket deletes a TableBucket by ARN, cascading to namespaces and tables.
func (b *InMemoryBackend) DeleteTableBucket(bucketARN string) error {
	b.muBuckets.Lock("DeleteTableBucket")
	b.muNamespaces.Lock("DeleteTableBucket")
	b.muTables.Lock("DeleteTableBucket")
	b.muState.Lock("DeleteTableBucket")
	defer b.muBuckets.Unlock()
	defer b.muNamespaces.Unlock()
	defer b.muTables.Unlock()
	defer b.muState.Unlock()

	if !b.tableBuckets.Has(bucketARN) {
		return fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	// Cascade: delete all tables belonging to this bucket. Clone the index
	// result before deleting, since Table.Delete mutates the same index
	// this slice is a view into.
	for _, t := range slices.Clone(b.tablesByBucket.Get(bucketARN)) {
		b.tables.Delete(t.ARN)
		delete(b.tableReplication, t.ARN)
		delete(b.tableReplicationConfigs, t.ARN)
		b.tableRecordExpiry.Delete(t.ARN)
		delete(b.tags, t.ARN)
	}

	// Cascade: delete all namespaces belonging to this bucket.
	for _, ns := range slices.Clone(b.namespacesByBucket.Get(bucketARN)) {
		b.namespaces.Delete(namespaceKey(ns.TableBucketARN, joinNamespace(ns.Namespace)))
	}

	b.bucketReplication.Delete(bucketARN)
	delete(b.tags, bucketARN)
	b.tableBuckets.Delete(bucketARN)

	return nil
}

// ListTableBuckets returns all TableBuckets sorted by name.
func (b *InMemoryBackend) ListTableBuckets(
	p ListTableBucketsParams,
) (page.Page[*TableBucket], error) {
	if err := page.ValidateToken(p.ContinuationToken); err != nil {
		return page.Page[*TableBucket]{}, fmt.Errorf(
			"%w: invalid continuationToken",
			ErrInvalidContinuationToken,
		)
	}

	b.muBuckets.RLock("ListTableBuckets")
	defer b.muBuckets.RUnlock()

	if p.Type != "" && p.Type != bucketTypeCustomer {
		// Every bucket this backend creates is of type "customer" -- an
		// "aws"-type filter matches nothing, matching real AWS.
		return page.New(
			[]*TableBucket{},
			p.ContinuationToken,
			p.MaxBuckets,
			s3tablesDefaultMaxBuckets,
		), nil
	}

	items := b.tableBuckets.All()
	list := make([]*TableBucket, 0, len(items))

	for _, tb := range items {
		if p.Prefix != "" && !strings.HasPrefix(tb.Name, p.Prefix) {
			continue
		}

		list = append(list, cloneTableBucket(tb))
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})

	return page.New(list, p.ContinuationToken, p.MaxBuckets, s3tablesDefaultMaxBuckets), nil
}

// ListTableBucketsParams holds the filter and pagination inputs for
// ListTableBuckets, mirroring ListTableBucketsInput's prefix/type/
// continuationToken/maxBuckets fields.
type ListTableBucketsParams struct {
	Prefix            string
	Type              string
	ContinuationToken string
	MaxBuckets        int
}

// GetTableBucketMaintenanceConfiguration returns the maintenance config for a bucket.
func (b *InMemoryBackend) GetTableBucketMaintenanceConfiguration(
	bucketARN string,
) (map[string]any, error) {
	b.muBuckets.RLock("GetTableBucketMaintenanceConfiguration")
	defer b.muBuckets.RUnlock()

	tb, ok := b.tableBuckets.Get(bucketARN)
	if !ok {
		return nil, fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	cfg := cloneAnyMap(tb.MaintenanceConfiguration)

	return cfg, nil
}

// PutTableBucketMaintenanceConfiguration sets maintenance config for a bucket.
func (b *InMemoryBackend) PutTableBucketMaintenanceConfiguration(
	bucketARN, maintenanceType string,
	value map[string]any,
) error {
	b.muBuckets.Lock("PutTableBucketMaintenanceConfiguration")
	defer b.muBuckets.Unlock()

	tb, ok := b.tableBuckets.Get(bucketARN)
	if !ok {
		return fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	if tb.MaintenanceConfiguration == nil {
		tb.MaintenanceConfiguration = make(map[string]any)
	}

	tb.MaintenanceConfiguration[maintenanceType] = value

	return nil
}

// GetTableBucketPolicy returns the resource policy for a bucket.
func (b *InMemoryBackend) GetTableBucketPolicy(bucketARN string) (string, error) {
	b.muBuckets.RLock("GetTableBucketPolicy")
	defer b.muBuckets.RUnlock()

	tb, ok := b.tableBuckets.Get(bucketARN)
	if !ok {
		return "", fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	if tb.Policy == "" {
		return "", fmt.Errorf(
			"%w: no policy for table bucket %q",
			ErrTableBucketNotFound,
			bucketARN,
		)
	}

	return tb.Policy, nil
}

// PutTableBucketPolicy sets the resource policy for a bucket.
func (b *InMemoryBackend) PutTableBucketPolicy(bucketARN, policy string) error {
	b.muBuckets.Lock("PutTableBucketPolicy")
	defer b.muBuckets.Unlock()

	tb, ok := b.tableBuckets.Get(bucketARN)
	if !ok {
		return fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	tb.Policy = policy

	return nil
}

// DeleteTableBucketPolicy removes the resource policy from a bucket.
func (b *InMemoryBackend) DeleteTableBucketPolicy(bucketARN string) error {
	b.muBuckets.Lock("DeleteTableBucketPolicy")
	defer b.muBuckets.Unlock()

	tb, ok := b.tableBuckets.Get(bucketARN)
	if !ok {
		return fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	tb.Policy = ""

	return nil
}

// PutTableBucketEncryption sets encryption config for a bucket.
func (b *InMemoryBackend) PutTableBucketEncryption(bucketARN string, config map[string]any) error {
	b.muBuckets.Lock("PutTableBucketEncryption")
	defer b.muBuckets.Unlock()

	tb, ok := b.tableBuckets.Get(bucketARN)
	if !ok {
		return fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	tb.Encryption = cloneAnyMap(config)

	return nil
}

// DeleteTableBucketEncryption clears the encryption configuration for a bucket,
// reverting GetTableBucketEncryption to the AWS default (no configuration set).
func (b *InMemoryBackend) DeleteTableBucketEncryption(bucketARN string) error {
	b.muBuckets.Lock("DeleteTableBucketEncryption")
	defer b.muBuckets.Unlock()

	tb, ok := b.tableBuckets.Get(bucketARN)
	if !ok {
		return fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	tb.Encryption = nil

	return nil
}

// PutTableBucketMetricsConfiguration enables metrics for a bucket, assigning it
// a unique metrics configuration ID.
func (b *InMemoryBackend) PutTableBucketMetricsConfiguration(bucketARN string) error {
	b.muBuckets.Lock("PutTableBucketMetricsConfiguration")
	defer b.muBuckets.Unlock()

	tb, ok := b.tableBuckets.Get(bucketARN)
	if !ok {
		return fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	tb.MetricsEnabled = true
	tb.MetricsConfigurationID = uuid.NewString()

	return nil
}

// GetTableBucketMetricsConfiguration returns the metrics configuration ID for a
// bucket. The second return value is false when no metrics configuration has
// ever been put for the bucket.
func (b *InMemoryBackend) GetTableBucketMetricsConfiguration(
	bucketARN string,
) (string, bool, error) {
	b.muBuckets.RLock("GetTableBucketMetricsConfiguration")
	defer b.muBuckets.RUnlock()

	tb, ok := b.tableBuckets.Get(bucketARN)
	if !ok {
		return "", false, fmt.Errorf(
			"%w: table bucket %q not found",
			ErrTableBucketNotFound,
			bucketARN,
		)
	}

	return tb.MetricsConfigurationID, tb.MetricsEnabled, nil
}

// DeleteTableBucketMetricsConfiguration clears the metrics configuration for a
// bucket, reverting GetTableBucketMetricsConfiguration to the unconfigured state.
func (b *InMemoryBackend) DeleteTableBucketMetricsConfiguration(bucketARN string) error {
	b.muBuckets.Lock("DeleteTableBucketMetricsConfiguration")
	defer b.muBuckets.Unlock()

	tb, ok := b.tableBuckets.Get(bucketARN)
	if !ok {
		return fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	tb.MetricsEnabled = false
	tb.MetricsConfigurationID = ""

	return nil
}

// PutTableBucketStorageClass sets storage class for a bucket.
func (b *InMemoryBackend) PutTableBucketStorageClass(bucketARN, storageClass string) error {
	b.muBuckets.Lock("PutTableBucketStorageClass")
	defer b.muBuckets.Unlock()

	tb, ok := b.tableBuckets.Get(bucketARN)
	if !ok {
		return fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	tb.StorageClass = storageClass

	return nil
}

// GetTableStorageClass returns the storage class for a table.
func (b *InMemoryBackend) GetTableStorageClass(
	bucketARN string,
	namespace []string,
	name string,
) (string, error) {
	b.muTables.RLock("GetTableStorageClass")
	defer b.muTables.RUnlock()

	nsStr := joinNamespace(namespace)

	t, ok := b.tableByComposite(bucketARN, nsStr, name)
	if !ok {
		return "", fmt.Errorf(
			"%w: table %q not found in namespace %s",
			ErrTableNotFound,
			name,
			nsStr,
		)
	}

	sc := t.StorageClass
	if sc == "" {
		sc = storageClassStandard
	}

	return sc, nil
}

// SetTableReplicationConfig sets the replication config (map form) for a table.
func (b *InMemoryBackend) SetTableReplicationConfig(tableArn string, config map[string]any) error {
	b.muTables.RLock("SetTableReplicationConfig")
	defer b.muTables.RUnlock()

	b.muState.Lock("SetTableReplicationConfig")
	defer b.muState.Unlock()

	if !b.tables.Has(tableArn) {
		return fmt.Errorf("%w: table %q not found", ErrTableNotFound, tableArn)
	}

	b.tableReplication[tableArn] = true
	b.tableReplicationConfigs[tableArn] = cloneAnyMap(config)

	return nil
}

// GetTableReplicationConfig returns the replication config for a table.
func (b *InMemoryBackend) GetTableReplicationConfig(tableArn string) (map[string]any, error) {
	b.muTables.RLock("GetTableReplicationConfig")
	defer b.muTables.RUnlock()

	b.muState.RLock("GetTableReplicationConfig")
	defer b.muState.RUnlock()

	if !b.tables.Has(tableArn) {
		return nil, fmt.Errorf("%w: table %q not found", ErrTableNotFound, tableArn)
	}

	cfg, ok := b.tableReplicationConfigs[tableArn]
	if !ok {
		return nil, fmt.Errorf(
			"%w: no replication configuration for table %q",
			ErrTableNotFound,
			tableArn,
		)
	}

	return cloneAnyMap(cfg), nil
}

// ValidateTableExists checks that a table ARN exists in the backend.
func (b *InMemoryBackend) ValidateTableExists(tableArn string) error {
	b.muTables.RLock("ValidateTableExists")
	defer b.muTables.RUnlock()

	if !b.tables.Has(tableArn) {
		return fmt.Errorf("%w: table %q not found", ErrTableNotFound, tableArn)
	}

	return nil
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

// CreateNamespace creates a new namespace within a table bucket.
func (b *InMemoryBackend) CreateNamespace(
	tableBucketARN string,
	namespace []string,
) (*Namespace, error) {
	b.muBuckets.RLock("CreateNamespace")
	defer b.muBuckets.RUnlock()

	b.muNamespaces.Lock("CreateNamespace")
	defer b.muNamespaces.Unlock()

	if !b.tableBuckets.Has(tableBucketARN) {
		return nil, fmt.Errorf(
			"%w: table bucket %q not found",
			ErrTableBucketNotFound,
			tableBucketARN,
		)
	}

	nsStr := joinNamespace(namespace)
	key := namespaceKey(tableBucketARN, nsStr)

	if b.namespaces.Has(key) {
		return nil, fmt.Errorf(
			"%w: namespace %q already exists in bucket %s",
			ErrNamespaceAlreadyExists,
			nsStr,
			tableBucketARN,
		)
	}

	ns := &Namespace{
		Namespace:      cloneStringSlice(namespace),
		TableBucketARN: tableBucketARN,
		OwnerAccountID: b.accountID,
		CreatedBy:      b.accountID,
		CreatedAt:      time.Now().UTC(),
		NamespaceID:    uuid.NewString(),
	}
	b.namespaces.Put(ns)

	return cloneNamespace(ns), nil
}

// GetNamespace returns a namespace by bucket ARN and namespace name.
func (b *InMemoryBackend) GetNamespace(
	tableBucketARN string,
	namespace []string,
) (*Namespace, error) {
	b.muNamespaces.RLock("GetNamespace")
	defer b.muNamespaces.RUnlock()

	nsStr := joinNamespace(namespace)
	key := namespaceKey(tableBucketARN, nsStr)

	ns, ok := b.namespaces.Get(key)
	if !ok {
		return nil, fmt.Errorf(
			"%w: namespace %q not found in bucket %s",
			ErrNamespaceNotFound,
			nsStr,
			tableBucketARN,
		)
	}

	return cloneNamespace(ns), nil
}

// DeleteNamespace deletes a namespace from a table bucket.
func (b *InMemoryBackend) DeleteNamespace(tableBucketARN string, namespace []string) error {
	b.muNamespaces.Lock("DeleteNamespace")
	defer b.muNamespaces.Unlock()

	nsStr := joinNamespace(namespace)
	key := namespaceKey(tableBucketARN, nsStr)

	if !b.namespaces.Has(key) {
		return fmt.Errorf(
			"%w: namespace %q not found in bucket %s",
			ErrNamespaceNotFound,
			nsStr,
			tableBucketARN,
		)
	}

	b.namespaces.Delete(key)

	return nil
}

// ListNamespaces returns all namespaces in a table bucket sorted by name.
func (b *InMemoryBackend) ListNamespaces(
	tableBucketARN string,
	p ListNamespacesParams,
) (page.Page[*Namespace], error) {
	if err := page.ValidateToken(p.ContinuationToken); err != nil {
		return page.Page[*Namespace]{}, fmt.Errorf(
			"%w: invalid continuationToken",
			ErrInvalidContinuationToken,
		)
	}

	b.muBuckets.RLock("ListNamespaces")
	defer b.muBuckets.RUnlock()

	b.muNamespaces.RLock("ListNamespaces")
	defer b.muNamespaces.RUnlock()

	if !b.tableBuckets.Has(tableBucketARN) {
		return page.Page[*Namespace]{}, fmt.Errorf(
			"%w: table bucket %q not found",
			ErrTableBucketNotFound,
			tableBucketARN,
		)
	}

	items := b.namespacesByBucket.Get(tableBucketARN)
	list := make([]*Namespace, 0, len(items))

	for _, ns := range items {
		if p.Prefix != "" && !strings.HasPrefix(joinNamespace(ns.Namespace), p.Prefix) {
			continue
		}

		list = append(list, cloneNamespace(ns))
	}

	sort.Slice(list, func(i, j int) bool {
		return joinNamespace(list[i].Namespace) < joinNamespace(list[j].Namespace)
	})

	return page.New(list, p.ContinuationToken, p.MaxNamespaces, s3tablesDefaultMaxNamespaces), nil
}

// ListNamespacesParams holds the filter and pagination inputs for
// ListNamespaces, mirroring ListNamespacesInput's prefix/continuationToken/
// maxNamespaces fields.
type ListNamespacesParams struct {
	Prefix            string
	ContinuationToken string
	MaxNamespaces     int
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

// CreateTable creates a new table within a namespace.
func (b *InMemoryBackend) CreateTable(
	tableBucketARN string,
	namespace []string,
	name, format string,
	opts CreateTableOptions,
) (*Table, error) {
	b.muBuckets.RLock("CreateTable")
	defer b.muBuckets.RUnlock()

	b.muNamespaces.RLock("CreateTable")
	defer b.muNamespaces.RUnlock()

	b.muTables.Lock("CreateTable")
	defer b.muTables.Unlock()

	tb, ok := b.tableBuckets.Get(tableBucketARN)
	if !ok {
		return nil, fmt.Errorf(
			"%w: table bucket %q not found",
			ErrTableBucketNotFound,
			tableBucketARN,
		)
	}

	nsStr := joinNamespace(namespace)
	nsKey := namespaceKey(tableBucketARN, nsStr)

	if !b.namespaces.Has(nsKey) {
		return nil, fmt.Errorf(
			"%w: namespace %q not found in bucket %s",
			ErrNamespaceNotFound,
			nsStr,
			tableBucketARN,
		)
	}

	if _, exists := b.tableByComposite(tableBucketARN, nsStr, name); exists {
		return nil, fmt.Errorf(
			"%w: table %q already exists in namespace %s",
			ErrTableAlreadyExists,
			name,
			nsStr,
		)
	}

	tableARN := b.TableARN(tb.Name, nsStr, name)

	storageClass := opts.StorageClass

	now := time.Now().UTC()
	table := &Table{
		ARN:               tableARN,
		Name:              name,
		Namespace:         cloneStringSlice(namespace),
		TableBucketARN:    tableBucketARN,
		Format:            format,
		VersionToken:      uuid.NewString(),
		WarehouseLocation: "s3://" + tb.Name + "/" + nsStr + "/" + name,
		CreatedAt:         now,
		ModifiedAt:        now,
		OwnerAccountID:    b.accountID,
		StorageClass:      storageClass,
		Encryption:        cloneAnyMap(opts.Encryption),
		MaintenanceConfiguration: map[string]any{
			maintenanceTypeIcebergCompaction: map[string]any{
				keyStatusField: statusEnabled,
				keySettings: map[string]any{
					maintenanceTypeIcebergCompaction: map[string]any{
						"targetFileSizeMB": float64(512), //nolint:mnd // AWS default: 512 MB target file size
						"strategy":         "binpack",
					},
				},
			},
			maintenanceTypeIcebergSnapshotManagement: map[string]any{
				keyStatusField: statusEnabled,
				keySettings: map[string]any{
					maintenanceTypeIcebergSnapshotManagement: map[string]any{
						"maxSnapshotAgeHours": float64(120), //nolint:mnd // AWS default: 120 hours (5 days)
						"minSnapshotsToKeep":  float64(1),
					},
				},
			},
		},
	}
	b.tables.Put(table)

	// TagResource only takes muState, which sits after muTables in the
	// documented lock order (muBuckets -> muNamespaces -> muTables ->
	// muState), so acquiring it here while muBuckets/muNamespaces/muTables
	// are already held is safe.
	if len(opts.Tags) > 0 {
		_ = b.TagResource(tableARN, opts.Tags)
	}

	return cloneTable(table), nil
}

// GetTableByARN returns a table by its ARN directly, without needing the
// caller to know its bucket/namespace/name. Real GetTable accepts either
// tableArn alone or the tableBucketARN+namespace+name triple (see
// GetTableInput's optional TableArn field) -- this backs the former.
func (b *InMemoryBackend) GetTableByARN(tableArn string) (*Table, error) {
	b.muTables.RLock("GetTableByARN")
	defer b.muTables.RUnlock()

	t, ok := b.tables.Get(tableArn)
	if !ok {
		return nil, fmt.Errorf("%w: table %q not found", ErrTableNotFound, tableArn)
	}

	return cloneTable(t), nil
}

// GetTableEncryption returns the effective encryption configuration for a
// table: the table's own override if CreateTable set one, else the owning
// bucket's configuration, else the AWS default (SSE-S3/AES256). There is no
// PutTableEncryption operation for individual tables in the real API, so
// GetTableEncryption never returns NotFound the way GetTableBucketEncryption
// can -- every table has an effective encryption configuration.
func (b *InMemoryBackend) GetTableEncryption(
	tableBucketARN string,
	namespace []string,
	name string,
) (map[string]any, error) {
	b.muBuckets.RLock("GetTableEncryption")
	defer b.muBuckets.RUnlock()

	b.muTables.RLock("GetTableEncryption")
	defer b.muTables.RUnlock()

	nsStr := joinNamespace(namespace)

	t, ok := b.tableByComposite(tableBucketARN, nsStr, name)
	if !ok {
		return nil, fmt.Errorf(
			"%w: table %q not found in namespace %s",
			ErrTableNotFound,
			name,
			nsStr,
		)
	}

	if t.Encryption != nil {
		return cloneAnyMap(t.Encryption), nil
	}

	if tb, tbOK := b.tableBuckets.Get(tableBucketARN); tbOK && tb.Encryption != nil {
		return cloneAnyMap(tb.Encryption), nil
	}

	return map[string]any{"sseAlgorithm": defaultSSEAlgorithm}, nil
}

// GetTable returns a table by bucket ARN, namespace, and name.
func (b *InMemoryBackend) GetTable(
	tableBucketARN string,
	namespace []string,
	name string,
) (*Table, error) {
	b.muTables.RLock("GetTable")
	defer b.muTables.RUnlock()

	nsStr := joinNamespace(namespace)

	t, ok := b.tableByComposite(tableBucketARN, nsStr, name)
	if !ok {
		return nil, fmt.Errorf(
			"%w: table %q not found in namespace %s",
			ErrTableNotFound,
			name,
			nsStr,
		)
	}

	return cloneTable(t), nil
}

// DeleteTable deletes a table by bucket ARN, namespace, and name.
func (b *InMemoryBackend) DeleteTable(
	tableBucketARN string,
	namespace []string,
	name string,
) error {
	b.muTables.Lock("DeleteTable")
	defer b.muTables.Unlock()

	nsStr := joinNamespace(namespace)

	t, ok := b.tableByComposite(tableBucketARN, nsStr, name)
	if !ok {
		return fmt.Errorf("%w: table %q not found in namespace %s", ErrTableNotFound, name, nsStr)
	}

	b.tables.Delete(t.ARN)

	return nil
}

// ListTables returns all tables in a table bucket, optionally filtered by namespace.
func (b *InMemoryBackend) ListTables(
	tableBucketARN, namespace string,
	p ListTablesParams,
) (page.Page[*Table], error) {
	if err := page.ValidateToken(p.ContinuationToken); err != nil {
		return page.Page[*Table]{}, fmt.Errorf(
			"%w: invalid continuationToken",
			ErrInvalidContinuationToken,
		)
	}

	b.muBuckets.RLock("ListTables")
	defer b.muBuckets.RUnlock()

	b.muTables.RLock("ListTables")
	defer b.muTables.RUnlock()

	if !b.tableBuckets.Has(tableBucketARN) {
		return page.Page[*Table]{}, fmt.Errorf(
			"%w: table bucket %q not found",
			ErrTableBucketNotFound,
			tableBucketARN,
		)
	}

	items := b.tablesByBucket.Get(tableBucketARN)
	list := make([]*Table, 0, len(items))

	for _, t := range items {
		if namespace != "" && joinNamespace(t.Namespace) != namespace {
			continue
		}

		if p.Prefix != "" && !strings.HasPrefix(t.Name, p.Prefix) {
			continue
		}

		list = append(list, cloneTable(t))
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})

	return page.New(list, p.ContinuationToken, p.MaxTables, s3tablesDefaultMaxTables), nil
}

// ListTablesParams holds the filter and pagination inputs for ListTables,
// mirroring ListTablesInput's prefix/continuationToken/maxTables fields
// (namespace remains its own positional parameter above since ListTables
// already took it before this pagination sweep, and existing callers key
// off that signature).
type ListTablesParams struct {
	Prefix            string
	ContinuationToken string
	MaxTables         int
}

// RenameTable renames a table or moves it to a different namespace.
func (b *InMemoryBackend) RenameTable(
	tableBucketARN string,
	namespace []string,
	name, newNamespace, newName, versionToken string,
) error {
	b.muBuckets.RLock("RenameTable")
	defer b.muBuckets.RUnlock()

	b.muNamespaces.RLock("RenameTable")
	defer b.muNamespaces.RUnlock()

	b.muTables.Lock("RenameTable")
	defer b.muTables.Unlock()

	nsStr := joinNamespace(namespace)

	found, ok := b.tableByComposite(tableBucketARN, nsStr, name)
	if !ok {
		return fmt.Errorf("%w: table %q not found in namespace %s", ErrTableNotFound, name, nsStr)
	}

	if versionToken != "" && versionToken != found.VersionToken {
		return fmt.Errorf("%w: stale version token for table %q", ErrTableVersionConflict, name)
	}

	if newName == "" {
		newName = name
	}

	if newNamespace == "" {
		newNamespace = nsStr
	}

	if !b.namespaces.Has(namespaceKey(tableBucketARN, newNamespace)) {
		return fmt.Errorf(
			"%w: namespace %q not found in bucket %s",
			ErrNamespaceNotFound,
			newNamespace,
			tableBucketARN,
		)
	}

	tb, _ := b.tableBuckets.Get(tableBucketARN)
	newARN := b.TableARN(tb.Name, newNamespace, newName)

	if _, exists := b.tableByComposite(tableBucketARN, newNamespace, newName); exists {
		return fmt.Errorf(
			"%w: table %q already exists in namespace %s",
			ErrTableAlreadyExists,
			newName,
			newNamespace,
		)
	}

	// found.ARN (the primary key) and its composite/bucket index keys are
	// all about to change, so the old entry must be explicitly removed
	// before the mutated value is re-inserted -- Put alone would leave the
	// stale entry under the old ARN behind, since Put only knows how to
	// replace whatever is already stored at the NEW key it derives.
	b.tables.Delete(found.ARN)

	found.Name = newName
	found.Namespace = splitNamespace(newNamespace)
	found.ARN = newARN
	found.ModifiedAt = time.Now().UTC()
	found.VersionToken = uuid.NewString()

	b.tables.Put(found)

	return nil
}

// UpdateTableMetadataLocation updates the metadata location of a table.
func (b *InMemoryBackend) UpdateTableMetadataLocation(
	tableBucketARN string,
	namespace []string,
	name, metadataLocation, versionToken string,
) (*Table, error) {
	b.muTables.Lock("UpdateTableMetadataLocation")
	defer b.muTables.Unlock()

	nsStr := joinNamespace(namespace)

	t, ok := b.tableByComposite(tableBucketARN, nsStr, name)
	if !ok {
		return nil, fmt.Errorf(
			"%w: table %q not found in namespace %s",
			ErrTableNotFound,
			name,
			nsStr,
		)
	}

	if versionToken != t.VersionToken {
		return nil, fmt.Errorf(
			"%w: stale version token for table %q",
			ErrTableVersionConflict,
			name,
		)
	}

	if !validMetadataLocation(t.WarehouseLocation, metadataLocation) {
		return nil, fmt.Errorf(
			"%w: metadata location %q is outside table warehouse or invalid",
			ErrInvalidTableMetadataLocation,
			metadataLocation,
		)
	}

	t.MetadataLocation = metadataLocation
	t.VersionToken = uuid.NewString()
	t.ModifiedAt = time.Now().UTC()

	return cloneTable(t), nil
}

func validMetadataLocation(_, metadataLocation string) bool {
	if !strings.HasPrefix(metadataLocation, "s3://") {
		return false
	}

	return strings.HasSuffix(metadataLocation, ".json") ||
		strings.HasSuffix(metadataLocation, ".json.gz")
}

// GetTableMaintenanceConfiguration returns the maintenance config for a table.
func (b *InMemoryBackend) GetTableMaintenanceConfiguration(
	tableBucketARN string,
	namespace []string,
	name string,
) (map[string]any, string, error) {
	b.muTables.RLock("GetTableMaintenanceConfiguration")
	defer b.muTables.RUnlock()

	nsStr := joinNamespace(namespace)

	t, ok := b.tableByComposite(tableBucketARN, nsStr, name)
	if !ok {
		return nil, "", fmt.Errorf(
			"%w: table %q not found in namespace %s",
			ErrTableNotFound,
			name,
			nsStr,
		)
	}

	return cloneAnyMap(t.MaintenanceConfiguration), t.ARN, nil
}

// PutTableMaintenanceConfiguration sets maintenance config for a table.
func (b *InMemoryBackend) PutTableMaintenanceConfiguration(
	tableBucketARN string,
	namespace []string,
	name, maintenanceType string,
	value map[string]any,
) error {
	b.muTables.Lock("PutTableMaintenanceConfiguration")
	defer b.muTables.Unlock()

	nsStr := joinNamespace(namespace)

	t, ok := b.tableByComposite(tableBucketARN, nsStr, name)
	if !ok {
		return fmt.Errorf("%w: table %q not found in namespace %s", ErrTableNotFound, name, nsStr)
	}

	if t.MaintenanceConfiguration == nil {
		t.MaintenanceConfiguration = make(map[string]any)
	}

	t.MaintenanceConfiguration[maintenanceType] = value

	return nil
}

// GetTablePolicy returns the resource policy for a table.
func (b *InMemoryBackend) GetTablePolicy(
	tableBucketARN string,
	namespace []string,
	name string,
) (string, error) {
	b.muTables.RLock("GetTablePolicy")
	defer b.muTables.RUnlock()

	nsStr := joinNamespace(namespace)

	t, ok := b.tableByComposite(tableBucketARN, nsStr, name)
	if !ok {
		return "", fmt.Errorf(
			"%w: table %q not found in namespace %s",
			ErrTableNotFound,
			name,
			nsStr,
		)
	}

	if t.Policy == "" {
		return "", fmt.Errorf("%w: no policy for table %q", ErrTableNotFound, name)
	}

	return t.Policy, nil
}

// PutTablePolicy sets the resource policy for a table.
func (b *InMemoryBackend) PutTablePolicy(
	tableBucketARN string,
	namespace []string,
	name, policy string,
) error {
	b.muTables.Lock("PutTablePolicy")
	defer b.muTables.Unlock()

	nsStr := joinNamespace(namespace)

	t, ok := b.tableByComposite(tableBucketARN, nsStr, name)
	if !ok {
		return fmt.Errorf("%w: table %q not found in namespace %s", ErrTableNotFound, name, nsStr)
	}

	t.Policy = policy

	return nil
}

// DeleteTablePolicy removes the resource policy from a table.
func (b *InMemoryBackend) DeleteTablePolicy(
	tableBucketARN string,
	namespace []string,
	name string,
) error {
	b.muTables.Lock("DeleteTablePolicy")
	defer b.muTables.Unlock()

	nsStr := joinNamespace(namespace)

	t, ok := b.tableByComposite(tableBucketARN, nsStr, name)
	if !ok {
		return fmt.Errorf("%w: table %q not found in namespace %s", ErrTableNotFound, name, nsStr)
	}

	t.Policy = ""

	return nil
}

func cloneTableBucket(tb *TableBucket) *TableBucket {
	cp := *tb
	cp.MaintenanceConfiguration = cloneAnyMap(tb.MaintenanceConfiguration)
	cp.Encryption = cloneAnyMap(tb.Encryption)

	return &cp
}

func cloneNamespace(ns *Namespace) *Namespace {
	cp := *ns
	cp.Namespace = cloneStringSlice(ns.Namespace)

	return &cp
}

func cloneTable(t *Table) *Table {
	cp := *t
	cp.Namespace = cloneStringSlice(t.Namespace)
	cp.MaintenanceConfiguration = cloneAnyMap(t.MaintenanceConfiguration)
	cp.Encryption = cloneAnyMap(t.Encryption)

	return &cp
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
