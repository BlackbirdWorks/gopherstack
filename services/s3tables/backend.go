package s3tables

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	statusEnabled        = "enabled"
	keySettings          = "settings"
	storageClassStandard = "STANDARD"
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
	// ErrNilAppContext is returned when a nil AppContext is passed to Init.
	ErrNilAppContext = awserr.New("InvalidParameter", awserr.ErrInvalidParameter)
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
	MetricsEnabled           bool           `json:"metricsEnabled"`
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
	Destinations []ReplicationDestination `json:"destinations"`
}

// ReplicationDestination is a single replication destination.
type ReplicationDestination struct {
	DestinationBucketARN string `json:"destinationBucketARN"`
}

// TableRecordExpiryConfig holds record expiration configuration for a table.
type TableRecordExpiryConfig struct {
	Status string `json:"status"`
}

// Table represents an S3 Tables table.
type Table struct {
	CreatedAt                time.Time      `json:"createdAt"`
	ModifiedAt               time.Time      `json:"modifiedAt"`
	MaintenanceConfiguration map[string]any `json:"maintenanceConfiguration"`
	TableBucketARN           string         `json:"tableBucketARN"`
	Format                   string         `json:"format"`
	VersionToken             string         `json:"versionToken"`
	MetadataLocation         string         `json:"metadataLocation"`
	WarehouseLocation        string         `json:"warehouseLocation"`
	ARN                      string         `json:"arn"`
	OwnerAccountID           string         `json:"ownerAccountID"`
	Policy                   string         `json:"policy"`
	Name                     string         `json:"name"`
	StorageClass             string         `json:"storageClass"`
	Namespace                []string       `json:"namespace"`
}

// InMemoryBackend is an in-memory store for S3 Tables resources.
type InMemoryBackend struct {
	tableBuckets            map[string]*TableBucket             // keyed by ARN
	namespaces              map[string]*Namespace               // keyed by tableBucketARN + "::" + namespace
	tables                  map[string]*Table                   // keyed by ARN
	tableIndex              map[string]string                   // composite key (bucketARN::ns::name) → table ARN
	bucketReplication       map[string]*BucketReplicationConfig // keyed by bucket ARN
	tableReplication        map[string]bool                     // keyed by table ARN
	tableRecordExpiry       map[string]*TableRecordExpiryConfig // keyed by table ARN
	tags                    map[string]map[string]string        // keyed by ARN
	tableReplicationConfigs map[string]map[string]any           // keyed by table ARN
	// Lock ordering: muBuckets → muNamespaces → muTables → muState
	muBuckets    *lockmetrics.RWMutex // covers tableBuckets
	muNamespaces *lockmetrics.RWMutex // covers namespaces
	muTables     *lockmetrics.RWMutex // covers tables + tableIndex
	muState      *lockmetrics.RWMutex // covers replication, expiry, tags, tableReplicationConfigs
	accountID    string
	region       string
}

// NewInMemoryBackend creates a new in-memory S3 Tables backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		tableBuckets:            make(map[string]*TableBucket),
		namespaces:              make(map[string]*Namespace),
		tables:                  make(map[string]*Table),
		tableIndex:              make(map[string]string),
		bucketReplication:       make(map[string]*BucketReplicationConfig),
		tableReplication:        make(map[string]bool),
		tableRecordExpiry:       make(map[string]*TableRecordExpiryConfig),
		tags:                    make(map[string]map[string]string),
		tableReplicationConfigs: make(map[string]map[string]any),
		accountID:               accountID,
		region:                  region,
		muBuckets:               lockmetrics.New("s3tables.buckets"),
		muNamespaces:            lockmetrics.New("s3tables.namespaces"),
		muTables:                lockmetrics.New("s3tables.tables"),
		muState:                 lockmetrics.New("s3tables.state"),
	}
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

	b.tableBuckets = make(map[string]*TableBucket)
	b.namespaces = make(map[string]*Namespace)
	b.tables = make(map[string]*Table)
	b.tableIndex = make(map[string]string)
	b.bucketReplication = make(map[string]*BucketReplicationConfig)
	b.tableReplication = make(map[string]bool)
	b.tableRecordExpiry = make(map[string]*TableRecordExpiryConfig)
	b.tags = make(map[string]map[string]string)
	b.tableReplicationConfigs = make(map[string]map[string]any)
}

// PutTableBucketReplication sets replication config for a table bucket.
func (b *InMemoryBackend) PutTableBucketReplication(bucketARN string, cfg *BucketReplicationConfig) error {
	b.muBuckets.RLock("PutTableBucketReplication")
	defer b.muBuckets.RUnlock()

	b.muState.Lock("PutTableBucketReplication")
	defer b.muState.Unlock()

	if _, ok := b.tableBuckets[bucketARN]; !ok {
		return fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	b.bucketReplication[bucketARN] = cfg

	return nil
}

// GetTableBucketReplication returns the replication config for a table bucket.
func (b *InMemoryBackend) GetTableBucketReplication(bucketARN string) (*BucketReplicationConfig, error) {
	b.muBuckets.RLock("GetTableBucketReplication")
	defer b.muBuckets.RUnlock()

	b.muState.RLock("GetTableBucketReplication")
	defer b.muState.RUnlock()

	if _, ok := b.tableBuckets[bucketARN]; !ok {
		return nil, fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	cfg, ok := b.bucketReplication[bucketARN]
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

	if _, ok := b.tableBuckets[bucketARN]; !ok {
		return fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	delete(b.bucketReplication, bucketARN)

	return nil
}

// PutTableReplication marks a table as having replication enabled (legacy boolean form).
func (b *InMemoryBackend) PutTableReplication(tableArn string) error {
	b.muTables.RLock("PutTableReplication")
	defer b.muTables.RUnlock()

	b.muState.Lock("PutTableReplication")
	defer b.muState.Unlock()

	if _, ok := b.tables[tableArn]; !ok {
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

	if _, ok := b.tables[tableArn]; !ok {
		return fmt.Errorf("%w: table %q not found", ErrTableNotFound, tableArn)
	}

	delete(b.tableReplication, tableArn)
	delete(b.tableReplicationConfigs, tableArn)

	return nil
}

// PutTableRecordExpirationConfiguration sets record expiration config for a table.
func (b *InMemoryBackend) PutTableRecordExpirationConfiguration(tableArn string, cfg *TableRecordExpiryConfig) error {
	b.muTables.RLock("PutTableRecordExpirationConfiguration")
	defer b.muTables.RUnlock()

	b.muState.Lock("PutTableRecordExpirationConfiguration")
	defer b.muState.Unlock()

	if _, ok := b.tables[tableArn]; !ok {
		return fmt.Errorf("%w: table %q not found", ErrTableNotFound, tableArn)
	}

	b.tableRecordExpiry[tableArn] = cfg

	return nil
}

// GetTableRecordExpirationConfiguration returns record expiry config for a table, defaulting to DISABLED.
func (b *InMemoryBackend) GetTableRecordExpirationConfiguration(tableArn string) (*TableRecordExpiryConfig, error) {
	b.muTables.RLock("GetTableRecordExpirationConfiguration")
	defer b.muTables.RUnlock()

	b.muState.RLock("GetTableRecordExpirationConfiguration")
	defer b.muState.RUnlock()

	if _, ok := b.tables[tableArn]; !ok {
		return nil, fmt.Errorf("%w: table %q not found", ErrTableNotFound, tableArn)
	}

	cfg, ok := b.tableRecordExpiry[tableArn]
	if !ok {
		return &TableRecordExpiryConfig{Status: "DISABLED"}, nil
	}

	return cfg, nil
}

func namespaceKey(tableBucketARN, namespace string) string {
	return tableBucketARN + "::" + namespace
}

// CreateTableBucket creates a new TableBucket.
func (b *InMemoryBackend) CreateTableBucket(name string) (*TableBucket, error) {
	b.muBuckets.Lock("CreateTableBucket")
	defer b.muBuckets.Unlock()

	bucketARN := b.TableBucketARN(name)
	if _, ok := b.tableBuckets[bucketARN]; ok {
		return nil, fmt.Errorf("%w: table bucket %q already exists", ErrTableBucketAlreadyExists, name)
	}

	tb := &TableBucket{
		ARN:            bucketARN,
		Name:           name,
		OwnerAccountID: b.accountID,
		CreatedAt:      time.Now().UTC(),
		StorageClass:   storageClassStandard,
		MaintenanceConfiguration: map[string]any{
			"icebergUnreferencedFileRemoval": map[string]any{
				keyStatusField: statusEnabled,
				keySettings: map[string]any{
					"icebergUnreferencedFileRemoval": map[string]any{
						"nonCurrentDays":   float64(1),
						"unreferencedDays": float64(3), //nolint:mnd // AWS default: 3 days for unreferenced files
					},
				},
			},
		},
	}
	b.tableBuckets[bucketARN] = tb

	return cloneTableBucket(tb), nil
}

// GetTableBucket returns a TableBucket by ARN.
func (b *InMemoryBackend) GetTableBucket(bucketARN string) (*TableBucket, error) {
	b.muBuckets.RLock("GetTableBucket")
	defer b.muBuckets.RUnlock()

	tb, ok := b.tableBuckets[bucketARN]
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

	if _, ok := b.tableBuckets[bucketARN]; !ok {
		return fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	// Cascade: delete all tables belonging to this bucket.
	for tableARN, t := range b.tables {
		if t.TableBucketARN == bucketARN {
			delete(b.tableIndex, tableCompositeKey(bucketARN, joinNamespace(t.Namespace), t.Name))
			delete(b.tables, tableARN)
			delete(b.tableReplication, tableARN)
			delete(b.tableReplicationConfigs, tableARN)
			delete(b.tableRecordExpiry, tableARN)
			delete(b.tags, tableARN)
		}
	}

	// Cascade: delete all namespaces belonging to this bucket.
	for key, ns := range b.namespaces {
		if ns.TableBucketARN == bucketARN {
			delete(b.namespaces, key)
		}
	}

	delete(b.bucketReplication, bucketARN)
	delete(b.tags, bucketARN)
	delete(b.tableBuckets, bucketARN)

	return nil
}

// ListTableBuckets returns all TableBuckets sorted by name.
func (b *InMemoryBackend) ListTableBuckets() []*TableBucket {
	b.muBuckets.RLock("ListTableBuckets")
	defer b.muBuckets.RUnlock()

	list := make([]*TableBucket, 0, len(b.tableBuckets))

	for _, tb := range b.tableBuckets {
		list = append(list, cloneTableBucket(tb))
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})

	return list
}

// GetTableBucketMaintenanceConfiguration returns the maintenance config for a bucket.
func (b *InMemoryBackend) GetTableBucketMaintenanceConfiguration(bucketARN string) (map[string]any, error) {
	b.muBuckets.RLock("GetTableBucketMaintenanceConfiguration")
	defer b.muBuckets.RUnlock()

	tb, ok := b.tableBuckets[bucketARN]
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

	tb, ok := b.tableBuckets[bucketARN]
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

	tb, ok := b.tableBuckets[bucketARN]
	if !ok {
		return "", fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	if tb.Policy == "" {
		return "", fmt.Errorf("%w: no policy for table bucket %q", ErrTableBucketNotFound, bucketARN)
	}

	return tb.Policy, nil
}

// PutTableBucketPolicy sets the resource policy for a bucket.
func (b *InMemoryBackend) PutTableBucketPolicy(bucketARN, policy string) error {
	b.muBuckets.Lock("PutTableBucketPolicy")
	defer b.muBuckets.Unlock()

	tb, ok := b.tableBuckets[bucketARN]
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

	tb, ok := b.tableBuckets[bucketARN]
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

	tb, ok := b.tableBuckets[bucketARN]
	if !ok {
		return fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	tb.Encryption = cloneAnyMap(config)

	return nil
}

// PutTableBucketMetricsConfiguration enables metrics for a bucket.
func (b *InMemoryBackend) PutTableBucketMetricsConfiguration(bucketARN string) error {
	b.muBuckets.Lock("PutTableBucketMetricsConfiguration")
	defer b.muBuckets.Unlock()

	tb, ok := b.tableBuckets[bucketARN]
	if !ok {
		return fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	tb.MetricsEnabled = true

	return nil
}

// PutTableBucketStorageClass sets storage class for a bucket.
func (b *InMemoryBackend) PutTableBucketStorageClass(bucketARN, storageClass string) error {
	b.muBuckets.Lock("PutTableBucketStorageClass")
	defer b.muBuckets.Unlock()

	tb, ok := b.tableBuckets[bucketARN]
	if !ok {
		return fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	tb.StorageClass = storageClass

	return nil
}

// GetTableStorageClass returns the storage class for a table.
func (b *InMemoryBackend) GetTableStorageClass(bucketARN string, namespace []string, name string) (string, error) {
	b.muTables.RLock("GetTableStorageClass")
	defer b.muTables.RUnlock()

	nsStr := joinNamespace(namespace)
	tableARN, ok := b.tableIndex[tableCompositeKey(bucketARN, nsStr, name)]
	if !ok {
		return "", fmt.Errorf("%w: table %q not found in namespace %s", ErrTableNotFound, name, nsStr)
	}

	t := b.tables[tableARN]
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

	if _, ok := b.tables[tableArn]; !ok {
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

	if _, ok := b.tables[tableArn]; !ok {
		return nil, fmt.Errorf("%w: table %q not found", ErrTableNotFound, tableArn)
	}

	cfg, ok := b.tableReplicationConfigs[tableArn]
	if !ok {
		return nil, fmt.Errorf("%w: no replication configuration for table %q", ErrTableNotFound, tableArn)
	}

	return cloneAnyMap(cfg), nil
}

// ValidateTableExists checks that a table ARN exists in the backend.
func (b *InMemoryBackend) ValidateTableExists(tableArn string) error {
	b.muTables.RLock("ValidateTableExists")
	defer b.muTables.RUnlock()

	if _, ok := b.tables[tableArn]; !ok {
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
func (b *InMemoryBackend) CreateNamespace(tableBucketARN string, namespace []string) (*Namespace, error) {
	b.muBuckets.RLock("CreateNamespace")
	defer b.muBuckets.RUnlock()

	b.muNamespaces.Lock("CreateNamespace")
	defer b.muNamespaces.Unlock()

	if _, ok := b.tableBuckets[tableBucketARN]; !ok {
		return nil, fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, tableBucketARN)
	}

	nsStr := joinNamespace(namespace)
	key := namespaceKey(tableBucketARN, nsStr)

	if _, ok := b.namespaces[key]; ok {
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
	b.namespaces[key] = ns

	return cloneNamespace(ns), nil
}

// GetNamespace returns a namespace by bucket ARN and namespace name.
func (b *InMemoryBackend) GetNamespace(tableBucketARN string, namespace []string) (*Namespace, error) {
	b.muNamespaces.RLock("GetNamespace")
	defer b.muNamespaces.RUnlock()

	nsStr := joinNamespace(namespace)
	key := namespaceKey(tableBucketARN, nsStr)

	ns, ok := b.namespaces[key]
	if !ok {
		return nil, fmt.Errorf("%w: namespace %q not found in bucket %s", ErrNamespaceNotFound, nsStr, tableBucketARN)
	}

	return cloneNamespace(ns), nil
}

// DeleteNamespace deletes a namespace from a table bucket.
func (b *InMemoryBackend) DeleteNamespace(tableBucketARN string, namespace []string) error {
	b.muNamespaces.Lock("DeleteNamespace")
	defer b.muNamespaces.Unlock()

	nsStr := joinNamespace(namespace)
	key := namespaceKey(tableBucketARN, nsStr)

	if _, ok := b.namespaces[key]; !ok {
		return fmt.Errorf("%w: namespace %q not found in bucket %s", ErrNamespaceNotFound, nsStr, tableBucketARN)
	}

	delete(b.namespaces, key)

	return nil
}

// ListNamespaces returns all namespaces in a table bucket sorted by name.
func (b *InMemoryBackend) ListNamespaces(tableBucketARN string) ([]*Namespace, error) {
	b.muBuckets.RLock("ListNamespaces")
	defer b.muBuckets.RUnlock()

	b.muNamespaces.RLock("ListNamespaces")
	defer b.muNamespaces.RUnlock()

	if _, ok := b.tableBuckets[tableBucketARN]; !ok {
		return nil, fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, tableBucketARN)
	}

	list := make([]*Namespace, 0, len(b.namespaces))

	for _, ns := range b.namespaces {
		if ns.TableBucketARN == tableBucketARN {
			list = append(list, cloneNamespace(ns))
		}
	}

	sort.Slice(list, func(i, j int) bool {
		return joinNamespace(list[i].Namespace) < joinNamespace(list[j].Namespace)
	})

	return list, nil
}

// CreateTable creates a new table within a namespace.
func (b *InMemoryBackend) CreateTable(tableBucketARN string, namespace []string, name, format string) (*Table, error) {
	b.muBuckets.RLock("CreateTable")
	defer b.muBuckets.RUnlock()

	b.muNamespaces.RLock("CreateTable")
	defer b.muNamespaces.RUnlock()

	b.muTables.Lock("CreateTable")
	defer b.muTables.Unlock()

	tb, ok := b.tableBuckets[tableBucketARN]
	if !ok {
		return nil, fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, tableBucketARN)
	}

	nsStr := joinNamespace(namespace)
	nsKey := namespaceKey(tableBucketARN, nsStr)

	if _, exists := b.namespaces[nsKey]; !exists {
		return nil, fmt.Errorf("%w: namespace %q not found in bucket %s", ErrNamespaceNotFound, nsStr, tableBucketARN)
	}

	compositeKey := tableCompositeKey(tableBucketARN, nsStr, name)
	if _, exists := b.tableIndex[compositeKey]; exists {
		return nil, fmt.Errorf("%w: table %q already exists in namespace %s", ErrTableAlreadyExists, name, nsStr)
	}

	tableARN := b.TableARN(tb.Name, nsStr, name)

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
		MaintenanceConfiguration: map[string]any{
			"icebergCompaction": map[string]any{
				keyStatusField: statusEnabled,
				keySettings: map[string]any{
					"icebergCompaction": map[string]any{
						"targetFileSizeMB": float64(512), //nolint:mnd // AWS default: 512 MB target file size
						"strategy":         "binpack",
					},
				},
			},
			"icebergSnapshotManagement": map[string]any{
				keyStatusField: statusEnabled,
				keySettings: map[string]any{
					"icebergSnapshotManagement": map[string]any{
						"maxSnapshotAgeHours": float64(120), //nolint:mnd // AWS default: 120 hours (5 days)
						"minSnapshotsToKeep":  float64(1),
					},
				},
			},
		},
	}
	b.tables[tableARN] = table
	b.tableIndex[compositeKey] = tableARN

	return cloneTable(table), nil
}

// GetTable returns a table by bucket ARN, namespace, and name.
func (b *InMemoryBackend) GetTable(tableBucketARN string, namespace []string, name string) (*Table, error) {
	b.muTables.RLock("GetTable")
	defer b.muTables.RUnlock()

	nsStr := joinNamespace(namespace)
	tableARN, ok := b.tableIndex[tableCompositeKey(tableBucketARN, nsStr, name)]
	if !ok {
		return nil, fmt.Errorf("%w: table %q not found in namespace %s", ErrTableNotFound, name, nsStr)
	}

	return cloneTable(b.tables[tableARN]), nil
}

// DeleteTable deletes a table by bucket ARN, namespace, and name.
func (b *InMemoryBackend) DeleteTable(tableBucketARN string, namespace []string, name string) error {
	b.muTables.Lock("DeleteTable")
	defer b.muTables.Unlock()

	nsStr := joinNamespace(namespace)
	compositeKey := tableCompositeKey(tableBucketARN, nsStr, name)

	tableARN, ok := b.tableIndex[compositeKey]
	if !ok {
		return fmt.Errorf("%w: table %q not found in namespace %s", ErrTableNotFound, name, nsStr)
	}

	delete(b.tables, tableARN)
	delete(b.tableIndex, compositeKey)

	return nil
}

// ListTables returns all tables in a table bucket, optionally filtered by namespace.
func (b *InMemoryBackend) ListTables(tableBucketARN, namespace string) ([]*Table, error) {
	b.muBuckets.RLock("ListTables")
	defer b.muBuckets.RUnlock()

	b.muTables.RLock("ListTables")
	defer b.muTables.RUnlock()

	if _, ok := b.tableBuckets[tableBucketARN]; !ok {
		return nil, fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, tableBucketARN)
	}

	list := make([]*Table, 0, len(b.tables))

	for _, t := range b.tables {
		if t.TableBucketARN != tableBucketARN {
			continue
		}

		if namespace != "" && joinNamespace(t.Namespace) != namespace {
			continue
		}

		list = append(list, cloneTable(t))
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})

	return list, nil
}

// RenameTable renames a table or moves it to a different namespace.
func (b *InMemoryBackend) RenameTable(
	tableBucketARN string,
	namespace []string,
	name, newNamespace, newName string,
) error {
	b.muBuckets.RLock("RenameTable")
	defer b.muBuckets.RUnlock()

	b.muTables.Lock("RenameTable")
	defer b.muTables.Unlock()

	nsStr := joinNamespace(namespace)
	compositeKey := tableCompositeKey(tableBucketARN, nsStr, name)

	oldARN, ok := b.tableIndex[compositeKey]
	if !ok {
		return fmt.Errorf("%w: table %q not found in namespace %s", ErrTableNotFound, name, nsStr)
	}

	found := b.tables[oldARN]

	if newName == "" {
		newName = name
	}

	if newNamespace == "" {
		newNamespace = nsStr
	}

	tb := b.tableBuckets[tableBucketARN]
	newARN := b.TableARN(tb.Name, newNamespace, newName)

	newCompositeKey := tableCompositeKey(tableBucketARN, newNamespace, newName)
	if _, exists := b.tableIndex[newCompositeKey]; exists {
		return fmt.Errorf("%w: table %q already exists in namespace %s", ErrTableAlreadyExists, newName, newNamespace)
	}

	found.Name = newName
	found.Namespace = splitNamespace(newNamespace)
	found.ARN = newARN
	found.ModifiedAt = time.Now().UTC()
	found.VersionToken = uuid.NewString()

	delete(b.tables, oldARN)
	delete(b.tableIndex, compositeKey)
	b.tables[newARN] = found
	b.tableIndex[newCompositeKey] = newARN

	return nil
}

// UpdateTableMetadataLocation updates the metadata location of a table.
func (b *InMemoryBackend) UpdateTableMetadataLocation(
	tableBucketARN string,
	namespace []string,
	name, metadataLocation, _ string,
) (*Table, error) {
	b.muTables.Lock("UpdateTableMetadataLocation")
	defer b.muTables.Unlock()

	nsStr := joinNamespace(namespace)
	tableARN, ok := b.tableIndex[tableCompositeKey(tableBucketARN, nsStr, name)]
	if !ok {
		return nil, fmt.Errorf("%w: table %q not found in namespace %s", ErrTableNotFound, name, nsStr)
	}

	t := b.tables[tableARN]
	t.MetadataLocation = metadataLocation
	t.VersionToken = uuid.NewString()
	t.ModifiedAt = time.Now().UTC()

	return cloneTable(t), nil
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
	tableARN, ok := b.tableIndex[tableCompositeKey(tableBucketARN, nsStr, name)]
	if !ok {
		return nil, "", fmt.Errorf("%w: table %q not found in namespace %s", ErrTableNotFound, name, nsStr)
	}

	t := b.tables[tableARN]

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
	tableARN, ok := b.tableIndex[tableCompositeKey(tableBucketARN, nsStr, name)]
	if !ok {
		return fmt.Errorf("%w: table %q not found in namespace %s", ErrTableNotFound, name, nsStr)
	}

	t := b.tables[tableARN]
	if t.MaintenanceConfiguration == nil {
		t.MaintenanceConfiguration = make(map[string]any)
	}

	t.MaintenanceConfiguration[maintenanceType] = value

	return nil
}

// GetTablePolicy returns the resource policy for a table.
func (b *InMemoryBackend) GetTablePolicy(tableBucketARN string, namespace []string, name string) (string, error) {
	b.muTables.RLock("GetTablePolicy")
	defer b.muTables.RUnlock()

	nsStr := joinNamespace(namespace)
	tableARN, ok := b.tableIndex[tableCompositeKey(tableBucketARN, nsStr, name)]
	if !ok {
		return "", fmt.Errorf("%w: table %q not found in namespace %s", ErrTableNotFound, name, nsStr)
	}

	t := b.tables[tableARN]
	if t.Policy == "" {
		return "", fmt.Errorf("%w: no policy for table %q", ErrTableNotFound, name)
	}

	return t.Policy, nil
}

// PutTablePolicy sets the resource policy for a table.
func (b *InMemoryBackend) PutTablePolicy(tableBucketARN string, namespace []string, name, policy string) error {
	b.muTables.Lock("PutTablePolicy")
	defer b.muTables.Unlock()

	nsStr := joinNamespace(namespace)
	tableARN, ok := b.tableIndex[tableCompositeKey(tableBucketARN, nsStr, name)]
	if !ok {
		return fmt.Errorf("%w: table %q not found in namespace %s", ErrTableNotFound, name, nsStr)
	}

	b.tables[tableARN].Policy = policy

	return nil
}

// DeleteTablePolicy removes the resource policy from a table.
func (b *InMemoryBackend) DeleteTablePolicy(tableBucketARN string, namespace []string, name string) error {
	b.muTables.Lock("DeleteTablePolicy")
	defer b.muTables.Unlock()

	nsStr := joinNamespace(namespace)
	tableARN, ok := b.tableIndex[tableCompositeKey(tableBucketARN, nsStr, name)]
	if !ok {
		return fmt.Errorf("%w: table %q not found in namespace %s", ErrTableNotFound, name, nsStr)
	}

	b.tables[tableARN].Policy = ""

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
