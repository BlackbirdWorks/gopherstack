package s3tables

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// PutTableBucketReplication sets the replication configuration for a table
// bucket. When expectedVersionToken is non-empty it must match the bucket's
// currently stored VersionToken (optimistic concurrency, mirroring
// PutTableBucketReplicationInput's optional versionToken query parameter);
// a mismatch, or a token supplied when no configuration yet exists, returns
// ErrTableVersionConflict. Returns the stored config (with a freshly minted
// VersionToken) so the caller can build the {status, versionToken} response
// PutTableBucketReplicationOutput requires.
func (b *InMemoryBackend) PutTableBucketReplication(
	bucketARN, role string,
	rules []ReplicationRule,
	expectedVersionToken string,
) (*BucketReplicationConfig, error) {
	b.muBuckets.RLock("PutTableBucketReplication")
	defer b.muBuckets.RUnlock()

	b.muState.Lock("PutTableBucketReplication")
	defer b.muState.Unlock()

	if !b.tableBuckets.Has(bucketARN) {
		return nil, fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	existing, hasExisting := b.bucketReplication.Get(bucketARN)
	if expectedVersionToken != "" && (!hasExisting || existing.VersionToken != expectedVersionToken) {
		return nil, fmt.Errorf(
			"%w: stale version token for table bucket replication %q",
			ErrTableVersionConflict,
			bucketARN,
		)
	}

	cfg := &BucketReplicationConfig{
		TableBucketARN: bucketARN,
		Role:           role,
		Rules:          rules,
		VersionToken:   uuid.NewString(),
	}
	b.bucketReplication.Put(cfg)

	return cfg, nil
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

// DeleteTableBucketReplication removes the replication config for a table
// bucket. When expectedVersionToken is non-empty it must match the stored
// VersionToken (mirroring DeleteTableBucketReplicationInput's optional
// versionToken query parameter); a mismatch returns ErrTableVersionConflict.
func (b *InMemoryBackend) DeleteTableBucketReplication(bucketARN, expectedVersionToken string) error {
	b.muBuckets.RLock("DeleteTableBucketReplication")
	defer b.muBuckets.RUnlock()

	b.muState.Lock("DeleteTableBucketReplication")
	defer b.muState.Unlock()

	if !b.tableBuckets.Has(bucketARN) {
		return fmt.Errorf("%w: table bucket %q not found", ErrTableBucketNotFound, bucketARN)
	}

	existing, hasExisting := b.bucketReplication.Get(bucketARN)
	if !hasExisting {
		return fmt.Errorf(
			"%w: no replication configuration for table bucket %q",
			ErrTableBucketNotFound,
			bucketARN,
		)
	}

	if expectedVersionToken != "" && existing.VersionToken != expectedVersionToken {
		return fmt.Errorf(
			"%w: stale version token for table bucket replication %q",
			ErrTableVersionConflict,
			bucketARN,
		)
	}

	b.bucketReplication.Delete(bucketARN)

	return nil
}

// CreateTableBucket creates a new TableBucket.
func (b *InMemoryBackend) CreateTableBucket(
	name string,
	opts CreateTableBucketOptions,
) (*TableBucket, error) {
	if err := validateBucketName(name); err != nil {
		return nil, err
	}

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
		b.tableReplication.Delete(t.ARN)
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

func cloneTableBucket(tb *TableBucket) *TableBucket {
	cp := *tb
	cp.MaintenanceConfiguration = cloneAnyMap(tb.MaintenanceConfiguration)
	cp.Encryption = cloneAnyMap(tb.Encryption)

	return &cp
}
