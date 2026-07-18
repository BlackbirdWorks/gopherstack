package s3tables

import "time"

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

// ListTableBucketsParams holds the filter and pagination inputs for
// ListTableBuckets, mirroring ListTableBucketsInput's prefix/type/
// continuationToken/maxBuckets fields.
type ListTableBucketsParams struct {
	Prefix            string
	Type              string
	ContinuationToken string
	MaxBuckets        int
}

// ListNamespacesParams holds the filter and pagination inputs for
// ListNamespaces, mirroring ListNamespacesInput's prefix/continuationToken/
// maxNamespaces fields.
type ListNamespacesParams struct {
	Prefix            string
	ContinuationToken string
	MaxNamespaces     int
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
