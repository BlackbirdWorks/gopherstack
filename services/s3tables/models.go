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
	// BucketID is the system-assigned unique identifier AWS returns as
	// "tableBucketId" on GetTableBucket/ListTableBuckets and, transitively,
	// on every Namespace/Table nested under this bucket (see
	// Namespace.TableBucketID/Table.TableBucketID) -- distinct from ARN,
	// confirmed via GetTableBucketOutput.TableBucketId in
	// aws-sdk-go-v2/service/s3tables (gopherstack-wla0).
	BucketID string `json:"bucketID"`
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
	// TableBucketID is the owning bucket's system-assigned ID (TableBucket.BucketID
	// at CreateNamespace time), the real GetNamespaceOutput/NamespaceSummary
	// "tableBucketId" member -- GetNamespaceOutput has no tableBucketARN
	// member at all (gopherstack-wla0).
	TableBucketID string   `json:"tableBucketID"`
	Namespace     []string `json:"namespace"`
}

// ReplicationDestination is a single replication destination, mirroring
// types.ReplicationDestination in aws-sdk-go-v2/service/s3tables. The wire
// field is destinationTableBucketARN -- NOT destinationBucketARN, which was
// a gopherstack-invented field name never present on the real shape
// (confirmed via deserializeDocumentReplicationDestination in
// aws-sdk-go-v2/service/s3tables's deserializers.go).
type ReplicationDestination struct {
	DestinationTableBucketARN string `json:"destinationTableBucketARN"`
}

// ReplicationRule is a single replication rule (a list of destinations).
// TableBucketReplicationConfiguration and TableReplicationConfiguration
// both nest their destinations one level deeper than gopherstack previously
// modeled -- rules: [{destinations: [...]}], not a flat destinations array
// -- confirmed via deserializeDocumentTableBucketReplicationRule /
// deserializeDocumentTableReplicationRule in the real SDK's deserializers.go.
type ReplicationRule struct {
	Destinations []ReplicationDestination `json:"destinations"`
}

// BucketReplicationConfig holds replication configuration for a table
// bucket, mirroring types.TableBucketReplicationConfiguration (role + rules)
// plus a VersionToken for optimistic concurrency: PutTableBucketReplication
// and DeleteTableBucketReplication both accept an optional versionToken
// query parameter that, when supplied, must match the stored token
// (GetTableBucketReplicationOutput.VersionToken).
type BucketReplicationConfig struct {
	TableBucketARN string            `json:"-"`
	Role           string            `json:"role"`
	VersionToken   string            `json:"versionToken"`
	Rules          []ReplicationRule `json:"rules"`
}

// TableReplicationConfig holds replication configuration for an individual
// table, mirroring types.TableReplicationConfiguration (role + rules) plus a
// VersionToken for optimistic concurrency: PutTableReplication accepts an
// optional versionToken query parameter and DeleteTableReplication requires
// one; both must match the stored token when supplied
// (GetTableReplicationOutput.VersionToken).
type TableReplicationConfig struct {
	TableARN     string            `json:"-"`
	Role         string            `json:"role"`
	VersionToken string            `json:"versionToken"`
	Rules        []ReplicationRule `json:"rules"`
}

// TableRecordExpiryConfig holds record expiration configuration for a
// table, mirroring types.TableRecordExpirationConfigurationValue (status +
// settings.days) in aws-sdk-go-v2/service/s3tables. Status is the real
// smithy enum's lowercase wire value ("enabled"/"disabled" --
// types.TableRecordExpirationStatus -- NOT "ENABLED"/"DISABLED", which does
// not match any TableRecordExpirationStatus.Values() entry).
type TableRecordExpiryConfig struct {
	Status   string `json:"status"`
	TableARN string `json:"-"`
	Days     int    `json:"days,omitempty"`
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
	Encryption     map[string]any `json:"encryption"`
	TableBucketARN string         `json:"tableBucketARN"`
	// TableBucketID is the owning bucket's system-assigned ID (TableBucket.BucketID
	// at CreateTable time), the real GetTableOutput/TableSummary "tableBucketId"
	// member -- neither shape has a tableBucketARN member (gopherstack-wla0).
	TableBucketID     string   `json:"tableBucketID"`
	Format            string   `json:"format"`
	VersionToken      string   `json:"versionToken"`
	MetadataLocation  string   `json:"metadataLocation"`
	WarehouseLocation string   `json:"warehouseLocation"`
	ARN               string   `json:"arn"`
	OwnerAccountID    string   `json:"ownerAccountID"`
	Policy            string   `json:"policy"`
	Name              string   `json:"name"`
	StorageClass      string   `json:"storageClass"`
	Namespace         []string `json:"namespace"`
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
