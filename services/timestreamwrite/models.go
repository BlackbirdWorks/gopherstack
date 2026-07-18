package timestreamwrite

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// RetentionProperties holds the memory and magnetic store retention durations.
type RetentionProperties struct {
	MemoryStoreRetentionPeriodInHours  int64 `json:"MemoryStoreRetentionPeriodInHours,omitempty"`
	MagneticStoreRetentionPeriodInDays int64 `json:"MagneticStoreRetentionPeriodInDays,omitempty"`
}

// S3Configuration holds S3 location config for rejected-record delivery.
type S3Configuration struct {
	BucketName       string `json:"bucket_name,omitempty"`
	ObjectKeyPrefix  string `json:"object_key_prefix,omitempty"`
	EncryptionOption string `json:"encryption_option,omitempty"`
	KmsKeyID         string `json:"kms_key_id,omitempty"`
}

// MagneticStoreRejectedDataLocation configures where rejected magnetic-store records are written.
type MagneticStoreRejectedDataLocation struct {
	S3Configuration *S3Configuration `json:"s3_configuration,omitempty"`
}

// MagneticStoreWriteProperties configures magnetic store writes and rejected-record delivery.
type MagneticStoreWriteProperties struct {
	MagneticStoreRejectedDataLocation *MagneticStoreRejectedDataLocation `json:"magnetic_store_rejected_data_location,omitempty"` //nolint:lll // AWS field name is inherently long
	EnableMagneticStoreWrites         bool                               `json:"enable_magnetic_store_writes"`
}

// PartitionKeyType specifies whether a partition key is a dimension or measure key.
type PartitionKeyType = string

const (
	PartitionKeyTypeDimension PartitionKeyType = "DIMENSION"
	PartitionKeyTypeMeasure   PartitionKeyType = "MEASURE"
)

// PartitionKeyEnforcementLevel controls whether a dimension partition key is required on write.
type PartitionKeyEnforcementLevel = string

const (
	PartitionKeyEnforcementRequired PartitionKeyEnforcementLevel = "REQUIRED"
	PartitionKeyEnforcementOptional PartitionKeyEnforcementLevel = "OPTIONAL"
)

// PartitionKey defines a single key in a table's composite partition key schema.
type PartitionKey struct {
	Type                PartitionKeyType             `json:"type"`
	Name                string                       `json:"name,omitempty"`
	EnforcementInRecord PartitionKeyEnforcementLevel `json:"enforcement_in_record,omitempty"`
}

// Schema defines the composite partition key configuration for a table.
type Schema struct {
	CompositePartitionKey []PartitionKey `json:"composite_partition_key,omitempty"`
}

// MeasureValue holds a name-value-type triple for multi-measure (MULTI type) records.
type MeasureValue struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	Type  string `json:"type"`
}

// BatchLoadProgressReport captures incremental progress metrics for a batch load task.
type BatchLoadProgressReport struct {
	BytesMetered            int64 `json:"bytes_metered,omitempty"`
	FileFailures            int64 `json:"file_failures,omitempty"`
	ParseFailures           int64 `json:"parse_failures,omitempty"`
	RecordIngestionFailures int64 `json:"record_ingestion_failures,omitempty"`
	RecordsIngested         int64 `json:"records_ingested,omitempty"`
	RecordsProcessed        int64 `json:"records_processed,omitempty"`
}

// Database represents a Timestream database.
type Database struct {
	CreationTime    time.Time `json:"creation_time"`
	LastUpdatedTime time.Time `json:"last_updated_time"`
	DatabaseName    string    `json:"database_name"`
	ARN             string    `json:"arn"`
	KmsKeyID        string    `json:"kms_key_id,omitempty"`
	TableCount      int       `json:"table_count"`
}

// Table represents a Timestream table within a database.
type Table struct {
	CreationTime                 time.Time                     `json:"creation_time"`
	LastUpdatedTime              time.Time                     `json:"last_updated_time"`
	RetentionProperties          *RetentionProperties          `json:"retention_properties,omitempty"`
	MagneticStoreWriteProperties *MagneticStoreWriteProperties `json:"magnetic_store_write_properties,omitempty"`
	Schema                       *Schema                       `json:"schema,omitempty"`
	DatabaseName                 string                        `json:"database_name"`
	TableName                    string                        `json:"table_name"`
	ARN                          string                        `json:"arn"`
	TableStatus                  string                        `json:"table_status"`
}

// Dimension holds a name/value pair for a time-series record.
type Dimension struct {
	Name               string `json:"name"`
	Value              string `json:"value"`
	DimensionValueType string `json:"dimension_value_type,omitempty"`
}

// Record represents a time-series data point written to a table.
type Record struct {
	// InternalTimestamp is the parsed value of Time, used for retention sweeping.
	InternalTimestamp time.Time      `json:"-"`
	MeasureName       string         `json:"measure_name"`
	MeasureValue      string         `json:"measure_value"`
	MeasureValueType  string         `json:"measure_value_type"`
	Time              string         `json:"time"`
	TimeUnit          string         `json:"time_unit"`
	Dimensions        []Dimension    `json:"dimensions,omitempty"`
	MeasureValues     []MeasureValue `json:"measure_values,omitempty"`
	Version           int64          `json:"version,omitempty"`
}

// DataSourceS3Configuration holds S3 source configuration for batch loads.
type DataSourceS3Configuration struct {
	BucketName      string `json:"BucketName"`
	ObjectKeyPrefix string `json:"ObjectKeyPrefix,omitempty"`
	DataFormat      string `json:"DataFormat,omitempty"`
}

// DataSourceConfiguration holds the data source for a batch load task.
type DataSourceConfiguration struct {
	DataSourceS3Configuration *DataSourceS3Configuration `json:"DataSourceS3Configuration,omitempty"`
	DataFormat                string                     `json:"DataFormat,omitempty"`
}

// ReportConfiguration holds the report output configuration for a batch load task.
type ReportConfiguration struct {
	ReportS3Configuration *DataSourceS3Configuration `json:"ReportS3Configuration,omitempty"`
}

// BatchLoadTask represents a Timestream batch load task.
type BatchLoadTask struct {
	CreationTime            time.Time                `json:"creation_time"`
	LastUpdatedTime         time.Time                `json:"last_updated_time"`
	ResumableUntil          *time.Time               `json:"resumable_until,omitempty"`
	DataSourceConfiguration *DataSourceConfiguration `json:"data_source_configuration,omitempty"`
	ReportConfiguration     *ReportConfiguration     `json:"report_configuration,omitempty"`
	ProgressReport          *BatchLoadProgressReport `json:"progress_report,omitempty"`
	TargetDatabaseName      string                   `json:"target_database_name"`
	TargetTableName         string                   `json:"target_table_name"`
	TaskID                  string                   `json:"task_id"`
	TaskStatus              string                   `json:"task_status"`
	ErrorMessage            string                   `json:"error_message,omitempty"`
	RecordVersion           int64                    `json:"record_version,omitempty"`
}

// tableRecords owns the records slice, dedup index, and per-table mutex for a single table.
// Storing the slice and lock together inside a pointer lets WriteRecords mutate
// the slice through the pointer without writing to the enclosing maps in
// b.records — which would race against concurrent WriteRecords calls to other
// tables in the same database under the global read-lock.
//
// recordIndex maps a record's dedup key (measure+time+dimensions) to its position in
// the records slice, enabling O(1) version-based upsert lookups.
type tableRecords struct {
	mu          *lockmetrics.RWMutex
	recordIndex map[string]int
	records     []Record
}

// InMemoryBackend is the in-memory store for Timestream Write resources.
type InMemoryBackend struct {
	// databases and tables are store.Table-backed (see store_setup.go); tables
	// is keyed by the composite "databaseName|tableName" string (tableKey),
	// with tablesByDatabase grouping entries by database for the per-database
	// scans (ListTables, DeleteDatabase, isKnownARNLocked, SweepRetention) the
	// old nested map[dbName]map[tblName]*Table used to answer directly.
	databases        *store.Table[Database]
	tables           *store.Table[Table]
	tablesByDatabase *store.Index[Table]
	// records is keyed dbName -> tblName -> *tableRecords. Each *tableRecords
	// carries its own RWMutex so WriteRecords mutates the slice via the pointer
	// (never the map), allowing parallel writes to different tables under a
	// global read-lock without racing on inner maps. Deliberately left as a
	// plain nested map, not store.Table-backed -- see store_setup.go's file
	// doc comment for why.
	records map[string]map[string]*tableRecords
	// tags is deliberately left as a plain nested map, not store.Table-backed
	// -- see store_setup.go's file doc comment for why.
	tags           map[string]map[string]string
	batchLoadTasks *store.Table[BatchLoadTask]
	registry       *store.Registry
	mu             *lockmetrics.RWMutex
	nextTaskID     int
}

// CreateTableInput holds the parameters for creating a table.
type CreateTableInput struct {
	RetentionProperties          *RetentionProperties
	MagneticStoreWriteProperties *MagneticStoreWriteProperties
	Schema                       *Schema
}

// UpdateTableInput holds the parameters for updating a table.
type UpdateTableInput struct {
	RetentionProperties          *RetentionProperties
	MagneticStoreWriteProperties *MagneticStoreWriteProperties
	Schema                       *Schema
}

// WriteRecordsOutput summarises the results of a WriteRecords call.
type WriteRecordsOutput struct {
	RejectedRecords []RejectedRecord
	// Total is the total number of records successfully ingested.
	Total int32
	// MemoryStore is the count of records written to the memory store
	// (records whose timestamp falls within the memory retention window).
	MemoryStore int32
	// MagneticStore is the count of records written to the magnetic store
	// (records whose timestamp is outside the memory retention window and the
	// table has magnetic store writes enabled).
	MagneticStore int32
}
