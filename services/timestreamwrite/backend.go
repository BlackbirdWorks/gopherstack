package timestreamwrite

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

var (
	// ErrDatabaseNotFound is returned when the requested database does not exist.
	ErrDatabaseNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrTableNotFound is returned when the requested table does not exist.
	ErrTableNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrDatabaseAlreadyExists is returned when a database with the same name already exists.
	ErrDatabaseAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrTableAlreadyExists is returned when a table with the same name already exists.
	ErrTableAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrBatchLoadTaskNotFound is returned when the requested batch load task does not exist.
	ErrBatchLoadTaskNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrInvalidBatchLoadStatus is returned when a task cannot be resumed from its current status.
	ErrInvalidBatchLoadStatus = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrValidation is returned for invalid request parameters.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrResourceNotFound is returned when tagging an ARN that is not registered in the backend.
	ErrResourceNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
)

// RejectedRecord describes a single record that could not be written due to a version conflict.
type RejectedRecord struct {
	Reason          string `json:"Reason"`
	ExistingVersion int64  `json:"ExistingVersion,omitempty"`
	RecordIndex     int    `json:"RecordIndex"`
}

// RejectedRecordsError is returned by WriteRecords when one or more records are
// rejected due to version conflicts.
type RejectedRecordsError struct {
	RejectedRecords []RejectedRecord
}

func (e *RejectedRecordsError) Error() string {
	return fmt.Sprintf(
		"RejectedRecordsException: %d record(s) rejected due to version conflict",
		len(e.RejectedRecords),
	)
}

// Is satisfies errors.Is so that errors.Is(err, ErrRejectedRecords) returns true.
func (e *RejectedRecordsError) Is(target error) bool {
	_, ok := target.(*RejectedRecordsError)

	return ok
}

// ErrRejectedRecords is the sentinel used with errors.Is for RejectedRecordsError.
var ErrRejectedRecords = &RejectedRecordsError{}

const (
	// BatchLoadStatusCreated indicates a task has been created and is pending execution.
	BatchLoadStatusCreated = "CREATED"
	// BatchLoadStatusInProgress indicates a task is currently loading data.
	BatchLoadStatusInProgress = "IN_PROGRESS"
	// BatchLoadStatusFailed indicates a task has failed.
	BatchLoadStatusFailed = "FAILED"
	// BatchLoadStatusSucceeded indicates a task completed successfully.
	BatchLoadStatusSucceeded = "SUCCEEDED"
	// BatchLoadStatusProgressStopped indicates a task was stopped before completion.
	BatchLoadStatusProgressStopped = "PROGRESS_STOPPED"
	// BatchLoadStatusPendingResume indicates a task is pending a resume operation.
	BatchLoadStatusPendingResume = "PENDING_RESUME"

	// tableStatusActive is the normal operational state for a table.
	tableStatusActive = "ACTIVE"

	// scheduledQueryARNFragment identifies scheduled-query ARNs from the Timestream
	// Query service.  These are accepted by TagResource so that the unified write-service
	// tag store can hold tags for both resource types.
	scheduledQueryARNFragment = "scheduled-query/"

	// defaultMemoryRetentionHours is the AWS default for MemoryStoreRetentionPeriodInHours
	// when no retention properties are specified at table creation time.
	defaultMemoryRetentionHours = int64(6)
	// defaultMagneticRetentionDays is the AWS default for MagneticStoreRetentionPeriodInDays
	// when no retention properties are specified at table creation time.
	defaultMagneticRetentionDays = int64(73)
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
	databases map[string]*Database
	tables    map[string]map[string]*Table
	// records is keyed dbName -> tblName -> *tableRecords. Each *tableRecords
	// carries its own RWMutex so WriteRecords mutates the slice via the pointer
	// (never the map), allowing parallel writes to different tables under a
	// global read-lock without racing on inner maps.
	records        map[string]map[string]*tableRecords
	tags           map[string]map[string]string
	batchLoadTasks map[string]*BatchLoadTask
	mu             *lockmetrics.RWMutex
	nextTaskID     int
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{mu: lockmetrics.New("timestreamwrite")}
	b.ensureNonNilMaps()

	return b
}

// Reset clears all stored state, returning the backend to its initial empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.closeAllTableMutexesLocked()
	b.nextTaskID = 0
	b.ensureNonNilMapsLocked()
}

// closeAllTableMutexesLocked closes every per-table lockmetrics.RWMutex held in
// b.records. Must be called with b.mu held in write mode before any operation
// that abandons the records map (Reset, Restore) or before the receiver is
// discarded; otherwise the mutexes leak in lockmetrics' liveCollector registry.
func (b *InMemoryBackend) closeAllTableMutexesLocked() {
	for _, dbSlots := range b.records {
		for _, slot := range dbSlots {
			if slot != nil && slot.mu != nil {
				slot.mu.Close()
			}
		}
	}
}

// AccountID returns the simulated AWS account ID.
func (b *InMemoryBackend) AccountID() string { return config.DefaultAccountID }

// Region returns the simulated AWS region.
func (b *InMemoryBackend) Region() string { return config.DefaultRegion }

// ensureNonNilMaps initialises all maps (called without lock held during construction or restore).
func (b *InMemoryBackend) ensureNonNilMaps() {
	b.databases = make(map[string]*Database)
	b.tables = make(map[string]map[string]*Table)
	b.records = make(map[string]map[string]*tableRecords)
	b.tags = make(map[string]map[string]string)
	b.batchLoadTasks = make(map[string]*BatchLoadTask)
}

// ensureNonNilMapsLocked initialises all maps when the lock is already held.
func (b *InMemoryBackend) ensureNonNilMapsLocked() {
	b.databases = make(map[string]*Database)
	b.tables = make(map[string]map[string]*Table)
	b.records = make(map[string]map[string]*tableRecords)
	b.tags = make(map[string]map[string]string)
	b.batchLoadTasks = make(map[string]*BatchLoadTask)
}

func databaseARN(name string) string {
	return arn.Build("timestream", config.DefaultRegion, config.DefaultAccountID, fmt.Sprintf("database/%s", name))
}

func tableARN(dbName, tblName string) string {
	return fmt.Sprintf(
		"arn:aws:timestream:%s:%s:database/%s/table/%s",
		config.DefaultRegion,
		config.DefaultAccountID,
		dbName,
		tblName,
	)
}

// isKnownARN reports whether the ARN is registered in the backend (database, table,
// or batch-load-task ARNs derived from the resource maps) or belongs to an external
// Timestream resource type (e.g. scheduled-query) that shares the same API endpoint.
// Must be called with at least a read-lock held.
func (b *InMemoryBackend) isKnownARNLocked(arn string) bool {
	// Accept scheduled-query ARNs from the Timestream Query service which shares
	// the same TagResource endpoint.
	if strings.Contains(arn, scheduledQueryARNFragment) {
		return true
	}

	// Check against database ARNs.
	for _, db := range b.databases {
		if db.ARN == arn {
			return true
		}

		// Check against table ARNs.
		for _, tbl := range b.tables[db.DatabaseName] {
			if tbl.ARN == arn {
				return true
			}
		}
	}

	return false
}

// CreateDatabase creates a new Timestream database with optional initial tags.
func (b *InMemoryBackend) CreateDatabase(name string, tags map[string]string) (*Database, error) {
	b.mu.Lock("CreateDatabase")
	defer b.mu.Unlock()

	if _, exists := b.databases[name]; exists {
		return nil, fmt.Errorf("%w: database %s already exists", ErrDatabaseAlreadyExists, name)
	}

	now := time.Now()
	db := &Database{
		DatabaseName:    name,
		ARN:             databaseARN(name),
		TableCount:      0,
		CreationTime:    now,
		LastUpdatedTime: now,
	}
	b.databases[name] = db
	b.tables[name] = make(map[string]*Table)
	b.records[name] = make(map[string]*tableRecords)

	if len(tags) > 0 {
		b.tags[db.ARN] = make(map[string]string, len(tags))
		maps.Copy(b.tags[db.ARN], tags)
	}

	cp := *db

	return &cp, nil
}

// DescribeDatabase returns information about a database.
func (b *InMemoryBackend) DescribeDatabase(name string) (*Database, error) {
	b.mu.RLock("DescribeDatabase")
	defer b.mu.RUnlock()

	db, ok := b.databases[name]
	if !ok {
		return nil, fmt.Errorf("%w: database %s not found", ErrDatabaseNotFound, name)
	}

	cp := *db

	return &cp, nil
}

// ListDatabases returns all databases sorted by name.
func (b *InMemoryBackend) ListDatabases() []Database {
	b.mu.RLock("ListDatabases")
	defer b.mu.RUnlock()

	out := make([]Database, 0, len(b.databases))
	for _, db := range b.databases {
		cp := *db
		out = append(out, cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].DatabaseName < out[j].DatabaseName
	})

	return out
}

// DeleteDatabase deletes a database and all its tables.
func (b *InMemoryBackend) DeleteDatabase(name string) error {
	b.mu.Lock("DeleteDatabase")
	defer b.mu.Unlock()

	if _, ok := b.databases[name]; !ok {
		return fmt.Errorf("%w: database %s not found", ErrDatabaseNotFound, name)
	}

	// Clean up tags and per-table mutexes for all tables in this database
	// before dropping the records map so lockmetrics doesn't leak handles.
	for tblName := range b.tables[name] {
		delete(b.tags, tableARN(name, tblName))
	}

	for _, slot := range b.records[name] {
		if slot != nil && slot.mu != nil {
			slot.mu.Close()
		}
	}

	delete(b.databases, name)
	delete(b.tables, name)
	delete(b.records, name)
	delete(b.tags, databaseARN(name))

	return nil
}

// UpdateDatabase updates the KMS key for a database.
func (b *InMemoryBackend) UpdateDatabase(name, kmsKeyID string) (*Database, error) {
	b.mu.Lock("UpdateDatabase")
	defer b.mu.Unlock()

	db, ok := b.databases[name]
	if !ok {
		return nil, fmt.Errorf("%w: database %s not found", ErrDatabaseNotFound, name)
	}

	db.KmsKeyID = kmsKeyID
	db.LastUpdatedTime = time.Now()
	cp := *db

	return &cp, nil
}

// CreateTableInput holds the parameters for creating a table.
type CreateTableInput struct {
	RetentionProperties          *RetentionProperties
	MagneticStoreWriteProperties *MagneticStoreWriteProperties
	Schema                       *Schema
}

// CreateTable creates a new table in the specified database with optional initial tags.
func (b *InMemoryBackend) CreateTable(
	dbName, tblName string,
	tags map[string]string,
	inp *CreateTableInput,
) (*Table, error) {
	b.mu.Lock("CreateTable")
	defer b.mu.Unlock()

	if _, ok := b.databases[dbName]; !ok {
		return nil, fmt.Errorf("%w: database %s not found", ErrDatabaseNotFound, dbName)
	}

	if _, exists := b.tables[dbName][tblName]; exists {
		return nil, fmt.Errorf("%w: table %s already exists", ErrTableAlreadyExists, tblName)
	}

	now := time.Now()
	tbl := &Table{
		DatabaseName:    dbName,
		TableName:       tblName,
		ARN:             tableARN(dbName, tblName),
		TableStatus:     tableStatusActive,
		CreationTime:    now,
		LastUpdatedTime: now,
	}

	if inp != nil {
		tbl.RetentionProperties = inp.RetentionProperties
		tbl.MagneticStoreWriteProperties = inp.MagneticStoreWriteProperties
		tbl.Schema = inp.Schema
	}

	if tbl.RetentionProperties == nil {
		tbl.RetentionProperties = &RetentionProperties{
			MemoryStoreRetentionPeriodInHours:  defaultMemoryRetentionHours,
			MagneticStoreRetentionPeriodInDays: defaultMagneticRetentionDays,
		}
	}

	b.tables[dbName][tblName] = tbl
	b.records[dbName][tblName] = &tableRecords{
		mu:          lockmetrics.New("timestreamwrite.table"),
		recordIndex: make(map[string]int),
	}
	b.databases[dbName].TableCount++

	if len(tags) > 0 {
		b.tags[tbl.ARN] = make(map[string]string, len(tags))
		maps.Copy(b.tags[tbl.ARN], tags)
	}

	cp := *tbl

	return &cp, nil
}

// DescribeTable returns information about a table.
func (b *InMemoryBackend) DescribeTable(dbName, tblName string) (*Table, error) {
	b.mu.RLock("DescribeTable")
	defer b.mu.RUnlock()

	if _, ok := b.databases[dbName]; !ok {
		return nil, fmt.Errorf("%w: database %s not found", ErrDatabaseNotFound, dbName)
	}

	tbl, ok := b.tables[dbName][tblName]
	if !ok {
		return nil, fmt.Errorf("%w: table %s not found", ErrTableNotFound, tblName)
	}

	cp := *tbl

	return &cp, nil
}

// ListTables returns all tables in a database sorted by name.
func (b *InMemoryBackend) ListTables(dbName string) ([]Table, error) {
	b.mu.RLock("ListTables")
	defer b.mu.RUnlock()

	if _, ok := b.databases[dbName]; !ok {
		return nil, fmt.Errorf("%w: database %s not found", ErrDatabaseNotFound, dbName)
	}

	out := make([]Table, 0, len(b.tables[dbName]))
	for _, tbl := range b.tables[dbName] {
		cp := *tbl
		out = append(out, cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].TableName < out[j].TableName
	})

	return out, nil
}

// DeleteTable deletes a table from a database.
func (b *InMemoryBackend) DeleteTable(dbName, tblName string) error {
	b.mu.Lock("DeleteTable")
	defer b.mu.Unlock()

	if _, ok := b.databases[dbName]; !ok {
		return fmt.Errorf("%w: database %s not found", ErrDatabaseNotFound, dbName)
	}

	if _, ok := b.tables[dbName][tblName]; !ok {
		return fmt.Errorf("%w: table %s not found", ErrTableNotFound, tblName)
	}

	arn := tableARN(dbName, tblName)

	if slot := b.records[dbName][tblName]; slot != nil && slot.mu != nil {
		slot.mu.Close()
	}

	delete(b.tables[dbName], tblName)
	delete(b.records[dbName], tblName)
	delete(b.tags, arn)
	b.databases[dbName].TableCount--

	return nil
}

// UpdateTableInput holds the parameters for updating a table.
type UpdateTableInput struct {
	RetentionProperties          *RetentionProperties
	MagneticStoreWriteProperties *MagneticStoreWriteProperties
	Schema                       *Schema
}

// UpdateTable updates a table's properties.
func (b *InMemoryBackend) UpdateTable(dbName, tblName string, inp *UpdateTableInput) (*Table, error) {
	b.mu.Lock("UpdateTable")
	defer b.mu.Unlock()

	if _, ok := b.databases[dbName]; !ok {
		return nil, fmt.Errorf("%w: database %s not found", ErrDatabaseNotFound, dbName)
	}

	tbl, ok := b.tables[dbName][tblName]
	if !ok {
		return nil, fmt.Errorf("%w: table %s not found", ErrTableNotFound, tblName)
	}

	if inp != nil {
		if inp.RetentionProperties != nil {
			tbl.RetentionProperties = inp.RetentionProperties
		}

		if inp.MagneticStoreWriteProperties != nil {
			tbl.MagneticStoreWriteProperties = inp.MagneticStoreWriteProperties
		}

		if inp.Schema != nil {
			tbl.Schema = inp.Schema
		}
	}

	tbl.LastUpdatedTime = time.Now()
	cp := *tbl

	return &cp, nil
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

// recordGoesToMemoryStore reports whether a record should be counted as a memory-store
// write based on the table's retention and magnetic store configuration.
//
// Rules (matching the AWS API routing behaviour):
//  1. If the table has no MagneticStoreWriteProperties, or magnetic store writes are
//     disabled, all records go to memory store regardless of timestamp.
//  2. If no memory retention period is configured, all records go to memory store.
//  3. Otherwise, records whose InternalTimestamp falls within the memory retention
//     window (i.e. after the cutoff) go to memory store; older records go to magnetic store.
func recordGoesToMemoryStore(r Record, tbl *Table, now time.Time) bool {
	if tbl == nil {
		return true
	}

	if tbl.MagneticStoreWriteProperties == nil || !tbl.MagneticStoreWriteProperties.EnableMagneticStoreWrites {
		return true
	}

	if tbl.RetentionProperties == nil || tbl.RetentionProperties.MemoryStoreRetentionPeriodInHours == 0 {
		return true
	}

	retention := time.Duration(tbl.RetentionProperties.MemoryStoreRetentionPeriodInHours) * time.Hour
	cutoff := now.Add(-retention)

	return r.InternalTimestamp.After(cutoff)
}

// recordKey computes a deterministic dedup key for a record using measure name,
// time, time unit, and sorted dimension name=value pairs.
func recordKey(r Record) string {
	dims := make([]string, 0, len(r.Dimensions))
	for _, d := range r.Dimensions {
		dims = append(dims, d.Name+"="+d.Value)
	}

	sort.Strings(dims)

	return strings.Join([]string{r.MeasureName, r.Time, r.TimeUnit, strings.Join(dims, ",")}, "\x00")
}

// WriteRecords appends records to the specified table.
//
// Lock ordering: global RLock first, then per-table WLock on the *tableRecords
// slot. The global read lock prevents structural changes
// (CreateTable/DeleteTable/CreateDatabase/DeleteDatabase) from racing with
// writes; the slot's write lock serialises concurrent writes to the same
// table while allowing writes to different tables to proceed in parallel.
//
// Records are mutated through the slot pointer (slot.records = append(...))
// rather than the enclosing map, so two writers in different tables of the
// same database never write to the b.records[dbName] map concurrently.
func (b *InMemoryBackend) WriteRecords(dbName, tblName string, records []Record) (*WriteRecordsOutput, error) {
	b.mu.RLock("WriteRecords")
	defer b.mu.RUnlock()

	if _, ok := b.databases[dbName]; !ok {
		return nil, fmt.Errorf("%w: database %s not found", ErrDatabaseNotFound, dbName)
	}

	slot, ok := b.records[dbName][tblName]
	if !ok {
		return nil, fmt.Errorf("%w: table %s not found", ErrTableNotFound, tblName)
	}

	tbl := b.tables[dbName][tblName]

	slot.mu.Lock("WriteRecords")
	defer slot.mu.Unlock()

	if slot.recordIndex == nil {
		slot.recordIndex = make(map[string]int)
	}

	rejected, memoryInserted, magneticInserted := writeRecordsIntoSlot(slot, records, tbl, time.Now().UTC())

	if len(rejected) > 0 {
		return nil, &RejectedRecordsError{RejectedRecords: rejected}
	}

	total := memoryInserted + magneticInserted

	// Record counts are bounded by request size limits (< MaxInt32).
	return &WriteRecordsOutput{ //#nosec G115
		Total:         total,
		MemoryStore:   memoryInserted,
		MagneticStore: magneticInserted,
	}, nil
}

// writeRecordsIntoSlot processes records into a slot, returning rejected records and store counts.
func writeRecordsIntoSlot(
	slot *tableRecords, records []Record, tbl *Table, now time.Time,
) ([]RejectedRecord, int32, int32) {
	var rejected []RejectedRecord

	var memoryInserted, magneticInserted int32

	for i, r := range records {
		key := recordKey(r)

		newVersion := r.Version
		if newVersion == 0 {
			newVersion = 1
		}

		if idx, exists := slot.recordIndex[key]; exists {
			mem, mag, rej := upsertRecord(slot, idx, i, r, newVersion, tbl, now)
			memoryInserted += mem
			magneticInserted += mag
			if rej != nil {
				rejected = append(rejected, *rej)
			}
		} else {
			mem, mag := insertRecord(slot, r, newVersion, tbl, now)
			memoryInserted += mem
			magneticInserted += mag
		}
	}

	return rejected, memoryInserted, magneticInserted
}

// upsertRecord updates an existing record if the new version is higher, or rejects it.
func upsertRecord(
	slot *tableRecords, idx, recIdx int, r Record, newVersion int64, tbl *Table, now time.Time,
) (int32, int32, *RejectedRecord) {
	existingVersion := slot.records[idx].Version
	if existingVersion == 0 {
		existingVersion = 1
	}

	if newVersion <= existingVersion {
		return 0, 0, &RejectedRecord{
			RecordIndex:     recIdx,
			Reason:          "Record with same dimensions, time and measure name already exists with same or higher version",
			ExistingVersion: existingVersion,
		}
	}

	cp := r
	cp.Version = newVersion
	cp.InternalTimestamp = parseTimestreamTime(r.Time, r.TimeUnit)
	slot.records[idx] = cp

	if recordGoesToMemoryStore(cp, tbl, now) {
		return 1, 0, nil
	}

	return 0, 1, nil
}

// insertRecord appends a new record to the slot and returns store routing counts.
func insertRecord(slot *tableRecords, r Record, newVersion int64, tbl *Table, now time.Time) (int32, int32) {
	cp := r
	cp.Version = newVersion
	cp.InternalTimestamp = parseTimestreamTime(r.Time, r.TimeUnit)
	slot.recordIndex[recordKey(r)] = len(slot.records)
	slot.records = append(slot.records, cp)

	if recordGoesToMemoryStore(cp, tbl, now) {
		return 1, 0
	}

	return 0, 1
}

func parseTimestreamTime(ts, unit string) time.Time {
	val, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return time.Now().UTC()
	}

	switch strings.ToUpper(unit) {
	case "SECONDS":
		return time.Unix(val, 0).UTC()
	case "MILLISECONDS":
		return time.UnixMilli(val).UTC()
	case "MICROSECONDS":
		return time.UnixMicro(val).UTC()
	case "NANOSECONDS":
		return time.Unix(0, val).UTC()
	default:
		return time.UnixMilli(val).UTC()
	}
}

// SweepRetention prunes records that exceed the memory store retention period.
func (b *InMemoryBackend) SweepRetention(ctx context.Context) {
	b.mu.Lock("SweepRetention")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	totalPruned := 0

	for dbName, dbTables := range b.tables {
		for tblName, tbl := range dbTables {
			totalPruned += b.pruneTableRecords(dbName, tblName, tbl, now)
		}
	}

	if totalPruned > 0 {
		telemetry.RecordWorkerItems("timestreamwrite", "RetentionSweeper", totalPruned)
		logger.Load(ctx).InfoContext(ctx, "Timestream janitor: expired records pruned", "count", totalPruned)
	}

	telemetry.RecordWorkerTask("timestreamwrite", "RetentionSweeper", "success")
}

// pruneTableRecords drops records older than the table's memory-store retention
// window. Returns the number of records removed. Caller must hold b.mu in write
// mode. Returns 0 (no-op) when the table has no retention configured or no slot.
func (b *InMemoryBackend) pruneTableRecords(dbName, tblName string, tbl *Table, now time.Time) int {
	if tbl.RetentionProperties == nil || tbl.RetentionProperties.MemoryStoreRetentionPeriodInHours == 0 {
		return 0
	}

	slot := b.records[dbName][tblName]
	if slot == nil {
		return 0
	}

	retention := time.Duration(tbl.RetentionProperties.MemoryStoreRetentionPeriodInHours) * time.Hour
	cutoff := now.Add(-retention)

	newRecords := make([]Record, 0, len(slot.records))
	for _, r := range slot.records {
		if r.InternalTimestamp.After(cutoff) {
			newRecords = append(newRecords, r)
		}
	}

	pruned := len(slot.records) - len(newRecords)
	if pruned > 0 {
		slot.records = newRecords
		// Rebuild the dedup index after pruning to keep offsets consistent.
		slot.recordIndex = make(map[string]int, len(newRecords))
		for i, r := range slot.records {
			slot.recordIndex[recordKey(r)] = i
		}
	}

	return pruned
}

// TagResource stores tags for the given ARN.
// It accepts database, table, and scheduled-query ARNs because the Timestream
// Write and Query services share a single TagResource endpoint.
func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.isKnownARNLocked(arn) {
		return fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, arn)
	}

	if b.tags[arn] == nil {
		b.tags[arn] = make(map[string]string)
	}

	maps.Copy(b.tags[arn], tags)

	return nil
}

// UntagResource removes tag keys from the given ARN.
func (b *InMemoryBackend) UntagResource(arn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if b.tags[arn] == nil {
		return nil
	}

	for _, k := range tagKeys {
		delete(b.tags[arn], k)
	}

	return nil
}

// ListTagsForResource returns tags for the given ARN.
func (b *InMemoryBackend) ListTagsForResource(arn string) map[string]string {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	result := make(map[string]string, len(b.tags[arn]))
	maps.Copy(result, b.tags[arn])

	return result
}

// CreateBatchLoadTask creates a new batch load task targeting the specified database and table.
func (b *InMemoryBackend) CreateBatchLoadTask(
	targetDatabase, targetTable string,
	dataSourceCfg *DataSourceConfiguration,
	reportCfg *ReportConfiguration,
) (*BatchLoadTask, error) {
	b.mu.Lock("CreateBatchLoadTask")
	defer b.mu.Unlock()

	if _, ok := b.databases[targetDatabase]; !ok {
		return nil, fmt.Errorf("%w: database %s not found", ErrDatabaseNotFound, targetDatabase)
	}

	if _, ok := b.tables[targetDatabase][targetTable]; !ok {
		return nil, fmt.Errorf("%w: table %s not found", ErrTableNotFound, targetTable)
	}

	b.nextTaskID++
	taskID := fmt.Sprintf("batch-load-task-%d", b.nextTaskID)

	now := time.Now()
	task := &BatchLoadTask{
		TaskID:                  taskID,
		TargetDatabaseName:      targetDatabase,
		TargetTableName:         targetTable,
		TaskStatus:              BatchLoadStatusCreated,
		CreationTime:            now,
		LastUpdatedTime:         now,
		DataSourceConfiguration: dataSourceCfg,
		ReportConfiguration:     reportCfg,
	}
	b.batchLoadTasks[taskID] = task

	cp := *task

	return &cp, nil
}

// DescribeBatchLoadTask returns information about a batch load task.
func (b *InMemoryBackend) DescribeBatchLoadTask(taskID string) (*BatchLoadTask, error) {
	b.mu.RLock("DescribeBatchLoadTask")
	defer b.mu.RUnlock()

	task, ok := b.batchLoadTasks[taskID]
	if !ok {
		return nil, fmt.Errorf("%w: batch load task %s not found", ErrBatchLoadTaskNotFound, taskID)
	}

	cp := *task

	return &cp, nil
}

// ListBatchLoadTasks returns all batch load tasks, optionally filtered by status.
// Results are sorted by creation time (oldest first).
func (b *InMemoryBackend) ListBatchLoadTasks(statusFilter string) []BatchLoadTask {
	b.mu.RLock("ListBatchLoadTasks")
	defer b.mu.RUnlock()

	out := make([]BatchLoadTask, 0, len(b.batchLoadTasks))

	for _, task := range b.batchLoadTasks {
		if statusFilter != "" && task.TaskStatus != statusFilter {
			continue
		}

		cp := *task
		out = append(out, cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].CreationTime.Before(out[j].CreationTime)
	})

	return out
}

// ResumeBatchLoadTask resumes a batch load task that is in PENDING_RESUME or FAILED status.
func (b *InMemoryBackend) ResumeBatchLoadTask(taskID string) error {
	b.mu.Lock("ResumeBatchLoadTask")
	defer b.mu.Unlock()

	task, ok := b.batchLoadTasks[taskID]
	if !ok {
		return fmt.Errorf("%w: batch load task %s not found", ErrBatchLoadTaskNotFound, taskID)
	}

	if task.TaskStatus != BatchLoadStatusProgressStopped && task.TaskStatus != BatchLoadStatusFailed {
		return fmt.Errorf(
			"%w: task %s cannot be resumed from status %s",
			ErrInvalidBatchLoadStatus,
			taskID,
			task.TaskStatus,
		)
	}

	task.TaskStatus = BatchLoadStatusCreated
	task.LastUpdatedTime = time.Now()

	return nil
}

// SetBatchLoadTaskStatus sets the status of a batch load task.
// This is a test seed helper to set specific task states.
func (b *InMemoryBackend) SetBatchLoadTaskStatus(taskID, status string) error {
	b.mu.Lock("SetBatchLoadTaskStatus")
	defer b.mu.Unlock()

	task, ok := b.batchLoadTasks[taskID]
	if !ok {
		return fmt.Errorf("%w: batch load task %s not found", ErrBatchLoadTaskNotFound, taskID)
	}

	task.TaskStatus = status
	task.LastUpdatedTime = time.Now()

	return nil
}

// AddDatabaseInternal directly inserts a database into the backend, bypassing
// validation. Intended only for test setup.
func (b *InMemoryBackend) AddDatabaseInternal(db *Database) {
	b.mu.Lock("AddDatabaseInternal")
	defer b.mu.Unlock()

	cp := *db
	b.databases[db.DatabaseName] = &cp

	if b.tables[db.DatabaseName] == nil {
		b.tables[db.DatabaseName] = make(map[string]*Table)
	}

	if b.records[db.DatabaseName] == nil {
		b.records[db.DatabaseName] = make(map[string]*tableRecords)
	}
}

// AddTableInternal directly inserts a table into the backend, bypassing
// validation. The parent database must exist. Intended only for test setup.
func (b *InMemoryBackend) AddTableInternal(tbl *Table) {
	b.mu.Lock("AddTableInternal")
	defer b.mu.Unlock()

	if b.tables[tbl.DatabaseName] == nil {
		b.tables[tbl.DatabaseName] = make(map[string]*Table)
	}

	if b.records[tbl.DatabaseName] == nil {
		b.records[tbl.DatabaseName] = make(map[string]*tableRecords)
	}

	cp := *tbl
	b.tables[tbl.DatabaseName][tbl.TableName] = &cp

	// If a slot already exists for this table, close its mutex before
	// overwriting so we don't leak the old lockmetrics.RWMutex.
	if existing := b.records[tbl.DatabaseName][tbl.TableName]; existing != nil && existing.mu != nil {
		existing.mu.Close()
	}

	b.records[tbl.DatabaseName][tbl.TableName] = &tableRecords{
		mu:          lockmetrics.New("timestreamwrite.table"),
		recordIndex: make(map[string]int),
	}

	if db, ok := b.databases[tbl.DatabaseName]; ok {
		db.TableCount++
	}
}

// AddBatchLoadTaskInternal directly inserts a batch load task, bypassing
// validation. Intended only for test setup.
func (b *InMemoryBackend) AddBatchLoadTaskInternal(task *BatchLoadTask) {
	b.mu.Lock("AddBatchLoadTaskInternal")
	defer b.mu.Unlock()

	cp := *task
	b.batchLoadTasks[task.TaskID] = &cp
}
