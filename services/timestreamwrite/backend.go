package timestreamwrite

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"

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
)

// RetentionProperties holds the memory and magnetic store retention durations.
type RetentionProperties struct {
	MemoryStoreRetentionPeriodInHours  int64 `json:"MemoryStoreRetentionPeriodInHours,omitempty"`
	MagneticStoreRetentionPeriodInDays int64 `json:"MagneticStoreRetentionPeriodInDays,omitempty"`
}

// MagneticStoreWriteProperties configures rejected-record delivery for the magnetic store.
type MagneticStoreWriteProperties struct {
	EnableMagneticStoreWrites bool `json:"EnableMagneticStoreWrites"`
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
	DatabaseName                 string                        `json:"database_name"`
	TableName                    string                        `json:"table_name"`
	ARN                          string                        `json:"arn"`
	TableStatus                  string                        `json:"table_status"`
}

// Dimension holds a name/value pair for a time-series record.
type Dimension struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Record represents a time-series data point written to a table.
type Record struct {
	// InternalTimestamp is the parsed value of Time, used for retention sweeping.
	InternalTimestamp time.Time   `json:"-"`
	MeasureName       string      `json:"measure_name"`
	MeasureValue      string      `json:"measure_value"`
	MeasureValueType  string      `json:"measure_value_type"`
	Time              string      `json:"time"`
	TimeUnit          string      `json:"time_unit"`
	Dimensions        []Dimension `json:"dimensions,omitempty"`
	Version           int64       `json:"version,omitempty"`
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
	TargetDatabaseName      string                   `json:"target_database_name"`
	TargetTableName         string                   `json:"target_table_name"`
	TaskID                  string                   `json:"task_id"`
	TaskStatus              string                   `json:"task_status"`
	ErrorMessage            string                   `json:"error_message,omitempty"`
	RecordVersion           int64                    `json:"record_version,omitempty"`
}

// InMemoryBackend is the in-memory store for Timestream Write resources.
type InMemoryBackend struct {
	databases      map[string]*Database
	tables         map[string]map[string]*Table
	records        map[string]map[string][]Record
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

	b.nextTaskID = 0
	b.ensureNonNilMapsLocked()
}

// AccountID returns the simulated AWS account ID.
func (b *InMemoryBackend) AccountID() string { return config.DefaultAccountID }

// Region returns the simulated AWS region.
func (b *InMemoryBackend) Region() string { return config.DefaultRegion }

// ensureNonNilMaps initialises all maps (called without lock held during construction or restore).
func (b *InMemoryBackend) ensureNonNilMaps() {
	b.databases = make(map[string]*Database)
	b.tables = make(map[string]map[string]*Table)
	b.records = make(map[string]map[string][]Record)
	b.tags = make(map[string]map[string]string)
	b.batchLoadTasks = make(map[string]*BatchLoadTask)
}

// ensureNonNilMapsLocked initialises all maps when the lock is already held.
func (b *InMemoryBackend) ensureNonNilMapsLocked() {
	b.databases = make(map[string]*Database)
	b.tables = make(map[string]map[string]*Table)
	b.records = make(map[string]map[string][]Record)
	b.tags = make(map[string]map[string]string)
	b.batchLoadTasks = make(map[string]*BatchLoadTask)
}

func databaseARN(name string) string {
	return fmt.Sprintf("arn:aws:timestream:%s:%s:database/%s", config.DefaultRegion, config.DefaultAccountID, name)
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
	b.records[name] = make(map[string][]Record)

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

	// Clean up tags for all tables in this database before deleting.
	for tblName := range b.tables[name] {
		delete(b.tags, tableARN(name, tblName))
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
	}

	b.tables[dbName][tblName] = tbl
	b.records[dbName][tblName] = []Record{}
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

	delete(b.tables[dbName], tblName)
	delete(b.records[dbName], tblName)
	delete(b.tags, tableARN(dbName, tblName))
	b.databases[dbName].TableCount--

	return nil
}

// UpdateTableInput holds the parameters for updating a table.
type UpdateTableInput struct {
	RetentionProperties          *RetentionProperties
	MagneticStoreWriteProperties *MagneticStoreWriteProperties
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
	}

	tbl.LastUpdatedTime = time.Now()
	cp := *tbl

	return &cp, nil
}

// WriteRecordsOutput summarises the results of a WriteRecords call.
type WriteRecordsOutput struct {
	Total       int32
	MemoryStore int32
}

// WriteRecords appends records to the specified table.
func (b *InMemoryBackend) WriteRecords(dbName, tblName string, records []Record) (*WriteRecordsOutput, error) {
	b.mu.Lock("WriteRecords")
	defer b.mu.Unlock()

	if _, ok := b.databases[dbName]; !ok {
		return nil, fmt.Errorf("%w: database %s not found", ErrDatabaseNotFound, dbName)
	}

	if _, ok := b.tables[dbName][tblName]; !ok {
		return nil, fmt.Errorf("%w: table %s not found", ErrTableNotFound, tblName)
	}

	for i := range records {
		records[i].InternalTimestamp = parseTimestreamTime(records[i].Time, records[i].TimeUnit)
	}

	b.records[dbName][tblName] = append(b.records[dbName][tblName], records...)

	// Record counts are bounded by request size limits (< MaxInt32).
	count := int32(len(records)) //#nosec G115

	return &WriteRecordsOutput{Total: count, MemoryStore: count}, nil
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
			if tbl.RetentionProperties == nil || tbl.RetentionProperties.MemoryStoreRetentionPeriodInHours == 0 {
				continue
			}

			retention := time.Duration(tbl.RetentionProperties.MemoryStoreRetentionPeriodInHours) * time.Hour
			cutoff := now.Add(-retention)

			records := b.records[dbName][tblName]
			newRecords := make([]Record, 0, len(records))
			for _, r := range records {
				if r.InternalTimestamp.After(cutoff) {
					newRecords = append(newRecords, r)
				}
			}

			pruned := len(records) - len(newRecords)
			if pruned > 0 {
				b.records[dbName][tblName] = newRecords
				totalPruned += pruned
			}
		}
	}

	if totalPruned > 0 {
		telemetry.RecordWorkerItems("timestreamwrite", "RetentionSweeper", totalPruned)
		logger.Load(ctx).InfoContext(ctx, "Timestream janitor: expired records pruned", "count", totalPruned)
	}
	telemetry.RecordWorkerTask("timestreamwrite", "RetentionSweeper", "success")
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

	if task.TaskStatus != BatchLoadStatusPendingResume && task.TaskStatus != BatchLoadStatusFailed {
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
		b.records[db.DatabaseName] = make(map[string][]Record)
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
		b.records[tbl.DatabaseName] = make(map[string][]Record)
	}

	cp := *tbl
	b.tables[tbl.DatabaseName][tbl.TableName] = &cp
	b.records[tbl.DatabaseName][tbl.TableName] = []Record{}

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
