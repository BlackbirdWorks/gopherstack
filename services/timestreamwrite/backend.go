package timestreamwrite

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
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
)

const (
	batchLoadStatusCreated       = "CREATED"
	batchLoadStatusPendingResume = "PENDING_RESUME"
	batchLoadStatusFailed        = "FAILED"
)

// Database represents a Timestream database.
type Database struct {
	CreationTime    time.Time
	LastUpdatedTime time.Time
	DatabaseName    string
	ARN             string
	KmsKeyID        string
	TableCount      int
}

// Table represents a Timestream table within a database.
type Table struct {
	CreationTime    time.Time
	LastUpdatedTime time.Time
	DatabaseName    string
	TableName       string
	ARN             string
	TableStatus     string
}

// Dimension holds a name/value pair for a time-series record.
type Dimension struct {
	Name  string
	Value string
}

// Record represents a time-series data point written to a table.
type Record struct {
	MeasureName      string
	MeasureValue     string
	MeasureValueType string
	Time             string
	TimeUnit         string
	Dimensions       []Dimension
	Version          int64
}

// BatchLoadTask represents a Timestream batch load task.
type BatchLoadTask struct {
	CreationTime       time.Time
	LastUpdatedTime    time.Time
	TargetDatabaseName string
	TargetTableName    string
	TaskID             string
	TaskStatus         string
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
	return &InMemoryBackend{
		databases:      make(map[string]*Database),
		tables:         make(map[string]map[string]*Table),
		records:        make(map[string]map[string][]Record),
		tags:           make(map[string]map[string]string),
		batchLoadTasks: make(map[string]*BatchLoadTask),
		mu:             lockmetrics.New("timestreamwrite"),
	}
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

// CreateDatabase creates a new Timestream database.
func (b *InMemoryBackend) CreateDatabase(name string) (*Database, error) {
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

// CreateTable creates a new table in the specified database.
func (b *InMemoryBackend) CreateTable(dbName, tblName string) (*Table, error) {
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
		TableStatus:     "ACTIVE",
		CreationTime:    now,
		LastUpdatedTime: now,
	}
	b.tables[dbName][tblName] = tbl
	b.records[dbName][tblName] = []Record{}
	b.databases[dbName].TableCount++

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

// UpdateTable updates a table's status.
func (b *InMemoryBackend) UpdateTable(dbName, tblName string) (*Table, error) {
	b.mu.Lock("UpdateTable")
	defer b.mu.Unlock()

	if _, ok := b.databases[dbName]; !ok {
		return nil, fmt.Errorf("%w: database %s not found", ErrDatabaseNotFound, dbName)
	}

	tbl, ok := b.tables[dbName][tblName]
	if !ok {
		return nil, fmt.Errorf("%w: table %s not found", ErrTableNotFound, tblName)
	}

	tbl.LastUpdatedTime = time.Now()
	cp := *tbl

	return &cp, nil
}

// WriteRecords appends records to the specified table.
func (b *InMemoryBackend) WriteRecords(dbName, tblName string, records []Record) error {
	b.mu.Lock("WriteRecords")
	defer b.mu.Unlock()

	if _, ok := b.databases[dbName]; !ok {
		return fmt.Errorf("%w: database %s not found", ErrDatabaseNotFound, dbName)
	}

	if _, ok := b.tables[dbName][tblName]; !ok {
		return fmt.Errorf("%w: table %s not found", ErrTableNotFound, tblName)
	}

	b.records[dbName][tblName] = append(b.records[dbName][tblName], records...)

	return nil
}

// TagResource stores tags for the given ARN.
func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

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

	result := make(map[string]string)
	maps.Copy(result, b.tags[arn])

	return result
}

// CreateBatchLoadTask creates a new batch load task targeting the specified database and table.
func (b *InMemoryBackend) CreateBatchLoadTask(targetDatabase, targetTable string) (*BatchLoadTask, error) {
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
		TaskID:             taskID,
		TargetDatabaseName: targetDatabase,
		TargetTableName:    targetTable,
		TaskStatus:         batchLoadStatusCreated,
		CreationTime:       now,
		LastUpdatedTime:    now,
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
		return out[i].TaskID < out[j].TaskID
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

	if task.TaskStatus != batchLoadStatusPendingResume && task.TaskStatus != batchLoadStatusFailed {
		return fmt.Errorf(
			"%w: task %s cannot be resumed from status %s",
			ErrInvalidBatchLoadStatus,
			taskID,
			task.TaskStatus,
		)
	}

	task.TaskStatus = batchLoadStatusCreated
	task.LastUpdatedTime = time.Now()

	return nil
}

// SetBatchLoadTaskStatus sets the status of a batch load task directly.
// This is intended as a test helper to seed specific task states.
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
