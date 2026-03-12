package glue

import (
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("EntityNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("AlreadyExistsException", awserr.ErrAlreadyExists)
)

// DatabaseInput is the input for creating or updating a Glue database.
type DatabaseInput struct {
	Name        string `json:"Name"`
	Description string `json:"Description,omitempty"`
}

// Database represents a Glue catalog database.
type Database struct {
	Tags        map[string]string `json:"-"`
	Name        string            `json:"Name"`
	Description string            `json:"Description,omitempty"`
	CatalogID   string            `json:"CatalogId"`
	ARN         string            `json:"Arn,omitempty"`
}

// Column represents a column in a Glue table.
type Column struct {
	Name    string `json:"Name"`
	Type    string `json:"Type,omitempty"`
	Comment string `json:"Comment,omitempty"`
}

// StorageDescriptor describes the physical storage of a table.
type StorageDescriptor struct {
	Location string   `json:"Location,omitempty"`
	Columns  []Column `json:"Columns,omitempty"`
}

// TableInput is the input for creating or updating a Glue table.
type TableInput struct {
	StorageDescriptor StorageDescriptor `json:"StorageDescriptor,omitzero"`
	Name              string            `json:"Name"`
	Description       string            `json:"Description,omitempty"`
	TableType         string            `json:"TableType,omitempty"`
	PartitionKeys     []Column          `json:"PartitionKeys,omitempty"`
}

// Table represents a Glue catalog table.
type Table struct {
	StorageDescriptor StorageDescriptor `json:"StorageDescriptor,omitzero"`
	Name              string            `json:"Name"`
	DatabaseName      string            `json:"DatabaseName"`
	CatalogID         string            `json:"CatalogId"`
	Description       string            `json:"Description,omitempty"`
	TableType         string            `json:"TableType,omitempty"`
	PartitionKeys     []Column          `json:"PartitionKeys,omitempty"`
}

// CrawlerTarget specifies S3 targets for a crawler.
type CrawlerTarget struct {
	S3Targets []S3Target `json:"S3Targets,omitempty"`
}

// S3Target is an S3 path for a crawler.
type S3Target struct {
	Path string `json:"Path,omitempty"`
}

// Crawler represents a Glue crawler.
type Crawler struct {
	Tags         map[string]string `json:"-"`
	Name         string            `json:"Name"`
	Role         string            `json:"Role"`
	DatabaseName string            `json:"DatabaseName"`
	State        string            `json:"State"`
	ARN          string            `json:"Arn,omitempty"`
	Targets      CrawlerTarget     `json:"Targets,omitzero"`
}

// ConnectionsList holds connections for a Glue job.
type ConnectionsList struct {
	Connections []string `json:"Connections,omitempty"`
}

// ExecutionProperty holds max concurrent runs for a Glue job.
type ExecutionProperty struct {
	MaxConcurrentRuns int `json:"MaxConcurrentRuns,omitempty"`
}

// JobCommand holds the command for a Glue job.
type JobCommand struct {
	Name           string `json:"Name,omitempty"`
	ScriptLocation string `json:"ScriptLocation,omitempty"`
	PythonVersion  string `json:"PythonVersion,omitempty"`
}

// Job represents a Glue job.
type Job struct {
	Tags              map[string]string `json:"-"`
	DefaultArguments  map[string]string `json:"DefaultArguments,omitempty"`
	Command           JobCommand        `json:"Command,omitzero"`
	WorkerType        string            `json:"WorkerType,omitempty"`
	Role              string            `json:"Role,omitempty"`
	GlueVersion       string            `json:"GlueVersion,omitempty"`
	Name              string            `json:"Name"`
	ARN               string            `json:"Arn,omitempty"`
	Description       string            `json:"Description,omitempty"`
	Connections       ConnectionsList   `json:"Connections,omitzero"`
	NumberOfWorkers   int               `json:"NumberOfWorkers,omitempty"`
	MaxRetries        int               `json:"MaxRetries,omitempty"`
	Timeout           int               `json:"Timeout,omitempty"`
	ExecutionProperty ExecutionProperty `json:"ExecutionProperty,omitzero"`
}

// InMemoryBackend stores Glue state in memory.
type InMemoryBackend struct {
	databases map[string]*Database // key: databaseName
	tables    map[string]*Table    // key: "databaseName|tableName"
	crawlers  map[string]*Crawler  // key: crawlerName
	jobs      map[string]*Job      // key: jobName
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
}

// NewInMemoryBackend creates a new in-memory Glue backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		databases: make(map[string]*Database),
		tables:    make(map[string]*Table),
		crawlers:  make(map[string]*Crawler),
		jobs:      make(map[string]*Job),
		mu:        lockmetrics.New("glue"),
		accountID: accountID,
		region:    region,
	}
}

// Region returns the backend region.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the backend account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// databaseARN returns the ARN for a Glue database.
func (b *InMemoryBackend) databaseARN(name string) string {
	return arn.Build("glue", b.region, b.accountID, "database/"+name)
}

// crawlerARN returns the ARN for a Glue crawler.
func (b *InMemoryBackend) crawlerARN(name string) string {
	return arn.Build("glue", b.region, b.accountID, "crawler/"+name)
}

// jobARN returns the ARN for a Glue job.
func (b *InMemoryBackend) jobARN(name string) string {
	return arn.Build("glue", b.region, b.accountID, "job/"+name)
}

// tableKey returns a map key for a table.
func tableKey(dbName, tableName string) string {
	return fmt.Sprintf("%s|%s", dbName, tableName)
}

// --- Database operations ---

// CreateDatabase creates a new Glue database.
func (b *InMemoryBackend) CreateDatabase(input DatabaseInput, tags map[string]string) (*Database, error) {
	b.mu.Lock("CreateDatabase")
	defer b.mu.Unlock()

	if _, ok := b.databases[input.Name]; ok {
		return nil, ErrAlreadyExists
	}

	db := &Database{
		Name:        input.Name,
		Description: input.Description,
		CatalogID:   b.accountID,
		ARN:         b.databaseARN(input.Name),
		Tags:        maps.Clone(tags),
	}
	b.databases[input.Name] = db

	return db, nil
}

// GetDatabase retrieves a Glue database by name.
func (b *InMemoryBackend) GetDatabase(name string) (*Database, error) {
	b.mu.RLock("GetDatabase")
	defer b.mu.RUnlock()

	db, ok := b.databases[name]
	if !ok {
		return nil, ErrNotFound
	}

	return db, nil
}

// GetDatabases returns all Glue databases.
func (b *InMemoryBackend) GetDatabases() []*Database {
	b.mu.RLock("GetDatabases")
	defer b.mu.RUnlock()

	out := make([]*Database, 0, len(b.databases))
	for _, db := range b.databases {
		out = append(out, db)
	}

	return out
}

// DeleteDatabase deletes a Glue database by name, also removing all its tables.
func (b *InMemoryBackend) DeleteDatabase(name string) error {
	b.mu.Lock("DeleteDatabase")
	defer b.mu.Unlock()

	if _, ok := b.databases[name]; !ok {
		return ErrNotFound
	}

	delete(b.databases, name)

	prefix := name + "|"
	for k := range b.tables {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			delete(b.tables, k)
		}
	}

	return nil
}

// UpdateDatabase updates an existing Glue database.
func (b *InMemoryBackend) UpdateDatabase(name string, input DatabaseInput) error {
	b.mu.Lock("UpdateDatabase")
	defer b.mu.Unlock()

	db, ok := b.databases[name]
	if !ok {
		return ErrNotFound
	}

	db.Description = input.Description

	return nil
}

// --- Table operations ---

// CreateTable creates a new Glue table in a database.
func (b *InMemoryBackend) CreateTable(dbName string, input TableInput) (*Table, error) {
	b.mu.Lock("CreateTable")
	defer b.mu.Unlock()

	if _, ok := b.databases[dbName]; !ok {
		return nil, ErrNotFound
	}

	key := tableKey(dbName, input.Name)
	if _, ok := b.tables[key]; ok {
		return nil, ErrAlreadyExists
	}

	t := &Table{
		Name:              input.Name,
		DatabaseName:      dbName,
		CatalogID:         b.accountID,
		Description:       input.Description,
		StorageDescriptor: input.StorageDescriptor,
		PartitionKeys:     input.PartitionKeys,
		TableType:         input.TableType,
	}
	b.tables[key] = t

	return t, nil
}

// GetTable retrieves a Glue table.
func (b *InMemoryBackend) GetTable(dbName, tableName string) (*Table, error) {
	b.mu.RLock("GetTable")
	defer b.mu.RUnlock()

	t, ok := b.tables[tableKey(dbName, tableName)]
	if !ok {
		return nil, ErrNotFound
	}

	return t, nil
}

// GetTables returns all tables in a database.
func (b *InMemoryBackend) GetTables(dbName string) ([]*Table, error) {
	b.mu.RLock("GetTables")
	defer b.mu.RUnlock()

	if _, ok := b.databases[dbName]; !ok {
		return nil, ErrNotFound
	}

	prefix := dbName + "|"
	var out []*Table

	for k, t := range b.tables {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			out = append(out, t)
		}
	}

	return out, nil
}

// UpdateTable updates an existing Glue table.
func (b *InMemoryBackend) UpdateTable(dbName string, input TableInput) error {
	b.mu.Lock("UpdateTable")
	defer b.mu.Unlock()

	key := tableKey(dbName, input.Name)

	t, ok := b.tables[key]
	if !ok {
		return ErrNotFound
	}

	t.Description = input.Description
	t.StorageDescriptor = input.StorageDescriptor
	t.PartitionKeys = input.PartitionKeys
	t.TableType = input.TableType

	return nil
}

// DeleteTable deletes a Glue table.
func (b *InMemoryBackend) DeleteTable(dbName, tableName string) error {
	b.mu.Lock("DeleteTable")
	defer b.mu.Unlock()

	key := tableKey(dbName, tableName)
	if _, ok := b.tables[key]; !ok {
		return ErrNotFound
	}

	delete(b.tables, key)

	return nil
}

// --- Crawler operations ---

// CreateCrawler creates a new Glue crawler.
func (b *InMemoryBackend) CreateCrawler(
	name, role, dbName string,
	targets CrawlerTarget,
	tags map[string]string,
) (*Crawler, error) {
	b.mu.Lock("CreateCrawler")
	defer b.mu.Unlock()

	if _, ok := b.databases[dbName]; !ok {
		return nil, ErrNotFound
	}

	if _, ok := b.crawlers[name]; ok {
		return nil, ErrAlreadyExists
	}

	c := &Crawler{
		Name:         name,
		Role:         role,
		DatabaseName: dbName,
		Targets:      targets,
		State:        "READY",
		ARN:          b.crawlerARN(name),
		Tags:         maps.Clone(tags),
	}
	b.crawlers[name] = c

	return c, nil
}

// GetCrawler retrieves a Glue crawler by name.
func (b *InMemoryBackend) GetCrawler(name string) (*Crawler, error) {
	b.mu.RLock("GetCrawler")
	defer b.mu.RUnlock()

	c, ok := b.crawlers[name]
	if !ok {
		return nil, ErrNotFound
	}

	return c, nil
}

// GetCrawlers returns all Glue crawlers.
func (b *InMemoryBackend) GetCrawlers() []*Crawler {
	b.mu.RLock("GetCrawlers")
	defer b.mu.RUnlock()

	out := make([]*Crawler, 0, len(b.crawlers))
	for _, c := range b.crawlers {
		out = append(out, c)
	}

	return out
}

// UpdateCrawler updates an existing Glue crawler.
func (b *InMemoryBackend) UpdateCrawler(name, role, dbName string, targets CrawlerTarget) error {
	b.mu.Lock("UpdateCrawler")
	defer b.mu.Unlock()

	c, ok := b.crawlers[name]
	if !ok {
		return ErrNotFound
	}

	c.Role = role
	c.DatabaseName = dbName
	c.Targets = targets

	return nil
}

// DeleteCrawler deletes a Glue crawler by name.
func (b *InMemoryBackend) DeleteCrawler(name string) error {
	b.mu.Lock("DeleteCrawler")
	defer b.mu.Unlock()

	if _, ok := b.crawlers[name]; !ok {
		return ErrNotFound
	}

	delete(b.crawlers, name)

	return nil
}

// --- Job operations ---

// CreateJob creates a new Glue job.
func (b *InMemoryBackend) CreateJob(input Job) (*Job, error) {
	b.mu.Lock("CreateJob")
	defer b.mu.Unlock()

	if _, ok := b.jobs[input.Name]; ok {
		return nil, ErrAlreadyExists
	}

	j := &Job{
		Name:              input.Name,
		Description:       input.Description,
		Role:              input.Role,
		Command:           input.Command,
		DefaultArguments:  input.DefaultArguments,
		GlueVersion:       input.GlueVersion,
		WorkerType:        input.WorkerType,
		NumberOfWorkers:   input.NumberOfWorkers,
		MaxRetries:        input.MaxRetries,
		Timeout:           input.Timeout,
		ARN:               b.jobARN(input.Name),
		Tags:              maps.Clone(input.Tags),
		ExecutionProperty: input.ExecutionProperty,
		Connections:       input.Connections,
	}
	b.jobs[input.Name] = j

	return j, nil
}

// GetJob retrieves a Glue job by name.
func (b *InMemoryBackend) GetJob(name string) (*Job, error) {
	b.mu.RLock("GetJob")
	defer b.mu.RUnlock()

	j, ok := b.jobs[name]
	if !ok {
		return nil, ErrNotFound
	}

	return j, nil
}

// GetJobs returns all Glue jobs.
func (b *InMemoryBackend) GetJobs() []*Job {
	b.mu.RLock("GetJobs")
	defer b.mu.RUnlock()

	out := make([]*Job, 0, len(b.jobs))
	for _, j := range b.jobs {
		out = append(out, j)
	}

	return out
}

// UpdateJob updates an existing Glue job.
func (b *InMemoryBackend) UpdateJob(name string, input Job) error {
	b.mu.Lock("UpdateJob")
	defer b.mu.Unlock()

	j, ok := b.jobs[name]
	if !ok {
		return ErrNotFound
	}

	j.Description = input.Description
	j.Role = input.Role
	j.Command = input.Command
	j.DefaultArguments = input.DefaultArguments
	j.GlueVersion = input.GlueVersion
	j.WorkerType = input.WorkerType
	j.NumberOfWorkers = input.NumberOfWorkers
	j.MaxRetries = input.MaxRetries
	j.Timeout = input.Timeout
	j.ExecutionProperty = input.ExecutionProperty
	j.Connections = input.Connections

	return nil
}

// DeleteJob deletes a Glue job by name.
func (b *InMemoryBackend) DeleteJob(name string) error {
	b.mu.Lock("DeleteJob")
	defer b.mu.Unlock()

	if _, ok := b.jobs[name]; !ok {
		return ErrNotFound
	}

	delete(b.jobs, name)

	return nil
}

// --- Tag operations ---

// TagResource adds tags to a resource by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	return b.tagResource(resourceARN, tags)
}

func (b *InMemoryBackend) tagResource(resourceARN string, tags map[string]string) error {
	if db := b.findDatabaseByARN(resourceARN); db != nil {
		if db.Tags == nil {
			db.Tags = make(map[string]string)
		}
		maps.Copy(db.Tags, tags)

		return nil
	}

	if c := b.findCrawlerByARN(resourceARN); c != nil {
		if c.Tags == nil {
			c.Tags = make(map[string]string)
		}
		maps.Copy(c.Tags, tags)

		return nil
	}

	if j := b.findJobByARN(resourceARN); j != nil {
		if j.Tags == nil {
			j.Tags = make(map[string]string)
		}
		maps.Copy(j.Tags, tags)

		return nil
	}

	return ErrNotFound
}

// UntagResource removes tags from a resource by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if db := b.findDatabaseByARN(resourceARN); db != nil {
		for _, k := range tagKeys {
			delete(db.Tags, k)
		}

		return nil
	}

	if c := b.findCrawlerByARN(resourceARN); c != nil {
		for _, k := range tagKeys {
			delete(c.Tags, k)
		}

		return nil
	}

	if j := b.findJobByARN(resourceARN); j != nil {
		for _, k := range tagKeys {
			delete(j.Tags, k)
		}

		return nil
	}

	return ErrNotFound
}

// GetTags retrieves tags for a resource by ARN.
func (b *InMemoryBackend) GetTags(resourceARN string) (map[string]string, error) {
	b.mu.RLock("GetTags")
	defer b.mu.RUnlock()

	if db := b.findDatabaseByARN(resourceARN); db != nil {
		return maps.Clone(db.Tags), nil
	}

	if c := b.findCrawlerByARN(resourceARN); c != nil {
		return maps.Clone(c.Tags), nil
	}

	if j := b.findJobByARN(resourceARN); j != nil {
		return maps.Clone(j.Tags), nil
	}

	return nil, ErrNotFound
}

func (b *InMemoryBackend) findDatabaseByARN(resourceARN string) *Database {
	for _, db := range b.databases {
		if db.ARN == resourceARN {
			return db
		}
	}

	return nil
}

func (b *InMemoryBackend) findCrawlerByARN(resourceARN string) *Crawler {
	for _, c := range b.crawlers {
		if c.ARN == resourceARN {
			return c
		}
	}

	return nil
}

func (b *InMemoryBackend) findJobByARN(resourceARN string) *Job {
	for _, j := range b.jobs {
		if j.ARN == resourceARN {
			return j
		}
	}

	return nil
}
