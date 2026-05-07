package databrew

import (
	"fmt"
	"maps"
	"sort"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrAlreadyExists)
	// ErrValidation is returned when a request contains invalid parameters.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

const (
	jobStatusStarting  = "STARTING"
	jobStatusRunning   = "RUNNING"
	jobStatusSucceeded = "SUCCEEDED"

	jobSimDelay = 2 * time.Second
)

// DatasetFormatOptions holds format-specific options for a dataset.
type DatasetFormatOptions struct {
	Csv   *CsvOptions   `json:"Csv,omitempty"`
	Excel *ExcelOptions `json:"Excel,omitempty"`
	JSON  *JSONOptions  `json:"Json,omitempty"`
}

// CsvOptions holds CSV-specific parsing options.
type CsvOptions struct {
	Delimiter string `json:"Delimiter,omitempty"`
	HeaderRow bool   `json:"HeaderRow,omitempty"`
}

// ExcelOptions holds Excel-specific parsing options.
type ExcelOptions struct {
	SheetIndexes []int    `json:"SheetIndexes,omitempty"`
	SheetNames   []string `json:"SheetNames,omitempty"`
	HeaderRow    bool     `json:"HeaderRow,omitempty"`
}

// JSONOptions holds JSON-specific parsing options.
type JSONOptions struct {
	MultiLine bool `json:"MultiLine,omitempty"`
}

// DatasetInput specifies the source of data for a dataset.
type DatasetInput struct {
	S3InputDefinition          *S3Location       `json:"S3InputDefinition,omitempty"`
	DataCatalogInputDefinition *DataCatalogInput `json:"DataCatalogInputDefinition,omitempty"`
	DatabaseInputDefinition    *DatabaseInput    `json:"DatabaseInputDefinition,omitempty"`
}

// S3Location specifies an S3 path.
type S3Location struct {
	Bucket string `json:"Bucket,omitempty"`
	Key    string `json:"Key,omitempty"`
}

// DataCatalogInput specifies a Glue Data Catalog table.
type DataCatalogInput struct {
	DatabaseName string `json:"DatabaseName,omitempty"`
	TableName    string `json:"TableName,omitempty"`
	CatalogID    string `json:"CatalogId,omitempty"`
}

// DatabaseInput specifies a database query input.
type DatabaseInput struct {
	TempDirectory      *S3Location `json:"TempDirectory,omitempty"`
	GlueConnectionName string      `json:"GlueConnectionName,omitempty"`
	QueryString        string      `json:"QueryString,omitempty"`
	DatabaseTableName  string      `json:"DatabaseTableName,omitempty"`
}

// RecipeStep represents a single transformation step in a recipe.
type RecipeStep struct {
	Action               RecipeAction          `json:"Action"`
	ConditionExpressions []ConditionExpression `json:"ConditionExpressions,omitempty"`
}

// RecipeAction is the action to perform in a recipe step.
type RecipeAction struct {
	Parameters map[string]string `json:"Parameters,omitempty"`
	Operation  string            `json:"Operation"`
}

// ConditionExpression filters rows based on a condition.
type ConditionExpression struct {
	Condition    string `json:"Condition"`
	TargetColumn string `json:"TargetColumn"`
	Value        string `json:"Value,omitempty"`
}

// Sample configures sampling for a project session.
type Sample struct {
	Type string `json:"Type,omitempty"`
	Size int    `json:"Size,omitempty"`
}

// Output specifies a job output destination.
type Output struct {
	Location          S3Location `json:"Location"`
	Format            string     `json:"Format,omitempty"`
	CompressionFormat string     `json:"CompressionFormat,omitempty"`
	Overwrite         bool       `json:"Overwrite,omitempty"`
}

// Dataset represents a DataBrew dataset.
type Dataset struct {
	Tags          map[string]string    `json:"Tags,omitempty"`
	Input         DatasetInput         `json:"Input"`
	CreateDate    time.Time            `json:"CreateDate"`
	LastModified  time.Time            `json:"LastModifiedDate"`
	FormatOptions DatasetFormatOptions `json:"FormatOptions,omitzero"`
	Name          string               `json:"Name"`
	Arn           string               `json:"ResourceArn"`
	Format        string               `json:"Format,omitempty"`
}

// Recipe represents a DataBrew recipe.
type Recipe struct {
	CreateDate   time.Time         `json:"CreateDate"`
	LastModified time.Time         `json:"LastModifiedDate"`
	Tags         map[string]string `json:"Tags,omitempty"`
	Name         string            `json:"Name"`
	Arn          string            `json:"ResourceArn"`
	Description  string            `json:"Description,omitempty"`
	PublishedBy  string            `json:"PublishedBy,omitempty"`
	Steps        []RecipeStep      `json:"Steps,omitempty"`
	Published    bool              `json:"Published,omitempty"`
}

// Project represents a DataBrew project.
type Project struct {
	CreateDate   time.Time         `json:"CreateDate"`
	LastModified time.Time         `json:"LastModifiedDate"`
	Tags         map[string]string `json:"Tags,omitempty"`
	Name         string            `json:"Name"`
	Arn          string            `json:"ResourceArn"`
	DatasetName  string            `json:"DatasetName,omitempty"`
	RecipeName   string            `json:"RecipeName"`
	RoleArn      string            `json:"RoleArn"`
	Sample       Sample            `json:"Sample,omitzero"`
}

// Job represents a DataBrew profile or recipe job.
type Job struct {
	LastModified time.Time         `json:"LastModifiedDate"`
	CreateDate   time.Time         `json:"CreateDate"`
	Tags         map[string]string `json:"Tags,omitempty"`
	Type         string            `json:"Type"`
	Name         string            `json:"Name"`
	Arn          string            `json:"ResourceArn"`
	DatasetName  string            `json:"DatasetName,omitempty"`
	ProjectName  string            `json:"ProjectName,omitempty"`
	RecipeName   string            `json:"RecipeName,omitempty"`
	RoleArn      string            `json:"RoleArn,omitempty"`
	Outputs      []Output          `json:"Outputs,omitempty"`
	MaxCapacity  int               `json:"MaxCapacity,omitempty"`
	MaxRetries   int               `json:"MaxRetries,omitempty"`
	Timeout      int               `json:"Timeout,omitempty"`
}

// JobRun represents a single execution of a DataBrew job.
type JobRun struct {
	CompletedOn  *time.Time `json:"CompletedOn,omitempty"`
	StartedOn    *time.Time `json:"StartedOn,omitempty"`
	RunID        string     `json:"RunId"`
	JobName      string     `json:"JobName"`
	State        string     `json:"State"`
	ErrorMessage string     `json:"ErrorMessage,omitempty"`
}

// StorageBackend is the interface for the DataBrew backend.
type StorageBackend interface {
	Reset()
	AccountID() string
	Region() string

	CreateDataset(
		name, format string,
		input DatasetInput,
		opts DatasetFormatOptions,
		tags map[string]string,
	) (*Dataset, error)
	DescribeDataset(name string) (*Dataset, error)
	ListDatasets() []*Dataset
	UpdateDataset(name, format string, input DatasetInput, opts DatasetFormatOptions) error
	DeleteDataset(name string) error

	CreateRecipe(name, description string, steps []RecipeStep, tags map[string]string) (*Recipe, error)
	DescribeRecipe(name string) (*Recipe, error)
	ListRecipes() []*Recipe
	PublishRecipe(name, description string) error
	UpdateRecipe(name, description string, steps []RecipeStep) error
	DeleteRecipe(name string) error

	CreateProject(
		name, datasetName, recipeName, roleArn string,
		sample Sample,
		tags map[string]string,
	) (*Project, error)
	DescribeProject(name string) (*Project, error)
	ListProjects() []*Project
	UpdateProject(name, datasetName, roleArn string, sample Sample) error
	DeleteProject(name string) error

	CreateJob(
		name, jobType, datasetName, projectName, recipeName, roleArn string,
		outputs []Output,
		tags map[string]string,
	) (*Job, error)
	DescribeJob(name string) (*Job, error)
	ListJobs() []*Job
	UpdateJob(name, roleArn string, outputs []Output, maxCapacity, maxRetries, timeout int) error
	DeleteJob(name string) error
	StartJobRun(name string) (*JobRun, error)
	ListJobRuns(name string) ([]*JobRun, error)
}

// Compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

// InMemoryBackend stores DataBrew state in memory.
type InMemoryBackend struct {
	datasets  map[string]*Dataset
	recipes   map[string]*Recipe
	projects  map[string]*Project
	jobs      map[string]*Job
	jobRuns   map[string][]*JobRun // job name → runs
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
	runSeq    atomic.Uint64
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		datasets:  make(map[string]*Dataset),
		recipes:   make(map[string]*Recipe),
		projects:  make(map[string]*Project),
		jobs:      make(map[string]*Job),
		jobRuns:   make(map[string][]*JobRun),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("databrew"),
	}
}

// AccountID returns the AWS account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the AWS region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state from the backend.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.datasets = make(map[string]*Dataset)
	b.recipes = make(map[string]*Recipe)
	b.projects = make(map[string]*Project)
	b.jobs = make(map[string]*Job)
	b.jobRuns = make(map[string][]*JobRun)
}

// -- Datasets --

// CreateDataset creates a new dataset.
func (b *InMemoryBackend) CreateDataset(
	name, format string,
	input DatasetInput,
	opts DatasetFormatOptions,
	tags map[string]string,
) (*Dataset, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	b.mu.Lock("CreateDataset")
	defer b.mu.Unlock()

	if _, exists := b.datasets[name]; exists {
		return nil, fmt.Errorf("%w: dataset %q already exists", ErrAlreadyExists, name)
	}

	tagsCopy := make(map[string]string)
	maps.Copy(tagsCopy, tags)

	now := time.Now()
	ds := &Dataset{
		Name:          name,
		Format:        format,
		Input:         input,
		FormatOptions: opts,
		Tags:          tagsCopy,
		Arn:           arn.Build("databrew", b.region, b.accountID, "dataset/"+name),
		CreateDate:    now,
		LastModified:  now,
	}
	b.datasets[name] = ds

	return ds, nil
}

// DescribeDataset returns a dataset by name.
func (b *InMemoryBackend) DescribeDataset(name string) (*Dataset, error) {
	b.mu.RLock("DescribeDataset")
	defer b.mu.RUnlock()

	ds, ok := b.datasets[name]
	if !ok {
		return nil, fmt.Errorf("%w: dataset %q not found", ErrNotFound, name)
	}

	return ds, nil
}

// ListDatasets returns all datasets sorted by name.
func (b *InMemoryBackend) ListDatasets() []*Dataset {
	b.mu.RLock("ListDatasets")
	defer b.mu.RUnlock()

	result := make([]*Dataset, 0, len(b.datasets))
	for _, ds := range b.datasets {
		result = append(result, ds)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result
}

// UpdateDataset updates an existing dataset.
func (b *InMemoryBackend) UpdateDataset(name, format string, input DatasetInput, opts DatasetFormatOptions) error {
	b.mu.Lock("UpdateDataset")
	defer b.mu.Unlock()

	ds, ok := b.datasets[name]
	if !ok {
		return fmt.Errorf("%w: dataset %q not found", ErrNotFound, name)
	}

	ds.Format = format
	ds.Input = input
	ds.FormatOptions = opts
	ds.LastModified = time.Now()

	return nil
}

// DeleteDataset removes a dataset by name.
func (b *InMemoryBackend) DeleteDataset(name string) error {
	b.mu.Lock("DeleteDataset")
	defer b.mu.Unlock()

	if _, ok := b.datasets[name]; !ok {
		return fmt.Errorf("%w: dataset %q not found", ErrNotFound, name)
	}

	delete(b.datasets, name)

	return nil
}

// -- Recipes --

// CreateRecipe creates a new recipe.
func (b *InMemoryBackend) CreateRecipe(
	name, description string,
	steps []RecipeStep,
	tags map[string]string,
) (*Recipe, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	b.mu.Lock("CreateRecipe")
	defer b.mu.Unlock()

	if _, exists := b.recipes[name]; exists {
		return nil, fmt.Errorf("%w: recipe %q already exists", ErrAlreadyExists, name)
	}

	tagsCopy := make(map[string]string)
	maps.Copy(tagsCopy, tags)

	stepsCopy := make([]RecipeStep, len(steps))
	copy(stepsCopy, steps)

	now := time.Now()
	r := &Recipe{
		Name:         name,
		Description:  description,
		Steps:        stepsCopy,
		Tags:         tagsCopy,
		Arn:          arn.Build("databrew", b.region, b.accountID, "recipe/"+name),
		CreateDate:   now,
		LastModified: now,
	}
	b.recipes[name] = r

	return r, nil
}

// DescribeRecipe returns a recipe by name.
func (b *InMemoryBackend) DescribeRecipe(name string) (*Recipe, error) {
	b.mu.RLock("DescribeRecipe")
	defer b.mu.RUnlock()

	r, ok := b.recipes[name]
	if !ok {
		return nil, fmt.Errorf("%w: recipe %q not found", ErrNotFound, name)
	}

	return r, nil
}

// ListRecipes returns all recipes sorted by name.
func (b *InMemoryBackend) ListRecipes() []*Recipe {
	b.mu.RLock("ListRecipes")
	defer b.mu.RUnlock()

	result := make([]*Recipe, 0, len(b.recipes))
	for _, r := range b.recipes {
		result = append(result, r)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result
}

// PublishRecipe marks a recipe as published.
func (b *InMemoryBackend) PublishRecipe(name, description string) error {
	b.mu.Lock("PublishRecipe")
	defer b.mu.Unlock()

	r, ok := b.recipes[name]
	if !ok {
		return fmt.Errorf("%w: recipe %q not found", ErrNotFound, name)
	}

	r.Published = true
	r.PublishedBy = "gopherstack"
	if description != "" {
		r.Description = description
	}

	r.LastModified = time.Now()

	return nil
}

// UpdateRecipe updates an existing recipe.
func (b *InMemoryBackend) UpdateRecipe(name, description string, steps []RecipeStep) error {
	b.mu.Lock("UpdateRecipe")
	defer b.mu.Unlock()

	r, ok := b.recipes[name]
	if !ok {
		return fmt.Errorf("%w: recipe %q not found", ErrNotFound, name)
	}

	r.Description = description
	stepsCopy := make([]RecipeStep, len(steps))
	copy(stepsCopy, steps)
	r.Steps = stepsCopy
	r.LastModified = time.Now()

	return nil
}

// DeleteRecipe removes a recipe by name.
func (b *InMemoryBackend) DeleteRecipe(name string) error {
	b.mu.Lock("DeleteRecipe")
	defer b.mu.Unlock()

	if _, ok := b.recipes[name]; !ok {
		return fmt.Errorf("%w: recipe %q not found", ErrNotFound, name)
	}

	delete(b.recipes, name)

	return nil
}

// -- Projects --

// CreateProject creates a new project.
func (b *InMemoryBackend) CreateProject(
	name, datasetName, recipeName, roleArn string,
	sample Sample,
	tags map[string]string,
) (*Project, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	b.mu.Lock("CreateProject")
	defer b.mu.Unlock()

	if _, exists := b.projects[name]; exists {
		return nil, fmt.Errorf("%w: project %q already exists", ErrAlreadyExists, name)
	}

	tagsCopy := make(map[string]string)
	maps.Copy(tagsCopy, tags)

	now := time.Now()
	p := &Project{
		Name:         name,
		DatasetName:  datasetName,
		RecipeName:   recipeName,
		RoleArn:      roleArn,
		Sample:       sample,
		Tags:         tagsCopy,
		Arn:          arn.Build("databrew", b.region, b.accountID, "project/"+name),
		CreateDate:   now,
		LastModified: now,
	}
	b.projects[name] = p

	return p, nil
}

// DescribeProject returns a project by name.
func (b *InMemoryBackend) DescribeProject(name string) (*Project, error) {
	b.mu.RLock("DescribeProject")
	defer b.mu.RUnlock()

	p, ok := b.projects[name]
	if !ok {
		return nil, fmt.Errorf("%w: project %q not found", ErrNotFound, name)
	}

	return p, nil
}

// ListProjects returns all projects sorted by name.
func (b *InMemoryBackend) ListProjects() []*Project {
	b.mu.RLock("ListProjects")
	defer b.mu.RUnlock()

	result := make([]*Project, 0, len(b.projects))
	for _, p := range b.projects {
		result = append(result, p)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result
}

// UpdateProject updates an existing project.
func (b *InMemoryBackend) UpdateProject(name, datasetName, roleArn string, sample Sample) error {
	b.mu.Lock("UpdateProject")
	defer b.mu.Unlock()

	p, ok := b.projects[name]
	if !ok {
		return fmt.Errorf("%w: project %q not found", ErrNotFound, name)
	}

	p.DatasetName = datasetName
	p.RoleArn = roleArn
	p.Sample = sample
	p.LastModified = time.Now()

	return nil
}

// DeleteProject removes a project by name.
func (b *InMemoryBackend) DeleteProject(name string) error {
	b.mu.Lock("DeleteProject")
	defer b.mu.Unlock()

	if _, ok := b.projects[name]; !ok {
		return fmt.Errorf("%w: project %q not found", ErrNotFound, name)
	}

	delete(b.projects, name)

	return nil
}

// -- Jobs --

// CreateJob creates a new profile or recipe job.
func (b *InMemoryBackend) CreateJob(
	name, jobType, datasetName, projectName, recipeName, roleArn string,
	outputs []Output,
	tags map[string]string,
) (*Job, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	b.mu.Lock("CreateJob")
	defer b.mu.Unlock()

	if _, exists := b.jobs[name]; exists {
		return nil, fmt.Errorf("%w: job %q already exists", ErrAlreadyExists, name)
	}

	tagsCopy := make(map[string]string)
	maps.Copy(tagsCopy, tags)

	outputsCopy := make([]Output, len(outputs))
	copy(outputsCopy, outputs)

	now := time.Now()
	j := &Job{
		Name:         name,
		Type:         jobType,
		DatasetName:  datasetName,
		ProjectName:  projectName,
		RecipeName:   recipeName,
		RoleArn:      roleArn,
		Outputs:      outputsCopy,
		Tags:         tagsCopy,
		Arn:          arn.Build("databrew", b.region, b.accountID, "job/"+name),
		CreateDate:   now,
		LastModified: now,
	}
	b.jobs[name] = j

	return j, nil
}

// DescribeJob returns a job by name.
func (b *InMemoryBackend) DescribeJob(name string) (*Job, error) {
	b.mu.RLock("DescribeJob")
	defer b.mu.RUnlock()

	j, ok := b.jobs[name]
	if !ok {
		return nil, fmt.Errorf("%w: job %q not found", ErrNotFound, name)
	}

	return j, nil
}

// ListJobs returns all jobs sorted by name.
func (b *InMemoryBackend) ListJobs() []*Job {
	b.mu.RLock("ListJobs")
	defer b.mu.RUnlock()

	result := make([]*Job, 0, len(b.jobs))
	for _, j := range b.jobs {
		result = append(result, j)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result
}

// UpdateJob updates an existing job.
func (b *InMemoryBackend) UpdateJob(
	name, roleArn string,
	outputs []Output,
	maxCapacity, maxRetries, timeout int,
) error {
	b.mu.Lock("UpdateJob")
	defer b.mu.Unlock()

	j, ok := b.jobs[name]
	if !ok {
		return fmt.Errorf("%w: job %q not found", ErrNotFound, name)
	}

	j.RoleArn = roleArn
	outputsCopy := make([]Output, len(outputs))
	copy(outputsCopy, outputs)
	j.Outputs = outputsCopy
	j.MaxCapacity = maxCapacity
	j.MaxRetries = maxRetries
	j.Timeout = timeout
	j.LastModified = time.Now()

	return nil
}

// DeleteJob removes a job by name.
func (b *InMemoryBackend) DeleteJob(name string) error {
	b.mu.Lock("DeleteJob")
	defer b.mu.Unlock()

	if _, ok := b.jobs[name]; !ok {
		return fmt.Errorf("%w: job %q not found", ErrNotFound, name)
	}

	delete(b.jobs, name)
	delete(b.jobRuns, name)

	return nil
}

// StartJobRun starts a new run for the given job and simulates async state transitions.
func (b *InMemoryBackend) StartJobRun(name string) (*JobRun, error) {
	b.mu.Lock("StartJobRun")

	if _, ok := b.jobs[name]; !ok {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: job %q not found", ErrNotFound, name)
	}

	seq := b.runSeq.Add(1)
	runID := fmt.Sprintf("%s-%s-%d", name, uuid.New().String(), seq)
	now := time.Now()
	run := &JobRun{
		RunID:     runID,
		JobName:   name,
		State:     jobStatusStarting,
		StartedOn: &now,
	}
	b.jobRuns[name] = append(b.jobRuns[name], run)
	b.mu.Unlock()

	go b.simulateJobRun(name, runID)

	return run, nil
}

// simulateJobRun transitions a job run through STARTING→RUNNING→SUCCEEDED.
func (b *InMemoryBackend) simulateJobRun(jobName, runID string) {
	time.Sleep(jobSimDelay / 2) //nolint:mnd // half delay for RUNNING

	b.mu.Lock("simulateJobRun-running")
	b.setRunState(jobName, runID, jobStatusRunning)
	b.mu.Unlock()

	time.Sleep(jobSimDelay / 2) //nolint:mnd // second half for SUCCEEDED

	b.mu.Lock("simulateJobRun-succeeded")
	b.setRunState(jobName, runID, jobStatusSucceeded)
	b.mu.Unlock()
}

// setRunState finds a run by ID and updates its state. Caller must hold the write lock.
func (b *InMemoryBackend) setRunState(jobName, runID, state string) {
	for _, r := range b.jobRuns[jobName] {
		if r.RunID != runID {
			continue
		}

		r.State = state
		if state == jobStatusSucceeded {
			now := time.Now()
			r.CompletedOn = &now
		}

		break
	}
}

// ListJobRuns returns all runs for a job, newest first.
func (b *InMemoryBackend) ListJobRuns(name string) ([]*JobRun, error) {
	b.mu.RLock("ListJobRuns")
	defer b.mu.RUnlock()

	if _, ok := b.jobs[name]; !ok {
		return nil, fmt.Errorf("%w: job %q not found", ErrNotFound, name)
	}

	runs := b.jobRuns[name]
	result := make([]*JobRun, len(runs))
	copy(result, runs)

	// Return newest first.
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	return result, nil
}
