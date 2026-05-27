// Package databrew implements an in-memory AWS Glue DataBrew service backend.
package databrew

import (
	"fmt"
	"maps"
	"sort"
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
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

// DatasetFormatOptions holds format-specific options for a dataset.
type DatasetFormatOptions struct {
	Csv   map[string]any `json:"Csv,omitempty"`
	Excel map[string]any `json:"Excel,omitempty"`
	JSON  map[string]any `json:"JSON,omitempty"`
}

// DatasetInput holds the data source for a dataset.
type DatasetInput struct {
	S3InputDefinition          *S3Location       `json:"S3InputDefinition,omitempty"`
	DataCatalogInputDefinition *DataCatalogInput `json:"DataCatalogInputDefinition,omitempty"`
}

// S3Location references an S3 path.
type S3Location struct {
	Bucket string `json:"Bucket"`
	Key    string `json:"Key,omitempty"`
}

// DataCatalogInput references a Glue Data Catalog table.
type DataCatalogInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
}

// Dataset represents a DataBrew dataset.
type Dataset struct {
	FormatOptions    DatasetFormatOptions `json:"FormatOptions,omitzero"`
	Input            DatasetInput         `json:"Input,omitzero"`
	Tags             map[string]string    `json:"Tags,omitempty"`
	Name             string               `json:"Name"`
	Arn              string               `json:"ResourceArn"`
	Format           string               `json:"Format,omitempty"`
	Source           string               `json:"Source,omitempty"`
	CreatedBy        string               `json:"CreatedBy,omitempty"`
	LastModifiedBy   string               `json:"LastModifiedBy,omitempty"`
	CreateDate       float64              `json:"CreateDate,omitempty"`
	LastModifiedDate float64              `json:"LastModifiedDate,omitempty"`
}

// RecipeStep is one transformation step in a recipe.
type RecipeStep struct {
	Action               map[string]any   `json:"Action,omitempty"`
	ConditionExpressions []map[string]any `json:"ConditionExpressions,omitempty"`
}

// Recipe represents a DataBrew recipe.
type Recipe struct {
	Tags             map[string]string `json:"Tags,omitempty"`
	Name             string            `json:"Name"`
	Arn              string            `json:"ResourceArn"`
	Description      string            `json:"Description,omitempty"`
	PublishedBy      string            `json:"PublishedBy,omitempty"`
	RecipeVersion    string            `json:"RecipeVersion,omitempty"`
	CreatedBy        string            `json:"CreatedBy,omitempty"`
	LastModifiedBy   string            `json:"LastModifiedBy,omitempty"`
	Steps            []RecipeStep      `json:"Steps,omitempty"`
	PublishedDate    float64           `json:"PublishedDate,omitempty"`
	CreateDate       float64           `json:"CreateDate,omitempty"`
	LastModifiedDate float64           `json:"LastModifiedDate,omitempty"`
}

// Sample describes a data sample for a project.
type Sample struct {
	Type string `json:"Type,omitempty"`
	Size int    `json:"Size,omitempty"`
}

// Project represents a DataBrew project.
type Project struct {
	Tags             map[string]string `json:"Tags,omitempty"`
	Name             string            `json:"Name"`
	Arn              string            `json:"ResourceArn"`
	DatasetName      string            `json:"DatasetName,omitempty"`
	RecipeName       string            `json:"RecipeName"`
	RoleArn          string            `json:"RoleArn,omitempty"`
	SessionStatus    string            `json:"SessionStatus,omitempty"`
	CreatedBy        string            `json:"CreatedBy,omitempty"`
	LastModifiedBy   string            `json:"LastModifiedBy,omitempty"`
	Sample           Sample            `json:"Sample,omitzero"`
	CreateDate       float64           `json:"CreateDate,omitempty"`
	LastModifiedDate float64           `json:"LastModifiedDate,omitempty"`
}

// Output describes a DataBrew job output destination.
type Output struct {
	FormatOptions     map[string]any `json:"FormatOptions,omitempty"`
	Location          S3Location     `json:"Location,omitzero"`
	Format            string         `json:"Format,omitempty"`
	CompressionFormat string         `json:"CompressionFormat,omitempty"`
	Overwrite         bool           `json:"Overwrite,omitempty"`
}

// Job represents a DataBrew job.
type Job struct {
	Tags             map[string]string `json:"Tags,omitempty"`
	RoleArn          string            `json:"RoleArn,omitempty"`
	LastModifiedBy   string            `json:"LastModifiedBy,omitempty"`
	Arn              string            `json:"ResourceArn"`
	Type             string            `json:"Type,omitempty"`
	DatasetName      string            `json:"DatasetName,omitempty"`
	ProjectName      string            `json:"ProjectName,omitempty"`
	Name             string            `json:"Name"`
	CreatedBy        string            `json:"CreatedBy,omitempty"`
	RecipeName       string            `json:"RecipeName,omitempty"`
	Outputs          []Output          `json:"Outputs,omitempty"`
	CreateDate       float64           `json:"CreateDate,omitempty"`
	LastModifiedDate float64           `json:"LastModifiedDate,omitempty"`
	MaxCapacity      int               `json:"MaxCapacity,omitempty"`
	MaxRetries       int               `json:"MaxRetries,omitempty"`
	Timeout          int               `json:"Timeout,omitempty"`
}

// JobRun represents a single execution of a DataBrew job.
type JobRun struct {
	DatasetName   string  `json:"DatasetName,omitempty"`
	JobName       string  `json:"JobName"`
	RunID         string  `json:"RunID"`
	State         string  `json:"State"`
	LogGroupName  string  `json:"LogGroupName,omitempty"`
	StartedOn     float64 `json:"StartedOn,omitempty"`
	CompletedOn   float64 `json:"CompletedOn,omitempty"`
	ExecutionTime int     `json:"ExecutionTime,omitempty"`
}

// Rule represents a data quality rule.
type Rule struct {
	SubstitutionMap map[string]string `json:"SubstitutionMap,omitempty"`
	Threshold       map[string]any    `json:"Threshold,omitempty"`
	Name            string            `json:"Name"`
	CheckExpression string            `json:"CheckExpression"`
	ColumnSelectors []map[string]any  `json:"ColumnSelectors,omitempty"`
	Disabled        bool              `json:"Disabled,omitempty"`
}

// Ruleset represents a DataBrew data quality ruleset.
type Ruleset struct {
	Tags             map[string]string `json:"Tags,omitempty"`
	Name             string            `json:"Name"`
	Arn              string            `json:"ResourceArn"`
	Description      string            `json:"Description,omitempty"`
	TargetArn        string            `json:"TargetArn"`
	CreatedBy        string            `json:"CreatedBy,omitempty"`
	LastModifiedBy   string            `json:"LastModifiedBy,omitempty"`
	Rules            []Rule            `json:"Rules"`
	CreateDate       float64           `json:"CreateDate,omitempty"`
	LastModifiedDate float64           `json:"LastModifiedDate,omitempty"`
}

// Schedule represents a DataBrew schedule.
type Schedule struct {
	Tags             map[string]string `json:"Tags,omitempty"`
	Name             string            `json:"Name"`
	Arn              string            `json:"ResourceArn"`
	CronExpression   string            `json:"CronExpression"`
	CreatedBy        string            `json:"CreatedBy,omitempty"`
	LastModifiedBy   string            `json:"LastModifiedBy,omitempty"`
	JobNames         []string          `json:"JobNames,omitempty"`
	CreateDate       float64           `json:"CreateDate,omitempty"`
	LastModifiedDate float64           `json:"LastModifiedDate,omitempty"`
}

// InMemoryBackend stores DataBrew state in memory.
type InMemoryBackend struct {
	datasets  map[string]*Dataset
	recipes   map[string]*Recipe
	projects  map[string]*Project
	jobs      map[string]*Job
	jobRuns   map[string][]*JobRun
	rulesets  map[string]*Ruleset
	schedules map[string]*Schedule
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
}

// NewInMemoryBackend creates a new in-memory DataBrew backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		datasets:  make(map[string]*Dataset),
		recipes:   make(map[string]*Recipe),
		projects:  make(map[string]*Project),
		jobs:      make(map[string]*Job),
		jobRuns:   make(map[string][]*JobRun),
		rulesets:  make(map[string]*Ruleset),
		schedules: make(map[string]*Schedule),
		mu:        lockmetrics.New("databrew"),
		accountID: accountID,
		region:    region,
	}
}

func (b *InMemoryBackend) Region() string    { return b.region }
func (b *InMemoryBackend) AccountID() string { return b.accountID }

func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()
	b.datasets = make(map[string]*Dataset)
	b.recipes = make(map[string]*Recipe)
	b.projects = make(map[string]*Project)
	b.jobs = make(map[string]*Job)
	b.jobRuns = make(map[string][]*JobRun)
	b.rulesets = make(map[string]*Ruleset)
	b.schedules = make(map[string]*Schedule)
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return keys
}

func (b *InMemoryBackend) datasetARN(name string) string {
	return arn.Build("databrew", b.region, b.accountID, "dataset/"+name)
}

func (b *InMemoryBackend) recipeARN(name string) string {
	return arn.Build("databrew", b.region, b.accountID, "recipe/"+name)
}

func (b *InMemoryBackend) projectARN(name string) string {
	return arn.Build("databrew", b.region, b.accountID, "project/"+name)
}

func (b *InMemoryBackend) jobARN(name string) string {
	return arn.Build("databrew", b.region, b.accountID, "job/"+name)
}

func (b *InMemoryBackend) rulesetARN(name string) string {
	return arn.Build("databrew", b.region, b.accountID, "ruleset/"+name)
}

func (b *InMemoryBackend) scheduleARN(name string) string {
	return arn.Build("databrew", b.region, b.accountID, "schedule/"+name)
}

func (b *InMemoryBackend) CreateDataset(
	name, format string,
	input DatasetInput,
	formatOpts DatasetFormatOptions,
	tags map[string]string,
) (*Dataset, error) {
	b.mu.Lock("CreateDataset")
	defer b.mu.Unlock()
	if name == "" {
		return nil, ErrValidation
	}
	if _, ok := b.datasets[name]; ok {
		return nil, ErrAlreadyExists
	}
	source := "S3"
	if input.DataCatalogInputDefinition != nil {
		source = "DATA_CATALOG"
	}
	ds := &Dataset{
		Name: name, Arn: b.datasetARN(name), Format: format,
		Input: input, FormatOptions: formatOpts, Tags: maps.Clone(tags),
		Source: source, CreateDate: float64(time.Now().Unix()),
		LastModifiedDate: float64(time.Now().Unix()),
	}
	b.datasets[name] = ds

	return ds, nil
}

func (b *InMemoryBackend) DescribeDataset(name string) (*Dataset, error) {
	b.mu.RLock("DescribeDataset")
	defer b.mu.RUnlock()
	ds, ok := b.datasets[name]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *ds
	cp.Tags = maps.Clone(ds.Tags)

	return &cp, nil
}

func (b *InMemoryBackend) ListDatasets() []*Dataset {
	b.mu.RLock("ListDatasets")
	defer b.mu.RUnlock()
	out := make([]*Dataset, 0, len(b.datasets))
	for _, k := range sortedKeys(b.datasets) {
		cp := *b.datasets[k]
		cp.Tags = maps.Clone(b.datasets[k].Tags)
		out = append(out, &cp)
	}

	return out
}

func (b *InMemoryBackend) UpdateDataset(
	name, format string,
	input DatasetInput,
	formatOpts DatasetFormatOptions,
) error {
	b.mu.Lock("UpdateDataset")
	defer b.mu.Unlock()
	ds, ok := b.datasets[name]
	if !ok {
		return ErrNotFound
	}
	ds.Format = format
	ds.Input = input
	ds.FormatOptions = formatOpts
	ds.LastModifiedDate = float64(time.Now().Unix())

	return nil
}

func (b *InMemoryBackend) DeleteDataset(name string) error {
	b.mu.Lock("DeleteDataset")
	defer b.mu.Unlock()
	if _, ok := b.datasets[name]; !ok {
		return ErrNotFound
	}
	delete(b.datasets, name)

	return nil
}

func (b *InMemoryBackend) CreateRecipe(
	name, description string,
	steps []RecipeStep,
	tags map[string]string,
) (*Recipe, error) {
	b.mu.Lock("CreateRecipe")
	defer b.mu.Unlock()
	if name == "" {
		return nil, ErrValidation
	}
	if _, ok := b.recipes[name]; ok {
		return nil, ErrAlreadyExists
	}
	r := &Recipe{
		Name: name, Arn: b.recipeARN(name), Description: description,
		Steps: steps, Tags: maps.Clone(tags), RecipeVersion: "0.1",
		CreateDate: float64(time.Now().Unix()), LastModifiedDate: float64(time.Now().Unix()),
	}
	b.recipes[name] = r

	return r, nil
}

func (b *InMemoryBackend) DescribeRecipe(name string) (*Recipe, error) {
	b.mu.RLock("DescribeRecipe")
	defer b.mu.RUnlock()
	r, ok := b.recipes[name]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *r
	cp.Tags = maps.Clone(r.Tags)
	cp.Steps = append([]RecipeStep(nil), r.Steps...)

	return &cp, nil
}

func (b *InMemoryBackend) ListRecipes() []*Recipe {
	b.mu.RLock("ListRecipes")
	defer b.mu.RUnlock()
	out := make([]*Recipe, 0, len(b.recipes))
	for _, k := range sortedKeys(b.recipes) {
		cp := *b.recipes[k]
		cp.Tags = maps.Clone(b.recipes[k].Tags)
		cp.Steps = append([]RecipeStep(nil), b.recipes[k].Steps...)
		out = append(out, &cp)
	}

	return out
}

func (b *InMemoryBackend) PublishRecipe(name, description string) error {
	b.mu.Lock("PublishRecipe")
	defer b.mu.Unlock()
	r, ok := b.recipes[name]
	if !ok {
		return ErrNotFound
	}
	r.RecipeVersion = "1.0"
	r.PublishedDate = float64(time.Now().Unix())
	r.PublishedBy = "admin"
	if description != "" {
		r.Description = description
	}

	return nil
}

func (b *InMemoryBackend) UpdateRecipe(name, description string, steps []RecipeStep) error {
	b.mu.Lock("UpdateRecipe")
	defer b.mu.Unlock()
	r, ok := b.recipes[name]
	if !ok {
		return ErrNotFound
	}
	if description != "" {
		r.Description = description
	}
	r.Steps = steps
	r.LastModifiedDate = float64(time.Now().Unix())

	return nil
}

func (b *InMemoryBackend) DeleteRecipe(name string) error {
	b.mu.Lock("DeleteRecipe")
	defer b.mu.Unlock()
	if _, ok := b.recipes[name]; !ok {
		return ErrNotFound
	}
	delete(b.recipes, name)

	return nil
}

func (b *InMemoryBackend) CreateProject(
	name, datasetName, recipeName, roleArn string,
	sample Sample,
	tags map[string]string,
) (*Project, error) {
	b.mu.Lock("CreateProject")
	defer b.mu.Unlock()
	if name == "" {
		return nil, ErrValidation
	}
	if _, ok := b.projects[name]; ok {
		return nil, ErrAlreadyExists
	}
	p := &Project{
		Name: name, Arn: b.projectARN(name), DatasetName: datasetName,
		RecipeName: recipeName, RoleArn: roleArn, Sample: sample,
		Tags: maps.Clone(tags), SessionStatus: "READY",
		CreateDate: float64(time.Now().Unix()), LastModifiedDate: float64(time.Now().Unix()),
	}
	b.projects[name] = p

	return p, nil
}

func (b *InMemoryBackend) DescribeProject(name string) (*Project, error) {
	b.mu.RLock("DescribeProject")
	defer b.mu.RUnlock()
	p, ok := b.projects[name]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *p
	cp.Tags = maps.Clone(p.Tags)

	return &cp, nil
}

func (b *InMemoryBackend) ListProjects() []*Project {
	b.mu.RLock("ListProjects")
	defer b.mu.RUnlock()
	out := make([]*Project, 0, len(b.projects))
	for _, k := range sortedKeys(b.projects) {
		cp := *b.projects[k]
		cp.Tags = maps.Clone(b.projects[k].Tags)
		out = append(out, &cp)
	}

	return out
}

func (b *InMemoryBackend) UpdateProject(name, datasetName, roleArn string, sample Sample) error {
	b.mu.Lock("UpdateProject")
	defer b.mu.Unlock()
	p, ok := b.projects[name]
	if !ok {
		return ErrNotFound
	}
	if datasetName != "" {
		p.DatasetName = datasetName
	}
	if roleArn != "" {
		p.RoleArn = roleArn
	}
	p.Sample = sample
	p.LastModifiedDate = float64(time.Now().Unix())

	return nil
}

func (b *InMemoryBackend) DeleteProject(name string) error {
	b.mu.Lock("DeleteProject")
	defer b.mu.Unlock()
	if _, ok := b.projects[name]; !ok {
		return ErrNotFound
	}
	delete(b.projects, name)

	return nil
}

func (b *InMemoryBackend) CreateJob(
	name, jobType, datasetName, projectName, recipeName, roleArn string,
	outputs []Output,
	tags map[string]string,
) (*Job, error) {
	b.mu.Lock("CreateJob")
	defer b.mu.Unlock()
	if name == "" {
		return nil, ErrValidation
	}
	if _, ok := b.jobs[name]; ok {
		return nil, ErrAlreadyExists
	}
	j := &Job{
		Name: name, Arn: b.jobARN(name), Type: jobType,
		DatasetName: datasetName, ProjectName: projectName,
		RecipeName: recipeName, RoleArn: roleArn, Outputs: outputs,
		Tags: maps.Clone(tags), CreateDate: float64(time.Now().Unix()),
		LastModifiedDate: float64(time.Now().Unix()),
	}
	b.jobs[name] = j

	return j, nil
}

func (b *InMemoryBackend) DescribeJob(name string) (*Job, error) {
	b.mu.RLock("DescribeJob")
	defer b.mu.RUnlock()
	j, ok := b.jobs[name]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *j
	cp.Tags = maps.Clone(j.Tags)
	cp.Outputs = append([]Output(nil), j.Outputs...)

	return &cp, nil
}

func (b *InMemoryBackend) ListJobs() []*Job {
	b.mu.RLock("ListJobs")
	defer b.mu.RUnlock()
	out := make([]*Job, 0, len(b.jobs))
	for _, k := range sortedKeys(b.jobs) {
		cp := *b.jobs[k]
		cp.Tags = maps.Clone(b.jobs[k].Tags)
		cp.Outputs = append([]Output(nil), b.jobs[k].Outputs...)
		out = append(out, &cp)
	}

	return out
}

func (b *InMemoryBackend) UpdateJob(
	name, roleArn string,
	outputs []Output,
	maxCapacity, maxRetries, timeout int,
) error {
	b.mu.Lock("UpdateJob")
	defer b.mu.Unlock()
	j, ok := b.jobs[name]
	if !ok {
		return ErrNotFound
	}
	if roleArn != "" {
		j.RoleArn = roleArn
	}
	if len(outputs) > 0 {
		j.Outputs = outputs
	}
	if maxCapacity > 0 {
		j.MaxCapacity = maxCapacity
	}
	if maxRetries >= 0 {
		j.MaxRetries = maxRetries
	}
	if timeout > 0 {
		j.Timeout = timeout
	}
	j.LastModifiedDate = float64(time.Now().Unix())

	return nil
}

func (b *InMemoryBackend) DeleteJob(name string) error {
	b.mu.Lock("DeleteJob")
	defer b.mu.Unlock()
	if _, ok := b.jobs[name]; !ok {
		return ErrNotFound
	}
	delete(b.jobs, name)
	delete(b.jobRuns, name)

	return nil
}

const (
	// jobRunTransitionDelay is how long to wait before transitioning a job run to SUCCEEDED.
	jobRunTransitionDelay = 100 * time.Millisecond
	// jobRunDefaultExecTime is the simulated execution time returned in a completed job run.
	jobRunDefaultExecTime = 30
)

// StartJobRun creates a new job run with STARTING state, transitioning to SUCCEEDED asynchronously.
func (b *InMemoryBackend) StartJobRun(jobName string) (*JobRun, error) {
	b.mu.Lock("StartJobRun")
	defer b.mu.Unlock()

	if _, ok := b.jobs[jobName]; !ok {
		return nil, fmt.Errorf("%w: job %q not found", ErrNotFound, jobName)
	}

	run := &JobRun{
		JobName:   jobName,
		RunID:     uuid.New().String(),
		State:     "STARTING",
		StartedOn: float64(time.Now().Unix()),
	}

	b.jobRuns[jobName] = append(b.jobRuns[jobName], run)

	go func() {
		time.Sleep(jobRunTransitionDelay)
		b.mu.Lock("StartJobRun.transition")
		defer b.mu.Unlock()
		run.State = "SUCCEEDED"
		run.CompletedOn = float64(time.Now().Unix())
		run.ExecutionTime = jobRunDefaultExecTime
	}()

	cp := *run

	return &cp, nil
}

func (b *InMemoryBackend) ListJobRuns(jobName string) ([]*JobRun, error) {
	b.mu.RLock("ListJobRuns")
	defer b.mu.RUnlock()
	if _, ok := b.jobs[jobName]; !ok {
		return nil, fmt.Errorf("%w: job %q not found", ErrNotFound, jobName)
	}
	src := b.jobRuns[jobName]
	out := make([]*JobRun, len(src))
	for i, r := range src {
		cp := *r
		out[len(src)-1-i] = &cp
	}

	return out, nil
}

func (b *InMemoryBackend) CreateRuleset(
	name, description, targetArn string,
	rules []Rule,
	tags map[string]string,
) (*Ruleset, error) {
	b.mu.Lock("CreateRuleset")
	defer b.mu.Unlock()
	if name == "" {
		return nil, ErrValidation
	}
	if _, ok := b.rulesets[name]; ok {
		return nil, ErrAlreadyExists
	}
	rs := &Ruleset{
		Name: name, Arn: b.rulesetARN(name), Description: description,
		TargetArn: targetArn, Rules: append([]Rule(nil), rules...),
		Tags: maps.Clone(tags), CreateDate: float64(time.Now().Unix()),
		LastModifiedDate: float64(time.Now().Unix()),
	}
	b.rulesets[name] = rs

	return rs, nil
}

func (b *InMemoryBackend) DescribeRuleset(name string) (*Ruleset, error) {
	b.mu.RLock("DescribeRuleset")
	defer b.mu.RUnlock()
	rs, ok := b.rulesets[name]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *rs
	cp.Tags = maps.Clone(rs.Tags)
	cp.Rules = append([]Rule(nil), rs.Rules...)

	return &cp, nil
}

func (b *InMemoryBackend) ListRulesets() []*Ruleset {
	b.mu.RLock("ListRulesets")
	defer b.mu.RUnlock()
	out := make([]*Ruleset, 0, len(b.rulesets))
	for _, k := range sortedKeys(b.rulesets) {
		cp := *b.rulesets[k]
		cp.Tags = maps.Clone(b.rulesets[k].Tags)
		cp.Rules = append([]Rule(nil), b.rulesets[k].Rules...)
		out = append(out, &cp)
	}

	return out
}

func (b *InMemoryBackend) UpdateRuleset(name, description string, rules []Rule) error {
	b.mu.Lock("UpdateRuleset")
	defer b.mu.Unlock()
	rs, ok := b.rulesets[name]
	if !ok {
		return ErrNotFound
	}
	rs.Description = description
	rs.Rules = rules
	rs.LastModifiedDate = float64(time.Now().Unix())

	return nil
}

func (b *InMemoryBackend) DeleteRuleset(name string) error {
	b.mu.Lock("DeleteRuleset")
	defer b.mu.Unlock()
	if _, ok := b.rulesets[name]; !ok {
		return ErrNotFound
	}
	delete(b.rulesets, name)

	return nil
}

func (b *InMemoryBackend) CreateSchedule(
	name string,
	jobNames []string,
	cron string,
	tags map[string]string,
) (*Schedule, error) {
	b.mu.Lock("CreateSchedule")
	defer b.mu.Unlock()
	if name == "" {
		return nil, ErrValidation
	}
	if _, ok := b.schedules[name]; ok {
		return nil, ErrAlreadyExists
	}
	sc := &Schedule{
		Name: name, Arn: b.scheduleARN(name), JobNames: append([]string(nil), jobNames...),
		CronExpression: cron, Tags: maps.Clone(tags),
		CreateDate: float64(time.Now().Unix()), LastModifiedDate: float64(time.Now().Unix()),
	}
	b.schedules[name] = sc

	return sc, nil
}

func (b *InMemoryBackend) DescribeSchedule(name string) (*Schedule, error) {
	b.mu.RLock("DescribeSchedule")
	defer b.mu.RUnlock()
	sc, ok := b.schedules[name]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *sc
	cp.Tags = maps.Clone(sc.Tags)
	cp.JobNames = append([]string(nil), sc.JobNames...)

	return &cp, nil
}

func (b *InMemoryBackend) ListSchedules() []*Schedule {
	b.mu.RLock("ListSchedules")
	defer b.mu.RUnlock()
	out := make([]*Schedule, 0, len(b.schedules))
	for _, k := range sortedKeys(b.schedules) {
		cp := *b.schedules[k]
		cp.Tags = maps.Clone(b.schedules[k].Tags)
		cp.JobNames = append([]string(nil), b.schedules[k].JobNames...)
		out = append(out, &cp)
	}

	return out
}

func (b *InMemoryBackend) UpdateSchedule(name string, jobNames []string, cron string) error {
	b.mu.Lock("UpdateSchedule")
	defer b.mu.Unlock()
	sc, ok := b.schedules[name]
	if !ok {
		return ErrNotFound
	}
	sc.JobNames = jobNames
	sc.CronExpression = cron
	sc.LastModifiedDate = float64(time.Now().Unix())

	return nil
}

func (b *InMemoryBackend) DeleteSchedule(name string) error {
	b.mu.Lock("DeleteSchedule")
	defer b.mu.Unlock()
	if _, ok := b.schedules[name]; !ok {
		return ErrNotFound
	}
	delete(b.schedules, name)

	return nil
}

func (b *InMemoryBackend) StopJobRun(name, runID string) (*JobRun, error) {
	b.mu.Lock("StopJobRun")
	defer b.mu.Unlock()

	runs, ok := b.jobRuns[name]
	if !ok {
		return nil, ErrNotFound
	}

	for _, run := range runs {
		if run.RunID == runID {
			if run.State != "SUCCEEDED" && run.State != "FAILED" && run.State != "STOPPED" {
				run.State = "STOPPED"
				run.CompletedOn = float64(time.Now().Unix())
			}
			cp := *run

			return &cp, nil
		}
	}

	return nil, ErrNotFound
}

func (b *InMemoryBackend) DescribeJobRun(name, runID string) (*JobRun, error) {
	b.mu.RLock("DescribeJobRun")
	defer b.mu.RUnlock()

	runs, ok := b.jobRuns[name]
	if !ok {
		return nil, ErrNotFound
	}

	for _, run := range runs {
		if run.RunID == runID {
			cp := *run

			return &cp, nil
		}
	}

	return nil, ErrNotFound
}

// FindTagsByArn searches all resources for a specific ARN and returns its tags.
func (b *InMemoryBackend) FindTagsByArn(arn string) (map[string]string, error) {
	b.mu.RLock("FindTagsByArn")
	defer b.mu.RUnlock()

	for _, ds := range b.datasets {
		if ds.Arn == arn {
			return maps.Clone(ds.Tags), nil
		}
	}
	for _, r := range b.recipes {
		if r.Arn == arn {
			return maps.Clone(r.Tags), nil
		}
	}
	for _, p := range b.projects {
		if p.Arn == arn {
			return maps.Clone(p.Tags), nil
		}
	}
	for _, j := range b.jobs {
		if j.Arn == arn {
			return maps.Clone(j.Tags), nil
		}
	}
	for _, rs := range b.rulesets {
		if rs.Arn == arn {
			return maps.Clone(rs.Tags), nil
		}
	}
	for _, sc := range b.schedules {
		if sc.Arn == arn {
			return maps.Clone(sc.Tags), nil
		}
	}

	return nil, ErrNotFound
}

// UpdateTagsByArn searches all resources and applies tags additions/removals.
func (b *InMemoryBackend) UpdateTagsByArn(arn string, add map[string]string, remove []string) error {
	b.mu.Lock("UpdateTagsByArn")
	defer b.mu.Unlock()

	applyTags := func(tags map[string]string) map[string]string {
		if tags == nil {
			tags = make(map[string]string)
		}
		maps.Copy(tags, add)
		for _, k := range remove {
			delete(tags, k)
		}

		return tags
	}

	if b.updateDatasetTags(arn, applyTags) {
		return nil
	}
	if b.updateRecipeTags(arn, applyTags) {
		return nil
	}
	if b.updateProjectTags(arn, applyTags) {
		return nil
	}
	if b.updateJobTags(arn, applyTags) {
		return nil
	}
	if b.updateRulesetTags(arn, applyTags) {
		return nil
	}
	if b.updateScheduleTags(arn, applyTags) {
		return nil
	}

	return ErrNotFound
}

func (b *InMemoryBackend) updateDatasetTags(arn string, apply func(map[string]string) map[string]string) bool {
	for _, x := range b.datasets {
		if x.Arn == arn {
			x.Tags = apply(x.Tags)

			return true
		}
	}

	return false
}

func (b *InMemoryBackend) updateRecipeTags(arn string, apply func(map[string]string) map[string]string) bool {
	for _, x := range b.recipes {
		if x.Arn == arn {
			x.Tags = apply(x.Tags)

			return true
		}
	}

	return false
}

func (b *InMemoryBackend) updateProjectTags(arn string, apply func(map[string]string) map[string]string) bool {
	for _, x := range b.projects {
		if x.Arn == arn {
			x.Tags = apply(x.Tags)

			return true
		}
	}

	return false
}

func (b *InMemoryBackend) updateJobTags(arn string, apply func(map[string]string) map[string]string) bool {
	for _, x := range b.jobs {
		if x.Arn == arn {
			x.Tags = apply(x.Tags)

			return true
		}
	}

	return false
}

func (b *InMemoryBackend) updateRulesetTags(arn string, apply func(map[string]string) map[string]string) bool {
	for _, x := range b.rulesets {
		if x.Arn == arn {
			x.Tags = apply(x.Tags)

			return true
		}
	}

	return false
}

func (b *InMemoryBackend) updateScheduleTags(arn string, apply func(map[string]string) map[string]string) bool {
	for _, x := range b.schedules {
		if x.Arn == arn {
			x.Tags = apply(x.Tags)

			return true
		}
	}

	return false
}
