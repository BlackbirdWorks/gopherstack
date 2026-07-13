// Package databrew implements an in-memory AWS Glue DataBrew service backend.
package databrew

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
// DataBrew resources are isolated per region: every backend operation resolves the
// caller's region from the request context and operates only on that region's
// nested store.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrAlreadyExists)
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

// DatasetFormatOptions holds format-specific options for a dataset.
//
// The JSON field's wire key is "Json" (mixed case), NOT "JSON" -- confirmed
// against aws-sdk-go-v2/service/databrew's deserializer
// (awsRestjson1_deserializeDocumentFormatOptions switches on the exact,
// case-sensitive key "Json"). A response emitting the Go-idiomatic "JSON"
// falls through that switch's default case and the client silently drops the
// field, so a dataset created with JSON format options would appear to have
// none on describe/list.
type DatasetFormatOptions struct {
	Csv   map[string]any `json:"Csv,omitempty"`
	Excel map[string]any `json:"Excel,omitempty"`
	JSON  map[string]any `json:"Json,omitempty"`
}

// DatasetInput holds the data source for a dataset.
type DatasetInput struct {
	S3InputDefinition          *S3Location       `json:"S3InputDefinition,omitempty"`
	DataCatalogInputDefinition *DataCatalogInput `json:"DataCatalogInputDefinition,omitempty"`
	DatabaseInputDefinition    *DatabaseInput    `json:"DatabaseInputDefinition,omitempty"`
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

// DatabaseInput references a database table.
type DatabaseInput struct {
	GlueConnectionName string `json:"GlueConnectionName"`
	DatabaseTableName  string `json:"DatabaseTableName"`
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
	PartitionColumns  []string       `json:"PartitionColumns,omitempty"`
	MaxOutputFiles    int            `json:"MaxOutputFiles,omitempty"`
	Overwrite         bool           `json:"Overwrite,omitempty"`
}

// RecipeRef holds a reference to a DataBrew recipe and optional version.
type RecipeRef struct {
	Name          string `json:"Name"`
	RecipeVersion string `json:"RecipeVersion,omitempty"`
}

// Job represents a DataBrew job.
type Job struct {
	ProfileConfiguration     map[string]any    `json:"ProfileConfiguration,omitempty"`
	JobSample                map[string]any    `json:"JobSample,omitempty"`
	Tags                     map[string]string `json:"Tags,omitempty"`
	RecipeReference          *RecipeRef        `json:"RecipeReference,omitempty"`
	EncryptionMode           string            `json:"EncryptionMode,omitempty"`
	EncryptionKeyArn         string            `json:"EncryptionKeyArn,omitempty"`
	DatasetName              string            `json:"DatasetName,omitempty"`
	ProjectName              string            `json:"ProjectName,omitempty"`
	Name                     string            `json:"Name"`
	CreatedBy                string            `json:"CreatedBy,omitempty"`
	RecipeName               string            `json:"-"`
	RoleArn                  string            `json:"RoleArn,omitempty"`
	LogSubscription          string            `json:"LogSubscription,omitempty"`
	Type                     string            `json:"Type,omitempty"`
	LastModifiedBy           string            `json:"LastModifiedBy,omitempty"`
	Arn                      string            `json:"ResourceArn"`
	ValidationConfigurations []map[string]any  `json:"ValidationConfigurations,omitempty"`
	DataCatalogOutputs       []map[string]any  `json:"DataCatalogOutputs,omitempty"`
	DatabaseOutputs          []map[string]any  `json:"DatabaseOutputs,omitempty"`
	Outputs                  []Output          `json:"Outputs,omitempty"`
	Timeout                  int               `json:"Timeout,omitempty"`
	MaxRetries               int               `json:"MaxRetries,omitempty"`
	MaxCapacity              int               `json:"MaxCapacity,omitempty"`
	LastModifiedDate         float64           `json:"LastModifiedDate,omitempty"`
	CreateDate               float64           `json:"CreateDate,omitempty"`
}

// JobRun represents a single execution of a DataBrew job.
type JobRun struct {
	DatasetName   string  `json:"DatasetName,omitempty"`
	JobName       string  `json:"JobName"`
	RunID         string  `json:"RunId"`
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
//
// All resource collections are nested by region (outer key = region) so that
// same-named resources are isolated across regions. datasets/recipes/
// projects/jobs/rulesets/schedules each hold one *store.Table[T] per region,
// created lazily via the *Table accessors in store_setup.go — mirroring the
// lazy map creation the hand-rolled *Store helpers did before the Phase 3.3
// pkgs/store conversion (see store_setup.go's package doc for why jobRuns is
// NOT converted). Callers must hold b.mu while accessing any of them.
type InMemoryBackend struct {
	svcCtx    context.Context
	schedules map[string]*store.Table[Schedule]
	projects  map[string]*store.Table[Project]
	jobs      map[string]*store.Table[Job]
	// jobRuns holds one map[jobName][]*JobRun per region. It is left as a
	// plain (non-store.Table) map: each job's run history is an
	// ORDER-SENSITIVE slice (chronological append order; ListJobRuns reverses
	// it), and store.Table/store.Index do not preserve insertion order — see
	// pkgs/store's package doc and .claude/memories on this rollout.
	jobRuns map[string]map[string][]*JobRun
	// registry is the lifecycle registry for every *store.Table this backend
	// owns; Reset/Snapshot/Restore drive it via ResetAll/SnapshotAll/RestoreAll
	// instead of hand-written per-map boilerplate. See store_setup.go.
	registry      *store.Registry
	rulesets      map[string]*store.Table[Ruleset]
	datasets      map[string]*store.Table[Dataset]
	mu            *lockmetrics.RWMutex
	recipes       map[string]*store.Table[Recipe]
	cancel        context.CancelFunc
	accountID     string
	defaultRegion string
	wg            sync.WaitGroup
}

// NewInMemoryBackend creates a new in-memory DataBrew backend with a background
// lifecycle context.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return NewInMemoryBackendWithContext(context.Background(), accountID, region)
}

// NewInMemoryBackendWithContext creates a new in-memory DataBrew backend whose
// delayed lifecycle goroutines are tied to svcCtx. When svcCtx (or the backend's
// Shutdown) is cancelled, in-flight transition goroutines exit promptly.
// If svcCtx is nil, [context.Background] is used.
func NewInMemoryBackendWithContext(
	svcCtx context.Context,
	accountID, region string,
) *InMemoryBackend {
	if svcCtx == nil {
		svcCtx = context.Background()
	}
	ctx, cancel := context.WithCancel(svcCtx)

	return &InMemoryBackend{
		registry:      store.NewRegistry(),
		datasets:      make(map[string]*store.Table[Dataset]),
		recipes:       make(map[string]*store.Table[Recipe]),
		projects:      make(map[string]*store.Table[Project]),
		jobs:          make(map[string]*store.Table[Job]),
		jobRuns:       make(map[string]map[string][]*JobRun),
		rulesets:      make(map[string]*store.Table[Ruleset]),
		schedules:     make(map[string]*store.Table[Schedule]),
		mu:            lockmetrics.New("databrew"),
		accountID:     accountID,
		defaultRegion: region,
		svcCtx:        ctx,
		cancel:        cancel,
	}
}

// jobRunsStore returns the per-region jobName -> runs map, lazily creating it.
// Callers must hold b.mu.
func (b *InMemoryBackend) jobRunsStore(region string) map[string][]*JobRun {
	if b.jobRuns[region] == nil {
		b.jobRuns[region] = make(map[string][]*JobRun)
	}

	return b.jobRuns[region]
}

// runDelayed schedules fn to run after delay on a tracked goroutine. The
// goroutine exits without invoking fn if the backend's lifecycle context is
// cancelled (Shutdown) before the delay elapses, preventing leaks and
// post-Shutdown state mutation.
func (b *InMemoryBackend) runDelayed(delay time.Duration, fn func()) {
	b.wg.Go(func() {
		select {
		case <-b.svcCtx.Done():
			return
		case <-time.After(delay):
		}
		fn()
	})
}

// Shutdown cancels the backend's lifecycle context and waits for in-flight
// delayed goroutines to finish, bounded by ctx. After Shutdown the backend
// no longer schedules state transitions.
func (b *InMemoryBackend) Shutdown(ctx context.Context) {
	if b.cancel != nil {
		b.cancel()
	}

	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}
}

func (b *InMemoryBackend) Region() string    { return b.defaultRegion }
func (b *InMemoryBackend) AccountID() string { return b.accountID }

func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()
	b.registry.ResetAll()
	b.jobRuns = make(map[string]map[string][]*JobRun)
}

func (b *InMemoryBackend) datasetARN(region, name string) string {
	return arn.Build("databrew", region, b.accountID, "dataset/"+name)
}

func (b *InMemoryBackend) recipeARN(region, name string) string {
	return arn.Build("databrew", region, b.accountID, "recipe/"+name)
}

func (b *InMemoryBackend) projectARN(region, name string) string {
	return arn.Build("databrew", region, b.accountID, "project/"+name)
}

func (b *InMemoryBackend) jobARN(region, name string) string {
	return arn.Build("databrew", region, b.accountID, "job/"+name)
}

func (b *InMemoryBackend) rulesetARN(region, name string) string {
	return arn.Build("databrew", region, b.accountID, "ruleset/"+name)
}

func (b *InMemoryBackend) scheduleARN(region, name string) string {
	return arn.Build("databrew", region, b.accountID, "schedule/"+name)
}

func (b *InMemoryBackend) CreateDataset(
	ctx context.Context,
	name, format string,
	input DatasetInput,
	formatOpts DatasetFormatOptions,
	tags map[string]string,
) (*Dataset, error) {
	b.mu.Lock("CreateDataset")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	if name == "" {
		return nil, ErrValidation
	}
	t := b.datasetsTable(region)
	if t.Has(name) {
		return nil, ErrAlreadyExists
	}
	source := "S3"
	if input.DataCatalogInputDefinition != nil {
		source = "DATA_CATALOG"
	} else if input.DatabaseInputDefinition != nil {
		source = "DATABASE"
	}
	ds := &Dataset{
		Name: name, Arn: b.datasetARN(region, name), Format: format,
		Input: input, FormatOptions: formatOpts, Tags: maps.Clone(tags),
		Source: source, CreateDate: float64(time.Now().Unix()),
		LastModifiedDate: float64(time.Now().Unix()),
	}
	t.Put(ds)

	return ds, nil
}

func (b *InMemoryBackend) DescribeDataset(ctx context.Context, name string) (*Dataset, error) {
	b.mu.RLock("DescribeDataset")
	defer b.mu.RUnlock()
	region := getRegion(ctx, b.defaultRegion)
	ds, ok := b.datasetsTable(region).Get(name)
	if !ok {
		return nil, ErrNotFound
	}
	cp := *ds
	cp.Tags = maps.Clone(ds.Tags)

	return &cp, nil
}

func (b *InMemoryBackend) ListDatasets(
	ctx context.Context,
	maxResults int,
	nextToken string,
) ([]*Dataset, string) {
	b.mu.RLock("ListDatasets")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	t := b.datasetsTable(region)
	keys := snapshotKeys(t, datasetKeyFn)
	pageKeys, next := paginateKeys(keys, maxResults, nextToken)
	out := make([]*Dataset, 0, len(pageKeys))
	for _, k := range pageKeys {
		v, _ := t.Get(k)
		cp := *v
		cp.Tags = maps.Clone(v.Tags)
		out = append(out, &cp)
	}

	return out, next
}

func (b *InMemoryBackend) UpdateDataset(
	ctx context.Context,
	name, format string,
	input DatasetInput,
	formatOpts DatasetFormatOptions,
) error {
	b.mu.Lock("UpdateDataset")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	ds, ok := b.datasetsTable(region).Get(name)
	if !ok {
		return ErrNotFound
	}
	ds.Format = format
	ds.Input = input
	ds.FormatOptions = formatOpts
	ds.LastModifiedDate = float64(time.Now().Unix())

	return nil
}

func (b *InMemoryBackend) DeleteDataset(ctx context.Context, name string) error {
	b.mu.Lock("DeleteDataset")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	if !b.datasetsTable(region).Delete(name) {
		return ErrNotFound
	}

	return nil
}

func (b *InMemoryBackend) CreateRecipe(
	ctx context.Context,
	name, description string,
	steps []RecipeStep,
	tags map[string]string,
) (*Recipe, error) {
	b.mu.Lock("CreateRecipe")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	if name == "" {
		return nil, ErrValidation
	}
	t := b.recipesTable(region)
	if t.Has(name) {
		return nil, ErrAlreadyExists
	}
	r := &Recipe{
		Name: name, Arn: b.recipeARN(region, name), Description: description,
		Steps: steps, Tags: maps.Clone(tags), RecipeVersion: "0.1",
		CreateDate: float64(time.Now().Unix()), LastModifiedDate: float64(time.Now().Unix()),
	}
	t.Put(r)

	return r, nil
}

func (b *InMemoryBackend) DescribeRecipe(ctx context.Context, name string) (*Recipe, error) {
	b.mu.RLock("DescribeRecipe")
	defer b.mu.RUnlock()
	region := getRegion(ctx, b.defaultRegion)
	r, ok := b.recipesTable(region).Get(name)
	if !ok {
		return nil, ErrNotFound
	}
	cp := *r
	cp.Tags = maps.Clone(r.Tags)
	cp.Steps = append([]RecipeStep(nil), r.Steps...)

	return &cp, nil
}

func (b *InMemoryBackend) ListRecipes(
	ctx context.Context,
	maxResults int,
	nextToken string,
) ([]*Recipe, string) {
	b.mu.RLock("ListRecipes")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	t := b.recipesTable(region)
	keys := snapshotKeys(t, recipeKeyFn)
	pageKeys, next := paginateKeys(keys, maxResults, nextToken)
	out := make([]*Recipe, 0, len(pageKeys))
	for _, k := range pageKeys {
		v, _ := t.Get(k)
		cp := *v
		cp.Tags = maps.Clone(v.Tags)
		cp.Steps = append([]RecipeStep(nil), v.Steps...)
		out = append(out, &cp)
	}

	return out, next
}

func (b *InMemoryBackend) PublishRecipe(ctx context.Context, name, description string) error {
	b.mu.Lock("PublishRecipe")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	r, ok := b.recipesTable(region).Get(name)
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

func (b *InMemoryBackend) UpdateRecipe(
	ctx context.Context,
	name, description string,
	steps []RecipeStep,
) error {
	b.mu.Lock("UpdateRecipe")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	r, ok := b.recipesTable(region).Get(name)
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

func (b *InMemoryBackend) DeleteRecipe(ctx context.Context, name string) error {
	b.mu.Lock("DeleteRecipe")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	if !b.recipesTable(region).Delete(name) {
		return ErrNotFound
	}

	return nil
}

func (b *InMemoryBackend) CreateProject(
	ctx context.Context,
	name, datasetName, recipeName, roleArn string,
	sample Sample,
	tags map[string]string,
) (*Project, error) {
	b.mu.Lock("CreateProject")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	if name == "" {
		return nil, ErrValidation
	}
	t := b.projectsTable(region)
	if t.Has(name) {
		return nil, ErrAlreadyExists
	}
	if sample.Type != "" && sample.Type != "FIRST_N" && sample.Type != "LAST_N" &&
		sample.Type != "RANDOM" {
		return nil, fmt.Errorf("%w: invalid Sample.Type %q", ErrValidation, sample.Type)
	}
	p := &Project{
		Name: name, Arn: b.projectARN(region, name), DatasetName: datasetName,
		RecipeName: recipeName, RoleArn: roleArn, Sample: sample,
		Tags: maps.Clone(tags), SessionStatus: "READY",
		CreateDate: float64(time.Now().Unix()), LastModifiedDate: float64(time.Now().Unix()),
	}
	t.Put(p)

	return p, nil
}

func (b *InMemoryBackend) DescribeProject(ctx context.Context, name string) (*Project, error) {
	b.mu.RLock("DescribeProject")
	defer b.mu.RUnlock()
	region := getRegion(ctx, b.defaultRegion)
	p, ok := b.projectsTable(region).Get(name)
	if !ok {
		return nil, ErrNotFound
	}
	cp := *p
	cp.Tags = maps.Clone(p.Tags)

	return &cp, nil
}

func (b *InMemoryBackend) ListProjects(
	ctx context.Context,
	maxResults int,
	nextToken string,
) ([]*Project, string) {
	b.mu.RLock("ListProjects")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	t := b.projectsTable(region)
	keys := snapshotKeys(t, projectKeyFn)
	pageKeys, next := paginateKeys(keys, maxResults, nextToken)
	out := make([]*Project, 0, len(pageKeys))
	for _, k := range pageKeys {
		v, _ := t.Get(k)
		cp := *v
		cp.Tags = maps.Clone(v.Tags)
		out = append(out, &cp)
	}

	return out, next
}

func (b *InMemoryBackend) UpdateProject(
	ctx context.Context,
	name, datasetName, roleArn string,
	sample Sample,
) error {
	b.mu.Lock("UpdateProject")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	p, ok := b.projectsTable(region).Get(name)
	if !ok {
		return ErrNotFound
	}
	if sample.Type != "" && sample.Type != "FIRST_N" && sample.Type != "LAST_N" &&
		sample.Type != "RANDOM" {
		return fmt.Errorf("%w: invalid Sample.Type %q", ErrValidation, sample.Type)
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

func (b *InMemoryBackend) DeleteProject(ctx context.Context, name string) error {
	b.mu.Lock("DeleteProject")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	if !b.projectsTable(region).Delete(name) {
		return ErrNotFound
	}

	return nil
}

func (b *InMemoryBackend) CreateJob(
	ctx context.Context,
	name, jobType, datasetName, projectName, recipeName, roleArn string,
	outputs []Output,
	tags map[string]string,
) (*Job, error) {
	b.mu.Lock("CreateJob")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	if name == "" {
		return nil, ErrValidation
	}
	t := b.jobsTable(region)
	if t.Has(name) {
		return nil, ErrAlreadyExists
	}
	j := &Job{
		Name: name, Arn: b.jobARN(region, name), Type: jobType,
		DatasetName: datasetName, ProjectName: projectName,
		RecipeName: recipeName, RoleArn: roleArn, Outputs: outputs,
		Tags: maps.Clone(tags), CreateDate: float64(time.Now().Unix()),
		LastModifiedDate: float64(time.Now().Unix()),
	}
	if recipeName != "" {
		j.RecipeReference = &RecipeRef{Name: recipeName, RecipeVersion: "LATEST_WORKING"}
	}
	t.Put(j)

	return j, nil
}

func (b *InMemoryBackend) DescribeJob(ctx context.Context, name string) (*Job, error) {
	b.mu.RLock("DescribeJob")
	defer b.mu.RUnlock()
	region := getRegion(ctx, b.defaultRegion)
	j, ok := b.jobsTable(region).Get(name)
	if !ok {
		return nil, ErrNotFound
	}
	cp := *j
	cp.Tags = maps.Clone(j.Tags)
	cp.Outputs = append([]Output(nil), j.Outputs...)

	return &cp, nil
}

func (b *InMemoryBackend) ListJobs(
	ctx context.Context,
	maxResults int,
	nextToken,
	datasetName,
	projectName string,
) ([]*Job, string) {
	b.mu.RLock("ListJobs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	t := b.jobsTable(region)
	keys := snapshotKeys(t, jobKeyFn)
	var filtered []string
	for _, k := range keys {
		j, _ := t.Get(k)
		if datasetName != "" && j.DatasetName != datasetName {
			continue
		}
		if projectName != "" && j.ProjectName != projectName {
			continue
		}
		filtered = append(filtered, k)
	}
	pageKeys, next := paginateKeys(filtered, maxResults, nextToken)
	out := make([]*Job, 0, len(pageKeys))
	for _, k := range pageKeys {
		v, _ := t.Get(k)
		cp := *v
		cp.Tags = maps.Clone(v.Tags)
		cp.Outputs = append([]Output(nil), v.Outputs...)
		out = append(out, &cp)
	}

	return out, next
}

func (b *InMemoryBackend) UpdateJob(
	ctx context.Context,
	name, roleArn string,
	outputs []Output,
	maxCapacity, maxRetries, timeout int,
) error {
	b.mu.Lock("UpdateJob")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	j, ok := b.jobsTable(region).Get(name)
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

func (b *InMemoryBackend) DeleteJob(ctx context.Context, name string) error {
	b.mu.Lock("DeleteJob")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	if !b.jobsTable(region).Delete(name) {
		return ErrNotFound
	}
	runStore := b.jobRunsStore(region)
	delete(runStore, name)

	return nil
}

const (
	// jobRunTransitionDelay is how long to wait before transitioning a job run to SUCCEEDED.
	jobRunTransitionDelay = 100 * time.Millisecond
	// jobRunDefaultExecTime is the simulated execution time returned in a completed job run.
	jobRunDefaultExecTime = 30
)

// StartJobRun creates a new job run with STARTING state, transitioning to SUCCEEDED asynchronously.
func (b *InMemoryBackend) StartJobRun(ctx context.Context, jobName string) (*JobRun, error) {
	b.mu.Lock("StartJobRun")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)
	if !b.jobsTable(region).Has(jobName) {
		return nil, fmt.Errorf("%w: job %q not found", ErrNotFound, jobName)
	}

	run := &JobRun{
		JobName:   jobName,
		RunID:     uuid.New().String(),
		State:     "STARTING",
		StartedOn: float64(time.Now().Unix()),
	}

	runStore := b.jobRunsStore(region)
	runStore[jobName] = append(runStore[jobName], run)

	b.runDelayed(jobRunTransitionDelay, func() {
		b.mu.Lock("StartJobRun.transition")
		defer b.mu.Unlock()
		// Re-check the run still exists: Reset may have cleared jobRuns while
		// the transition was pending, in which case there is nothing to update.
		if !b.jobRunExists(region, jobName, run.RunID) {
			return
		}
		run.State = "SUCCEEDED"
		run.CompletedOn = float64(time.Now().Unix())
		run.ExecutionTime = jobRunDefaultExecTime
	})

	cp := *run

	return &cp, nil
}

// jobRunExists reports whether a run with runID still exists for jobName in the given region.
// Callers must hold b.mu.
func (b *InMemoryBackend) jobRunExists(region, jobName, runID string) bool {
	regionRuns := b.jobRuns[region]
	if regionRuns == nil {
		return false
	}
	for _, r := range regionRuns[jobName] {
		if r.RunID == runID {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) ListJobRuns(
	ctx context.Context,
	jobName string,
	maxResults int,
	nextToken string,
) ([]*JobRun, string, error) {
	b.mu.RLock("ListJobRuns")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	if !b.jobsTable(region).Has(jobName) {
		return nil, "", fmt.Errorf("%w: job %q", ErrNotFound, jobName)
	}

	runStore := b.jobRunsStore(region)
	runs := runStore[jobName]

	// runs are stored in chronological order, ListJobRuns expects reverse chronological
	var reversed []*JobRun
	//nolint:modernize // simple loop
	for i := len(runs) - 1; i >= 0; i-- {
		cp := *runs[i]
		reversed = append(reversed, &cp)
	}

	if maxResults <= 0 {
		maxResults = 100
	}
	startIdx := 0
	if nextToken != "" {
		startIdx = len(reversed)
		for i, r := range reversed {
			// nextToken for runs is RunID (or we can just compare RunID)
			// Wait, the test might rely on RunID for token.
			// Let's assume nextToken is RunID and find the run *after* the token
			if r.RunID == nextToken {
				startIdx = i + 1

				break
			}
		}
	}

	endIdx := startIdx + maxResults
	endIdx = min(endIdx, len(reversed))

	var next string
	if endIdx < len(reversed) {
		next = reversed[endIdx-1].RunID
	}

	if startIdx < len(reversed) {
		return reversed[startIdx:endIdx], next, nil
	}

	return nil, "", nil
}

func (b *InMemoryBackend) CreateRuleset(
	ctx context.Context,
	name, description, targetArn string,
	rules []Rule,
	tags map[string]string,
) (*Ruleset, error) {
	b.mu.Lock("CreateRuleset")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	if name == "" {
		return nil, ErrValidation
	}
	t := b.rulesetsTable(region)
	if t.Has(name) {
		return nil, ErrAlreadyExists
	}
	rs := &Ruleset{
		Name: name, Arn: b.rulesetARN(region, name), Description: description,
		TargetArn: targetArn, Rules: append([]Rule(nil), rules...),
		Tags: maps.Clone(tags), CreateDate: float64(time.Now().Unix()),
		LastModifiedDate: float64(time.Now().Unix()),
	}
	t.Put(rs)

	return rs, nil
}

func (b *InMemoryBackend) DescribeRuleset(ctx context.Context, name string) (*Ruleset, error) {
	b.mu.RLock("DescribeRuleset")
	defer b.mu.RUnlock()
	region := getRegion(ctx, b.defaultRegion)
	rs, ok := b.rulesetsTable(region).Get(name)
	if !ok {
		return nil, ErrNotFound
	}
	cp := *rs
	cp.Tags = maps.Clone(rs.Tags)
	cp.Rules = append([]Rule(nil), rs.Rules...)

	return &cp, nil
}

func (b *InMemoryBackend) ListRulesets(
	ctx context.Context,
	maxResults int,
	nextToken, targetArn string,
) ([]*Ruleset, string) {
	b.mu.RLock("ListRulesets")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	t := b.rulesetsTable(region)
	keys := snapshotKeys(t, rulesetKeyFn)
	filtered := keys
	if targetArn != "" {
		filtered = make([]string, 0, len(keys))
		for _, k := range keys {
			v, _ := t.Get(k)
			if v.TargetArn == targetArn {
				filtered = append(filtered, k)
			}
		}
	}
	pageKeys, next := paginateKeys(filtered, maxResults, nextToken)
	out := make([]*Ruleset, 0, len(pageKeys))
	for _, k := range pageKeys {
		v, _ := t.Get(k)
		cp := *v
		cp.Tags = maps.Clone(v.Tags)
		cp.Rules = append([]Rule(nil), v.Rules...)
		out = append(out, &cp)
	}

	return out, next
}

func (b *InMemoryBackend) UpdateRuleset(
	ctx context.Context,
	name, description string,
	rules []Rule,
) error {
	b.mu.Lock("UpdateRuleset")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	rs, ok := b.rulesetsTable(region).Get(name)
	if !ok {
		return ErrNotFound
	}
	rs.Description = description
	rs.Rules = rules
	rs.LastModifiedDate = float64(time.Now().Unix())

	return nil
}

func (b *InMemoryBackend) DeleteRuleset(ctx context.Context, name string) error {
	b.mu.Lock("DeleteRuleset")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	if !b.rulesetsTable(region).Delete(name) {
		return ErrNotFound
	}

	return nil
}

func (b *InMemoryBackend) CreateSchedule(
	ctx context.Context,
	name string,
	jobNames []string,
	cron string,
	tags map[string]string,
) (*Schedule, error) {
	b.mu.Lock("CreateSchedule")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	if name == "" {
		return nil, ErrValidation
	}
	t := b.schedulesTable(region)
	if t.Has(name) {
		return nil, ErrAlreadyExists
	}
	sc := &Schedule{
		Name: name, Arn: b.scheduleARN(region, name), JobNames: append([]string(nil), jobNames...),
		CronExpression: cron, Tags: maps.Clone(tags),
		CreateDate: float64(time.Now().Unix()), LastModifiedDate: float64(time.Now().Unix()),
	}
	t.Put(sc)

	return sc, nil
}

func (b *InMemoryBackend) DescribeSchedule(ctx context.Context, name string) (*Schedule, error) {
	b.mu.RLock("DescribeSchedule")
	defer b.mu.RUnlock()
	region := getRegion(ctx, b.defaultRegion)
	sc, ok := b.schedulesTable(region).Get(name)
	if !ok {
		return nil, ErrNotFound
	}
	cp := *sc
	cp.Tags = maps.Clone(sc.Tags)
	cp.JobNames = append([]string(nil), sc.JobNames...)

	return &cp, nil
}

func (b *InMemoryBackend) ListSchedules(
	ctx context.Context,
	maxResults int,
	nextToken string,
) ([]*Schedule, string) {
	b.mu.RLock("ListSchedules")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	t := b.schedulesTable(region)
	keys := snapshotKeys(t, scheduleKeyFn)
	pageKeys, next := paginateKeys(keys, maxResults, nextToken)
	out := make([]*Schedule, 0, len(pageKeys))
	for _, k := range pageKeys {
		v, _ := t.Get(k)
		cp := *v
		cp.Tags = maps.Clone(v.Tags)
		cp.JobNames = append([]string(nil), v.JobNames...)
		out = append(out, &cp)
	}

	return out, next
}

func (b *InMemoryBackend) UpdateSchedule(
	ctx context.Context,
	name string,
	jobNames []string,
	cron string,
) error {
	b.mu.Lock("UpdateSchedule")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	sc, ok := b.schedulesTable(region).Get(name)
	if !ok {
		return ErrNotFound
	}
	sc.JobNames = jobNames
	sc.CronExpression = cron
	sc.LastModifiedDate = float64(time.Now().Unix())

	return nil
}

func (b *InMemoryBackend) DeleteSchedule(ctx context.Context, name string) error {
	b.mu.Lock("DeleteSchedule")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	if !b.schedulesTable(region).Delete(name) {
		return ErrNotFound
	}

	return nil
}

func (b *InMemoryBackend) StopJobRun(ctx context.Context, name, runID string) (*JobRun, error) {
	b.mu.Lock("StopJobRun")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)
	runStore := b.jobRunsStore(region)
	runs, ok := runStore[name]
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

func (b *InMemoryBackend) DescribeJobRun(ctx context.Context, name, runID string) (*JobRun, error) {
	b.mu.RLock("DescribeJobRun")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	runStore := b.jobRunsStore(region)
	runs, ok := runStore[name]
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

// FindTagsByArn searches all resources in the request region for a specific ARN and returns its tags.
func (b *InMemoryBackend) FindTagsByArn(
	ctx context.Context,
	arnVal string,
) (map[string]string, error) {
	b.mu.RLock("FindTagsByArn")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	for _, ds := range b.datasetsTable(region).All() {
		if ds.Arn == arnVal {
			return maps.Clone(ds.Tags), nil
		}
	}
	for _, r := range b.recipesTable(region).All() {
		if r.Arn == arnVal {
			return maps.Clone(r.Tags), nil
		}
	}
	for _, p := range b.projectsTable(region).All() {
		if p.Arn == arnVal {
			return maps.Clone(p.Tags), nil
		}
	}
	for _, j := range b.jobsTable(region).All() {
		if j.Arn == arnVal {
			return maps.Clone(j.Tags), nil
		}
	}
	for _, rs := range b.rulesetsTable(region).All() {
		if rs.Arn == arnVal {
			return maps.Clone(rs.Tags), nil
		}
	}
	for _, sc := range b.schedulesTable(region).All() {
		if sc.Arn == arnVal {
			return maps.Clone(sc.Tags), nil
		}
	}

	return nil, ErrNotFound
}

// UpdateTagsByArn searches all resources in the request region and applies tags additions/removals.
func (b *InMemoryBackend) UpdateTagsByArn(
	ctx context.Context,
	arnVal string,
	add map[string]string,
	remove []string,
) error {
	b.mu.Lock("UpdateTagsByArn")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

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

	if b.updateDatasetTags(region, arnVal, applyTags) {
		return nil
	}
	if b.updateRecipeTags(region, arnVal, applyTags) {
		return nil
	}
	if b.updateProjectTags(region, arnVal, applyTags) {
		return nil
	}
	if b.updateJobTags(region, arnVal, applyTags) {
		return nil
	}
	if b.updateRulesetTags(region, arnVal, applyTags) {
		return nil
	}
	if b.updateScheduleTags(region, arnVal, applyTags) {
		return nil
	}

	return ErrNotFound
}

func (b *InMemoryBackend) updateDatasetTags(
	region, arnVal string,
	apply func(map[string]string) map[string]string,
) bool {
	for _, x := range b.datasetsTable(region).All() {
		if x.Arn == arnVal {
			x.Tags = apply(x.Tags)

			return true
		}
	}

	return false
}

func (b *InMemoryBackend) updateRecipeTags(
	region, arnVal string,
	apply func(map[string]string) map[string]string,
) bool {
	for _, x := range b.recipesTable(region).All() {
		if x.Arn == arnVal {
			x.Tags = apply(x.Tags)

			return true
		}
	}

	return false
}

func (b *InMemoryBackend) updateProjectTags(
	region, arnVal string,
	apply func(map[string]string) map[string]string,
) bool {
	for _, x := range b.projectsTable(region).All() {
		if x.Arn == arnVal {
			x.Tags = apply(x.Tags)

			return true
		}
	}

	return false
}

func (b *InMemoryBackend) updateJobTags(
	region, arnVal string,
	apply func(map[string]string) map[string]string,
) bool {
	for _, x := range b.jobsTable(region).All() {
		if x.Arn == arnVal {
			x.Tags = apply(x.Tags)

			return true
		}
	}

	return false
}

func (b *InMemoryBackend) updateRulesetTags(
	region, arnVal string,
	apply func(map[string]string) map[string]string,
) bool {
	for _, x := range b.rulesetsTable(region).All() {
		if x.Arn == arnVal {
			x.Tags = apply(x.Tags)

			return true
		}
	}

	return false
}

func (b *InMemoryBackend) updateScheduleTags(
	region, arnVal string,
	apply func(map[string]string) map[string]string,
) bool {
	for _, x := range b.schedulesTable(region).All() {
		if x.Arn == arnVal {
			x.Tags = apply(x.Tags)

			return true
		}
	}

	return false
}
