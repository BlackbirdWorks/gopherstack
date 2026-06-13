package databrew

import "context"

// StorageBackend defines the interface for all DataBrew backend operations.
type StorageBackend interface {
	Region() string
	AccountID() string
	Reset()

	// Dataset operations.
	CreateDataset(
		ctx context.Context,
		name, format string,
		input DatasetInput,
		formatOpts DatasetFormatOptions,
		tags map[string]string,
	) (*Dataset, error)
	DescribeDataset(ctx context.Context, name string) (*Dataset, error)
	ListDatasets(ctx context.Context, maxResults int, nextToken string) ([]*Dataset, string)
	UpdateDataset(
		ctx context.Context,
		name, format string,
		input DatasetInput,
		formatOpts DatasetFormatOptions,
	) error
	DeleteDataset(ctx context.Context, name string) error

	// Recipe operations.
	CreateRecipe(
		ctx context.Context,
		name, description string,
		steps []RecipeStep,
		tags map[string]string,
	) (*Recipe, error)
	DescribeRecipe(ctx context.Context, name string) (*Recipe, error)
	ListRecipes(ctx context.Context, maxResults int, nextToken string) ([]*Recipe, string)
	PublishRecipe(ctx context.Context, name, description string) error
	UpdateRecipe(ctx context.Context, name, description string, steps []RecipeStep) error
	DeleteRecipe(ctx context.Context, name string) error

	// Project operations.
	CreateProject(
		ctx context.Context,
		name, datasetName, recipeName, roleArn string,
		sample Sample,
		tags map[string]string,
	) (*Project, error)
	DescribeProject(ctx context.Context, name string) (*Project, error)
	ListProjects(ctx context.Context, maxResults int, nextToken string) ([]*Project, string)
	UpdateProject(ctx context.Context, name, datasetName, roleArn string, sample Sample) error
	DeleteProject(ctx context.Context, name string) error

	// Job operations.
	CreateJob(
		ctx context.Context,
		name, jobType, datasetName, projectName, recipeName, roleArn string,
		outputs []Output,
		tags map[string]string,
	) (*Job, error)
	DescribeJob(ctx context.Context, name string) (*Job, error)
	ListJobs(ctx context.Context, maxResults int, nextToken string) ([]*Job, string)
	UpdateJob(
		ctx context.Context,
		name, roleArn string,
		outputs []Output,
		maxCapacity, maxRetries, timeout int,
	) error
	DeleteJob(ctx context.Context, name string) error
	StartJobRun(ctx context.Context, jobName string) (*JobRun, error)
	ListJobRuns(
		ctx context.Context,
		jobName string,
		maxResults int,
		nextToken string,
	) ([]*JobRun, string, error)
	DescribeJobRun(ctx context.Context, name, runID string) (*JobRun, error)
	StopJobRun(ctx context.Context, name, runID string) (*JobRun, error)

	// Ruleset operations.
	CreateRuleset(
		ctx context.Context,
		name, description, targetArn string,
		rules []Rule,
		tags map[string]string,
	) (*Ruleset, error)
	DescribeRuleset(ctx context.Context, name string) (*Ruleset, error)
	ListRulesets(ctx context.Context, maxResults int, nextToken string) ([]*Ruleset, string)
	UpdateRuleset(ctx context.Context, name, description string, rules []Rule) error
	DeleteRuleset(ctx context.Context, name string) error

	// Schedule operations.
	CreateSchedule(
		ctx context.Context,
		name string,
		jobNames []string,
		cron string,
		tags map[string]string,
	) (*Schedule, error)
	DescribeSchedule(ctx context.Context, name string) (*Schedule, error)
	ListSchedules(ctx context.Context, maxResults int, nextToken string) ([]*Schedule, string)
	UpdateSchedule(ctx context.Context, name string, jobNames []string, cron string) error
	DeleteSchedule(ctx context.Context, name string) error

	// Tag operations.
	FindTagsByArn(ctx context.Context, arn string) (map[string]string, error)
	UpdateTagsByArn(ctx context.Context, arn string, add map[string]string, remove []string) error
}

// compile-time assertion that InMemoryBackend implements StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
