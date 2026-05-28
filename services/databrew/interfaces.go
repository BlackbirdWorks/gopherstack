package databrew

// StorageBackend defines the interface for all DataBrew backend operations.
type StorageBackend interface {
	Region() string
	AccountID() string
	Reset()

	// Dataset operations.
	CreateDataset(
		name, format string,
		input DatasetInput,
		formatOpts DatasetFormatOptions,
		tags map[string]string,
	) (*Dataset, error)
	DescribeDataset(name string) (*Dataset, error)
	ListDatasets(maxResults int, nextToken string) ([]*Dataset, string)
	UpdateDataset(name, format string, input DatasetInput, formatOpts DatasetFormatOptions) error
	DeleteDataset(name string) error

	// Recipe operations.
	CreateRecipe(
		name, description string,
		steps []RecipeStep,
		tags map[string]string,
	) (*Recipe, error)
	DescribeRecipe(name string) (*Recipe, error)
	ListRecipes(maxResults int, nextToken string) ([]*Recipe, string)
	PublishRecipe(name, description string) error
	UpdateRecipe(name, description string, steps []RecipeStep) error
	DeleteRecipe(name string) error

	// Project operations.
	CreateProject(
		name, datasetName, recipeName, roleArn string,
		sample Sample,
		tags map[string]string,
	) (*Project, error)
	DescribeProject(name string) (*Project, error)
	ListProjects(maxResults int, nextToken string) ([]*Project, string)
	UpdateProject(name, datasetName, roleArn string, sample Sample) error
	DeleteProject(name string) error

	// Job operations.
	CreateJob(
		name, jobType, datasetName, projectName, recipeName, roleArn string,
		outputs []Output,
		tags map[string]string,
	) (*Job, error)
	DescribeJob(name string) (*Job, error)
	ListJobs(maxResults int, nextToken string) ([]*Job, string)
	UpdateJob(name, roleArn string, outputs []Output, maxCapacity, maxRetries, timeout int) error
	DeleteJob(name string) error
	StartJobRun(jobName string) (*JobRun, error)
	ListJobRuns(jobName string, maxResults int, nextToken string) ([]*JobRun, string, error)
	DescribeJobRun(name, runID string) (*JobRun, error)
	StopJobRun(name, runID string) (*JobRun, error)

	// Ruleset operations.
	CreateRuleset(name, description, targetArn string, rules []Rule, tags map[string]string) (*Ruleset, error)
	DescribeRuleset(name string) (*Ruleset, error)
	ListRulesets(maxResults int, nextToken string) ([]*Ruleset, string)
	UpdateRuleset(name, description string, rules []Rule) error
	DeleteRuleset(name string) error

	// Schedule operations.
	CreateSchedule(name string, jobNames []string, cron string, tags map[string]string) (*Schedule, error)
	DescribeSchedule(name string) (*Schedule, error)
	ListSchedules(maxResults int, nextToken string) ([]*Schedule, string)
	UpdateSchedule(name string, jobNames []string, cron string) error
	DeleteSchedule(name string) error

	// Tag operations.
	FindTagsByArn(arn string) (map[string]string, error)
	UpdateTagsByArn(arn string, add map[string]string, remove []string) error
}

// compile-time assertion that InMemoryBackend implements StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
