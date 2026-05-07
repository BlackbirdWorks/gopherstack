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
	ListDatasets() []*Dataset
	UpdateDataset(name, format string, input DatasetInput, formatOpts DatasetFormatOptions) error
	DeleteDataset(name string) error

	// Recipe operations.
	CreateRecipe(
		name, description string,
		steps []RecipeStep,
		tags map[string]string,
	) (*Recipe, error)
	DescribeRecipe(name string) (*Recipe, error)
	ListRecipes() []*Recipe
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
	ListProjects() []*Project
	UpdateProject(name, datasetName, roleArn string, sample Sample) error
	DeleteProject(name string) error

	// Job operations.
	CreateJob(
		name, jobType, datasetName, projectName, recipeName, roleArn string,
		outputs []Output,
		tags map[string]string,
	) (*Job, error)
	DescribeJob(name string) (*Job, error)
	ListJobs() []*Job
	UpdateJob(name, roleArn string, outputs []Output, maxCapacity, maxRetries, timeout int) error
	DeleteJob(name string) error
	StartJobRun(jobName string) (*JobRun, error)
	ListJobRuns(jobName string) ([]*JobRun, error)
}

// compile-time assertion that InMemoryBackend implements StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
