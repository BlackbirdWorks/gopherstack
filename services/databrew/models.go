package databrew

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
