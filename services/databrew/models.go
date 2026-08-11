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
	Csv   *CsvOptions   `json:"Csv,omitempty"`
	Excel *ExcelOptions `json:"Excel,omitempty"`
	JSON  *JSONOptions  `json:"Json,omitempty"`
}

// CsvOptions defines how DataBrew reads a CSV dataset input
// (aws-sdk-go-v2/service/databrew/types.CsvOptions).
type CsvOptions struct {
	HeaderRow *bool  `json:"HeaderRow,omitempty"`
	Delimiter string `json:"Delimiter,omitempty"`
}

// ExcelOptions defines how DataBrew reads an Excel dataset input
// (aws-sdk-go-v2/service/databrew/types.ExcelOptions).
type ExcelOptions struct {
	HeaderRow    *bool    `json:"HeaderRow,omitempty"`
	SheetIndexes []int32  `json:"SheetIndexes,omitempty"`
	SheetNames   []string `json:"SheetNames,omitempty"`
}

// JSONOptions defines how DataBrew reads a JSON dataset input
// (aws-sdk-go-v2/service/databrew/types.JSONOptions).
type JSONOptions struct {
	MultiLine bool `json:"MultiLine,omitempty"`
}

// DatasetInput holds the data source for a dataset.
type DatasetInput struct {
	S3InputDefinition          *S3Location       `json:"S3InputDefinition,omitempty"`
	DataCatalogInputDefinition *DataCatalogInput `json:"DataCatalogInputDefinition,omitempty"`
	DatabaseInputDefinition    *DatabaseInput    `json:"DatabaseInputDefinition,omitempty"`
}

// PathOptions defines how DataBrew selects files for a given Amazon S3 path
// in a dataset (aws-sdk-go-v2/service/databrew/types.PathOptions).
type PathOptions struct {
	FilesLimit                *FilesLimit                 `json:"FilesLimit,omitempty"`
	LastModifiedDateCondition *FilterExpression           `json:"LastModifiedDateCondition,omitempty"`
	Parameters                map[string]DatasetParameter `json:"Parameters,omitempty"`
}

// FilesLimit imposes a limit on the number of Amazon S3 files selected for a
// dataset from a connected Amazon S3 path.
type FilesLimit struct {
	Order     string `json:"Order,omitempty"`
	OrderedBy string `json:"OrderedBy,omitempty"`
	MaxFiles  int    `json:"MaxFiles"`
}

// FilterExpression defines parameter-matching conditions (e.g. for dynamic
// dataset paths or datetime parameter filters).
type FilterExpression struct {
	ValuesMap  map[string]string `json:"ValuesMap"`
	Expression string            `json:"Expression"`
}

// DatasetParameter maps a name used in a dataset's Amazon S3 path to its
// definition.
type DatasetParameter struct {
	DatetimeOptions *DatetimeOptions  `json:"DatetimeOptions,omitempty"`
	Filter          *FilterExpression `json:"Filter,omitempty"`
	Name            string            `json:"Name"`
	Type            string            `json:"Type"`
	CreateColumn    bool              `json:"CreateColumn,omitempty"`
}

// DatetimeOptions holds additional options for interpreting datetime
// parameters used in a dataset's Amazon S3 path.
type DatetimeOptions struct {
	Format         string `json:"Format"`
	LocaleCode     string `json:"LocaleCode,omitempty"`
	TimezoneOffset string `json:"TimezoneOffset,omitempty"`
}

// S3Location references an S3 path.
type S3Location struct {
	Bucket      string `json:"Bucket"`
	Key         string `json:"Key,omitempty"`
	BucketOwner string `json:"BucketOwner,omitempty"`
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

// Dataset represents a DataBrew dataset. AccountID mirrors
// aws-sdk-go-v2/service/databrew/types.Dataset's AccountId member -- present
// on ListDatasets items (and harmlessly ignored by the real SDK's
// DescribeDataset deserializer, which has no AccountId case).
type Dataset struct {
	PathOptions      *PathOptions         `json:"PathOptions,omitempty"`
	FormatOptions    DatasetFormatOptions `json:"FormatOptions,omitzero"`
	Input            DatasetInput         `json:"Input,omitzero"`
	Tags             map[string]string    `json:"Tags,omitempty"`
	Name             string               `json:"Name"`
	Arn              string               `json:"ResourceArn"`
	Format           string               `json:"Format,omitempty"`
	Source           string               `json:"Source,omitempty"`
	CreatedBy        string               `json:"CreatedBy,omitempty"`
	LastModifiedBy   string               `json:"LastModifiedBy,omitempty"`
	AccountID        string               `json:"AccountId,omitempty"`
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

// RecipeVersionErrorDetail describes a single recipe version's failure
// within a BatchDeleteRecipeVersion partial-failure response.
type RecipeVersionErrorDetail struct {
	RecipeVersion string `json:"RecipeVersion,omitempty"`
	ErrorCode     string `json:"ErrorCode,omitempty"`
	ErrorMessage  string `json:"ErrorMessage,omitempty"`
}

// Sample describes a data sample for a project.
type Sample struct {
	Type string `json:"Type,omitempty"`
	Size int    `json:"Size,omitempty"`
}

// Project represents a DataBrew project. AccountID mirrors
// aws-sdk-go-v2/service/databrew/types.Project's AccountId member -- see
// Dataset's AccountID doc comment for why it's safe to include on Describe
// responses too.
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
	AccountID        string            `json:"AccountId,omitempty"`
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

// DatabaseTableOutputOptions specifies how and where a recipe job writes
// database output (aws-sdk-go-v2/service/databrew/types.DatabaseTableOutputOptions).
type DatabaseTableOutputOptions struct {
	TempDirectory *S3Location `json:"TempDirectory,omitempty"`
	TableName     string      `json:"TableName"`
}

// S3TableOutputOptions specifies the S3 location for a recipe job's Glue Data
// Catalog output (aws-sdk-go-v2/service/databrew/types.S3TableOutputOptions).
type S3TableOutputOptions struct {
	Location S3Location `json:"Location,omitzero"`
}

// DataCatalogOutput represents where a recipe job writes Glue Data Catalog
// output (aws-sdk-go-v2/service/databrew/types.DataCatalogOutput).
type DataCatalogOutput struct {
	DatabaseOptions *DatabaseTableOutputOptions `json:"DatabaseOptions,omitempty"`
	S3Options       *S3TableOutputOptions       `json:"S3Options,omitempty"`
	DatabaseName    string                      `json:"DatabaseName"`
	TableName       string                      `json:"TableName"`
	CatalogID       string                      `json:"CatalogId,omitempty"`
	Overwrite       bool                        `json:"Overwrite,omitempty"`
}

// DatabaseOutput represents a JDBC database output destination for a recipe
// job (aws-sdk-go-v2/service/databrew/types.DatabaseOutput).
type DatabaseOutput struct {
	DatabaseOptions    *DatabaseTableOutputOptions `json:"DatabaseOptions"`
	GlueConnectionName string                      `json:"GlueConnectionName"`
	DatabaseOutputMode string                      `json:"DatabaseOutputMode,omitempty"`
}

// JobSample determines how many rows a profile job processes
// (aws-sdk-go-v2/service/databrew/types.JobSample).
type JobSample struct {
	Mode string `json:"Mode,omitempty"`
	Size int64  `json:"Size,omitempty"`
}

// Job represents a DataBrew job. AccountID mirrors
// aws-sdk-go-v2/service/databrew/types.Job's AccountId member -- see
// Dataset's AccountID doc comment for why it's safe to include on Describe
// responses too.
type Job struct {
	// ProfileConfiguration is left untyped: it nests 4 levels deep
	// (ProfileConfiguration -> ColumnStatisticsConfigurations ->
	// Statistics(StatisticsConfiguration) -> Overrides([]StatisticOverride)),
	// spans 6 distinct struct shapes, and carries two independent
	// list-of-struct branches (column overrides and entity-detector
	// AllowedStatistics) -- deep enough that a partial model risks silently
	// dropping fields a client can't tell were never implemented.
	ProfileConfiguration     map[string]any      `json:"ProfileConfiguration,omitempty"`
	JobSample                *JobSample          `json:"JobSample,omitempty"`
	Tags                     map[string]string   `json:"Tags,omitempty"`
	RecipeReference          *RecipeRef          `json:"RecipeReference,omitempty"`
	EncryptionMode           string              `json:"EncryptionMode,omitempty"`
	EncryptionKeyArn         string              `json:"EncryptionKeyArn,omitempty"`
	DatasetName              string              `json:"DatasetName,omitempty"`
	ProjectName              string              `json:"ProjectName,omitempty"`
	Name                     string              `json:"Name"`
	CreatedBy                string              `json:"CreatedBy,omitempty"`
	AccountID                string              `json:"AccountId,omitempty"`
	RecipeName               string              `json:"-"`
	RoleArn                  string              `json:"RoleArn,omitempty"`
	LogSubscription          string              `json:"LogSubscription,omitempty"`
	Type                     string              `json:"Type,omitempty"`
	LastModifiedBy           string              `json:"LastModifiedBy,omitempty"`
	Arn                      string              `json:"ResourceArn"`
	ValidationConfigurations []map[string]any    `json:"ValidationConfigurations,omitempty"`
	DataCatalogOutputs       []DataCatalogOutput `json:"DataCatalogOutputs,omitempty"`
	DatabaseOutputs          []DatabaseOutput    `json:"DatabaseOutputs,omitempty"`
	Outputs                  []Output            `json:"Outputs,omitempty"`
	Timeout                  int                 `json:"Timeout,omitempty"`
	MaxRetries               int                 `json:"MaxRetries,omitempty"`
	MaxCapacity              int                 `json:"MaxCapacity,omitempty"`
	LastModifiedDate         float64             `json:"LastModifiedDate,omitempty"`
	CreateDate               float64             `json:"CreateDate,omitempty"`
}

// JobExtras bundles the optional job fields that are specific to one of the
// two job types (profile vs. recipe) but modeled on the shared Job entity,
// so CreateJob/UpdateJob's core positional signature doesn't grow further.
// ProfileConfiguration/JobSample/ValidationConfigurations are populated only
// by the profile-job handlers; DataCatalogOutputs/DatabaseOutputs only by
// the recipe-job handlers. EncryptionMode/EncryptionKeyArn/LogSubscription
// are accepted by both real job types. An unset (zero-value) field on
// UpdateJob leaves the corresponding Job field unchanged. MaxCapacity/
// MaxRetries/Timeout are here (rather than positional, unlike UpdateJob)
// purely because CreateJob has no existing positional slots for them --
// CreateProfileJobInput/CreateRecipeJobInput both accept all three but the
// pre-existing CreateJob signature silently dropped them.
type JobExtras struct {
	// ProfileConfiguration stays untyped -- see Job.ProfileConfiguration's
	// doc comment.
	ProfileConfiguration     map[string]any
	JobSample                *JobSample
	EncryptionMode           string
	EncryptionKeyArn         string
	LogSubscription          string
	DataCatalogOutputs       []DataCatalogOutput
	DatabaseOutputs          []DatabaseOutput
	ValidationConfigurations []map[string]any
	MaxCapacity              int
	MaxRetries               int
	Timeout                  int
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
//
// ListRulesets and DescribeRuleset use genuinely different wire shapes in
// the real SDK: DescribeRulesetOutput carries Rules (the full rule list, no
// AccountId/RuleCount), while ListRulesetsOutput.Rulesets is
// []types.RulesetItem (AccountId + RuleCount -- an integer count -- instead
// of the full Rules list; confirmed against
// awsRestjson1_deserializeDocumentRulesetItem, whose key switch has
// "RuleCount", not "Rules"). Rather than maintaining two marshal shapes,
// this type carries both Rules and RuleCount together: DescribeRuleset's
// real client ignores the extra RuleCount/AccountId keys it doesn't
// recognize, and ListRulesets' real client ignores the extra Rules key it
// doesn't recognize, so one shared struct is wire-safe both ways.
type Ruleset struct {
	Tags             map[string]string `json:"Tags,omitempty"`
	Name             string            `json:"Name"`
	Arn              string            `json:"ResourceArn"`
	Description      string            `json:"Description,omitempty"`
	TargetArn        string            `json:"TargetArn"`
	CreatedBy        string            `json:"CreatedBy,omitempty"`
	LastModifiedBy   string            `json:"LastModifiedBy,omitempty"`
	AccountID        string            `json:"AccountId,omitempty"`
	Rules            []Rule            `json:"Rules"`
	RuleCount        int               `json:"RuleCount"`
	CreateDate       float64           `json:"CreateDate,omitempty"`
	LastModifiedDate float64           `json:"LastModifiedDate,omitempty"`
}

// Schedule represents a DataBrew schedule. AccountID mirrors
// aws-sdk-go-v2/service/databrew/types.Schedule's AccountId member -- see
// Dataset's AccountID doc comment for why it's safe to include on Describe
// responses too.
type Schedule struct {
	Tags             map[string]string `json:"Tags,omitempty"`
	Name             string            `json:"Name"`
	Arn              string            `json:"ResourceArn"`
	CronExpression   string            `json:"CronExpression"`
	CreatedBy        string            `json:"CreatedBy,omitempty"`
	LastModifiedBy   string            `json:"LastModifiedBy,omitempty"`
	AccountID        string            `json:"AccountId,omitempty"`
	JobNames         []string          `json:"JobNames,omitempty"`
	CreateDate       float64           `json:"CreateDate,omitempty"`
	LastModifiedDate float64           `json:"LastModifiedDate,omitempty"`
}
