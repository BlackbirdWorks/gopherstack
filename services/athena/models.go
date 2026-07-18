package athena

// EncryptionConfiguration holds encryption settings for query results.
type EncryptionConfiguration struct {
	EncryptionOption string `json:"EncryptionOption,omitempty"`
	KmsKey           string `json:"KmsKey,omitempty"`
}

// ACLConfiguration controls S3 canned ACL for query results.
type ACLConfiguration struct {
	S3AclOption string `json:"S3AclOption,omitempty"`
}

// CustomerEncCfg holds KMS key for user data encryption.
type CustomerEncCfg struct {
	KmsKey string `json:"KmsKey,omitempty"`
}

// ResultConfiguration holds the configuration for where query results are stored.
type ResultConfiguration struct {
	// ACLConfiguration is tagged "AclConfiguration" (not "ACLConfiguration") to
	// match the real Athena wire shape: aws-sdk-go-v2's generated deserializer
	// switches on the exact-case JSON key "AclConfiguration", so a mismatched
	// tag here would make the SDK silently drop the field.
	ACLConfiguration        *ACLConfiguration       `json:"AclConfiguration,omitempty"`
	EncryptionConfiguration EncryptionConfiguration `json:"EncryptionConfiguration,omitzero"`
	ExpectedBucketOwner     string                  `json:"ExpectedBucketOwner,omitempty"`
	OutputLocation          string                  `json:"OutputLocation,omitempty"`
}

// ResultReuseByAgeConfiguration controls result reuse by result age.
type ResultReuseByAgeConfiguration struct {
	Enabled         bool  `json:"Enabled"`
	MaxAgeInMinutes int32 `json:"MaxAgeInMinutes,omitempty"`
}

// ResultReuseConfiguration controls whether previous query results can be reused.
type ResultReuseConfiguration struct {
	ResultReuseByAgeConfiguration *ResultReuseByAgeConfiguration `json:"ResultReuseByAgeConfiguration,omitempty"`
}

// EngineVersion holds the engine version configuration for a workgroup.
type EngineVersion struct {
	SelectedEngineVersion  string `json:"SelectedEngineVersion,omitempty"`
	EffectiveEngineVersion string `json:"EffectiveEngineVersion,omitempty"`
}

// WorkGroupConfiguration holds configuration for a workgroup.
type WorkGroupConfiguration struct {
	CustomerContentEncryptionConfiguration *CustomerEncCfg     `json:"CustomerContentEncryptionConfiguration,omitempty"`
	ResultConfiguration                    ResultConfiguration `json:"ResultConfiguration,omitzero"`
	EngineVersion                          EngineVersion       `json:"EngineVersion,omitzero"`
	AdditionalConfiguration                string              `json:"AdditionalConfiguration,omitempty"`
	ExecutionRole                          string              `json:"ExecutionRole,omitempty"`
	BytesScannedCutoffPerQuery             int64               `json:"BytesScannedCutoffPerQuery,omitempty"`
	EnableMinEnc                           bool                `json:"EnableMinimumEncryptionConfiguration,omitempty"`
	EnforceWGCfg                           bool                `json:"EnforceWorkGroupConfiguration,omitempty"`
	PublishCWMetrics                       bool                `json:"PublishCloudWatchMetricsEnabled,omitempty"`
	RequesterPays                          bool                `json:"RequesterPaysEnabled,omitempty"`
}

// WorkGroup represents an Athena workgroup.
type WorkGroup struct {
	Name          string                 `json:"Name"`
	Description   string                 `json:"Description,omitempty"`
	State         string                 `json:"State"`
	Tags          map[string]string      `json:"Tags,omitempty"`
	Configuration WorkGroupConfiguration `json:"Configuration,omitzero"`
	CreationTime  float64                `json:"CreationTime,omitempty"`
}

// WorkGroupSummary is a reduced view of a WorkGroup for list responses.
type WorkGroupSummary struct {
	EngineVersion *EngineVersion `json:"EngineVersion,omitempty"`
	Name          string         `json:"Name"`
	Description   string         `json:"Description,omitempty"`
	State         string         `json:"State"`
	CreationTime  float64        `json:"CreationTime,omitempty"`
}

// NamedQuery represents a saved Athena query.
type NamedQuery struct {
	NamedQueryID string `json:"NamedQueryId"`
	Name         string `json:"Name"`
	Description  string `json:"Description,omitempty"`
	Database     string `json:"Database"`
	QueryString  string `json:"QueryString"`
	WorkGroup    string `json:"WorkGroup,omitempty"`
}

// DataCatalog represents an Athena data catalog.
type DataCatalog struct {
	Parameters     map[string]string `json:"Parameters,omitempty"`
	Tags           map[string]string `json:"Tags,omitempty"`
	Name           string            `json:"Name"`
	Type           string            `json:"Type"`
	Description    string            `json:"Description,omitempty"`
	ConnectionType string            `json:"ConnectionType,omitempty"`
	Error          string            `json:"Error,omitempty"`
	Status         string            `json:"Status,omitempty"`
}

// DataCatalogSummary is a reduced view of a DataCatalog for list responses.
type DataCatalogSummary struct {
	CatalogName    string `json:"CatalogName"`
	Type           string `json:"Type"`
	ConnectionType string `json:"ConnectionType,omitempty"`
	Error          string `json:"Error,omitempty"`
	Status         string `json:"Status,omitempty"`
}

// QueryExecutionContext holds the database and catalog for a query execution.
type QueryExecutionContext struct {
	Database string `json:"Database,omitempty"`
	Catalog  string `json:"Catalog,omitempty"`
}

// QueryExecutionStatus holds the status of a query execution.
type QueryExecutionStatus struct {
	AthenaError        *QueryExecutionError `json:"AthenaError,omitempty"`
	State              string               `json:"State"`
	StateChangeReason  string               `json:"StateChangeReason,omitempty"`
	SubmissionDateTime float64              `json:"SubmissionDateTime,omitempty"`
	CompletionDateTime float64              `json:"CompletionDateTime,omitempty"`
}

// QueryExecutionError describes why a query execution reached the FAILED state,
// mirroring the AthenaError shape AWS returns inside QueryExecutionStatus (the
// JSON field is still "AthenaError").
type QueryExecutionError struct {
	ErrorMessage  string `json:"ErrorMessage,omitempty"`
	ErrorCategory int32  `json:"ErrorCategory,omitempty"`
	ErrorType     int32  `json:"ErrorType,omitempty"`
	Retryable     bool   `json:"Retryable,omitempty"`
}

// QueryExecutionStatistics holds statistics for a query execution.
type QueryExecutionStatistics struct {
	DataManifestLocation             string  `json:"DataManifestLocation,omitempty"`
	DpuCount                         float64 `json:"DpuCount,omitempty"`
	EngineExecutionTimeInMillis      int64   `json:"EngineExecutionTimeInMillis,omitempty"`
	DataScannedInBytes               int64   `json:"DataScannedInBytes,omitempty"`
	QueryPlanningTimeInMillis        int64   `json:"QueryPlanningTimeInMillis,omitempty"`
	QueryQueueTimeInMillis           int64   `json:"QueryQueueTimeInMillis,omitempty"`
	ServicePreProcessingTimeInMillis int64   `json:"ServicePreProcessingTimeInMillis,omitempty"`
	ServiceProcessingTimeInMillis    int64   `json:"ServiceProcessingTimeInMillis,omitempty"`
	TotalExecutionTimeInMillis       int64   `json:"TotalExecutionTimeInMillis,omitempty"`
	ReusedPreviousResult             bool    `json:"ReusedPreviousResult,omitempty"`
}

// QueryExecution represents an Athena query execution.
type QueryExecution struct {
	ResultConfiguration      ResultConfiguration       `json:"ResultConfiguration,omitzero"`
	QueryExecutionContext    QueryExecutionContext     `json:"QueryExecutionContext,omitzero"`
	ResultReuseConfiguration *ResultReuseConfiguration `json:"ResultReuseConfiguration,omitempty"`
	EngineVersion            *EngineVersion            `json:"EngineVersion,omitempty"`
	QueryExecutionID         string                    `json:"QueryExecutionId"`
	Query                    string                    `json:"Query"`
	WorkGroup                string                    `json:"WorkGroup,omitempty"`
	StatementType            string                    `json:"StatementType,omitempty"`
	ExecutionParameters      []string                  `json:"ExecutionParameters,omitempty"`
	Status                   QueryExecutionStatus      `json:"Status"`
	Statistics               QueryExecutionStatistics  `json:"Statistics,omitzero"`
}

// Tag is a key-value pair.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// PreparedStatement represents an Athena prepared statement.
type PreparedStatement struct {
	StatementName    string  `json:"StatementName"`
	WorkGroupName    string  `json:"WorkGroupName"`
	QueryStatement   string  `json:"QueryStatement"`
	Description      string  `json:"Description,omitempty"`
	LastModifiedTime float64 `json:"LastModifiedTime,omitempty"`
}

// PreparedStatementSummary is a reduced view returned by ListPreparedStatements.
type PreparedStatementSummary struct {
	StatementName    string  `json:"StatementName"`
	LastModifiedTime float64 `json:"LastModifiedTime,omitempty"`
}

// UnprocessedPreparedStatementName describes a prepared statement that could not be retrieved.
type UnprocessedPreparedStatementName struct {
	StatementName string `json:"StatementName"`
	ErrorMessage  string `json:"ErrorMessage"`
}

// UnprocessedNamedQueryID describes a named query that could not be retrieved.
type UnprocessedNamedQueryID struct {
	NamedQueryID string `json:"NamedQueryId"`
	ErrorCode    string `json:"ErrorCode,omitempty"`
	ErrorMessage string `json:"ErrorMessage,omitempty"`
}

// UnprocessedQueryExecutionID describes a query execution that could not be retrieved.
type UnprocessedQueryExecutionID struct {
	QueryExecutionID string `json:"QueryExecutionId"`
	ErrorCode        string `json:"ErrorCode,omitempty"`
	ErrorMessage     string `json:"ErrorMessage,omitempty"`
}

// CapacityAllocation describes a single capacity allocation attempt.
type CapacityAllocation struct {
	Status                string  `json:"Status,omitempty"`
	StatusMessage         string  `json:"StatusMessage,omitempty"`
	RequestTime           float64 `json:"RequestTime,omitempty"`
	RequestCompletionTime float64 `json:"RequestCompletionTime,omitempty"`
}

// CapacityReservation represents an Athena capacity reservation.
type CapacityReservation struct {
	Tags                         map[string]string   `json:"Tags,omitempty"`
	LastAllocation               *CapacityAllocation `json:"LastAllocation,omitempty"`
	Name                         string              `json:"Name"`
	Status                       string              `json:"Status"`
	CreationTime                 float64             `json:"CreationTime,omitempty"`
	LastSuccessfulAllocationTime float64             `json:"LastSuccessfulAllocationTime,omitempty"`
	TargetDpus                   int32               `json:"TargetDpus"`
	AllocatedDpus                int32               `json:"AllocatedDpus"`
}

// NotebookMetadata holds metadata for an Athena notebook.
type NotebookMetadata struct {
	NotebookID       string  `json:"NotebookId"`
	Name             string  `json:"Name"`
	WorkGroup        string  `json:"WorkGroup"`
	Type             string  `json:"Type"`
	CreationTime     float64 `json:"CreationTime,omitempty"`
	LastModifiedTime float64 `json:"LastModifiedTime,omitempty"`
}

// Notebook represents an Athena notebook with its content.
type Notebook struct {
	NotebookID       string  `json:"NotebookId"`
	Name             string  `json:"Name"`
	WorkGroup        string  `json:"WorkGroup"`
	Type             string  `json:"Type"`
	Content          string  `json:"Content"`
	CreationTime     float64 `json:"CreationTime,omitempty"`
	LastModifiedTime float64 `json:"LastModifiedTime,omitempty"`
}

// EngineConfiguration is the engine configuration for a session.
type EngineConfiguration struct {
	AdditionalConfigs      map[string]string `json:"AdditionalConfigs,omitempty"`
	SparkProperties        map[string]string `json:"SparkProperties,omitempty"`
	DefaultExecutorDpuSize int32             `json:"DefaultExecutorDpuSize,omitempty"`
	MaxConcurrentDpus      int32             `json:"MaxConcurrentDpus,omitempty"`
	CoordinatorDpuSize     int32             `json:"CoordinatorDpuSize,omitempty"`
}

// SessionConfiguration is the configuration for a session.
type SessionConfiguration struct {
	EncryptionConfiguration EncryptionConfiguration `json:"EncryptionConfiguration,omitzero"`
	WorkingDirectory        string                  `json:"WorkingDirectory,omitempty"`
	ExecutionRole           string                  `json:"ExecutionRole,omitempty"`
	IdleTimeoutSeconds      int64                   `json:"IdleTimeoutSeconds,omitempty"`
}

// SessionStatus tracks the lifecycle of a session.
type SessionStatus struct {
	StateChangeReason    string  `json:"StateChangeReason,omitempty"`
	State                string  `json:"State"`
	StartDateTime        float64 `json:"StartDateTime,omitempty"`
	LastModifiedDateTime float64 `json:"LastModifiedDateTime,omitempty"`
	EndDateTime          float64 `json:"EndDateTime,omitempty"`
	IdleSinceDateTime    float64 `json:"IdleSinceDateTime,omitempty"`
}

// SessionStatistics holds session-level statistics.
type SessionStatistics struct {
	DpuExecutionInMillis int64 `json:"DpuExecutionInMillis,omitempty"`
}

// Session represents an interactive notebook session.
type Session struct {
	EngineConfiguration  EngineConfiguration  `json:"EngineConfiguration,omitzero"`
	SessionConfiguration SessionConfiguration `json:"SessionConfiguration,omitzero"`
	SessionID            string               `json:"SessionId"`
	Description          string               `json:"Description,omitempty"`
	WorkGroup            string               `json:"WorkGroup"`
	NotebookVersion      string               `json:"NotebookVersion,omitempty"`
	NotebookID           string               `json:"NotebookId,omitempty"`
	Status               SessionStatus        `json:"Status"`
	Statistics           SessionStatistics    `json:"Statistics,omitzero"`
}

// SessionSummary is the list view of a session.
type SessionSummary struct {
	SessionID       string        `json:"SessionId"`
	Description     string        `json:"Description,omitempty"`
	NotebookVersion string        `json:"NotebookVersion,omitempty"`
	Status          SessionStatus `json:"Status,omitzero"`
}

// CalculationStatistics holds calculation runtime stats.
type CalculationStatistics struct {
	DpuExecutionInMillis int64 `json:"DpuExecutionInMillis,omitempty"`
	Progress             int64 `json:"Progress,omitempty"`
}

// CalculationStatus holds the lifecycle of a calculation.
type CalculationStatus struct {
	StateChangeReason  string  `json:"StateChangeReason,omitempty"`
	State              string  `json:"State"`
	SubmissionDateTime float64 `json:"SubmissionDateTime,omitempty"`
	CompletionDateTime float64 `json:"CompletionDateTime,omitempty"`
}

// CalculationResult holds output references for a calculation.
type CalculationResult struct {
	StdOutS3URI   string `json:"StdOutS3Uri,omitempty"`
	StdErrorS3URI string `json:"StdErrorS3Uri,omitempty"`
	ResultS3URI   string `json:"ResultS3Uri,omitempty"`
	ResultType    string `json:"ResultType,omitempty"`
}

// CalculationExecution is a Spark calculation run within a session.
type CalculationExecution struct {
	Result        CalculationResult     `json:"Result,omitzero"`
	CalculationID string                `json:"CalculationExecutionId"`
	SessionID     string                `json:"SessionId"`
	Description   string                `json:"Description,omitempty"`
	WorkingDir    string                `json:"WorkingDirectory,omitempty"`
	CodeBlock     string                `json:"CodeBlock,omitempty"`
	Status        CalculationStatus     `json:"Status"`
	Statistics    CalculationStatistics `json:"Statistics,omitzero"`
}

// CalculationSummary is the list view of a calculation execution.
type CalculationSummary struct {
	CalculationID string            `json:"CalculationExecutionId"`
	Description   string            `json:"Description,omitempty"`
	Status        CalculationStatus `json:"Status,omitzero"`
}

// Column describes a single column in a table.
type Column struct {
	Name    string `json:"Name"`
	Type    string `json:"Type,omitempty"`
	Comment string `json:"Comment,omitempty"`
}

// Database describes an Athena database.
type Database struct {
	Parameters map[string]string `json:"Parameters,omitempty"`
	Name       string            `json:"Name"`
	// Catalog is the data catalog this database belongs to. It is the first
	// component of the composite key store.Table's keyFn derives (see
	// databaseKeyFn in store_setup.go) and of the databasesByCatalog
	// secondary index -- Database itself carries no other notion of which
	// catalog it lives in, since AWS's own Database shape does not either.
	// Tagged json:"-" because a real persistence layer would round-trip it
	// through a dedicated DTO (see services/ses's IdentityRecord.Identity for
	// the established pattern) rather than relying on this field surviving a
	// direct JSON marshal.
	Catalog     string `json:"-"`
	Description string `json:"Description,omitempty"`
}

// TableMetadata describes a table.
type TableMetadata struct {
	Parameters map[string]string `json:"Parameters,omitempty"`
	Name       string            `json:"Name"`
	TableType  string            `json:"TableType,omitempty"`
	// Catalog and Database identify which data catalog and database this
	// table belongs to. Together with Name they form the composite key
	// store.Table's keyFn derives (see tableMetadataKeyFn in
	// store_setup.go); Database alone is the tablesByDatabase secondary
	// index's group key. Tagged json:"-" for the same reason as
	// Database.Catalog above.
	Catalog        string   `json:"-"`
	Database       string   `json:"-"`
	Columns        []Column `json:"Columns,omitempty"`
	PartitionKeys  []Column `json:"PartitionKeys,omitempty"`
	CreateTime     float64  `json:"CreateTime,omitempty"`
	LastAccessTime float64  `json:"LastAccessTime,omitempty"`
}

// CapacityAssignment maps a list of workgroup ARNs to a reservation.
type CapacityAssignment struct {
	WorkGroupNames []string `json:"WorkGroupNames"`
}

// CapacityAssignmentConfiguration is the config attached to a capacity reservation.
type CapacityAssignmentConfiguration struct {
	CapacityReservationName string               `json:"CapacityReservationName"`
	CapacityAssignments     []CapacityAssignment `json:"CapacityAssignments,omitempty"`
}

// Executor describes a Spark executor.
type Executor struct {
	ExecutorID          string  `json:"ExecutorId"`
	ExecutorType        string  `json:"ExecutorType"`
	ExecutorState       string  `json:"ExecutorState"`
	StartDateTime       float64 `json:"StartDateTime,omitempty"`
	TerminationDateTime float64 `json:"TerminationDateTime,omitempty"`
	ExecutorSize        int64   `json:"ExecutorSize,omitempty"`
}

// EngineVersionDescriptor describes an available engine version.
type EngineVersionDescriptor struct {
	AuthEngineVersion      string `json:"AuthEngineVersion,omitempty"`
	EffectiveEngineVersion string `json:"EffectiveEngineVersion,omitempty"`
	SelectedEngineVersion  string `json:"SelectedEngineVersion,omitempty"`
}

// ApplicationDPUSizes lists DPU sizes for a Spark application.
type ApplicationDPUSizes struct {
	ApplicationRuntimeID string  `json:"ApplicationRuntimeId"`
	SupportedDPUSizes    []int32 `json:"SupportedDPUSizes,omitempty"`
}

// QueryRuntimeStatistics aggregates runtime stats for a query execution.
type QueryRuntimeStatistics struct {
	OutputStage QueryStage                     `json:"OutputStage,omitzero"`
	Timeline    QueryRuntimeStatisticsTimeline `json:"Timeline,omitzero"`
	Rows        QueryRuntimeStatisticsRows     `json:"Rows,omitzero"`
}

// QueryRuntimeStatisticsTimeline is the timeline portion.
type QueryRuntimeStatisticsTimeline struct {
	QueryQueueTimeInMillis        int64 `json:"QueryQueueTimeInMillis,omitempty"`
	QueryPlanningTimeInMillis     int64 `json:"QueryPlanningTimeInMillis,omitempty"`
	EngineExecutionTimeInMillis   int64 `json:"EngineExecutionTimeInMillis,omitempty"`
	ServiceProcessingTimeInMillis int64 `json:"ServiceProcessingTimeInMillis,omitempty"`
	TotalExecutionTimeInMillis    int64 `json:"TotalExecutionTimeInMillis,omitempty"`
}

// QueryRuntimeStatisticsRows is the rows portion.
type QueryRuntimeStatisticsRows struct {
	InputRows   int64 `json:"InputRows,omitempty"`
	InputBytes  int64 `json:"InputBytes,omitempty"`
	OutputRows  int64 `json:"OutputRows,omitempty"`
	OutputBytes int64 `json:"OutputBytes,omitempty"`
}

// QueryStage is a single stage in the runtime statistics tree.
type QueryStage struct {
	State         string `json:"State,omitempty"`
	StageID       int64  `json:"StageId,omitempty"`
	OutputBytes   int64  `json:"OutputBytes,omitempty"`
	OutputRows    int64  `json:"OutputRows,omitempty"`
	InputBytes    int64  `json:"InputBytes,omitempty"`
	InputRows     int64  `json:"InputRows,omitempty"`
	ExecutionTime int64  `json:"ExecutionTime,omitempty"`
}
