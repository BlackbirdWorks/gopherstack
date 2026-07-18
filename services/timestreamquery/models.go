package timestreamquery

import "time"

// ScheduledQuery represents a Timestream scheduled query.
type ScheduledQuery struct {
	LastRunTime             time.Time         `json:"last_run_time"`
	CreationTime            time.Time         `json:"creation_time"`
	Tags                    map[string]string `json:"tags"`
	NotificationTopicArn    string            `json:"notification_topic_arn"`
	ScheduleExpression      string            `json:"schedule_expression"`
	ExecutionRoleArn        string            `json:"execution_role_arn"`
	QueryString             string            `json:"query_string"`
	ErrorReportS3BucketName string            `json:"error_report_s3_bucket_name"`
	TargetDatabase          string            `json:"target_database"`
	TargetTable             string            `json:"target_table"`
	State                   string            `json:"state"`
	Name                    string            `json:"name"`
	Arn                     string            `json:"arn"`
}

// ScheduledQuerySummary is a reduced view used in list responses.
type ScheduledQuerySummary struct {
	Arn   string `json:"Arn"`
	Name  string `json:"Name"`
	State string `json:"State"`
}

// QueryResult represents the result of a Query call (typed).
type QueryResult struct {
	QueryID     string
	Rows        []Row
	Columns     []ColumnInfo
	Insights    QueryInsightsResponse
	QueryStatus QueryStatusDetail
	// Cancelled records whether CancelQuery has already been issued for this
	// query. CancelQueryOutput.CancellationMessage is documented as returned
	// "when a CancelQuery request for the query ... has already been issued",
	// i.e. cancellation is idempotent -- a repeat CancelQuery call for the
	// same QueryId must still succeed, not 404.
	Cancelled bool
}

// AccountSettings holds the account-level settings for Timestream Query.
type AccountSettings struct {
	LastUpdatedTime   *time.Time
	MaxQueryTCU       *int32
	QueryCompute      *QueryCompute
	QueryPricingModel string
}

// PrepareQueryResult holds the result of a PrepareQuery call (typed).
type PrepareQueryResult struct {
	QueryString string
	Columns     []ColumnInfo
	Parameters  []ColumnInfo
}

// QueryOptions holds parameters for a Query call.
type QueryOptions struct {
	QueryString  string
	ClientToken  string
	NextToken    string
	InsightsMode string
	MaxRows      int32
}

// QueryPage is the page-level result returned by QueryWithOptions.
type QueryPage struct {
	Insights    *QueryInsightsResponse
	QueryID     string
	NextToken   string
	Rows        []Row
	Columns     []ColumnInfo
	QueryStatus QueryStatusDetail
}

// TimeSeriesDataPoint is one entry in a TimeSeries datum.
type TimeSeriesDataPoint struct {
	Time  string `json:"Time"`
	Value Datum  `json:"Value"`
}

// Datum is the AWS Timestream Query typed union cell.
// Exactly one of the pointer fields is non-nil for any given cell.
type Datum struct {
	ScalarValue     *string               `json:"ScalarValue,omitempty"`
	NullValue       *bool                 `json:"NullValue,omitempty"`
	ArrayValue      []Datum               `json:"ArrayValue,omitempty"`
	RowValue        *Row                  `json:"RowValue,omitempty"`
	TimeSeriesValue []TimeSeriesDataPoint `json:"TimeSeriesValue,omitempty"`
}

// Row is a result row containing typed Data cells.
type Row struct {
	Data []Datum `json:"Data"`
}

// ColumnType describes the type of a Timestream Query column.
// For scalar columns only ScalarType is set; for complex types the sub-
// fields carry the nested ColumnInfo.
type ColumnType struct {
	ArrayColumnInfo                  *ColumnInfo  `json:"ArrayColumnInfo,omitempty"`
	TimeSeriesMeasureValueColumnInfo *ColumnInfo  `json:"TimeSeriesMeasureValueColumnInfo,omitempty"`
	ScalarType                       string       `json:"ScalarType,omitempty"`
	RowColumnInfo                    []ColumnInfo `json:"RowColumnInfo,omitempty"`
}

// ColumnInfo describes one column in a Timestream Query result set.
type ColumnInfo struct {
	Name string     `json:"Name,omitempty"`
	Type ColumnType `json:"Type"`
}

// QueryStatusDetail holds byte-level progress info for a Query call.
type QueryStatusDetail struct {
	ProgressPercentage     float64 `json:"ProgressPercentage"`
	CumulativeBytesScanned int64   `json:"CumulativeBytesScanned"`
	CumulativeBytesMetered int64   `json:"CumulativeBytesMetered"`
}

// clientTokenEntry holds a cached opaque value (a QueryID for Query, or a
// scheduled-query ARN for CreateScheduledQuery) for idempotency.
type clientTokenEntry struct {
	expiresAt time.Time
	value     string
}

// QueryInsightsResponse holds the insights payload returned alongside query results.
type QueryInsightsResponse struct {
	OutputRows           int64   `json:"OutputRows"`
	OutputBytes          int64   `json:"OutputBytes"`
	UnloadPartitionCount int64   `json:"UnloadPartitionCount,omitempty"`
	UnloadWrittenRows    int64   `json:"UnloadWrittenRows,omitempty"`
	UnloadWrittenBytes   int64   `json:"UnloadWrittenBytes,omitempty"`
	QuerySpatialCoverage float64 `json:"QuerySpatialCoverage,omitempty"`
}

// ProvisionedCapacity holds the response-side provisioned-TCU configuration
// returned by DescribeAccountSettings/UpdateAccountSettings
// (types.ProvisionedCapacityResponse on the wire). The active-capacity field
// is named "ActiveQueryTCU" on the response -- "TargetQueryTCU" is the
// *request*-side field name (types.ProvisionedCapacityRequest) and is only
// used transiently while parsing an UpdateAccountSettings request body (see
// QueryComputeUpdate).
type ProvisionedCapacity struct {
	ActiveQueryTCU *int32      `json:"ActiveQueryTCU,omitempty"`
	LastUpdate     *LastUpdate `json:"LastUpdate,omitempty"`
}

// LastUpdate reports the status of the most recent account-settings update
// affecting provisioned capacity (types.LastUpdate on the wire). This
// emulator applies QueryCompute changes synchronously, so Status is always
// SUCCEEDED.
type LastUpdate struct {
	TargetQueryTCU *int32 `json:"TargetQueryTCU,omitempty"`
	Status         string `json:"Status,omitempty"`
}

// QueryCompute holds the compute mode and optional provisioned capacity
// (types.QueryComputeResponse on the wire).
type QueryCompute struct {
	ProvisionedCapacity *ProvisionedCapacity `json:"ProvisionedCapacity,omitempty"`
	ComputeMode         string               `json:"ComputeMode,omitempty"` // ON_DEMAND | PROVISIONED
}

// QueryComputeUpdate is the parsed request-side shape for
// UpdateAccountSettingsInput.QueryCompute (types.QueryComputeRequest on the
// wire): the requested ComputeMode plus, when PROVISIONED, the requested
// TargetQueryTCU.
type QueryComputeUpdate struct {
	TargetQueryTCU *int32
	ComputeMode    string
}

// ExecutionStats holds statistics from a scheduled query execution.
// The wire field is "ExecutionTimeInMillis" (no trailing "ecs") per the real
// aws-sdk-go-v2 deserializer (types.ExecutionStats / awsAwsjson10_deserializeDocumentExecutionStats).
type ExecutionStats struct {
	ExecutionTimeInMillis  int64 `json:"ExecutionTimeInMillis,omitempty"`
	DataWrites             int64 `json:"DataWrites,omitempty"`
	BytesMetered           int64 `json:"BytesMetered,omitempty"`
	CumulativeBytesScanned int64 `json:"CumulativeBytesScanned,omitempty"`
	QueryResultRows        int64 `json:"QueryResultRows,omitempty"`
	RecordsIngested        int64 `json:"RecordsIngested,omitempty"`
}

// ErrorReportLocation holds the S3 error report location for a scheduled query run.
type ErrorReportLocation struct {
	S3ReportLocation *S3ReportLocation `json:"S3ReportLocation,omitempty"`
}

// S3ReportLocation is the S3 bucket + key for an error report.
type S3ReportLocation struct {
	ObjectKey  string `json:"ObjectKey,omitempty"`
	BucketName string `json:"BucketName,omitempty"`
}

// LastRunSummary holds the full summary of the most recent execution of a scheduled query.
// Timestamps are float64 (Unix epoch seconds) to match the AWS JSON protocol 1.0 wire format.
type LastRunSummary struct {
	ErrorReportLocation *ErrorReportLocation `json:"ErrorReportLocation,omitempty"`
	ExecutionStats      *ExecutionStats      `json:"ExecutionStats,omitempty"`
	FailureReason       string               `json:"FailureReason,omitempty"`
	RunStatus           string               `json:"RunStatus,omitempty"`
	TriggerTime         float64              `json:"TriggerTime,omitempty"`
	InvocationTime      float64              `json:"InvocationTime,omitempty"`
}

// TargetDestinationForList summarises the write destination for list responses.
type TargetDestinationForList struct {
	TimestreamDestination *TimestreamDestinationForList `json:"TimestreamDestination,omitempty"`
}

// TimestreamDestinationForList is the Timestream write target in a list summary.
type TimestreamDestinationForList struct {
	DatabaseName string `json:"DatabaseName,omitempty"`
	TableName    string `json:"TableName,omitempty"`
}

// ScheduledQueryListEntry is the enriched summary used in list responses (gap #19).
// Timestamps are float64 (Unix epoch seconds) to match the AWS JSON protocol 1.0 wire format.
type ScheduledQueryListEntry struct {
	TargetDestination      *TargetDestinationForList `json:"TargetDestination,omitempty"`
	Arn                    string                    `json:"Arn"`
	Name                   string                    `json:"Name"`
	State                  string                    `json:"State"`
	LastRunStatus          string                    `json:"LastRunStatus,omitempty"`
	CreationTime           float64                   `json:"CreationTime,omitempty"`
	NextInvocationTime     float64                   `json:"NextInvocationTime,omitempty"`
	PreviousInvocationTime float64                   `json:"PreviousInvocationTime,omitempty"`
}

// ListScheduledQueriesResult is the paginated result of a ListScheduledQueries call.
type ListScheduledQueriesResult struct {
	NextToken string
	Items     []ScheduledQueryListEntry
}
