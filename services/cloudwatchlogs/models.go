package cloudwatchlogs

import "time"

// LogGroupClass constants match the AWS CloudWatch Logs API enum.
const (
	LogGroupClassStandard         = "STANDARD"
	LogGroupClassInfrequentAccess = "INFREQUENT_ACCESS"
)

// LogGroup represents a CloudWatch Logs log group.
type LogGroup struct {
	RetentionInDays *int32 `json:"retentionInDays,omitempty"`
	LogGroupName    string `json:"logGroupName"`
	Arn             string `json:"arn"`
	LogGroupClass   string `json:"logGroupClass,omitempty"`
	KmsKeyID        string `json:"kmsKeyId,omitempty"`
	// region is the AWS region this group lives under. It is unexported (never
	// marshaled, so the wire response shape is unaffected) and exists solely so
	// the store.Table[LogGroup] holding every region's groups can derive a
	// region-qualified primary key purely from the value itself -- see
	// groupTableKey in store_setup.go. Persistence round-trips it through the
	// separate logGroupSnapshot DTO (see persistence.go) since an unexported
	// field cannot survive Table.Snapshot's json.Marshal on its own.
	region            string
	CreationTime      int64 `json:"creationTime"`
	StoredBytes       int64 `json:"storedBytes"`
	MetricFilterCount int32 `json:"metricFilterCount"`
}

// LogStream represents a CloudWatch Logs log stream.
type LogStream struct {
	FirstEventTimestamp *int64 `json:"firstEventTimestamp,omitempty"`
	LastEventTimestamp  *int64 `json:"lastEventTimestamp,omitempty"`
	LastIngestionTime   *int64 `json:"lastIngestionTime,omitempty"`
	LogStreamName       string `json:"logStreamName"`
	Arn                 string `json:"arn"`
	UploadSequenceToken string `json:"uploadSequenceToken"`
	// region and logGroupName are unexported identity metadata (see LogGroup.region)
	// letting store.Table[LogStream] key every region+group's streams from the
	// value alone. events holds this stream's log events inline (matching the
	// sqs/s3 pattern of nesting child records on the parent entity rather than a
	// separate table) and is likewise unexported; both round-trip through the
	// logStreamSnapshot DTO in persistence.go.
	region       string
	logGroupName string
	events       []*OutputLogEvent
	CreationTime int64 `json:"creationTime"`
	StoredBytes  int64 `json:"storedBytes"`
}

// InputLogEvent represents a single log event for PutLogEvents.
type InputLogEvent struct {
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"`
}

// OutputLogEvent represents a single log event returned by GetLogEvents.
type OutputLogEvent struct {
	Message       string `json:"message"`
	Ptr           string `json:"ptr,omitempty"`
	IngestionTime int64  `json:"ingestionTime"`
	Timestamp     int64  `json:"timestamp"`
}

// FilteredLogEvent represents a single matched event returned by FilterLogEvents.
// Unlike OutputLogEvent (used by GetLogEvents), it carries the originating log
// stream name and a unique eventId, matching the AWS FilteredLogEvent shape.
type FilteredLogEvent struct {
	EventID       string `json:"eventId"`
	LogStreamName string `json:"logStreamName"`
	Message       string `json:"message"`
	IngestionTime int64  `json:"ingestionTime"`
	Timestamp     int64  `json:"timestamp"`
}

// SearchedLogStream indicates whether a log stream was searched completely by
// FilterLogEvents. AWS deprecated populating this list (it returns empty) but the
// field remains part of the response shape.
type SearchedLogStream struct {
	LogStreamName      string `json:"logStreamName"`
	SearchedCompletely bool   `json:"searchedCompletely"`
}

// LogGroupField is a field name and estimated percentage of log events that contain the field.
type LogGroupField struct {
	Name    string `json:"name"`
	Percent int32  `json:"percent"`
}

// Anomaly represents a detected log anomaly.
type Anomaly struct {
	AnomalyDetectorArn string `json:"anomalyDetectorArn"`
	AnomalyID          string `json:"anomalyId"`
	Description        string `json:"description"`
	SuppressedState    string `json:"suppressedState,omitempty"`
	FirstSeen          int64  `json:"firstSeen"`
	LastSeen           int64  `json:"lastSeen"`
	SuppressedDate     int64  `json:"suppressedDate,omitempty"`
	Active             bool   `json:"active"`
}

// ScheduledQueryRunSummary describes a single scheduled query execution.
type ScheduledQueryRunSummary struct {
	Arn            string `json:"arn"`
	FailureReason  string `json:"failureReason,omitempty"`
	RunStatus      string `json:"runStatus"`
	ExecutionTime  int64  `json:"executionTime"`
	InvocationTime int64  `json:"invocationTime"`
}

// Distribution constants for subscription filter event routing.
const (
	DistributionRandom      = "Random"
	DistributionByLogStream = "ByLogStream"
)

// SubscriptionFilter represents a CloudWatch Logs subscription filter.
type SubscriptionFilter struct {
	FilterPattern  string `json:"filterPattern"`
	FilterName     string `json:"filterName"`
	LogGroupName   string `json:"logGroupName"`
	DestinationArn string `json:"destinationArn"`
	RoleArn        string `json:"roleArn,omitempty"`
	Distribution   string `json:"distribution,omitempty"`
	// region is unexported identity metadata (see LogGroup.region) letting
	// store.Table[SubscriptionFilter] key every region+group's filters from the
	// value alone; it round-trips through the subscriptionFilterSnapshot DTO.
	region       string
	CreationTime int64 `json:"creationTime"`
}

// subscriptionLogEvent is one event in a subscription filter delivery payload.
type subscriptionLogEvent struct {
	ID        string `json:"id"`
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"`
}

// subscriptionPayload is the CloudWatch Logs subscription filter delivery payload.
type subscriptionPayload struct {
	SubscriptionFilters []string               `json:"subscriptionFilters"`
	MessageType         string                 `json:"messageType"`
	Owner               string                 `json:"owner"`
	LogGroup            string                 `json:"logGroup"`
	LogStream           string                 `json:"logStream"`
	LogEvents           []subscriptionLogEvent `json:"logEvents"`
}

// QueryStatus represents the lifecycle status of a Logs Insights query.
type QueryStatus string

const (
	QueryStatusScheduled QueryStatus = "Scheduled"
	QueryStatusRunning   QueryStatus = "Running"
	QueryStatusComplete  QueryStatus = "Complete"
	QueryStatusFailed    QueryStatus = "Failed"
	QueryStatusCancelled QueryStatus = "Cancelled"
)

// ResultField is a single field in a Logs Insights result row.
type ResultField struct {
	Field string `json:"field"`
	Value string `json:"value"`
}

// QueryStatistics contains execution statistics for a Logs Insights query.
type QueryStatistics struct {
	BytesScanned   float64 `json:"bytesScanned"`
	RecordsMatched float64 `json:"recordsMatched"`
	RecordsScanned float64 `json:"recordsScanned"`
}

// QueryInfo contains metadata about a Logs Insights query.
type QueryInfo struct {
	QueryID      string      `json:"queryId"`
	QueryString  string      `json:"queryString"`
	LogGroupName string      `json:"logGroupName,omitempty"`
	Status       QueryStatus `json:"status"`
	CreateTime   int64       `json:"createTime"`
}

// ExportTask represents a CloudWatch Logs export task.
type ExportTask struct {
	TaskName            string `json:"taskName,omitempty"`
	TaskID              string `json:"taskId"`
	LogGroupName        string `json:"logGroupName"`
	Destination         string `json:"destination"`
	DestinationPrefix   string `json:"destinationPrefix,omitempty"`
	LogStreamNamePrefix string `json:"logStreamNamePrefix,omitempty"`
	Status              string `json:"status"`
	StatusMessage       string `json:"statusMessage,omitempty"`
	From                int64  `json:"from"`
	To                  int64  `json:"to"`
	CreationTime        int64  `json:"creationTime"`
	CompletionTime      int64  `json:"completionTime,omitempty"`
}

// ImportTask represents a CloudWatch Logs import task (from CloudTrail Lake).
// Status values must be one of the real aws-sdk-go-v2 types.ImportStatus enum
// members: IN_PROGRESS, CANCELLED, COMPLETED, FAILED (NOT "ACTIVE"/"SUCCEEDED",
// which are not real CloudWatch Logs import statuses). The wire key for this
// field is importStatus (aws-sdk-go-v2 types.Import.ImportStatus), not status;
// ImportRoleArn is accepted on CreateImportTask but is deliberately not part
// of the real Import describe/list shape (types.Import has no such field), so
// it is kept here for this backend's own bookkeeping but is excluded when
// serialized to the wire (see wireImportTask in handler_export_tasks.go).
type ImportTask struct {
	ImportID             string `json:"importId"`
	ImportSourceArn      string `json:"importSourceArn"`
	ImportRoleArn        string `json:"-"`
	ImportDestinationArn string `json:"importDestinationArn"`
	Status               string `json:"importStatus"`
	CreationTime         int64  `json:"creationTime"`
	LastUpdatedTime      int64  `json:"lastUpdatedTime"`
}

// Delivery represents a CloudWatch Logs delivery configuration.
type Delivery struct {
	Tags                   map[string]string `json:"tags,omitempty"`
	ID                     string            `json:"id"`
	Arn                    string            `json:"arn"`
	DeliverySourceName     string            `json:"deliverySourceName"`
	DeliveryDestinationArn string            `json:"deliveryDestinationArn"`
	FieldDelimiter         string            `json:"fieldDelimiter,omitempty"`
	RecordFields           []string          `json:"recordFields,omitempty"`
	CreationTime           int64             `json:"creationTime"`
}

// LogAnomalyDetector represents a CloudWatch Logs anomaly detector.
// LogAnomalyDetector represents a CloudWatch Logs log anomaly detector.
// Field-diffed against aws-sdk-go-v2 types.AnomalyDetector /
// GetLogAnomalyDetectorOutput: the status wire key is anomalyDetectorStatus,
// not detectorStatus (a previous version of this struct used the wrong key,
// so a real SDK client would always see an empty/unknown status).
// evaluationLookback and filterAnomalies were also removed here: neither
// exists anywhere in the real SDK (types package, any api_op_*.go input, or
// any doc comment) -- they were invented fields with no wire representation,
// never read anywhere in this codebase either (grep found only their own
// declaration), so they were dead weight rather than a real gap.
type LogAnomalyDetector struct {
	AnomalyDetectorArn    string   `json:"anomalyDetectorArn"`
	DetectorName          string   `json:"detectorName,omitempty"`
	AnomalyDetectorStatus string   `json:"anomalyDetectorStatus,omitempty"`
	EvaluationFrequency   string   `json:"evaluationFrequency,omitempty"`
	FilterPattern         string   `json:"filterPattern,omitempty"`
	KmsKeyID              string   `json:"kmsKeyId,omitempty"`
	LogGroupArnList       []string `json:"logGroupArnList"`
	AnomalyVisibilityTime int64    `json:"anomalyVisibilityTime,omitempty"`
	CreationTimeStamp     int64    `json:"creationTimeStamp"`
	LastModifiedTimeStamp int64    `json:"lastModifiedTimeStamp,omitempty"`
}

// ScheduledQuery represents a CloudWatch Logs scheduled query. The wire key
// for the identifying ARN is scheduledQueryArn, not arn (aws-sdk-go-v2
// GetScheduledQueryOutput.ScheduledQueryArn) -- a previous version of this
// struct used the wrong key, so a real SDK client's ScheduledQueryArn field
// would always deserialize empty. This struct only models the subset of
// GetScheduledQueryOutput this backend currently tracks; see PARITY.md for
// the fields (description, destinationConfiguration, executionRoleArn,
// lastExecutionStatus/lastTriggeredTime/lastUpdatedTime, logGroupIdentifiers,
// queryLanguage, scheduleEndTime/scheduleStartTime, startTimeOffset,
// timezone) not yet implemented.
type ScheduledQuery struct {
	ScheduledQueryArn  string `json:"scheduledQueryArn"`
	Name               string `json:"name"`
	QueryString        string `json:"queryString"`
	ScheduleExpression string `json:"scheduleExpression,omitempty"`
	State              string `json:"state"`
	CreationTime       int64  `json:"creationTime"`
}

// AccountPolicy represents a CloudWatch Logs account-level policy.
type AccountPolicy struct {
	PolicyName        string `json:"policyName"`
	PolicyType        string `json:"policyType"`
	PolicyDocument    string `json:"policyDocument,omitempty"`
	Scope             string `json:"scope,omitempty"`
	SelectionCriteria string `json:"selectionCriteria,omitempty"`
}

// RejectedLogEventsInfo describes log events that were rejected by PutLogEvents.
// Field names/wire keys match aws-sdk-go-v2 types.RejectedLogEventsInfo exactly:
// TooOldLogEventEndIndex (not "...StartIndex") is the exclusive end index of the
// too-old run, mirroring ExpiredLogEventEndIndex's exclusive-end semantics.
type RejectedLogEventsInfo struct {
	TooNewLogEventStartIndex *int32 `json:"tooNewLogEventStartIndex,omitempty"`
	TooOldLogEventEndIndex   *int32 `json:"tooOldLogEventEndIndex,omitempty"`
	ExpiredLogEventEndIndex  *int32 `json:"expiredLogEventEndIndex,omitempty"`
}

// PutLogEventsResult is the result of a PutLogEvents call.
type PutLogEventsResult struct {
	RejectedLogEventsInfo *RejectedLogEventsInfo `json:"rejectedLogEventsInfo,omitempty"`
	NextSequenceToken     string                 `json:"nextSequenceToken"`
}

// MetricTransformation describes how to extract a metric from a log event.
type MetricTransformation struct {
	Dimensions      map[string]string `json:"dimensions,omitempty"`
	DefaultValue    *float64          `json:"defaultValue,omitempty"`
	MetricNamespace string            `json:"metricNamespace"`
	MetricName      string            `json:"metricName"`
	MetricValue     string            `json:"metricValue"`
	Unit            string            `json:"unit,omitempty"`
}

// MetricFilter represents a CloudWatch Logs metric filter.
type MetricFilter struct {
	RetentionInDays       *int32 `json:"retentionInDays,omitempty"`
	FilterPattern         string `json:"filterPattern"`
	FilterName            string `json:"filterName"`
	LogGroupName          string `json:"logGroupName"`
	region                string
	MetricTransformations []MetricTransformation `json:"metricTransformations"`
	CreationTime          int64                  `json:"creationTime"`
}

// MetricFilterMatchRecord represents one event that matched a TestMetricFilter call.
type MetricFilterMatchRecord struct {
	ExtractedValues map[string]string `json:"extractedValues"`
	EventMessage    string            `json:"eventMessage"`
	EventNumber     int64             `json:"eventNumber"`
}

// QueryDefinition represents a saved CloudWatch Logs Insights query definition.
type QueryDefinition struct {
	QueryDefinitionID string   `json:"queryDefinitionId"`
	Name              string   `json:"name"`
	QueryString       string   `json:"queryString"`
	LogGroupNames     []string `json:"logGroupNames,omitempty"`
	LastModified      int64    `json:"lastModified"`
}

// ResourcePolicy represents a CloudWatch Logs resource policy.
type ResourcePolicy struct {
	LastUpdated    time.Time `json:"-"`
	PolicyName     string    `json:"policyName"`
	PolicyDocument string    `json:"policyDocument"`
}

// DeliveryDestination represents a CloudWatch Logs delivery destination.
// These json tags describe this backend's own internal snapshot/persistence
// encoding, not the AWS wire shape: the real aws-sdk-go-v2 types.
// DeliveryDestination nests the target resource ARN under a
// deliveryDestinationConfiguration.destinationResourceArn object (not a bare
// string under that key, as TargetArn's tag alone would suggest) and carries
// a separate deliveryDestinationType enum field; Policy is not part of this
// type at all on the wire (it is returned by the dedicated
// GetDeliveryDestinationPolicy operation instead). See
// deliveryDestinationWireShape in handler_deliveries.go for the actual
// AWS-shaped response mapping used by the handlers.
type DeliveryDestination struct {
	CreatedAt               time.Time         `json:"-"`
	Tags                    map[string]string `json:"tags,omitempty"`
	Name                    string            `json:"name"`
	Arn                     string            `json:"arn"`
	OutputFormat            string            `json:"outputFormat,omitempty"`
	TargetArn               string            `json:"deliveryDestinationConfiguration,omitempty"`
	DeliveryDestinationType string            `json:"deliveryDestinationType,omitempty"`
	Policy                  string            `json:"policy,omitempty"`
}

// DeliverySource represents a CloudWatch Logs delivery source.
// Service is not client-supplied (aws-sdk-go-v2 types.PutDeliverySourceInput
// has no such field); AWS derives it server-side from the ARN of the
// resource that is actually sending logs (types.DeliverySource.Service doc:
// "The Amazon Web Services service that is sending logs"), so this backend
// derives it the same way -- see serviceFromARN in deliveries.go.
type DeliverySource struct {
	CreatedAt    time.Time         `json:"-"`
	Tags         map[string]string `json:"tags,omitempty"`
	Name         string            `json:"name"`
	Arn          string            `json:"arn"`
	LogType      string            `json:"logType,omitempty"`
	Service      string            `json:"service,omitempty"`
	ResourceArns []string          `json:"resourceArns,omitempty"`
}

// CWLDestination represents a CloudWatch Logs log routing destination.
type CWLDestination struct {
	CreatedAt       time.Time `json:"-"`
	DestinationName string    `json:"destinationName"`
	TargetArn       string    `json:"targetArn"`
	RoleArn         string    `json:"roleArn"`
	AccessPolicy    string    `json:"accessPolicy,omitempty"`
	Arn             string    `json:"arn"`
}

// IndexPolicy represents a CloudWatch Logs field index policy.
type IndexPolicy struct {
	LastUpdated        time.Time `json:"lastUpdateTime"`
	LogGroupIdentifier string    `json:"logGroupIdentifier"`
	PolicyDocument     string    `json:"policyDocument"`
}

// Transformer represents a CloudWatch Logs log transformer.
type Transformer struct {
	CreatedAt          time.Time        `json:"-"`
	LogGroupIdentifier string           `json:"logGroupIdentifier"`
	Processors         []map[string]any `json:"transformerConfig"`
}

// CWLIntegration represents a CloudWatch Logs integration (e.g. OpenSearch).
type CWLIntegration struct {
	CreatedAt time.Time `json:"-"`
	Name      string    `json:"integrationName"`
	Type      string    `json:"integrationType"`
	Status    string    `json:"integrationStatus"`
}

// AggregateLogGroupSummary describes aggregated statistics for a single log group.
type AggregateLogGroupSummary struct {
	LogGroupName  string `json:"logGroupName"`
	LogGroupArn   string `json:"logGroupArn"`
	LogGroupClass string `json:"logGroupClass,omitempty"`
	StoredBytes   int64  `json:"storedBytes"`
	LogEventCount int64  `json:"logEventCount"`
}

// TestTransformerOutput is a single transformed log event result. It mirrors the
// AWS TransformedLogRecord shape, carrying both the original and transformed
// message plus the 1-based event number.
type TestTransformerOutput struct {
	EventMessage            string `json:"eventMessage"`
	TransformedEventMessage string `json:"transformedEventMessage"`
	EventNumber             int64  `json:"eventNumber"`
}

// LookupTable represents a CloudWatch Logs lookup table. Field-diffed against
// aws-sdk-go-v2 types.LookupTable (the metadata shape returned by
// DescribeLookupTables) and GetLookupTableOutput (the full-content shape,
// which additionally carries TableBody). Unlike several other CloudWatch
// Logs "ingest from elsewhere" resources, CreateLookupTable's CSV content
// (TableBody) is supplied directly in the request body -- there is no S3
// reference anywhere in this operation's real input/output shape
// (CreateLookupTableInput.TableBody and UpdateLookupTableInput.TableBody are
// both a plain *string of CSV text, verified against the real SDK's
// serializers.go) -- so this backend stores and parses the real CSV content
// (see lookup_tables.go's parseLookupTableCSV) rather than modeling a
// reference to data it never actually reads.
type LookupTable struct {
	LookupTableArn  string   `json:"lookupTableArn"`
	LookupTableName string   `json:"lookupTableName"`
	Description     string   `json:"description,omitempty"`
	KmsKeyID        string   `json:"kmsKeyId,omitempty"`
	TableBody       string   `json:"tableBody"`
	TableFields     []string `json:"tableFields"`
	RecordsCount    int64    `json:"recordsCount"`
	SizeBytes       int64    `json:"sizeBytes"`
	CreatedAt       int64    `json:"createdAt"`
	LastUpdatedTime int64    `json:"lastUpdatedTime"`
}

// syslogSourceTypeVPCE is the only real aws-sdk-go-v2 types.SyslogSourceType
// enum member ("VPCE"): VPC-endpoint ingestion is the only syslog source this
// API supports today.
const syslogSourceTypeVPCE = "VPCE"

// SyslogConfiguration represents a CloudWatch Logs syslog ingestion
// configuration for a log group. Field-diffed against aws-sdk-go-v2
// types.SyslogConfiguration (createdAt/logGroupArn/sourceType/vpcEndpointId).
// PutSyslogConfigurationInput/DeleteSyslogConfigurationInput both require
// LogGroupIdentifier but only optionally accept VpcEndpointId, so this
// backend models at most one syslog configuration per log group (Put
// replaces), matching the per-log-group-identifier keying this codebase
// already uses for IndexPolicy/Transformer (see indexPolicyKeyFn/
// transformerKeyFn in store_setup.go).
type SyslogConfiguration struct {
	LogGroupIdentifier string `json:"logGroupIdentifier"`
	LogGroupArn        string `json:"logGroupArn"`
	SourceType         string `json:"sourceType"`
	VpcEndpointID      string `json:"vpcEndpointId,omitempty"`
	CreatedAt          int64  `json:"createdAt"`
}

// StorageTier constants match the real aws-sdk-go-v2 types.StorageTier enum
// (StorageTierStandard/StorageTierIntelligentTiering).
const (
	StorageTierStandard           = "STANDARD"
	StorageTierIntelligentTiering = "INTELLIGENT_TIERING"
)

// StorageTierPolicy represents the account-level CloudWatch Logs storage
// tier policy. Field-diffed against aws-sdk-go-v2
// GetStorageTierPolicyOutput/PutStorageTierPolicyOutput: both are
// account-scoped, not per-log-group -- GetStorageTierPolicyInput carries no
// fields at all, and PutStorageTierPolicyInput carries only StorageTier (no
// LogGroupIdentifier) -- so this is a singleton, not attached to any
// individual log group. It is a related-but-distinct axis from LogGroup's
// existing LogGroupClass (STANDARD/INFREQUENT_ACCESS, set per log group at
// CreateLogGroup time, gating feature availability): StorageTier
// (STANDARD/INTELLIGENT_TIERING) instead governs whether CloudWatch Logs
// automatically moves already-ingested data between storage tiers
// account-wide over time to optimize cost. Nothing in the real API wires the
// two together (GetStorageTierPolicyOutput carries no log-group-class
// information and LogGroup carries no storage-tier information), so this
// backend keeps them as two independent concepts rather than inventing a
// dependency between them.
type StorageTierPolicy struct {
	StorageTier     string `json:"storageTier"`
	LastUpdatedTime int64  `json:"lastUpdatedTime,omitempty"`
}
