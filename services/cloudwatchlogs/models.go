package cloudwatchlogs

// LogGroup represents a CloudWatch Logs log group.
type LogGroup struct {
	RetentionInDays   *int32 `json:"retentionInDays,omitempty"`
	LogGroupName      string `json:"logGroupName"`
	Arn               string `json:"arn"`
	CreationTime      int64  `json:"creationTime"`
	StoredBytes       int64  `json:"storedBytes"`
	MetricFilterCount int32  `json:"metricFilterCount"`
}

// LogStream represents a CloudWatch Logs log stream.
type LogStream struct {
	FirstEventTimestamp *int64 `json:"firstEventTimestamp,omitempty"`
	LastEventTimestamp  *int64 `json:"lastEventTimestamp,omitempty"`
	LastIngestionTime   *int64 `json:"lastIngestionTime,omitempty"`
	LogStreamName       string `json:"logStreamName"`
	Arn                 string `json:"arn"`
	UploadSequenceToken string `json:"uploadSequenceToken"`
	CreationTime        int64  `json:"creationTime"`
	StoredBytes         int64  `json:"storedBytes"`
}

// InputLogEvent represents a single log event for PutLogEvents.
type InputLogEvent struct {
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"`
}

// OutputLogEvent represents a single log event returned by GetLogEvents/FilterLogEvents.
type OutputLogEvent struct {
	Message       string `json:"message"`
	Ptr           string `json:"ptr,omitempty"`
	IngestionTime int64  `json:"ingestionTime"`
	Timestamp     int64  `json:"timestamp"`
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
	FirstSeen          int64  `json:"firstSeen"`
	LastSeen           int64  `json:"lastSeen"`
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

// SubscriptionFilter represents a CloudWatch Logs subscription filter.
type SubscriptionFilter struct {
	FilterPattern  string `json:"filterPattern"`
	FilterName     string `json:"filterName"`
	LogGroupName   string `json:"logGroupName"`
	DestinationArn string `json:"destinationArn"`
	CreationTime   int64  `json:"creationTime"`
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
	TaskName          string `json:"taskName,omitempty"`
	TaskID            string `json:"taskId"`
	LogGroupName      string `json:"logGroupName"`
	Destination       string `json:"destination"`
	DestinationPrefix string `json:"destinationPrefix,omitempty"`
	Status            string `json:"status"`
	From              int64  `json:"from"`
	To                int64  `json:"to"`
	CreationTime      int64  `json:"creationTime"`
}

// ImportTask represents a CloudWatch Logs import task (from CloudTrail Lake).
type ImportTask struct {
	ImportID             string `json:"importId"`
	ImportSourceArn      string `json:"importSourceArn"`
	ImportRoleArn        string `json:"importRoleArn"`
	ImportDestinationArn string `json:"importDestinationArn"`
	Status               string `json:"status"`
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
type LogAnomalyDetector struct {
	AnomalyDetectorArn    string   `json:"anomalyDetectorArn"`
	DetectorName          string   `json:"detectorName,omitempty"`
	EvaluationFrequency   string   `json:"evaluationFrequency,omitempty"`
	FilterPattern         string   `json:"filterPattern,omitempty"`
	KmsKeyID              string   `json:"kmsKeyId,omitempty"`
	LogGroupArnList       []string `json:"logGroupArnList"`
	AnomalyVisibilityTime int64    `json:"anomalyVisibilityTime,omitempty"`
	CreationTimeStamp     int64    `json:"creationTimeStamp"`
}

// ScheduledQuery represents a CloudWatch Logs scheduled query.
type ScheduledQuery struct {
	Arn                string `json:"arn"`
	Name               string `json:"name"`
	QueryString        string `json:"queryString"`
	ScheduleExpression string `json:"scheduleExpression,omitempty"`
	State              string `json:"state"`
	CreationTime       int64  `json:"creationTime"`
}

// AccountPolicy represents a CloudWatch Logs account-level policy.
type AccountPolicy struct {
	PolicyName     string `json:"policyName"`
	PolicyType     string `json:"policyType"`
	PolicyDocument string `json:"policyDocument,omitempty"`
}

// MetricTransformation describes how to extract a metric from a log event.
type MetricTransformation struct {
	DefaultValue    *float64 `json:"defaultValue,omitempty"`
	MetricNamespace string   `json:"metricNamespace"`
	MetricName      string   `json:"metricName"`
	MetricValue     string   `json:"metricValue"`
}

// MetricFilter represents a CloudWatch Logs metric filter.
type MetricFilter struct {
	RetentionInDays       *int32                 `json:"retentionInDays,omitempty"`
	FilterPattern         string                 `json:"filterPattern"`
	FilterName            string                 `json:"filterName"`
	LogGroupName          string                 `json:"logGroupName"`
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
