package cloudwatchlogs

// LogGroup represents a CloudWatch Logs log group.
type LogGroup struct {
	CreationTime      int64  `json:"creationTime"`
	LogGroupName      string `json:"logGroupName"`
	Arn               string `json:"arn"`
	RetentionInDays   *int32 `json:"retentionInDays,omitempty"`
	MetricFilterCount int32  `json:"metricFilterCount"`
	StoredBytes       int64  `json:"storedBytes"`
}

// LogStream represents a CloudWatch Logs log stream.
type LogStream struct {
	CreationTime        int64  `json:"creationTime"`
	FirstEventTimestamp *int64 `json:"firstEventTimestamp,omitempty"`
	LastEventTimestamp  *int64 `json:"lastEventTimestamp,omitempty"`
	LastIngestionTime   *int64 `json:"lastIngestionTime,omitempty"`
	LogStreamName       string `json:"logStreamName"`
	Arn                 string `json:"arn"`
	UploadSequenceToken string `json:"uploadSequenceToken"`
	StoredBytes         int64  `json:"storedBytes"`
}

// InputLogEvent represents a single log event for PutLogEvents.
type InputLogEvent struct {
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"`
}

// OutputLogEvent represents a single log event returned by GetLogEvents/FilterLogEvents.
type OutputLogEvent struct {
	IngestionTime int64  `json:"ingestionTime"`
	Message       string `json:"message"`
	Timestamp     int64  `json:"timestamp"`
}

// ErrorResponse is the standard error format.
type ErrorResponse struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}
