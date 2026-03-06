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
	IngestionTime int64  `json:"ingestionTime"`
	Timestamp     int64  `json:"timestamp"`
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
