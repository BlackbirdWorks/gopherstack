package pipes

import (
	"maps"
	"time"
)

const (
	stateRunning      = "RUNNING"
	stateStopped      = "STOPPED"
	stateCreating     = "CREATING"
	stateUpdating     = "UPDATING"
	stateDeleting     = "DELETING"
	stateStarting     = "STARTING"
	stateStopping     = "STOPPING"
	stateCreateFailed = "CREATE_FAILED"
	stateUpdateFailed = "UPDATE_FAILED"
	stateDeleteFailed = "DELETE_FAILED"
	stateStartFailed  = "START_FAILED"
	stateStopFailed   = "STOP_FAILED"

	// stateTransitionDelay is the simulated delay for async state transitions.
	stateTransitionDelay = 10 * time.Millisecond

	maxPipeNameLen  = 64
	maxTagKeyLen    = 128
	maxTagValueLen  = 256
	maxTagsPerPipe  = 50
	maxPipesPerAcct = 1000

	// nextTokenSep separates cursor values in pagination tokens.
	nextTokenSep = "\x00"
)

// FilterCriteria holds event filter patterns applied before forwarding to the target.
type FilterCriteria struct {
	Filters []Filter `json:"Filters,omitempty"`
}

// Filter is a single JSON-pattern filter.
type Filter struct {
	Pattern string `json:"Pattern,omitempty"`
}

// DeadLetterConfig identifies the DLQ for failed pipe events.
type DeadLetterConfig struct {
	Arn string `json:"Arn,omitempty"`
}

// CloudwatchLogsLogDestination is a CloudWatch Logs target.
type CloudwatchLogsLogDestination struct {
	LogGroupArn string `json:"LogGroupArn,omitempty"`
}

// FirehoseLogDestination is a Firehose delivery stream log target.
type FirehoseLogDestination struct {
	DeliveryStreamArn string `json:"DeliveryStreamArn,omitempty"`
}

// S3LogDestination is an S3 bucket log target.
type S3LogDestination struct {
	BucketName   string `json:"BucketName,omitempty"`
	BucketOwner  string `json:"BucketOwner,omitempty"`
	Prefix       string `json:"Prefix,omitempty"`
	OutputFormat string `json:"OutputFormat,omitempty"`
}

// LogDestination wraps possible log destination types.
type LogDestination struct {
	CloudwatchLogsLogDestination *CloudwatchLogsLogDestination `json:"CloudwatchLogsLogDestination,omitempty"`
	FirehoseLogDestination       *FirehoseLogDestination       `json:"FirehoseLogDestination,omitempty"`
	S3LogDestination             *S3LogDestination             `json:"S3LogDestination,omitempty"`
}

// LogConfiguration controls pipe execution logging.
type LogConfiguration struct {
	Level                string           `json:"Level,omitempty"`
	Destinations         []LogDestination `json:"Destinations,omitempty"`
	IncludeExecutionData []string         `json:"IncludeExecutionData,omitempty"`
}

// CloudWatchMetricsDestination configures a CloudWatch metrics destination.
type CloudWatchMetricsDestination struct {
	Namespace string `json:"Namespace,omitempty"`
}

// MetricsDestination wraps the destination for pipe runtime metrics.
type MetricsDestination struct {
	CloudwatchMetrics *CloudWatchMetricsDestination `json:"CloudwatchMetrics,omitempty"`
}

// RuntimeMetricsStreaming configures runtime metrics streaming for a pipe.
type RuntimeMetricsStreaming struct {
	MetricsDestination *MetricsDestination `json:"MetricsDestination,omitempty"`
	Level              string              `json:"Level,omitempty"`
}

// Pipe represents an EventBridge Pipe.
type Pipe struct {
	SourceParameters        *SourceParameters        `json:"sourceParameters,omitempty"`
	TargetParameters        *TargetParameters        `json:"targetParameters,omitempty"`
	DeadLetterConfig        *DeadLetterConfig        `json:"deadLetterConfig,omitempty"`
	LogConfiguration        *LogConfiguration        `json:"logConfiguration,omitempty"`
	EnrichmentParameters    *EnrichmentParameters    `json:"enrichmentParameters,omitempty"`
	RuntimeMetricsStreaming *RuntimeMetricsStreaming `json:"runtimeMetricsStreaming,omitempty"`
	LastModifiedTime        time.Time                `json:"lastModifiedTime"`
	CreationTime            time.Time                `json:"creationTime"`
	Tags                    map[string]string        `json:"tags,omitempty"`
	Description             string                   `json:"description,omitempty"`
	Enrichment              string                   `json:"enrichment,omitempty"`
	KmsKeyIdentifier        string                   `json:"kmsKeyIdentifier,omitempty"`
	Source                  string                   `json:"source"`
	Target                  string                   `json:"target"`
	RoleARN                 string                   `json:"roleArn"`
	StateReason             string                   `json:"stateReason,omitempty"`
	DesiredState            string                   `json:"desiredState"`
	CurrentState            string                   `json:"currentState"`
	AccountID               string                   `json:"accountID"`
	Region                  string                   `json:"region"`
	ARN                     string                   `json:"arn"`
	Name                    string                   `json:"name"`
}

func cloneDeadLetterConfig(src *DeadLetterConfig) *DeadLetterConfig {
	if src == nil {
		return nil
	}
	v := *src

	return &v
}

func clonePipe(p *Pipe) *Pipe {
	cp := *p
	cp.Tags = maps.Clone(p.Tags)
	if p.SourceParameters != nil {
		cp.SourceParameters = cloneSourceParameters(p.SourceParameters)
	}
	if p.TargetParameters != nil {
		cp.TargetParameters = cloneTargetParameters(p.TargetParameters)
	}
	if p.DeadLetterConfig != nil {
		dlc := *p.DeadLetterConfig
		cp.DeadLetterConfig = &dlc
	}
	if p.EnrichmentParameters != nil {
		cp.EnrichmentParameters = cloneEnrichmentParameters(p.EnrichmentParameters)
	}
	if p.LogConfiguration != nil {
		lc := *p.LogConfiguration
		lc.Destinations = append([]LogDestination(nil), p.LogConfiguration.Destinations...)
		lc.IncludeExecutionData = append([]string(nil), p.LogConfiguration.IncludeExecutionData...)
		cp.LogConfiguration = &lc
	}
	if p.RuntimeMetricsStreaming != nil {
		rms := *p.RuntimeMetricsStreaming
		if rms.MetricsDestination != nil {
			md := *rms.MetricsDestination
			if md.CloudwatchMetrics != nil {
				cw := *md.CloudwatchMetrics
				md.CloudwatchMetrics = &cw
			}
			rms.MetricsDestination = &md
		}
		cp.RuntimeMetricsStreaming = &rms
	}

	return &cp
}

// CreatePipeInput holds the full set of fields for pipe creation.
type CreatePipeInput struct {
	Tags                    map[string]string
	SourceParameters        *SourceParameters
	TargetParameters        *TargetParameters
	DeadLetterConfig        *DeadLetterConfig
	LogConfiguration        *LogConfiguration
	EnrichmentParameters    *EnrichmentParameters
	RuntimeMetricsStreaming *RuntimeMetricsStreaming
	Name                    string
	RoleARN                 string
	Source                  string
	Target                  string
	Description             string
	Enrichment              string
	KmsKeyIdentifier        string
	DesiredState            string
}

// UpdatePipeInput holds the fields that can be updated on an existing pipe.
type UpdatePipeInput struct {
	SourceParameters        *SourceParameters
	TargetParameters        *TargetParameters
	DeadLetterConfig        *DeadLetterConfig
	LogConfiguration        *LogConfiguration
	EnrichmentParameters    *EnrichmentParameters
	RuntimeMetricsStreaming *RuntimeMetricsStreaming
	Description             *string
	RoleARN                 string
	Target                  string
	Enrichment              string
	KmsKeyIdentifier        string
	DesiredState            string
}

// ListPipesFilter holds optional query parameters for ListPipes.
type ListPipesFilter struct {
	NamePrefix   string
	DesiredState string
	CurrentState string
	SourcePrefix string
	TargetPrefix string
	NextToken    string
	Limit        int
}

// ListPipesResult holds the paginated result of a ListPipes call.
type ListPipesResult struct {
	NextToken string
	Pipes     []*Pipe
}

// pipeDesiredStateOpts parameterises the common logic of StartPipe and StopPipe.
type pipeDesiredStateOpts struct {
	completeAfter func(region, name string)
	lockName      string
	blockedState  string
	desiredState  string
	transitState  string
}
