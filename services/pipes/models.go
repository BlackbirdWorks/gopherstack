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

// LogConfiguration controls pipe execution logging. Matches the real API's flat
// shape (aws-sdk-go-v2/service/pipes/types.PipeLogConfiguration): the three
// destinations are direct top-level fields, not wrapped in a "Destinations" list.
type LogConfiguration struct {
	CloudwatchLogsLogDestination *CloudwatchLogsLogDestination `json:"CloudwatchLogsLogDestination,omitempty"`
	FirehoseLogDestination       *FirehoseLogDestination       `json:"FirehoseLogDestination,omitempty"`
	S3LogDestination             *S3LogDestination             `json:"S3LogDestination,omitempty"`
	Level                        string                        `json:"Level,omitempty"`
	IncludeExecutionData         []string                      `json:"IncludeExecutionData,omitempty"`
}

// Pipe represents an EventBridge Pipe.
type Pipe struct {
	SourceParameters     *SourceParameters     `json:"sourceParameters,omitempty"`
	TargetParameters     *TargetParameters     `json:"targetParameters,omitempty"`
	LogConfiguration     *LogConfiguration     `json:"logConfiguration,omitempty"`
	EnrichmentParameters *EnrichmentParameters `json:"enrichmentParameters,omitempty"`
	LastModifiedTime     time.Time             `json:"lastModifiedTime"`
	CreationTime         time.Time             `json:"creationTime"`
	Tags                 map[string]string     `json:"tags,omitempty"`
	Description          string                `json:"description,omitempty"`
	Enrichment           string                `json:"enrichment,omitempty"`
	KmsKeyIdentifier     string                `json:"kmsKeyIdentifier,omitempty"`
	Source               string                `json:"source"`
	Target               string                `json:"target"`
	RoleARN              string                `json:"roleArn"`
	StateReason          string                `json:"stateReason,omitempty"`
	DesiredState         string                `json:"desiredState"`
	CurrentState         string                `json:"currentState"`
	AccountID            string                `json:"accountID"`
	Region               string                `json:"region"`
	ARN                  string                `json:"arn"`
	Name                 string                `json:"name"`
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
	if p.EnrichmentParameters != nil {
		cp.EnrichmentParameters = cloneEnrichmentParameters(p.EnrichmentParameters)
	}
	if p.LogConfiguration != nil {
		lc := *p.LogConfiguration
		if lc.CloudwatchLogsLogDestination != nil {
			v := *lc.CloudwatchLogsLogDestination
			lc.CloudwatchLogsLogDestination = &v
		}
		if lc.FirehoseLogDestination != nil {
			v := *lc.FirehoseLogDestination
			lc.FirehoseLogDestination = &v
		}
		if lc.S3LogDestination != nil {
			v := *lc.S3LogDestination
			lc.S3LogDestination = &v
		}
		lc.IncludeExecutionData = append([]string(nil), p.LogConfiguration.IncludeExecutionData...)
		cp.LogConfiguration = &lc
	}

	return &cp
}

// CreatePipeInput holds the full set of fields for pipe creation.
type CreatePipeInput struct {
	Tags                 map[string]string
	SourceParameters     *SourceParameters
	TargetParameters     *TargetParameters
	LogConfiguration     *LogConfiguration
	EnrichmentParameters *EnrichmentParameters
	Name                 string
	RoleARN              string
	Source               string
	Target               string
	Description          string
	Enrichment           string
	KmsKeyIdentifier     string
	DesiredState         string
}

// UpdatePipeInput holds the fields that can be updated on an existing pipe.
type UpdatePipeInput struct {
	SourceParameters     *SourceParameters
	TargetParameters     *TargetParameters
	LogConfiguration     *LogConfiguration
	EnrichmentParameters *EnrichmentParameters
	Description          *string
	KmsKeyIdentifier     *string
	RoleARN              string
	Target               string
	Enrichment           string
	DesiredState         string
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
