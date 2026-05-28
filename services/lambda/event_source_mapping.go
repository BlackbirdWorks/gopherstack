package lambda

import "time"

// EventSourceMappingState represents the lifecycle state of an event source mapping.
type EventSourceMappingState string

const (
	// ESMStateEnabled means the mapping is active and will be invoked.
	ESMStateEnabled EventSourceMappingState = "Enabled"
	// ESMStateDisabled means the mapping is paused.
	ESMStateDisabled EventSourceMappingState = "Disabled"
	// ESMStateEnabling means the mapping is transitioning to Enabled.
	ESMStateEnabling EventSourceMappingState = "Enabling"
	// ESMStateDisabling means the mapping is transitioning to Disabled.
	ESMStateDisabling EventSourceMappingState = "Disabling"
	// ESMStateDeleting means the mapping is being deleted.
	ESMStateDeleting EventSourceMappingState = "Deleting"
)

// EventSourceMapping represents a Lambda event source mapping.
type EventSourceMapping struct {
	LastModified                        time.Time                            `json:"lastModified"`
	FilterCriteria                      *FilterCriteria                      `json:"filterCriteria,omitempty"`
	DestinationConfig                   *ESMDestinationConfig                `json:"destinationConfig,omitempty"`
	AmazonManagedKafkaEventSourceConfig *AmazonManagedKafkaEventSourceConfig `json:"mskConfig,omitempty"`
	SelfManagedKafkaEventSourceConfig   *SelfManagedKafkaEventSourceConfig   `json:"selfManagedKafkaConfig,omitempty"`
	SelfManagedEventSource              *SelfManagedEventSource              `json:"selfManagedEventSource,omitempty"`
	DocumentDBEventSourceConfig         *DocumentDBEventSourceConfig         `json:"docdbConfig,omitempty"`
	EventSourceARN                      string                               `json:"eventSourceARN"`
	FunctionARN                         string                               `json:"functionARN"`
	UUID                                string                               `json:"uuid"`
	State                               EventSourceMappingState              `json:"state"`
	StartingPosition                    string                               `json:"startingPosition"`
	LastProcessingResult                string                               `json:"lastProcessingResult"`
	SourceAccessConfigurations          []SourceAccessConfiguration          `json:"sourceAccessConfigurations,omitempty"`
	Topics                              []string                             `json:"topics,omitempty"`
	Queues                              []string                             `json:"queues,omitempty"`
	BatchSize                           int                                  `json:"batchSize"`
	MaximumBatchingWindowInSeconds      int                                  `json:"maxBatchingWindowSecs,omitempty"`
	TumblingWindowInSeconds             int                                  `json:"tumblingWindowInSeconds,omitempty"`
	MaximumRecordAgeInSeconds           int                                  `json:"maximumRecordAgeInSeconds,omitempty"`
	MaximumRetryAttempts                int                                  `json:"maximumRetryAttempts,omitempty"`
	ParallelizationFactor               int                                  `json:"parallelizationFactor,omitempty"`
	BisectBatchOnFunctionError          bool                                 `json:"bisectBatchOnFunctionError,omitempty"`
}

// ESMDestinationConfig holds on-failure destination for event source mappings.
type ESMDestinationConfig struct {
	OnFailure *ESMDestination `json:"OnFailure,omitempty"`
}

// ESMDestination holds the ARN for an event source mapping destination.
type ESMDestination struct {
	Destination string `json:"Destination"`
}

// FilterCriteria mirrors the AWS Lambda EventSourceMapping FilterCriteria field.
// Each filter is a JSON pattern object; AWS allows up to 5 filters per mapping.
type FilterCriteria struct {
	Filters []Filter `json:"Filters,omitempty"`
}

// Filter is a single filter pattern. Pattern is a serialized JSON document
// describing the expected event shape (AWS event-pattern matching syntax).
type Filter struct {
	Pattern string `json:"Pattern,omitempty"`
}

// AmazonManagedKafkaEventSourceConfig holds configuration for Amazon MSK event sources.
type AmazonManagedKafkaEventSourceConfig struct {
	ConsumerGroupID string `json:"ConsumerGroupId,omitempty"`
}

// SelfManagedKafkaEventSourceConfig holds configuration for self-managed Apache Kafka event sources.
type SelfManagedKafkaEventSourceConfig struct {
	ConsumerGroupID string `json:"ConsumerGroupId,omitempty"`
}

// SelfManagedEventSource holds the bootstrap broker endpoints for a self-managed Kafka cluster.
// Endpoints is a map from endpoint type (e.g. "KAFKA_BOOTSTRAP_SERVERS") to list of broker addresses.
type SelfManagedEventSource struct {
	Endpoints map[string][]string `json:"Endpoints,omitempty"`
}

// DocumentDBEventSourceConfig holds configuration for Amazon DocumentDB event sources.
type DocumentDBEventSourceConfig struct {
	CollectionName string `json:"CollectionName,omitempty"`
	DatabaseName   string `json:"DatabaseName,omitempty"`
	FullDocument   string `json:"FullDocument,omitempty"`
}

// SourceAccessConfiguration specifies an auth protocol, VPC component, or virtual host
// used to secure access to an event source.
type SourceAccessConfiguration struct {
	URI  string `json:"URI,omitempty"`
	Type string `json:"Type,omitempty"`
}

// CreateEventSourceMappingInput is the input for CreateEventSourceMapping.
type CreateEventSourceMappingInput struct {
	FilterCriteria                      *FilterCriteria
	DestinationConfig                   *ESMDestinationConfig
	AmazonManagedKafkaEventSourceConfig *AmazonManagedKafkaEventSourceConfig
	SelfManagedKafkaEventSourceConfig   *SelfManagedKafkaEventSourceConfig
	SelfManagedEventSource              *SelfManagedEventSource
	DocumentDBEventSourceConfig         *DocumentDBEventSourceConfig
	EventSourceARN                      string
	FunctionName                        string
	StartingPosition                    string
	SourceAccessConfigurations          []SourceAccessConfiguration
	Topics                              []string
	Queues                              []string
	BatchSize                           int
	MaximumBatchingWindowInSeconds      int
	TumblingWindowInSeconds             int
	MaximumRecordAgeInSeconds           int
	MaximumRetryAttempts                int
	ParallelizationFactor               int
	BisectBatchOnFunctionError          bool
	Enabled                             bool
}

// UpdateEventSourceMappingInput is the input for UpdateEventSourceMapping.
type UpdateEventSourceMappingInput struct {
	Enabled                        *bool
	FilterCriteria                 *FilterCriteria
	DestinationConfig              *ESMDestinationConfig
	BisectBatchOnFunctionError     *bool
	UUID                           string
	SourceAccessConfigurations     []SourceAccessConfiguration
	Topics                         []string
	Queues                         []string
	BatchSize                      int
	MaximumBatchingWindowInSeconds int
	TumblingWindowInSeconds        int
	MaximumRecordAgeInSeconds      int
	MaximumRetryAttempts           int
	ParallelizationFactor          int
}

// jsonESMResponse is the JSON representation of an event source mapping.
type jsonESMResponse struct {
	FilterCriteria                      *FilterCriteria                      `json:"FilterCriteria,omitempty"`
	DestinationConfig                   *ESMDestinationConfig                `json:"DestinationConfig,omitempty"`
	AmazonManagedKafkaEventSourceConfig *AmazonManagedKafkaEventSourceConfig `json:"AmazonManagedKafkaEventSourceConfig,omitempty"` //nolint:lll // AWS field name
	SelfManagedKafkaEventSourceConfig   *SelfManagedKafkaEventSourceConfig   `json:"SelfManagedKafkaEventSourceConfig,omitempty"`   //nolint:lll // AWS field name
	SelfManagedEventSource              *SelfManagedEventSource              `json:"SelfManagedEventSource,omitempty"`
	DocumentDBEventSourceConfig         *DocumentDBEventSourceConfig         `json:"DocumentDBEventSourceConfig,omitempty"`
	UUID                                string                               `json:"UUID"`
	EventSourceARN                      string                               `json:"EventSourceArn"`
	FunctionARN                         string                               `json:"FunctionArn"`
	State                               string                               `json:"State"`
	StartingPosition                    string                               `json:"StartingPosition,omitempty"`
	LastProcessingResult                string                               `json:"LastProcessingResult,omitempty"`
	SourceAccessConfigurations          []SourceAccessConfiguration          `json:"SourceAccessConfigurations,omitempty"`
	Topics                              []string                             `json:"Topics,omitempty"`
	Queues                              []string                             `json:"Queues,omitempty"`
	BatchSize                           int                                  `json:"BatchSize"`
	MaximumBatchingWindowInSeconds      int                                  `json:"MaximumBatchingWindowInSeconds,omitempty"` //nolint:lll // AWS field name
	TumblingWindowInSeconds             int                                  `json:"TumblingWindowInSeconds,omitempty"`
	MaximumRecordAgeInSeconds           int                                  `json:"MaximumRecordAgeInSeconds,omitempty"`
	MaximumRetryAttempts                int                                  `json:"MaximumRetryAttempts,omitempty"`
	ParallelizationFactor               int                                  `json:"ParallelizationFactor,omitempty"`
	BisectBatchOnFunctionError          bool                                 `json:"BisectBatchOnFunctionError,omitempty"`
}

// jsonListESMResponse is the JSON response for ListEventSourceMappings.
type jsonListESMResponse struct {
	NextMarker          string            `json:"NextMarker,omitempty"`
	EventSourceMappings []jsonESMResponse `json:"EventSourceMappings"`
}

// toJSONESMResponse converts an EventSourceMapping to its JSON representation.
func toJSONESMResponse(m *EventSourceMapping) jsonESMResponse {
	return jsonESMResponse{
		UUID:                                m.UUID,
		EventSourceARN:                      m.EventSourceARN,
		FunctionARN:                         m.FunctionARN,
		State:                               string(m.State),
		BatchSize:                           m.BatchSize,
		StartingPosition:                    m.StartingPosition,
		LastProcessingResult:                m.LastProcessingResult,
		FilterCriteria:                      m.FilterCriteria,
		DestinationConfig:                   m.DestinationConfig,
		AmazonManagedKafkaEventSourceConfig: m.AmazonManagedKafkaEventSourceConfig,
		SelfManagedKafkaEventSourceConfig:   m.SelfManagedKafkaEventSourceConfig,
		SelfManagedEventSource:              m.SelfManagedEventSource,
		DocumentDBEventSourceConfig:         m.DocumentDBEventSourceConfig,
		SourceAccessConfigurations:          m.SourceAccessConfigurations,
		Topics:                              m.Topics,
		Queues:                              m.Queues,
		MaximumBatchingWindowInSeconds:      m.MaximumBatchingWindowInSeconds,
		TumblingWindowInSeconds:             m.TumblingWindowInSeconds,
		MaximumRecordAgeInSeconds:           m.MaximumRecordAgeInSeconds,
		MaximumRetryAttempts:                m.MaximumRetryAttempts,
		ParallelizationFactor:               m.ParallelizationFactor,
		BisectBatchOnFunctionError:          m.BisectBatchOnFunctionError,
	}
}
