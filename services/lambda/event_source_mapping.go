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
	LastModified                    time.Time               `json:"lastModified"`
	FilterCriteria                  *FilterCriteria         `json:"filterCriteria,omitempty"`
	DestinationConfig               *ESMDestinationConfig   `json:"destinationConfig,omitempty"`
	EventSourceARN                  string                  `json:"eventSourceARN"`
	FunctionARN                     string                  `json:"functionARN"`
	UUID                            string                  `json:"uuid"`
	State                           EventSourceMappingState `json:"state"`
	StartingPosition                string                  `json:"startingPosition"`
	LastProcessingResult            string                  `json:"lastProcessingResult"`
	BatchSize                       int                     `json:"batchSize"`
	MaximumBatchingWindowInSeconds  int                     `json:"maximumBatchingWindowInSeconds,omitempty"`
	TumblingWindowInSeconds         int                     `json:"tumblingWindowInSeconds,omitempty"`
	MaximumRecordAgeInSeconds       int                     `json:"maximumRecordAgeInSeconds,omitempty"`
	MaximumRetryAttempts            int                     `json:"maximumRetryAttempts,omitempty"`
	ParallelizationFactor           int                     `json:"parallelizationFactor,omitempty"`
	BisectBatchOnFunctionError      bool                    `json:"bisectBatchOnFunctionError,omitempty"`
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

// CreateEventSourceMappingInput is the input for CreateEventSourceMapping.
type CreateEventSourceMappingInput struct {
	FilterCriteria                 *FilterCriteria
	DestinationConfig              *ESMDestinationConfig
	EventSourceARN                 string
	FunctionName                   string
	StartingPosition               string
	BatchSize                      int
	MaximumBatchingWindowInSeconds int
	TumblingWindowInSeconds        int
	MaximumRecordAgeInSeconds      int
	MaximumRetryAttempts           int
	ParallelizationFactor          int
	BisectBatchOnFunctionError     bool
	Enabled                        bool
}

// UpdateEventSourceMappingInput is the input for UpdateEventSourceMapping.
type UpdateEventSourceMappingInput struct {
	Enabled                        *bool
	FilterCriteria                 *FilterCriteria
	DestinationConfig              *ESMDestinationConfig
	UUID                           string
	BatchSize                      int
	MaximumBatchingWindowInSeconds int
	TumblingWindowInSeconds        int
	MaximumRecordAgeInSeconds      int
	MaximumRetryAttempts           int
	ParallelizationFactor          int
	BisectBatchOnFunctionError     *bool
}

// jsonESMResponse is the JSON representation of an event source mapping.
type jsonESMResponse struct {
	FilterCriteria                  *FilterCriteria       `json:"FilterCriteria,omitempty"`
	DestinationConfig               *ESMDestinationConfig `json:"DestinationConfig,omitempty"`
	UUID                            string                `json:"UUID"`
	EventSourceARN                  string                `json:"EventSourceArn"`
	FunctionARN                     string                `json:"FunctionArn"`
	State                           string                `json:"State"`
	StartingPosition                string                `json:"StartingPosition,omitempty"`
	LastProcessingResult            string                `json:"LastProcessingResult,omitempty"`
	BatchSize                       int                   `json:"BatchSize"`
	MaximumBatchingWindowInSeconds  int                   `json:"MaximumBatchingWindowInSeconds,omitempty"`
	TumblingWindowInSeconds         int                   `json:"TumblingWindowInSeconds,omitempty"`
	MaximumRecordAgeInSeconds       int                   `json:"MaximumRecordAgeInSeconds,omitempty"`
	MaximumRetryAttempts            int                   `json:"MaximumRetryAttempts,omitempty"`
	ParallelizationFactor           int                   `json:"ParallelizationFactor,omitempty"`
	BisectBatchOnFunctionError      bool                  `json:"BisectBatchOnFunctionError,omitempty"`
}

// jsonListESMResponse is the JSON response for ListEventSourceMappings.
type jsonListESMResponse struct {
	NextMarker          string            `json:"NextMarker,omitempty"`
	EventSourceMappings []jsonESMResponse `json:"EventSourceMappings"`
}

// toJSONESMResponse converts an EventSourceMapping to its JSON representation.
func toJSONESMResponse(m *EventSourceMapping) jsonESMResponse {
	return jsonESMResponse{
		UUID:                            m.UUID,
		EventSourceARN:                  m.EventSourceARN,
		FunctionARN:                     m.FunctionARN,
		State:                           string(m.State),
		BatchSize:                       m.BatchSize,
		StartingPosition:                m.StartingPosition,
		LastProcessingResult:            m.LastProcessingResult,
		FilterCriteria:                  m.FilterCriteria,
		DestinationConfig:               m.DestinationConfig,
		MaximumBatchingWindowInSeconds:  m.MaximumBatchingWindowInSeconds,
		TumblingWindowInSeconds:         m.TumblingWindowInSeconds,
		MaximumRecordAgeInSeconds:       m.MaximumRecordAgeInSeconds,
		MaximumRetryAttempts:            m.MaximumRetryAttempts,
		ParallelizationFactor:           m.ParallelizationFactor,
		BisectBatchOnFunctionError:      m.BisectBatchOnFunctionError,
	}
}
