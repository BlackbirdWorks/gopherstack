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

// EventSourceMapping represents a Kinesis → Lambda event source mapping.
type EventSourceMapping struct {
	EventSourceARN        string
	FunctionARN           string
	UUID                  string
	State                 EventSourceMappingState
	LastModified          time.Time
	BatchSize             int
	StartingPosition      string // TRIM_HORIZON or LATEST
	LastProcessingResult  string
}

// CreateEventSourceMappingInput is the input for CreateEventSourceMapping.
type CreateEventSourceMappingInput struct {
	EventSourceARN   string
	FunctionName     string
	StartingPosition string
	BatchSize        int
	Enabled          bool
}

// UpdateEventSourceMappingInput is the input for UpdateEventSourceMapping.
type UpdateEventSourceMappingInput struct {
	UUID      string
	BatchSize int
	Enabled   *bool
}

// jsonESMResponse is the JSON representation of an event source mapping.
type jsonESMResponse struct {
	UUID                  string `json:"UUID"`
	EventSourceARN        string `json:"EventSourceArn"`
	FunctionARN           string `json:"FunctionArn"`
	State                 string `json:"State"`
	BatchSize             int    `json:"BatchSize"`
	StartingPosition      string `json:"StartingPosition,omitempty"`
	LastProcessingResult  string `json:"LastProcessingResult,omitempty"`
}

// jsonListESMResponse is the JSON response for ListEventSourceMappings.
type jsonListESMResponse struct {
	EventSourceMappings []jsonESMResponse `json:"EventSourceMappings"`
}

// toJSONESMResponse converts an EventSourceMapping to its JSON representation.
func toJSONESMResponse(m *EventSourceMapping) jsonESMResponse {
	return jsonESMResponse{
		UUID:                 m.UUID,
		EventSourceARN:       m.EventSourceARN,
		FunctionARN:          m.FunctionARN,
		State:                string(m.State),
		BatchSize:            m.BatchSize,
		StartingPosition:     m.StartingPosition,
		LastProcessingResult: m.LastProcessingResult,
	}
}
