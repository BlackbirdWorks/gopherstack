package azureeventgrid

import (
	"encoding/json"
	"time"
)

// Event schemas and endpoint types.
const (
	SchemaEventGrid  = "EventGridSchema"
	SchemaCloudEvent = "CloudEventSchemaV1_0"

	EndpointWebHook      = "WebHook"
	EndpointServiceBusQ  = "ServiceBusQueue"
	EndpointServiceBusT  = "ServiceBusTopic"
	EndpointEventHub     = "EventHub"
	EndpointStorageQueue = "StorageQueue"
	EndpointStorageBlob  = "StorageBlob"

	StateSucceeded      = "Succeeded"
	StateAwaitingManual = "AwaitingManualAction"

	DefaultMaxDeliveryAttempts = 30
	DefaultEventTTLMinutes     = 1440
	maxNameLen                 = 50
	maxPendingPerSub           = 10000
	maxPullEventsPerSub        = 10000
)

// RetryPolicy bounds the delivery retries of an event subscription.
type RetryPolicy struct {
	MaxDeliveryAttempts      int `json:"maxDeliveryAttempts"`
	EventTimeToLiveInMinutes int `json:"eventTimeToLiveInMinutes"`
}

// Destination is a delivery or dead-letter endpoint in ARM wire shape.
type Destination struct {
	Properties   map[string]any `json:"properties,omitempty"`
	EndpointType string         `json:"endpointType"`
}

// AdvancedFilter is one entry of filter.advancedFilters.
type AdvancedFilter struct {
	Value        any    `json:"value,omitempty"`
	OperatorType string `json:"operatorType"`
	Key          string `json:"key,omitempty"`
	Values       []any  `json:"values,omitempty"`
}

// Filter selects the events an event subscription receives.
type Filter struct {
	SubjectBeginsWith               string           `json:"subjectBeginsWith"`
	SubjectEndsWith                 string           `json:"subjectEndsWith"`
	IncludedEventTypes              []string         `json:"includedEventTypes,omitempty"`
	AdvancedFilters                 []AdvancedFilter `json:"advancedFilters,omitempty"`
	IsSubjectCaseSensitive          bool             `json:"isSubjectCaseSensitive"`
	EnableAdvancedFilteringOnArrays bool             `json:"enableAdvancedFilteringOnArrays,omitempty"`
}

// Spec is the configurable part of an event subscription, in ARM property shape.
type Spec struct {
	DeadLetterDestination *Destination `json:"deadLetterDestination,omitempty"`
	ExpirationTimeUTC     string       `json:"expirationTimeUtc,omitempty"`
	EventDeliverySchema   string       `json:"eventDeliverySchema,omitempty"`
	Destination           Destination  `json:"destination"`
	Labels                []string     `json:"labels,omitempty"`
	Filter                Filter       `json:"filter"`
	RetryPolicy           RetryPolicy  `json:"retryPolicy"`
}

type delivery struct {
	Due         time.Time       `json:"due"`
	Published   time.Time       `json:"published"`
	LastAttempt time.Time       `json:"lastAttempt"`
	ID          string          `json:"id"`
	LastOutcome string          `json:"lastOutcome,omitempty"`
	Event       json.RawMessage `json:"event"`
	Attempts    int             `json:"attempts"`
	InFlight    bool            `json:"-"`
}

type subscription struct {
	CreatedAt      time.Time   `json:"createdAt"`
	Name           string      `json:"name"`
	State          string      `json:"state"`
	ValidationCode string      `json:"validationCode,omitempty"`
	Pending        []*delivery `json:"pending"`
	Spec           Spec        `json:"spec"`
}

type topic struct {
	CreatedAt   time.Time                `json:"createdAt"`
	Subs        map[string]*subscription `json:"subs"`
	Name        string                   `json:"name"`
	ResourceID  string                   `json:"resourceId"`
	Key1        string                   `json:"key1"`
	Key2        string                   `json:"key2"`
	InputSchema string                   `json:"inputSchema"`
}

type pullEvent struct {
	AvailableAt time.Time       `json:"availableAt"`
	LockedUntil time.Time       `json:"lockedUntil"`
	ExpiresAt   time.Time       `json:"expiresAt"`
	LockToken   string          `json:"lockToken,omitempty"`
	Event       json.RawMessage `json:"event"`
	Count       int             `json:"count"`
}

type pullSub struct {
	CreatedAt   time.Time    `json:"createdAt"`
	Name        string       `json:"name"`
	Events      []*pullEvent `json:"events"`
	Filter      Filter       `json:"filter"`
	LockSeconds int          `json:"lockSeconds"`
	MaxDelivery int          `json:"maxDelivery"`
	TTLSeconds  int          `json:"ttlSeconds"`
}

type nsTopic struct {
	CreatedAt time.Time           `json:"createdAt"`
	Subs      map[string]*pullSub `json:"subs"`
	Name      string              `json:"name"`
}

type namespace struct {
	CreatedAt time.Time           `json:"createdAt"`
	Topics    map[string]*nsTopic `json:"topics"`
	Name      string              `json:"name"`
	Key1      string              `json:"key1"`
	Key2      string              `json:"key2"`
}

// TopicInfo is the exported view of a custom topic.
type TopicInfo struct {
	CreatedAt   time.Time
	Name        string
	ResourceID  string
	InputSchema string
}

// Keys is a primary/secondary access key pair.
type Keys struct {
	Key1 string
	Key2 string
}
