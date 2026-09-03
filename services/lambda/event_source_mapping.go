package lambda

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

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
	FunctionResponseTypes               []string                             `json:"functionResponseTypes,omitempty"`
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
	FunctionResponseTypes               []string
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
	FunctionResponseTypes          []string
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
	LastProcessingResult                string                               `json:"LastProcessingResult,omitempty"`
	UUID                                string                               `json:"UUID"`
	FunctionARN                         string                               `json:"FunctionArn"`
	State                               string                               `json:"State"`
	EventSourceARN                      string                               `json:"EventSourceArn"`
	StartingPosition                    string                               `json:"StartingPosition,omitempty"`
	Queues                              []string                             `json:"Queues,omitempty"`
	SourceAccessConfigurations          []SourceAccessConfiguration          `json:"SourceAccessConfigurations,omitempty"`
	Topics                              []string                             `json:"Topics,omitempty"`
	FunctionResponseTypes               []string                             `json:"FunctionResponseTypes,omitempty"`
	LastModified                        float64                              `json:"LastModified"`
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
		LastModified:                        awstime.Epoch(m.LastModified),
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
		FunctionResponseTypes:               m.FunctionResponseTypes,
		MaximumBatchingWindowInSeconds:      m.MaximumBatchingWindowInSeconds,
		TumblingWindowInSeconds:             m.TumblingWindowInSeconds,
		MaximumRecordAgeInSeconds:           m.MaximumRecordAgeInSeconds,
		MaximumRetryAttempts:                m.MaximumRetryAttempts,
		ParallelizationFactor:               m.ParallelizationFactor,
		BisectBatchOnFunctionError:          m.BisectBatchOnFunctionError,
	}
}

// esmFunctionName normalizes a function reference (bare name or full function ARN)
// to the bare function name used for event-source-mapping indexing.
func esmFunctionName(functionName string) string {
	if !strings.HasPrefix(functionName, "arn:aws:lambda:") {
		return functionName
	}

	// Bug fix (parity-sweep-3): previously took the last colon-separated
	// segment of the ARN, which for a qualified ARN
	// (arn:...:function:my-func:PROD) returned just "PROD" — discarding the
	// actual function name and causing the mapping to be registered (and the
	// poller to invoke) a nonexistent function named after the qualifier.
	// Preserve the "name:qualifier" suffix so the mapping keeps routing to
	// the specific version/alias, matching real Lambda's FunctionArn echo.
	name, qualifier := functionNameAndQualifierFromARN(functionName)
	if qualifier != "" {
		return name + ":" + qualifier
	}

	return name
}

// CreateEventSourceMapping creates a new event source mapping.
func (b *InMemoryBackend) CreateEventSourceMapping(
	input *CreateEventSourceMappingInput,
) (*EventSourceMapping, error) {
	b.mu.Lock("CreateEventSourceMapping")
	defer b.mu.Unlock()

	if input.EventSourceARN == "" {
		return nil, fmt.Errorf("%w: EventSourceARN must not be empty", ErrInvalidParameterValue)
	}

	id := uuid.New().String()
	state := ESMStateEnabled
	if !input.Enabled {
		state = ESMStateDisabled
	}

	batchSize := input.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	startingPosition := input.StartingPosition
	if startingPosition == "" {
		startingPosition = "TRIM_HORIZON"
	}

	// The function may be supplied as a bare name or a full function ARN. Normalize
	// to the bare name so the stored index key matches lookups by name.
	fnARN := arn.Build(
		"lambda",
		b.region,
		b.accountID,
		"function:"+esmFunctionName(input.FunctionName),
	)

	m := &EventSourceMapping{
		UUID:                                id,
		EventSourceARN:                      input.EventSourceARN,
		FunctionARN:                         fnARN,
		State:                               state,
		BatchSize:                           batchSize,
		StartingPosition:                    startingPosition,
		LastProcessingResult:                "No records processed",
		LastModified:                        time.Now(),
		FilterCriteria:                      input.FilterCriteria,
		DestinationConfig:                   input.DestinationConfig,
		AmazonManagedKafkaEventSourceConfig: input.AmazonManagedKafkaEventSourceConfig,
		SelfManagedKafkaEventSourceConfig:   input.SelfManagedKafkaEventSourceConfig,
		SelfManagedEventSource:              input.SelfManagedEventSource,
		DocumentDBEventSourceConfig:         input.DocumentDBEventSourceConfig,
		SourceAccessConfigurations:          input.SourceAccessConfigurations,
		Topics:                              input.Topics,
		Queues:                              input.Queues,
		MaximumBatchingWindowInSeconds:      input.MaximumBatchingWindowInSeconds,
		TumblingWindowInSeconds:             input.TumblingWindowInSeconds,
		MaximumRecordAgeInSeconds:           input.MaximumRecordAgeInSeconds,
		MaximumRetryAttempts:                input.MaximumRetryAttempts,
		ParallelizationFactor:               input.ParallelizationFactor,
		BisectBatchOnFunctionError:          input.BisectBatchOnFunctionError,
		FunctionResponseTypes:               input.FunctionResponseTypes,
	}

	b.eventSourceMappings.Put(m)

	if b.esmByFunctionARN[fnARN] == nil {
		b.esmByFunctionARN[fnARN] = make(map[string]struct{})
	}
	b.esmByFunctionARN[fnARN][id] = struct{}{}

	if input.Enabled && b.kinesisPoller != nil {
		b.kinesisPoller.Notify()
	}

	return m, nil
}

// GetEventSourceMapping retrieves an event source mapping by UUID.
func (b *InMemoryBackend) GetEventSourceMapping(uuid string) (*EventSourceMapping, error) {
	b.mu.RLock("GetEventSourceMapping")
	defer b.mu.RUnlock()

	m, ok := b.eventSourceMappings.Get(uuid)
	if !ok {
		return nil, ErrESMNotFound
	}

	return m, nil
}

// ListEventSourceMappings returns a page of event source mappings, optionally filtered by function name.
func (b *InMemoryBackend) ListEventSourceMappings(
	functionName, eventSourceARN, marker string,
	maxItems int,
) page.Page[*EventSourceMapping] {
	b.mu.RLock("ListEventSourceMappings")
	defer b.mu.RUnlock()

	var result []*EventSourceMapping

	if functionName != "" {
		fnARN := arn.Build(
			"lambda",
			b.region,
			b.accountID,
			"function:"+esmFunctionName(functionName),
		)
		ids := b.esmByFunctionARN[fnARN]
		result = make([]*EventSourceMapping, 0, len(ids))
		for id := range ids {
			if m, ok := b.eventSourceMappings.Get(id); ok {
				result = append(result, m)
			}
		}
	} else {
		result = b.eventSourceMappings.All()
	}

	// Apply optional EventSourceArn filter.
	if eventSourceARN != "" {
		filtered := result[:0]
		for _, m := range result {
			if m.EventSourceARN == eventSourceARN {
				filtered = append(filtered, m)
			}
		}
		result = filtered
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].UUID < result[j].UUID
	})

	return page.New(result, marker, maxItems, lambdaDefaultMaxItems)
}

// DeleteEventSourceMapping removes an event source mapping by UUID.
func (b *InMemoryBackend) DeleteEventSourceMapping(id string) (*EventSourceMapping, error) {
	b.mu.Lock("DeleteEventSourceMapping")
	defer b.mu.Unlock()

	m, ok := b.eventSourceMappings.Get(id)
	if !ok {
		return nil, ErrESMNotFound
	}

	b.eventSourceMappings.Delete(id)
	if ids := b.esmByFunctionARN[m.FunctionARN]; ids != nil {
		delete(ids, id)
		if len(ids) == 0 {
			delete(b.esmByFunctionARN, m.FunctionARN)
		}
	}
	if b.kinesisPoller != nil {
		b.kinesisPoller.RemoveMapping(id)
	}

	return m, nil
}

// applyESMUpdate patches esm fields from input (non-zero / non-nil values only).
// Returns true if the mapping was enabled by this update.
func applyESMUpdate(esm *EventSourceMapping, input *UpdateEventSourceMappingInput) bool {
	var nowEnabled bool

	if input.Enabled != nil {
		if *input.Enabled {
			esm.State = ESMStateEnabled
			nowEnabled = true
		} else {
			esm.State = ESMStateDisabled
		}
	}

	if input.BatchSize > 0 {
		esm.BatchSize = input.BatchSize
	}

	if input.FilterCriteria != nil {
		esm.FilterCriteria = input.FilterCriteria
	}

	if input.DestinationConfig != nil {
		esm.DestinationConfig = input.DestinationConfig
	}

	if input.BisectBatchOnFunctionError != nil {
		esm.BisectBatchOnFunctionError = *input.BisectBatchOnFunctionError
	}

	applyESMWindowFields(esm, input)
	applyESMSourceFields(esm, input)

	esm.LastModified = time.Now()

	return nowEnabled
}

// applyESMWindowFields applies the windowing / retry fields from input.
func applyESMWindowFields(esm *EventSourceMapping, input *UpdateEventSourceMappingInput) {
	if input.MaximumBatchingWindowInSeconds > 0 {
		esm.MaximumBatchingWindowInSeconds = input.MaximumBatchingWindowInSeconds
	}

	if input.TumblingWindowInSeconds > 0 {
		esm.TumblingWindowInSeconds = input.TumblingWindowInSeconds
	}

	if input.MaximumRecordAgeInSeconds > 0 {
		esm.MaximumRecordAgeInSeconds = input.MaximumRecordAgeInSeconds
	}

	if input.MaximumRetryAttempts > 0 {
		esm.MaximumRetryAttempts = input.MaximumRetryAttempts
	}

	if input.ParallelizationFactor > 0 {
		esm.ParallelizationFactor = input.ParallelizationFactor
	}
}

// applyESMSourceFields applies source-access, topics, queues, and response types from input.
func applyESMSourceFields(esm *EventSourceMapping, input *UpdateEventSourceMappingInput) {
	if len(input.SourceAccessConfigurations) > 0 {
		esm.SourceAccessConfigurations = input.SourceAccessConfigurations
	}

	if len(input.Topics) > 0 {
		esm.Topics = input.Topics
	}

	if len(input.Queues) > 0 {
		esm.Queues = input.Queues
	}

	if len(input.FunctionResponseTypes) > 0 {
		esm.FunctionResponseTypes = input.FunctionResponseTypes
	}
}

// UpdateEventSourceMapping updates an existing event source mapping.
func (b *InMemoryBackend) UpdateEventSourceMapping(
	id string,
	input *UpdateEventSourceMappingInput,
) (*EventSourceMapping, error) {
	var (
		esm        *EventSourceMapping
		found      bool
		nowEnabled bool
		poller     *EventSourcePoller
	)

	func() {
		b.mu.Lock("UpdateEventSourceMapping")
		defer b.mu.Unlock()

		var ok bool

		esm, ok = b.eventSourceMappings.Get(id)
		if !ok {
			return
		}

		found = true
		nowEnabled = applyESMUpdate(esm, input)
		poller = b.kinesisPoller
	}()

	if !found {
		return nil, ErrESMNotFound
	}

	if nowEnabled && poller != nil {
		poller.Notify()
	}

	return esm, nil
}
