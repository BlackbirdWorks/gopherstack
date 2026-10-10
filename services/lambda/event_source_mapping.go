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
	ScalingConfig                       *ESMScalingConfig                    `json:"scalingConfig,omitempty"`
	LoggingConfig                       *ESMLoggingConfig                    `json:"loggingConfig,omitempty"`
	MetricsConfig                       *ESMMetricsConfig                    `json:"metricsConfig,omitempty"`
	ProvisionedPollerConfig             *ESMProvisionedPollerConfig          `json:"provisionedPollerConfig,omitempty"`
	FilterCriteria                      *FilterCriteria                      `json:"filterCriteria,omitempty"`
	DestinationConfig                   *ESMDestinationConfig                `json:"destinationConfig,omitempty"`
	AmazonManagedKafkaEventSourceConfig *AmazonManagedKafkaEventSourceConfig `json:"mskConfig,omitempty"`
	SelfManagedKafkaEventSourceConfig   *SelfManagedKafkaEventSourceConfig   `json:"selfManagedKafkaConfig,omitempty"`
	SelfManagedEventSource              *SelfManagedEventSource              `json:"selfManagedEventSource,omitempty"`
	DocumentDBEventSourceConfig         *DocumentDBEventSourceConfig         `json:"docdbConfig,omitempty"`
	MaximumRetryAttempts                *int                                 `json:"maximumRetryAttempts,omitempty"`
	KMSKeyArn                           string                               `json:"kmsKeyArn,omitempty"`
	UUID                                string                               `json:"uuid"`
	FunctionARN                         string                               `json:"functionARN"`
	State                               EventSourceMappingState              `json:"state"`
	StartingPosition                    string                               `json:"startingPosition"`
	LastProcessingResult                string                               `json:"lastProcessingResult"`
	EventSourceARN                      string                               `json:"eventSourceARN"`
	FunctionResponseTypes               []string                             `json:"functionResponseTypes,omitempty"`
	Queues                              []string                             `json:"queues,omitempty"`
	Topics                              []string                             `json:"topics,omitempty"`
	SourceAccessConfigurations          []SourceAccessConfiguration          `json:"sourceAccessConfigurations,omitempty"`
	StartingPositionTimestamp           float64                              `json:"startingPositionTimestamp,omitempty"`
	BatchSize                           int                                  `json:"batchSize"`
	MaximumBatchingWindowInSeconds      int                                  `json:"maxBatchingWindowSecs,omitempty"`
	TumblingWindowInSeconds             int                                  `json:"tumblingWindowInSeconds,omitempty"`
	MaximumRecordAgeInSeconds           int                                  `json:"maximumRecordAgeInSeconds,omitempty"`
	ParallelizationFactor               int                                  `json:"parallelizationFactor,omitempty"`
	BisectBatchOnFunctionError          bool                                 `json:"bisectBatchOnFunctionError,omitempty"`
}

// ESMScalingConfig caps concurrent invokes for SQS mappings.
type ESMScalingConfig struct {
	MaximumConcurrency *int32 `json:"MaximumConcurrency,omitempty"`
}

// ESMLoggingConfig is the mapping's system log level.
type ESMLoggingConfig struct {
	SystemLogLevel string `json:"SystemLogLevel,omitempty"`
}

// ESMMetricsConfig lists the enabled mapping metrics.
type ESMMetricsConfig struct {
	Metrics []string `json:"Metrics,omitempty"`
}

// ESMProvisionedPollerConfig sizes the provisioned pollers of a mapping.
type ESMProvisionedPollerConfig struct {
	MaximumPollers  *int32 `json:"MaximumPollers,omitempty"`
	MinimumPollers  *int32 `json:"MinimumPollers,omitempty"`
	PollerGroupName string `json:"PollerGroupName,omitempty"`
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
	ScalingConfig                       *ESMScalingConfig
	LoggingConfig                       *ESMLoggingConfig
	MetricsConfig                       *ESMMetricsConfig
	ProvisionedPollerConfig             *ESMProvisionedPollerConfig
	FilterCriteria                      *FilterCriteria
	DestinationConfig                   *ESMDestinationConfig
	AmazonManagedKafkaEventSourceConfig *AmazonManagedKafkaEventSourceConfig
	SelfManagedKafkaEventSourceConfig   *SelfManagedKafkaEventSourceConfig
	SelfManagedEventSource              *SelfManagedEventSource
	DocumentDBEventSourceConfig         *DocumentDBEventSourceConfig
	MaximumRetryAttempts                *int
	StartingPosition                    string
	EventSourceARN                      string
	KMSKeyArn                           string
	FunctionName                        string
	SourceAccessConfigurations          []SourceAccessConfiguration
	Topics                              []string
	Queues                              []string
	FunctionResponseTypes               []string
	StartingPositionTimestamp           float64
	MaximumBatchingWindowInSeconds      int
	TumblingWindowInSeconds             int
	MaximumRecordAgeInSeconds           int
	BatchSize                           int
	ParallelizationFactor               int
	BisectBatchOnFunctionError          bool
	Enabled                             bool
}

// UpdateEventSourceMappingInput is the input for UpdateEventSourceMapping.
type UpdateEventSourceMappingInput struct {
	ScalingConfig                  *ESMScalingConfig
	LoggingConfig                  *ESMLoggingConfig
	MetricsConfig                  *ESMMetricsConfig
	ProvisionedPollerConfig        *ESMProvisionedPollerConfig
	FunctionName                   string
	MaximumBatchingWindowInSeconds *int32
	FilterCriteria                 *FilterCriteria
	DestinationConfig              *ESMDestinationConfig
	BisectBatchOnFunctionError     *bool
	Enabled                        *bool
	KMSKeyArn                      *string
	ParallelizationFactor          *int32
	MaximumRetryAttempts           *int32
	MaximumRecordAgeInSeconds      *int32
	TumblingWindowInSeconds        *int32
	BatchSize                      *int32
	UUID                           string
	FunctionResponseTypes          []string
	Queues                         []string
	Topics                         []string
	SourceAccessConfigurations     []SourceAccessConfiguration
}

// jsonESMResponse is the JSON representation of an event source mapping.
type jsonESMResponse struct {
	ScalingConfig                       *ESMScalingConfig                    `json:"ScalingConfig,omitempty"`
	LoggingConfig                       *ESMLoggingConfig                    `json:"LoggingConfig,omitempty"`
	MetricsConfig                       *ESMMetricsConfig                    `json:"MetricsConfig,omitempty"`
	ProvisionedPollerConfig             *ESMProvisionedPollerConfig          `json:"ProvisionedPollerConfig,omitempty"`
	MaximumRetryAttempts                *int                                 `json:"MaximumRetryAttempts,omitempty"`
	FilterCriteria                      *FilterCriteria                      `json:"FilterCriteria,omitempty"`
	DestinationConfig                   *ESMDestinationConfig                `json:"DestinationConfig,omitempty"`
	AmazonManagedKafkaEventSourceConfig *AmazonManagedKafkaEventSourceConfig `json:"AmazonManagedKafkaEventSourceConfig,omitempty"` //nolint:lll // AWS field name
	SelfManagedKafkaEventSourceConfig   *SelfManagedKafkaEventSourceConfig   `json:"SelfManagedKafkaEventSourceConfig,omitempty"`   //nolint:lll // AWS field name
	SelfManagedEventSource              *SelfManagedEventSource              `json:"SelfManagedEventSource,omitempty"`
	DocumentDBEventSourceConfig         *DocumentDBEventSourceConfig         `json:"DocumentDBEventSourceConfig,omitempty"`
	KMSKeyArn                           string                               `json:"KMSKeyArn,omitempty"`
	UUID                                string                               `json:"UUID"`
	FunctionARN                         string                               `json:"FunctionArn"`
	LastProcessingResult                string                               `json:"LastProcessingResult,omitempty"`
	State                               string                               `json:"State"`
	EventSourceARN                      string                               `json:"EventSourceArn,omitempty"`
	StartingPosition                    string                               `json:"StartingPosition,omitempty"`
	EventSourceMappingArn               string                               `json:"EventSourceMappingArn,omitempty"`
	Topics                              []string                             `json:"Topics,omitempty"`
	SourceAccessConfigurations          []SourceAccessConfiguration          `json:"SourceAccessConfigurations,omitempty"`
	FunctionResponseTypes               []string                             `json:"FunctionResponseTypes,omitempty"`
	Queues                              []string                             `json:"Queues,omitempty"`
	LastModified                        float64                              `json:"LastModified"`
	StartingPositionTimestamp           float64                              `json:"StartingPositionTimestamp,omitempty"`
	BatchSize                           int                                  `json:"BatchSize"`
	MaximumBatchingWindowInSeconds      int                                  `json:"MaximumBatchingWindowInSeconds,omitempty"` //nolint:lll // AWS field name
	TumblingWindowInSeconds             int                                  `json:"TumblingWindowInSeconds,omitempty"`
	MaximumRecordAgeInSeconds           int                                  `json:"MaximumRecordAgeInSeconds,omitempty"`
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
		ScalingConfig:                       m.ScalingConfig,
		LoggingConfig:                       m.LoggingConfig,
		MetricsConfig:                       m.MetricsConfig,
		ProvisionedPollerConfig:             m.ProvisionedPollerConfig,
		EventSourceMappingArn:               esmARN(m),
		UUID:                                m.UUID,
		EventSourceARN:                      m.EventSourceARN,
		FunctionARN:                         m.FunctionARN,
		KMSKeyArn:                           m.KMSKeyArn,
		State:                               string(m.State),
		LastModified:                        awstime.Epoch(m.LastModified),
		BatchSize:                           m.BatchSize,
		StartingPosition:                    m.StartingPosition,
		StartingPositionTimestamp:           m.StartingPositionTimestamp,
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

// esmARNParts is the field count of arn:partition:service:region:account:resource.
const esmARNParts = 6

// esmARN builds the mapping's own ARN from its function ARN's region/account.
func esmARN(m *EventSourceMapping) string {
	parts := strings.SplitN(m.FunctionARN, ":", esmARNParts)
	if len(parts) < esmARNParts {
		return ""
	}

	return arn.Build("lambda", parts[3], parts[4], "event-source-mapping:"+m.UUID)
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

	if input.EventSourceARN == "" && len(kafkaSourceBootstrap(input.SelfManagedEventSource)) == 0 {
		return nil, fmt.Errorf(
			"%w: EventSourceArn or SelfManagedEventSource with KAFKA_BOOTSTRAP_SERVERS is required",
			ErrInvalidParameterValue,
		)
	}

	if err := validateESMTuning(
		input.EventSourceARN, input.SelfManagedEventSource != nil,
		input.ScalingConfig, input.MetricsConfig, input.ProvisionedPollerConfig,
	); err != nil {
		return nil, err
	}

	if err := validateFailureDestination(
		input.EventSourceARN, input.SelfManagedEventSource != nil, input.Topics, input.DestinationConfig,
	); err != nil {
		return nil, err
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
		KMSKeyArn:                           input.KMSKeyArn,
		State:                               state,
		BatchSize:                           batchSize,
		StartingPosition:                    startingPosition,
		StartingPositionTimestamp:           input.StartingPositionTimestamp,
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
		ScalingConfig:                       input.ScalingConfig,
		LoggingConfig:                       input.LoggingConfig,
		MetricsConfig:                       input.MetricsConfig,
		ProvisionedPollerConfig:             input.ProvisionedPollerConfig,
	}

	b.eventSourceMappings.Put(m)

	if b.esmByFunctionARN[fnARN] == nil {
		b.esmByFunctionARN[fnARN] = make(map[string]struct{})
	}
	b.esmByFunctionARN[fnARN][id] = struct{}{}

	if input.Enabled && b.kinesisPoller != nil {
		b.kinesisPoller.Notify()
	}

	return cloneESM(m), nil
}

// cloneESM stops a caller from racing UpdateEventSourceMapping or the
// janitor's sweepESMs, which mutate m's fields under the lock.
func cloneESM(m *EventSourceMapping) *EventSourceMapping {
	cp := *m

	return &cp
}

// GetEventSourceMapping retrieves an event source mapping by UUID.
func (b *InMemoryBackend) GetEventSourceMapping(uuid string) (*EventSourceMapping, error) {
	b.mu.RLock("GetEventSourceMapping")
	defer b.mu.RUnlock()

	m, ok := b.eventSourceMappings.Get(uuid)
	if !ok {
		return nil, ErrESMNotFound
	}

	return cloneESM(m), nil
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
				result = append(result, cloneESM(m))
			}
		}
	} else {
		stored := b.eventSourceMappings.All()
		result = make([]*EventSourceMapping, len(stored))

		for i, m := range stored {
			result[i] = cloneESM(m)
		}
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

	return cloneESM(m), nil
}

// applyESMUpdate patches esm fields from input (non-zero / non-nil values only).
func applyESMUpdate(esm *EventSourceMapping, input *UpdateEventSourceMappingInput) {
	if input.Enabled != nil {
		if *input.Enabled {
			esm.State = ESMStateEnabled
		} else {
			esm.State = ESMStateDisabled
		}
	}

	if input.BatchSize != nil {
		esm.BatchSize = int(*input.BatchSize)
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

	if input.KMSKeyArn != nil {
		esm.KMSKeyArn = *input.KMSKeyArn
	}

	if input.ScalingConfig != nil {
		esm.ScalingConfig = input.ScalingConfig
	}

	if input.LoggingConfig != nil {
		esm.LoggingConfig = input.LoggingConfig
	}

	if input.MetricsConfig != nil {
		esm.MetricsConfig = input.MetricsConfig
	}

	if input.ProvisionedPollerConfig != nil {
		esm.ProvisionedPollerConfig = input.ProvisionedPollerConfig
	}

	applyESMWindowFields(esm, input)
	applyESMSourceFields(esm, input)

	esm.LastModified = time.Now()
}

// applyESMWindowFields applies the windowing / retry fields from input.
func applyESMWindowFields(esm *EventSourceMapping, input *UpdateEventSourceMappingInput) {
	if input.MaximumBatchingWindowInSeconds != nil {
		esm.MaximumBatchingWindowInSeconds = int(*input.MaximumBatchingWindowInSeconds)
	}

	if input.TumblingWindowInSeconds != nil {
		esm.TumblingWindowInSeconds = int(*input.TumblingWindowInSeconds)
	}

	if input.MaximumRecordAgeInSeconds != nil {
		esm.MaximumRecordAgeInSeconds = int(*input.MaximumRecordAgeInSeconds)
	}

	if input.MaximumRetryAttempts != nil {
		v := int(*input.MaximumRetryAttempts)
		esm.MaximumRetryAttempts = &v
	}

	if input.ParallelizationFactor != nil {
		esm.ParallelizationFactor = int(*input.ParallelizationFactor)
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
		result      *EventSourceMapping
		found       bool
		poller      *EventSourcePoller
		validateErr error
	)

	func() {
		b.mu.Lock("UpdateEventSourceMapping")
		defer b.mu.Unlock()

		esm, ok := b.eventSourceMappings.Get(id)
		if !ok {
			return
		}

		found = true

		validateErr = validateESMTuning(
			esm.EventSourceARN, esm.SelfManagedEventSource != nil,
			input.ScalingConfig, input.MetricsConfig, input.ProvisionedPollerConfig,
		)
		if validateErr != nil {
			return
		}

		validateErr = validateFailureDestination(
			esm.EventSourceARN, esm.SelfManagedEventSource != nil, cmpTopics(esm.Topics, input.Topics),
			cmpDest(esm.DestinationConfig, input.DestinationConfig),
		)
		if validateErr != nil {
			return
		}

		b.reindexESMFunctionLocked(esm, input.FunctionName)
		applyESMUpdate(esm, input)
		poller = b.kinesisPoller
		result = cloneESM(esm)
	}()

	if !found {
		return nil, ErrESMNotFound
	}

	if validateErr != nil {
		return nil, validateErr
	}

	if poller != nil {
		poller.Notify()
	}

	return result, nil
}

// reindexESMFunctionLocked repoints esm at functionName (bare name or ARN); caller holds b.mu.
func (b *InMemoryBackend) reindexESMFunctionLocked(esm *EventSourceMapping, functionName string) {
	if functionName == "" {
		return
	}

	newARN := arn.Build("lambda", b.region, b.accountID, "function:"+esmFunctionName(functionName))
	if newARN == esm.FunctionARN {
		return
	}

	if ids := b.esmByFunctionARN[esm.FunctionARN]; ids != nil {
		delete(ids, esm.UUID)

		if len(ids) == 0 {
			delete(b.esmByFunctionARN, esm.FunctionARN)
		}
	}

	if b.esmByFunctionARN[newARN] == nil {
		b.esmByFunctionARN[newARN] = make(map[string]struct{})
	}

	b.esmByFunctionARN[newARN][esm.UUID] = struct{}{}
	esm.FunctionARN = newARN
}

// setESMLastProcessingResult records the last poller outcome on a mapping.
func (b *InMemoryBackend) setESMLastProcessingResult(id, result string) {
	b.mu.Lock("setESMLastProcessingResult")
	defer b.mu.Unlock()

	if m, ok := b.eventSourceMappings.Get(id); ok {
		m.LastProcessingResult = result
	}
}

const (
	sqsMaxPollersMin   = 2
	sqsMaxPollersMax   = 10000
	kafkaMaxPollersMin = 1
	kafkaMaxPollersMax = 2000
)

// validateESMTuning enforces the source-type and range rules documented on
// ScalingConfig, EventSourceMappingMetricsConfig and ProvisionedPollerConfig.
func validateESMTuning(
	eventSourceARN string,
	selfManaged bool,
	scaling *ESMScalingConfig,
	metrics *ESMMetricsConfig,
	ppc *ESMProvisionedPollerConfig,
) error {
	sqs := isSQSARN(eventSourceARN)
	kafka := selfManaged || strings.Contains(eventSourceARN, ":kafka:")

	if scaling != nil && scaling.MaximumConcurrency != nil && !sqs {
		return fmt.Errorf("%w: ScalingConfig applies to Amazon SQS event sources only", ErrInvalidParameterValue)
	}

	if metrics != nil {
		for _, m := range metrics.Metrics {
			switch m {
			case "EventCount":
			case "ErrorCount", "KafkaMetrics":
				if !kafka {
					return fmt.Errorf(
						"%w: metric %s applies to Amazon MSK and self-managed Apache Kafka sources only",
						ErrInvalidParameterValue, m,
					)
				}
			default:
				return fmt.Errorf("%w: unknown mapping metric %q", ErrInvalidParameterValue, m)
			}
		}
	}

	return validateProvisionedPollers(sqs, kafka, ppc)
}

func validateProvisionedPollers(sqs, kafka bool, ppc *ESMProvisionedPollerConfig) error {
	if ppc == nil {
		return nil
	}

	if ppc.PollerGroupName != "" && !kafka {
		return fmt.Errorf(
			"%w: PollerGroupName applies to Amazon MSK and self-managed Apache Kafka sources only",
			ErrInvalidParameterValue,
		)
	}

	lo, hi := int32(kafkaMaxPollersMin), int32(kafkaMaxPollersMax)
	if sqs {
		lo, hi = sqsMaxPollersMin, sqsMaxPollersMax
	} else if !kafka {
		return nil
	}

	if ppc.MaximumPollers != nil && (*ppc.MaximumPollers < lo || *ppc.MaximumPollers > hi) {
		return fmt.Errorf("%w: MaximumPollers must be between %d and %d", ErrInvalidParameterValue, lo, hi)
	}

	if ppc.MinimumPollers != nil && *ppc.MinimumPollers < lo {
		return fmt.Errorf("%w: MinimumPollers must be at least %d", ErrInvalidParameterValue, lo)
	}

	return nil
}

func cmpTopics(cur, upd []string) []string {
	if len(upd) > 0 {
		return upd
	}

	return cur
}

func cmpDest(cur, upd *ESMDestinationConfig) *ESMDestinationConfig {
	if upd != nil {
		return upd
	}

	return cur
}
