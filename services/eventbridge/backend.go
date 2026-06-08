package eventbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
)

const (
	stateActive         = "ACTIVE"
	replayStateStarting = "STARTING"
)

// regionContextKey is the context key for the per-request AWS region.
type regionContextKey struct{}

// getRegionFromContext extracts the region from context, falling back to defaultRegion.
func getRegionFromContext(ctx context.Context, defaultRegion string) string {
	if region, ok := ctx.Value(regionContextKey{}).(string); ok && region != "" {
		return region
	}

	return defaultRegion
}

// ebBusKey returns the region-free inner key used to store a bus, its rules and
// its rule index within a region-scoped map. The region is the outer map key, so
// this key only needs to disambiguate buses within a single region.
func ebBusKey(busName string) string {
	if busName == "" {
		busName = defaultEventBusName
	}

	return busName
}

var (
	ErrEventBusNotFound       = errors.New("ResourceNotFoundException")
	ErrEventBusAlreadyExists  = errors.New("ResourceAlreadyExistsException")
	ErrRuleNotFound           = errors.New("ResourceNotFoundException")
	ErrCannotDeleteDefaultBus = errors.New("IllegalArgumentException")
	ErrInvalidParameter       = errors.New("InvalidParameterException")
	ErrNotFound               = errors.New("ResourceNotFoundException")
	ErrAlreadyExists          = errors.New("ResourceAlreadyExistsException")
	ErrInvalidState           = errors.New("InvalidStateException")
	ErrResourceLimitExceeded  = errors.New("ResourceLimitExceededException")
	// ErrForbiddenOperation is returned when an operation is forbidden (e.g., modifying built-in registries).
	ErrForbiddenOperation = errors.New("ForbiddenException")
)

const (
	defaultEventBusName    = "default"
	maxEventLogSize        = 1000
	ruleStateEnabled       = "ENABLED"
	ruleStateDisabled      = "DISABLED"
	defaultDeliveryWorkers = 10
	// defaultShutdownTimeout is the maximum time Close waits for in-flight delivery
	// goroutines to finish after cancelling the lifecycle context.
	defaultShutdownTimeout = 5 * time.Second
	// defaultDeliveryTimeout is the default maximum time allowed for a single target delivery call.
	defaultDeliveryTimeout = 30 * time.Second
	ruleIndexAny           = "\x00"

	// maxEventBusNameLength is the maximum allowed event bus name length (AWS limit).
	maxEventBusNameLength = 256
	// maxArchiveNameLength is the maximum allowed archive name length (AWS limit).
	maxArchiveNameLength = 48
	// maxTargetsPerRule is the maximum number of targets allowed per rule (AWS default limit).
	maxTargetsPerRule = 5
	// maxEventBusesPerAccount is the AWS limit for custom event buses per account.
	maxEventBusesPerAccount = 200
	// maxRulesPerBus is the AWS limit for rules per event bus.
	maxRulesPerBus = 300
)

type ruleIndexKey struct {
	source     string
	detailType string
}

// StorageBackend is the interface for an EventBridge in-memory store.
type StorageBackend interface {
	CreateEventBus(ctx context.Context, name, description string) (*EventBus, error)
	DeleteEventBus(ctx context.Context, name string) error
	ListEventBuses(ctx context.Context, namePrefix, nextToken string) ([]EventBus, string, error)
	DescribeEventBus(ctx context.Context, name string) (*EventBus, error)
	PutRule(ctx context.Context, input PutRuleInput) (*Rule, error)
	DeleteRule(ctx context.Context, name, eventBusName string) error
	ListRules(ctx context.Context, eventBusName, namePrefix, nextToken string) ([]Rule, string, error)
	DescribeRule(ctx context.Context, name, eventBusName string) (*Rule, error)
	EnableRule(ctx context.Context, name, eventBusName string) error
	DisableRule(ctx context.Context, name, eventBusName string) error
	PutTargets(ctx context.Context, ruleName, eventBusName string, targets []Target) ([]FailedEntry, error)
	RemoveTargets(ctx context.Context, ruleName, eventBusName string, ids []string) ([]FailedEntry, error)
	ListTargetsByRule(ctx context.Context, ruleName, eventBusName, nextToken string) ([]Target, string, error)
	PutEvents(ctx context.Context, entries []EventEntry) []EventResultEntry
	GetEventLog(ctx context.Context) []EventLogEntry
	ActivateEventSource(ctx context.Context, name string) error
	DeactivateEventSource(ctx context.Context, name string) error
	CreatePartnerEventSource(ctx context.Context, name, account string) (*PartnerEventSource, error)
	CancelReplay(ctx context.Context, replayName string) (*Replay, error)
	CreateAPIDestination(ctx context.Context, input CreateAPIDestinationInput) (*APIDestination, error)
	CreateArchive(ctx context.Context, input CreateArchiveInput) (*Archive, error)
	CreateConnection(ctx context.Context, input CreateConnectionInput) (*Connection, error)
	CreateEndpoint(ctx context.Context, input CreateEndpointInput) (*Endpoint, error)
	DeauthorizeConnection(ctx context.Context, name string) (*Connection, error)
	DeleteAPIDestination(ctx context.Context, name string) error
	DeleteArchive(ctx context.Context, name string) error
	DescribeArchive(ctx context.Context, name string) (*Archive, error)
	ListArchives(ctx context.Context, namePrefix, nextToken string) ([]Archive, string, error)
	UpdateArchive(ctx context.Context, input UpdateArchiveInput) (*Archive, error)
	DeleteConnection(ctx context.Context, name string) error
	DescribeConnection(ctx context.Context, name string) (*Connection, error)
	ListConnections(ctx context.Context, namePrefix, nextToken string) ([]Connection, string, error)
	UpdateConnection(ctx context.Context, input UpdateConnectionInput) (*Connection, error)
	DeleteEndpoint(ctx context.Context, name string) error
	DescribeEndpoint(ctx context.Context, name string) (*Endpoint, error)
	ListEndpoints(ctx context.Context, namePrefix, nextToken string) ([]Endpoint, string, error)
	UpdateEndpoint(ctx context.Context, input UpdateEndpointInput) (*Endpoint, error)
	DescribeAPIDestination(ctx context.Context, name string) (*APIDestination, error)
	ListAPIDestinations(ctx context.Context, namePrefix, nextToken string) ([]APIDestination, string, error)
	UpdateAPIDestination(ctx context.Context, input UpdateAPIDestinationInput) (*APIDestination, error)
	DescribeEventSource(ctx context.Context, name string) (*EventSource, error)
	ListEventSources(ctx context.Context, namePrefix, nextToken string) ([]EventSource, string, error)
	DescribePartnerEventSource(ctx context.Context, name string) (*PartnerEventSource, error)
	DeletePartnerEventSource(ctx context.Context, name string) error
	ListPartnerEventSources(ctx context.Context, namePrefix, nextToken string) ([]PartnerEventSource, string, error)
	PutPartnerEvents(ctx context.Context, entries []EventEntry) []EventResultEntry
	DescribeReplay(ctx context.Context, name string) (*Replay, error)
	ListReplays(ctx context.Context, namePrefix, nextToken string) ([]Replay, string, error)
	StartReplay(ctx context.Context, input StartReplayInput) (*Replay, error)
	ListRuleNamesByTarget(ctx context.Context, targetARN, eventBusName, nextToken string) ([]string, string, error)
	TestEventPattern(ctx context.Context, pattern, event string) (bool, error)
	UpdateEventBus(ctx context.Context, input UpdateEventBusInput) (*EventBus, error)
	PutPermission(ctx context.Context, input PutPermissionInput) error
	RemovePermission(ctx context.Context, input RemovePermissionInput) error
	GetEventBusPolicy(ctx context.Context, eventBusName string) (string, error)
	PutEventBusPolicy(ctx context.Context, input PutEventBusPolicyInput) error
	CreatePipe(ctx context.Context, input CreatePipeInput) (*Pipe, error)
	DeletePipe(ctx context.Context, name string) error
	DescribePipe(ctx context.Context, name string) (*Pipe, error)
	ListPipes(ctx context.Context, namePrefix, nextToken string) ([]Pipe, string, error)
	UpdatePipe(ctx context.Context, input UpdatePipeInput) (*Pipe, error)
	// Schema Registry operations.
	CreateRegistry(ctx context.Context, input CreateRegistryInput) (*SchemaRegistry, error)
	DeleteRegistry(ctx context.Context, registryName string) error
	DescribeRegistry(ctx context.Context, registryName string) (*SchemaRegistry, error)
	ListRegistries(ctx context.Context, namePrefix, nextToken string) ([]SchemaRegistry, string, error)
	UpdateRegistry(ctx context.Context, input UpdateRegistryInput) (*SchemaRegistry, error)
	CreateSchema(ctx context.Context, input CreateSchemaInput) (*Schema, error)
	DeleteSchema(ctx context.Context, registryName, schemaName string) error
	DescribeSchema(ctx context.Context, registryName, schemaName, schemaVersion string) (*Schema, error)
	ListSchemas(ctx context.Context, registryName, namePrefix, nextToken string) ([]Schema, string, error)
	SearchSchemas(ctx context.Context, registryName, keywords, nextToken string) ([]Schema, string, error)
	UpdateSchema(ctx context.Context, input UpdateSchemaInput) (*Schema, error)
	ListSchemaVersions(ctx context.Context, registryName, schemaName, nextToken string) ([]SchemaVersion, string, error)
	DescribeSchemaVersion(ctx context.Context, registryName, schemaName, schemaVersion string) (*SchemaVersion, error)
	DeleteSchemaVersion(ctx context.Context, registryName, schemaName, schemaVersion string) error
	GetDiscoveredSchema(ctx context.Context, input GetDiscoveredSchemaInput) (string, error)
	PutCodeBinding(ctx context.Context, input PutCodeBindingInput) (*CodeBinding, error)
	DescribeCodeBinding(ctx context.Context, input DescribeCodeBindingInput) (*CodeBinding, error)
	ListCodeBindings(ctx context.Context, input ListCodeBindingsInput) ([]CodeBinding, string, error)
	GetCodeBindingSource(ctx context.Context, registryName, schemaName, language, schemaVersion string) (string, error)
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	ctx context.Context
	mu  *lockmetrics.RWMutex
	// Region-isolated stores. The outer key is the AWS region; the inner keys are
	// region-free (bus name, rule name, resource name, etc).
	connections     map[string]map[string]*Connection
	rules           map[string]map[string]map[string]*Rule
	targets         map[string]map[string]map[string]*Target
	eventSources    map[string]map[string]*EventSource
	replays         map[string]map[string]*Replay
	apiDestinations map[string]map[string]*APIDestination
	cancel          context.CancelFunc
	deliveryTargets *DeliveryTargets
	endpoints       map[string]map[string]*Endpoint
	buses           map[string]map[string]*EventBus
	partnerSources  map[string]map[string]*PartnerEventSource
	archives        map[string]map[string]*Archive
	archivedEvents  map[string]map[string][]EventEntry
	busePolicies    map[string]map[string]*EventBusPolicy
	pipes           map[string]*Pipe
	registries      map[string]*SchemaRegistry
	schemas         map[string]map[string]*Schema // registryName → schemaName → Schema
	schemaVersions  map[string][]*SchemaVersion   // "registryName/schemaName" → ordered versions
	codeBindings    map[string]*CodeBinding       // "registryName/schemaName/language" → binding
	workerSem       chan struct{}
	ruleIndex       map[string]map[string]map[ruleIndexKey]map[string]*Rule
	patternCache    sync.Map
	region          string
	accountID       string
	eventLog        []EventLogEntry
	wg              sync.WaitGroup
	shutdownTimeout time.Duration
	deliveryTimeout time.Duration
	closing         atomic.Bool
}

// NewInMemoryBackend creates a new InMemoryBackend with default configuration.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion)
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with given account and region.
// The backend's lifecycle context is derived from [context.Background]; use
// NewInMemoryBackendWithContext to bind it to a parent service context instead.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	return NewInMemoryBackendWithContext(context.Background(), accountID, region)
}

// NewInMemoryBackendWithContext creates a new InMemoryBackend whose lifecycle context
// is derived from the provided parent. When the parent is cancelled (e.g. on server
// shutdown), all in-flight delivery workers are also cancelled.
// If svcCtx is nil, [context.Background] is used.
func NewInMemoryBackendWithContext(
	svcCtx context.Context,
	accountID, region string,
) *InMemoryBackend {
	if svcCtx == nil {
		svcCtx = context.Background()
	}

	ctx, cancel := context.WithCancel(svcCtx)
	b := &InMemoryBackend{
		accountID:       accountID,
		region:          region,
		buses:           make(map[string]map[string]*EventBus),
		rules:           make(map[string]map[string]map[string]*Rule),
		targets:         make(map[string]map[string]map[string]*Target),
		eventSources:    make(map[string]map[string]*EventSource),
		replays:         make(map[string]map[string]*Replay),
		apiDestinations: make(map[string]map[string]*APIDestination),
		archives:        make(map[string]map[string]*Archive),
		archivedEvents:  make(map[string]map[string][]EventEntry),
		connections:     make(map[string]map[string]*Connection),
		endpoints:       make(map[string]map[string]*Endpoint),
		partnerSources:  make(map[string]map[string]*PartnerEventSource),
		busePolicies:    make(map[string]map[string]*EventBusPolicy),
		pipes:           make(map[string]*Pipe),
		registries:      make(map[string]*SchemaRegistry),
		schemas:         make(map[string]map[string]*Schema),
		schemaVersions:  make(map[string][]*SchemaVersion),
		codeBindings:    make(map[string]*CodeBinding),
		deliveryTargets: &DeliveryTargets{},
		mu:              lockmetrics.New("eventbridge"),
		ctx:             ctx,
		cancel:          cancel,
		workerSem:       make(chan struct{}, defaultDeliveryWorkers),
		shutdownTimeout: defaultShutdownTimeout,
		deliveryTimeout: defaultDeliveryTimeout,
		ruleIndex:       make(map[string]map[string]map[ruleIndexKey]map[string]*Rule),
	}
	// Create the default event bus in the backend's own region.
	b.busesStore(b.region)[defaultEventBusName] = &EventBus{
		Name:        defaultEventBusName,
		Arn:         b.busARN(b.region, defaultEventBusName),
		CreatedTime: time.Now(),
	}

	return b
}

// Close marks the backend as closing, cancels the lifecycle context, and waits
// for all in-flight delivery goroutines to finish. It returns after at most
// shutdownTimeout to prevent a hung target service from blocking service
// shutdown indefinitely. Once Close is called, PutEvents will no longer spawn
// new delivery goroutines. The internal wg.Wait goroutine completes on its own
// once all delivery goroutines exit — either because the lifecycle context was
// cancelled (propagated to each delivery) or because the per-delivery deadline
// fired.
func (b *InMemoryBackend) Close() {
	// Mark as closing before cancelling so PutEvents stops scheduling new work.
	b.closing.Store(true)

	// Read shutdownTimeout under the same lock used by SetShutdownTimeout so
	// there is no data race between a concurrent setter and Close.
	b.mu.RLock("Close")
	timeout := b.shutdownTimeout
	b.mu.RUnlock()

	b.cancel()

	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
	case <-timer.C:
	}
}

// SetShutdownTimeout overrides the maximum time Close waits for in-flight goroutines.
// Primarily intended for tests.
func (b *InMemoryBackend) SetShutdownTimeout(d time.Duration) {
	b.mu.Lock("SetShutdownTimeout")
	defer b.mu.Unlock()
	b.shutdownTimeout = d
}

// SetDeliveryTimeout overrides the per-target delivery timeout.
// Primarily intended for tests.
func (b *InMemoryBackend) SetDeliveryTimeout(d time.Duration) {
	b.mu.Lock("SetDeliveryTimeout")
	defer b.mu.Unlock()
	b.deliveryTimeout = d
}

// SetDeliveryTargets configures the service references used for fan-out delivery.
func (b *InMemoryBackend) SetDeliveryTargets(dt *DeliveryTargets) {
	b.mu.Lock("SetDeliveryTargets")
	defer b.mu.Unlock()
	b.deliveryTargets = dt
}

func (b *InMemoryBackend) busARN(region, name string) string {
	return arn.Build("events", region, b.accountID, "event-bus/"+name)
}

func (b *InMemoryBackend) ruleARN(region, busName, ruleName string) string {
	return arn.Build("events", region, b.accountID, "rule/"+busName+"/"+ruleName)
}

func (b *InMemoryBackend) apiDestinationARN(name string) string {
	return arn.Build("events", b.region, b.accountID, "api-destination/"+name)
}

func (b *InMemoryBackend) archiveARN(name string) string {
	return arn.Build("events", b.region, b.accountID, "archive/"+name)
}

func (b *InMemoryBackend) connectionARN(name string) string {
	return arn.Build("events", b.region, b.accountID, "connection/"+name)
}

func (b *InMemoryBackend) endpointARN(name string) string {
	return arn.Build("events", b.region, b.accountID, "endpoint/"+name)
}

func (b *InMemoryBackend) partnerSourceARN(name string) string {
	return arn.Build("events", b.region, b.accountID, "event-source/aws.partner/"+name)
}

func (b *InMemoryBackend) replayARN(name string) string {
	return arn.Build("events", b.region, b.accountID, "replay/"+name)
}

// targetKey returns the region-free inner key used to store a rule's targets
// within a region-scoped target map.
func (b *InMemoryBackend) targetKey(busName, ruleName string) string {
	return ebBusKey(busName) + "/" + ruleName
}

// busesStore returns the bus map for the given region, lazily creating it.
// Callers must hold b.mu.
func (b *InMemoryBackend) busesStore(region string) map[string]*EventBus {
	if b.buses[region] == nil {
		b.buses[region] = make(map[string]*EventBus)
	}

	return b.buses[region]
}

// rulesStore returns the rules map for the given region, lazily creating it.
// Callers must hold b.mu.
func (b *InMemoryBackend) rulesStore(region string) map[string]map[string]*Rule {
	if b.rules[region] == nil {
		b.rules[region] = make(map[string]map[string]*Rule)
	}

	return b.rules[region]
}

// targetsStore returns the targets map for the given region, lazily creating it.
// Callers must hold b.mu.
func (b *InMemoryBackend) targetsStore(region string) map[string]map[string]*Target {
	if b.targets[region] == nil {
		b.targets[region] = make(map[string]map[string]*Target)
	}

	return b.targets[region]
}

// ruleIndexStore returns the rule index map for the given region, lazily creating it.
// Callers must hold b.mu.
func (b *InMemoryBackend) ruleIndexStore(region string) map[string]map[ruleIndexKey]map[string]*Rule {
	if b.ruleIndex[region] == nil {
		b.ruleIndex[region] = make(map[string]map[ruleIndexKey]map[string]*Rule)
	}

	return b.ruleIndex[region]
}

// eventSourcesStore returns the event source map for the given region.
// Callers must hold b.mu.
func (b *InMemoryBackend) eventSourcesStore(region string) map[string]*EventSource {
	if b.eventSources[region] == nil {
		b.eventSources[region] = make(map[string]*EventSource)
	}

	return b.eventSources[region]
}

// replaysStore returns the replay map for the given region.
// Callers must hold b.mu.
func (b *InMemoryBackend) replaysStore(region string) map[string]*Replay {
	if b.replays[region] == nil {
		b.replays[region] = make(map[string]*Replay)
	}

	return b.replays[region]
}

// apiDestinationsStore returns the API destination map for the given region.
// Callers must hold b.mu.
func (b *InMemoryBackend) apiDestinationsStore(region string) map[string]*APIDestination {
	if b.apiDestinations[region] == nil {
		b.apiDestinations[region] = make(map[string]*APIDestination)
	}

	return b.apiDestinations[region]
}

// archivesStore returns the archive map for the given region.
// Callers must hold b.mu.
func (b *InMemoryBackend) archivesStore(region string) map[string]*Archive {
	if b.archives[region] == nil {
		b.archives[region] = make(map[string]*Archive)
	}

	return b.archives[region]
}

// archivedEventsStore returns the archived-events map for the given region.
// Callers must hold b.mu.
func (b *InMemoryBackend) archivedEventsStore(region string) map[string][]EventEntry {
	if b.archivedEvents[region] == nil {
		b.archivedEvents[region] = make(map[string][]EventEntry)
	}

	return b.archivedEvents[region]
}

// connectionsStore returns the connection map for the given region.
// Callers must hold b.mu.
func (b *InMemoryBackend) connectionsStore(region string) map[string]*Connection {
	if b.connections[region] == nil {
		b.connections[region] = make(map[string]*Connection)
	}

	return b.connections[region]
}

// endpointsStore returns the endpoint map for the given region.
// Callers must hold b.mu.
func (b *InMemoryBackend) endpointsStore(region string) map[string]*Endpoint {
	if b.endpoints[region] == nil {
		b.endpoints[region] = make(map[string]*Endpoint)
	}

	return b.endpoints[region]
}

// partnerSourcesStore returns the partner event source map for the given region.
// Callers must hold b.mu.
func (b *InMemoryBackend) partnerSourcesStore(region string) map[string]*PartnerEventSource {
	if b.partnerSources[region] == nil {
		b.partnerSources[region] = make(map[string]*PartnerEventSource)
	}

	return b.partnerSources[region]
}

// busePoliciesStore returns the event bus policy map for the given region.
// Callers must hold b.mu.
func (b *InMemoryBackend) busePoliciesStore(region string) map[string]*EventBusPolicy {
	if b.busePolicies[region] == nil {
		b.busePolicies[region] = make(map[string]*EventBusPolicy)
	}

	return b.busePolicies[region]
}

func (b *InMemoryBackend) getOrCompilePattern(patternJSON string) (*compiledPattern, error) {
	if cached, ok := b.patternCache.Load(patternJSON); ok {
		compiled, castOK := cached.(*compiledPattern)
		if castOK {
			return compiled, nil
		}
	}

	compiled, err := compilePattern(patternJSON)
	if err != nil {
		return nil, err
	}

	actual, _ := b.patternCache.LoadOrStore(patternJSON, compiled)
	cachedCompiled, castOK := actual.(*compiledPattern)
	if castOK {
		return cachedCompiled, nil
	}

	return compiled, nil
}

func (b *InMemoryBackend) addRuleToIndex(region, busKey string, rule *Rule) {
	if rule.compiledPattern == nil && rule.EventPattern == "" {
		return
	}
	regionIndex := b.ruleIndexStore(region)
	indexes := regionIndex[busKey]
	if indexes == nil {
		indexes = make(map[ruleIndexKey]map[string]*Rule)
		regionIndex[busKey] = indexes
	}

	keys := indexKeysFromRule(rule)
	for _, key := range keys {
		bucket := indexes[key]
		if bucket == nil {
			bucket = make(map[string]*Rule)
			indexes[key] = bucket
		}
		bucket[rule.Name] = rule
	}
	rule.indexKeys = keys
}

func (b *InMemoryBackend) removeRuleFromIndex(region, busKey string, rule *Rule) {
	regionIndex := b.ruleIndex[region]
	if regionIndex == nil {
		return
	}
	indexes := regionIndex[busKey]
	if indexes == nil {
		return
	}

	for _, key := range rule.indexKeys {
		bucket := indexes[key]
		if bucket == nil {
			continue
		}
		delete(bucket, rule.Name)
		if len(bucket) == 0 {
			delete(indexes, key)
		}
	}

	if len(indexes) == 0 {
		delete(regionIndex, busKey)
	}
}

func indexKeysFromRule(rule *Rule) []ruleIndexKey {
	sources := []string{ruleIndexAny}
	detailTypes := []string{ruleIndexAny}

	if rule.compiledPattern != nil && len(rule.compiledPattern.sourceExactValues) > 0 {
		sources = rule.compiledPattern.sourceExactValues
	}
	if rule.compiledPattern != nil && len(rule.compiledPattern.detailTypeExactValues) > 0 {
		detailTypes = rule.compiledPattern.detailTypeExactValues
	}

	keys := make([]ruleIndexKey, 0, len(sources)*len(detailTypes))
	for _, source := range sources {
		for _, detailType := range detailTypes {
			keys = append(keys, ruleIndexKey{
				source:     source,
				detailType: detailType,
			})
		}
	}

	return keys
}

// CreateEventBus creates a new event bus.
func (b *InMemoryBackend) CreateEventBus(ctx context.Context, name, description string) (*EventBus, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	if len(name) > maxEventBusNameLength {
		return nil, fmt.Errorf(
			"%w: Name must be %d characters or fewer",
			ErrInvalidParameter,
			maxEventBusNameLength,
		)
	}

	if strings.HasPrefix(name, "aws.") {
		return nil, fmt.Errorf(
			"%w: Event bus name cannot start with the reserved prefix \"aws.\"",
			ErrInvalidParameter,
		)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("CreateEventBus")
	defer b.mu.Unlock()

	buses := b.busesStore(region)
	if _, exists := buses[ebBusKey(name)]; exists {
		return nil, fmt.Errorf("%w: Event bus %s already exists", ErrEventBusAlreadyExists, name)
	}

	// Count custom buses (exclude the default bus against the limit).
	customBusCount := 0
	for busName := range buses {
		if busName != defaultEventBusName {
			customBusCount++
		}
	}
	if customBusCount >= maxEventBusesPerAccount {
		return nil, fmt.Errorf(
			"%w: account has reached the maximum of %d custom event buses",
			ErrResourceLimitExceeded,
			maxEventBusesPerAccount,
		)
	}

	bus := &EventBus{
		Name:        name,
		Arn:         b.busARN(region, name),
		Description: description,
		CreatedTime: time.Now(),
	}
	buses[ebBusKey(name)] = bus

	return bus, nil
}

// DeleteEventBus deletes an event bus by name (default bus cannot be deleted).
// It also removes all rules and targets associated with the bus.
func (b *InMemoryBackend) DeleteEventBus(ctx context.Context, name string) error {
	if name == defaultEventBusName {
		return fmt.Errorf("%w: cannot delete the default event bus", ErrCannotDeleteDefaultBus)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("DeleteEventBus")
	defer b.mu.Unlock()

	busKey := ebBusKey(name)
	buses := b.busesStore(region)
	if _, exists := buses[busKey]; !exists {
		return fmt.Errorf("%w: Event bus %s not found", ErrEventBusNotFound, name)
	}

	delete(buses, busKey)
	delete(b.ruleIndexStore(region), busKey)

	// Clean up all rules and targets for this bus.
	rules := b.rulesStore(region)
	targets := b.targetsStore(region)
	if busRules, ok := rules[busKey]; ok {
		for ruleName := range busRules {
			delete(targets, b.targetKey(name, ruleName))
		}

		delete(rules, busKey)
	}

	return nil
}

// ListEventBuses returns event buses optionally filtered by name prefix, with pagination.
func (b *InMemoryBackend) ListEventBuses(
	ctx context.Context,
	namePrefix, nextToken string,
) ([]EventBus, string, error) {
	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("ListEventBuses")
	defer b.mu.RUnlock()

	all := make([]EventBus, 0)
	for _, bus := range b.busesStore(region) {
		if namePrefix == "" || strings.HasPrefix(bus.Name, namePrefix) {
			all = append(all, *bus)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// DescribeEventBus returns details for a single event bus.
func (b *InMemoryBackend) DescribeEventBus(ctx context.Context, name string) (*EventBus, error) {
	if name == "" {
		name = defaultEventBusName
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("DescribeEventBus")
	defer b.mu.RUnlock()

	bus, exists := b.busesStore(region)[ebBusKey(name)]
	if !exists {
		return nil, fmt.Errorf("%w: Event bus %s not found", ErrEventBusNotFound, name)
	}

	cp := *bus

	return &cp, nil
}

// validatePutRuleInput validates the rule input fields before any locking.
func validatePutRuleInput(input PutRuleInput) error {
	if input.Name == "" {
		return fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	const maxRuleNameLength = 64
	if len(input.Name) > maxRuleNameLength {
		return fmt.Errorf(
			"%w: Name must not exceed %d characters",
			ErrInvalidParameter,
			maxRuleNameLength,
		)
	}

	if input.EventPattern != "" && input.ScheduleExpression != "" {
		return fmt.Errorf(
			"%w: ScheduleExpression and EventPattern are mutually exclusive",
			ErrInvalidParameter,
		)
	}

	if input.EventPattern == "" && input.ScheduleExpression == "" {
		return fmt.Errorf(
			"%w: either EventPattern or ScheduleExpression must be provided",
			ErrInvalidParameter,
		)
	}

	if input.ScheduleExpression != "" {
		if _, err := parseScheduleExpression(input.ScheduleExpression); err != nil {
			return fmt.Errorf(
				"%w: invalid ScheduleExpression %q: %w",
				ErrInvalidParameter,
				input.ScheduleExpression,
				err,
			)
		}
	}

	return nil
}

// PutRule creates or updates a rule on an event bus.
func (b *InMemoryBackend) PutRule(ctx context.Context, input PutRuleInput) (*Rule, error) {
	if err := validatePutRuleInput(input); err != nil {
		return nil, err
	}

	region := getRegionFromContext(ctx, b.region)

	busName := input.EventBusName
	if busName == "" {
		busName = defaultEventBusName
	}

	var compiled *compiledPattern
	if input.EventPattern != "" {
		var err error
		compiled, err = b.getOrCompilePattern(input.EventPattern)
		if err != nil {
			return nil, fmt.Errorf("%w: EventPattern is not valid JSON", ErrInvalidParameter)
		}
	}

	busKey := ebBusKey(busName)

	b.mu.Lock("PutRule")
	defer b.mu.Unlock()

	if _, exists := b.busesStore(region)[busKey]; !exists {
		return nil, fmt.Errorf("%w: Event bus %s not found", ErrEventBusNotFound, busName)
	}

	state := input.State
	if state == "" {
		state = ruleStateEnabled
	}

	rules := b.rulesStore(region)
	if rules[busKey] == nil {
		rules[busKey] = make(map[string]*Rule)
	}

	// Enforce per-bus rule limit only for new rules (not updates).
	if _, exists := rules[busKey][input.Name]; !exists {
		if len(rules[busKey]) >= maxRulesPerBus {
			return nil, fmt.Errorf(
				"%w: event bus %s has reached the maximum of %d rules",
				ErrResourceLimitExceeded,
				busName,
				maxRulesPerBus,
			)
		}
	}

	rule := &Rule{
		Name:               input.Name,
		Arn:                b.ruleARN(region, busName, input.Name),
		EventBusName:       busName,
		EventPattern:       input.EventPattern,
		State:              state,
		Description:        input.Description,
		ScheduleExpression: input.ScheduleExpression,
		RoleArn:            input.RoleArn,
		ManagedBy:          input.ManagedBy,
		compiledPattern:    compiled,
	}

	if existing, exists := rules[busKey][input.Name]; exists {
		b.removeRuleFromIndex(region, busKey, existing)
	}
	rules[busKey][input.Name] = rule
	b.addRuleToIndex(region, busKey, rule)

	return rule, nil
}

// DeleteRule removes a rule from an event bus.
func (b *InMemoryBackend) DeleteRule(ctx context.Context, name, eventBusName string) error {
	if eventBusName == "" {
		eventBusName = defaultEventBusName
	}

	region := getRegionFromContext(ctx, b.region)
	busKey := ebBusKey(eventBusName)

	b.mu.Lock("DeleteRule")
	defer b.mu.Unlock()

	busRules, exists := b.rulesStore(region)[busKey]
	if !exists {
		return fmt.Errorf("%w: Rule %s not found", ErrRuleNotFound, name)
	}

	rule, ruleExists := busRules[name]
	if !ruleExists {
		return fmt.Errorf("%w: Rule %s not found", ErrRuleNotFound, name)
	}

	b.removeRuleFromIndex(region, busKey, rule)
	delete(busRules, name)
	// Also remove targets for this rule.
	delete(b.targetsStore(region), b.targetKey(eventBusName, name))

	return nil
}

// ListRules returns rules for an event bus optionally filtered by name prefix.
func (b *InMemoryBackend) ListRules(ctx context.Context,
	eventBusName, namePrefix, nextToken string,
) ([]Rule, string, error) {
	if eventBusName == "" {
		eventBusName = defaultEventBusName
	}

	region := getRegionFromContext(ctx, b.region)
	busKey := ebBusKey(eventBusName)

	b.mu.RLock("ListRules")
	defer b.mu.RUnlock()

	busRules := b.rulesStore(region)[busKey]
	all := make([]Rule, 0, len(busRules))
	for _, r := range busRules {
		if namePrefix == "" || strings.HasPrefix(r.Name, namePrefix) {
			all = append(all, *r)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// DescribeRule returns a single rule.
func (b *InMemoryBackend) DescribeRule(ctx context.Context, name, eventBusName string) (*Rule, error) {
	if eventBusName == "" {
		eventBusName = defaultEventBusName
	}

	region := getRegionFromContext(ctx, b.region)
	busKey := ebBusKey(eventBusName)

	b.mu.RLock("DescribeRule")
	defer b.mu.RUnlock()

	busRules, exists := b.rulesStore(region)[busKey]
	if !exists {
		return nil, fmt.Errorf("%w: Rule %s not found", ErrRuleNotFound, name)
	}

	rule, exists := busRules[name]
	if !exists {
		return nil, fmt.Errorf("%w: Rule %s not found", ErrRuleNotFound, name)
	}

	cp := *rule

	return &cp, nil
}

// EnableRule sets a rule's state to ENABLED.
func (b *InMemoryBackend) EnableRule(ctx context.Context, name, eventBusName string) error {
	return b.setRuleState(ctx, name, eventBusName, ruleStateEnabled)
}

// DisableRule sets a rule's state to DISABLED.
func (b *InMemoryBackend) DisableRule(ctx context.Context, name, eventBusName string) error {
	return b.setRuleState(ctx, name, eventBusName, ruleStateDisabled)
}

func (b *InMemoryBackend) setRuleState(ctx context.Context, name, eventBusName, state string) error {
	if eventBusName == "" {
		eventBusName = defaultEventBusName
	}

	region := getRegionFromContext(ctx, b.region)
	busKey := ebBusKey(eventBusName)

	b.mu.Lock("setRuleState")
	defer b.mu.Unlock()

	busRules, exists := b.rulesStore(region)[busKey]
	if !exists {
		return fmt.Errorf("%w: Rule %s not found", ErrRuleNotFound, name)
	}

	rule, exists := busRules[name]
	if !exists {
		return fmt.Errorf("%w: Rule %s not found", ErrRuleNotFound, name)
	}

	rule.State = state

	return nil
}

// PutTargets adds or updates targets for a rule.
func (b *InMemoryBackend) PutTargets(ctx context.Context,
	ruleName, eventBusName string,
	targets []Target,
) ([]FailedEntry, error) {
	if eventBusName == "" {
		eventBusName = defaultEventBusName
	}

	if len(targets) == 0 {
		return nil, fmt.Errorf("%w: at least one target is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)
	busKey := ebBusKey(eventBusName)

	b.mu.Lock("PutTargets")
	defer b.mu.Unlock()

	busRules, exists := b.rulesStore(region)[busKey]
	if !exists {
		return nil, fmt.Errorf("%w: Rule %s not found", ErrRuleNotFound, ruleName)
	}

	if _, ruleExists := busRules[ruleName]; !ruleExists {
		return nil, fmt.Errorf("%w: Rule %s not found", ErrRuleNotFound, ruleName)
	}

	targetsStore := b.targetsStore(region)
	key := b.targetKey(eventBusName, ruleName)
	if targetsStore[key] == nil {
		targetsStore[key] = make(map[string]*Target)
	}

	// Reject if adding these targets would exceed the per-rule limit.
	if len(targetsStore[key])+len(targets) > maxTargetsPerRule {
		return nil, fmt.Errorf(
			"%w: rule %s already has the maximum number of targets (%d)",
			ErrInvalidParameter,
			ruleName,
			maxTargetsPerRule,
		)
	}

	var failed []FailedEntry
	for _, t := range targets {
		if t.ID == "" {
			failed = append(failed, FailedEntry{
				TargetID:     t.ID,
				ErrorCode:    "InvalidParameter",
				ErrorMessage: "Target Id is required",
			})

			continue
		}
		if t.InputTransformer != nil {
			if err := validateInputTransformer(t.InputTransformer); err != nil {
				failed = append(failed, FailedEntry{
					TargetID:     t.ID,
					ErrorCode:    "InvalidParameter",
					ErrorMessage: err.Error(),
				})

				continue
			}
		}
		cp := t
		targetsStore[key][t.ID] = &cp
	}

	return failed, nil
}

// RemoveTargets removes targets from a rule by their IDs.
func (b *InMemoryBackend) RemoveTargets(ctx context.Context,
	ruleName, eventBusName string,
	ids []string,
) ([]FailedEntry, error) {
	if eventBusName == "" {
		eventBusName = defaultEventBusName
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("RemoveTargets")
	defer b.mu.Unlock()

	key := b.targetKey(eventBusName, ruleName)
	ruleTargets := b.targetsStore(region)[key]

	var failed []FailedEntry
	for _, id := range ids {
		if _, exists := ruleTargets[id]; !exists {
			failed = append(failed, FailedEntry{
				TargetID:     id,
				ErrorCode:    "ResourceNotFoundException",
				ErrorMessage: fmt.Sprintf("Target %s not found", id),
			})

			continue
		}
		delete(ruleTargets, id)
	}

	return failed, nil
}

// ListTargetsByRule returns targets for a rule with optional pagination.
func (b *InMemoryBackend) ListTargetsByRule(ctx context.Context,
	ruleName, eventBusName, nextToken string,
) ([]Target, string, error) {
	if eventBusName == "" {
		eventBusName = defaultEventBusName
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("ListTargetsByRule")
	defer b.mu.RUnlock()

	key := b.targetKey(eventBusName, ruleName)
	ruleTargets := b.targetsStore(region)[key]
	all := make([]Target, 0, len(ruleTargets))
	for _, t := range ruleTargets {
		all = append(all, *t)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// PutEvents records events in the event log and returns result entries.
//
// AWS EventBridge constrains PutEvents requests to 256 KiB of total entry
// payload (sum of Source, DetailType, Detail, Resources, Time across every
// entry). Entries that, combined with what's been accepted so far, would
// exceed the cap are rejected individually with the AWS error code
// `EventSizeLimitExceeded`. The remaining entries continue to be accepted.
func (b *InMemoryBackend) PutEvents(ctx context.Context, entries []EventEntry) []EventResultEntry {
	const maxBatchBytes = 256 * 1024

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("PutEvents")

	results := make([]EventResultEntry, 0, len(entries))
	accepted := make([]EventEntry, 0, len(entries))
	totalBytes := 0
	for _, entry := range entries {
		entryBytes := putEventsEntryBytes(entry)
		if totalBytes+entryBytes > maxBatchBytes {
			results = append(results, EventResultEntry{
				ErrorCode:    "EventSizeLimitExceeded",
				ErrorMessage: "Event size exceeds 256 KB total batch limit",
			})

			continue
		}

		totalBytes += entryBytes
		eventID := uuid.New().String()
		busName := entry.EventBusName
		if busName == "" {
			busName = defaultEventBusName
		}
		eventTime := time.Now()
		if entry.Time != nil {
			eventTime = *entry.Time
		}
		logEntry := EventLogEntry{
			ID:           eventID,
			Source:       entry.Source,
			DetailType:   entry.DetailType,
			Detail:       entry.Detail,
			EventBusName: busName,
			Time:         eventTime,
		}
		b.eventLog = append(b.eventLog, logEntry)
		// Trim event log to last 1000 entries.
		if len(b.eventLog) > maxEventLogSize {
			b.eventLog = b.eventLog[len(b.eventLog)-maxEventLogSize:]
		}
		// Capture event into matching archives.
		b.captureEventInArchives(region, entry, busName)
		accepted = append(accepted, entry)
		results = append(results, EventResultEntry{EventID: eventID})
	}

	dt := b.deliveryTargets
	workerSem := b.workerSem
	svcCtx := b.ctx
	delivTimeout := b.deliveryTimeout
	b.mu.Unlock()

	// Trigger async fan-out delivery after releasing the lock.
	// Skip if the backend is already closing to prevent wg.Add concurrent with
	// wg.Wait (which would panic per sync.WaitGroup semantics).
	if dt != nil && !b.closing.Load() && len(accepted) > 0 {
		entriesCopy := make([]EventEntry, len(accepted))
		copy(entriesCopy, accepted)
		dtCopy := *dt
		b.wg.Go(func() {
			// Acquire a worker slot or abort if the backend is shutting down.
			select {
			case workerSem <- struct{}{}:
				defer func() { <-workerSem }()
			case <-svcCtx.Done():
				return
			}
			b.deliverEvents(svcCtx, region, entriesCopy, dtCopy, delivTimeout)
		})
	}

	return results
}

// GetEventLog returns a copy of the current event log.
func (b *InMemoryBackend) GetEventLog(ctx context.Context) []EventLogEntry { //nolint:revive // existing issue.
	b.mu.RLock("GetEventLog")
	defer b.mu.RUnlock()

	log := make([]EventLogEntry, len(b.eventLog))
	copy(log, b.eventLog)

	return log
}

func parseNextToken(token string) int {
	if token == "" {
		return 0
	}
	idx, err := strconv.Atoi(token)
	if err != nil || idx < 0 {
		return 0
	}

	return idx
}

// paginate applies offset-based pagination to a pre-sorted slice.
// It returns the page slice and the next-page token (or "").
func paginate[T any](all []T, nextToken string) ([]T, string) {
	const defaultLimit = 100

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []T{}, ""
	}

	end := startIdx + defaultLimit
	var outToken string
	if end < len(all) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.buses = make(map[string]map[string]*EventBus)
	b.rules = make(map[string]map[string]map[string]*Rule)
	b.targets = make(map[string]map[string]map[string]*Target)
	b.eventLog = nil
	b.eventSources = make(map[string]map[string]*EventSource)
	b.replays = make(map[string]map[string]*Replay)
	b.apiDestinations = make(map[string]map[string]*APIDestination)
	b.archives = make(map[string]map[string]*Archive)
	b.archivedEvents = make(map[string]map[string][]EventEntry)
	b.connections = make(map[string]map[string]*Connection)
	b.endpoints = make(map[string]map[string]*Endpoint)
	b.partnerSources = make(map[string]map[string]*PartnerEventSource)
	b.busePolicies = make(map[string]map[string]*EventBusPolicy)
	b.pipes = make(map[string]*Pipe)
	b.registries = make(map[string]*SchemaRegistry)
	b.schemas = make(map[string]map[string]*Schema)
	b.schemaVersions = make(map[string][]*SchemaVersion)
	b.codeBindings = make(map[string]*CodeBinding)
	b.ruleIndex = make(map[string]map[string]map[ruleIndexKey]map[string]*Rule)
	b.patternCache = sync.Map{}

	// Re-create the default event bus so it is always available after reset.
	b.busesStore(b.region)[defaultEventBusName] = &EventBus{
		Name:        defaultEventBusName,
		Arn:         b.busARN(b.region, defaultEventBusName),
		CreatedTime: time.Now(),
	}
}

// ActivateEventSource activates a partner event source.
func (b *InMemoryBackend) ActivateEventSource(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("ActivateEventSource")
	defer b.mu.Unlock()

	src, exists := b.eventSourcesStore(region)[name]
	if !exists {
		return fmt.Errorf("%w: event source %s not found", ErrNotFound, name)
	}

	src.State = stateActive

	return nil
}

// DeactivateEventSource deactivates a partner event source.
func (b *InMemoryBackend) DeactivateEventSource(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("DeactivateEventSource")
	defer b.mu.Unlock()

	src, exists := b.eventSourcesStore(region)[name]
	if !exists {
		return fmt.Errorf("%w: event source %s not found", ErrNotFound, name)
	}

	src.State = "INACTIVE"

	return nil
}

// CreatePartnerEventSource creates a new partner event source.
func (b *InMemoryBackend) CreatePartnerEventSource(ctx context.Context,
	name, account string,
) (*PartnerEventSource, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("CreatePartnerEventSource")
	defer b.mu.Unlock()

	if _, exists := b.partnerSourcesStore(region)[name]; exists {
		return nil, fmt.Errorf("%w: partner event source %s already exists", ErrAlreadyExists, name)
	}

	src := &PartnerEventSource{
		Arn:     b.partnerSourceARN(name),
		Name:    name,
		Account: account,
	}
	b.partnerSourcesStore(region)[name] = src

	// Mirror as a PENDING EventSource in the customer account — matches AWS
	// behaviour where creating a partner source causes it to appear in the
	// customer's ListEventSources as PENDING until they call ActivateEventSource.
	now := time.Now()
	esrc := &EventSource{
		Arn:          b.partnerSourceARN(name),
		CreatedBy:    name,
		CreationTime: now,
		Name:         name,
		State:        "PENDING",
	}
	b.eventSourcesStore(region)[name] = esrc

	cp := *src

	return &cp, nil
}

// CancelReplay cancels a running or starting replay.
func (b *InMemoryBackend) CancelReplay(ctx context.Context, replayName string) (*Replay, error) {
	if replayName == "" {
		return nil, fmt.Errorf("%w: ReplayName is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("CancelReplay")
	defer b.mu.Unlock()

	replay, exists := b.replaysStore(region)[replayName]
	if !exists {
		return nil, fmt.Errorf("%w: replay %s not found", ErrNotFound, replayName)
	}

	if replay.State != "RUNNING" && replay.State != replayStateStarting {
		return nil, fmt.Errorf(
			"%w: replay %s is not in a cancellable state (current: %s)",
			ErrInvalidState,
			replayName,
			replay.State,
		)
	}

	replay.State = "CANCELLING"

	cp := *replay

	return &cp, nil
}

// CreateAPIDestination creates a new API destination.
func (b *InMemoryBackend) CreateAPIDestination(ctx context.Context,
	input CreateAPIDestinationInput,
) (*APIDestination, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	if input.ConnectionArn == "" {
		return nil, fmt.Errorf("%w: ConnectionArn is required", ErrInvalidParameter)
	}

	if input.InvocationEndpoint == "" {
		return nil, fmt.Errorf("%w: InvocationEndpoint is required", ErrInvalidParameter)
	}

	if input.HTTPMethod == "" {
		return nil, fmt.Errorf("%w: HttpMethod is required", ErrInvalidParameter)
	}

	if !isValidHTTPMethod(input.HTTPMethod) {
		return nil, fmt.Errorf(
			"%w: HttpMethod must be one of GET, HEAD, POST, OPTIONS, PUT, DELETE, PATCH",
			ErrInvalidParameter,
		)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("CreateAPIDestination")
	defer b.mu.Unlock()

	if _, exists := b.apiDestinationsStore(region)[input.Name]; exists {
		return nil, fmt.Errorf(
			"%w: API destination %s already exists",
			ErrAlreadyExists,
			input.Name,
		)
	}

	now := time.Now()
	dst := &APIDestination{
		APIDestinationArn:            b.apiDestinationARN(input.Name),
		APIDestinationState:          stateActive,
		ConnectionArn:                input.ConnectionArn,
		CreationTime:                 now,
		Description:                  input.Description,
		HTTPMethod:                   input.HTTPMethod,
		InvocationEndpoint:           input.InvocationEndpoint,
		InvocationRateLimitPerSecond: input.InvocationRateLimitPerSecond,
		LastModifiedTime:             now,
		Name:                         input.Name,
	}
	b.apiDestinationsStore(region)[input.Name] = dst

	cp := *dst

	return &cp, nil
}

// CreateArchive creates a new event archive.
func (b *InMemoryBackend) CreateArchive(ctx context.Context, input CreateArchiveInput) (*Archive, error) {
	if input.ArchiveName == "" {
		return nil, fmt.Errorf("%w: ArchiveName is required", ErrInvalidParameter)
	}

	if len(input.ArchiveName) > maxArchiveNameLength {
		return nil, fmt.Errorf(
			"%w: ArchiveName must be %d characters or fewer",
			ErrInvalidParameter,
			maxArchiveNameLength,
		)
	}

	if input.EventSourceArn == "" {
		return nil, fmt.Errorf("%w: EventSourceArn is required", ErrInvalidParameter)
	}

	if input.RetentionDays < 0 {
		return nil, fmt.Errorf(
			"%w: RetentionDays must be 0 (indefinite) or a positive integer",
			ErrInvalidParameter,
		)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("CreateArchive")
	defer b.mu.Unlock()

	if _, exists := b.archivesStore(region)[input.ArchiveName]; exists {
		return nil, fmt.Errorf("%w: archive %s already exists", ErrAlreadyExists, input.ArchiveName)
	}

	archive := &Archive{
		ArchiveName:    input.ArchiveName,
		ArchiveArn:     b.archiveARN(input.ArchiveName),
		CreationTime:   time.Now(),
		Description:    input.Description,
		EventPattern:   input.EventPattern,
		EventSourceArn: input.EventSourceArn,
		RetentionDays:  input.RetentionDays,
		State:          "ENABLED",
	}
	b.archivesStore(region)[input.ArchiveName] = archive

	cp := *archive

	return &cp, nil
}

// CreateConnection creates a new connection.
func (b *InMemoryBackend) CreateConnection(ctx context.Context, input CreateConnectionInput) (*Connection, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	if input.AuthorizationType == "" {
		return nil, fmt.Errorf("%w: AuthorizationType is required", ErrInvalidParameter)
	}

	if !isValidConnectionAuthType(input.AuthorizationType) {
		return nil, fmt.Errorf(
			"%w: AuthorizationType must be one of API_KEY, BASIC, OAUTH_CLIENT_CREDENTIALS",
			ErrInvalidParameter,
		)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("CreateConnection")
	defer b.mu.Unlock()

	if _, exists := b.connectionsStore(region)[input.Name]; exists {
		return nil, fmt.Errorf("%w: connection %s already exists", ErrAlreadyExists, input.Name)
	}

	now := time.Now()
	conn := &Connection{
		ConnectionArn:     b.connectionARN(input.Name),
		AuthorizationType: input.AuthorizationType,
		AuthParameters:    maskConnectionAuthParameters(input.AuthParameters),
		ConnectionState:   "AUTHORIZED",
		CreationTime:      now,
		Description:       input.Description,
		LastModifiedTime:  now,
		Name:              input.Name,
	}
	b.connectionsStore(region)[input.Name] = conn

	cp := *conn

	return &cp, nil
}

// CreateEndpoint creates a new global endpoint.
func (b *InMemoryBackend) CreateEndpoint(ctx context.Context, input CreateEndpointInput) (*Endpoint, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("CreateEndpoint")
	defer b.mu.Unlock()

	if _, exists := b.endpointsStore(region)[input.Name]; exists {
		return nil, fmt.Errorf("%w: endpoint %s already exists", ErrAlreadyExists, input.Name)
	}

	now := time.Now()
	buses := input.EventBuses
	if buses == nil {
		buses = []EndpointEventBus{}
	}

	ep := &Endpoint{
		Arn:               b.endpointARN(input.Name),
		CreationTime:      now,
		Description:       input.Description,
		EndpointID:        input.Name + "-" + b.region,
		EndpointURL:       "https://" + input.Name + ".endpoint.events." + b.region + ".amazonaws.com",
		EventBuses:        buses,
		LastModifiedTime:  now,
		Name:              input.Name,
		ReplicationConfig: input.ReplicationConfig,
		RoleArn:           input.RoleArn,
		RoutingConfig:     input.RoutingConfig,
		State:             stateActive,
	}
	b.endpointsStore(region)[input.Name] = ep

	cp := *ep

	return &cp, nil
}

// DeauthorizeConnection deauthorizes a connection.
func (b *InMemoryBackend) DeauthorizeConnection(ctx context.Context, name string) (*Connection, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("DeauthorizeConnection")
	defer b.mu.Unlock()

	conn, exists := b.connectionsStore(region)[name]
	if !exists {
		return nil, fmt.Errorf("%w: connection %s not found", ErrNotFound, name)
	}

	conn.ConnectionState = "DEAUTHORIZED"

	cp := *conn

	return &cp, nil
}

// DeleteAPIDestination deletes an API destination.
func (b *InMemoryBackend) DeleteAPIDestination(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("DeleteAPIDestination")
	defer b.mu.Unlock()

	store := b.apiDestinationsStore(region)
	if _, exists := store[name]; !exists {
		return fmt.Errorf("%w: API destination %s not found", ErrNotFound, name)
	}

	delete(store, name)

	return nil
}

// DeleteArchive deletes an archive.
func (b *InMemoryBackend) DeleteArchive(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("%w: ArchiveName is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("DeleteArchive")
	defer b.mu.Unlock()

	store := b.archivesStore(region)
	if _, exists := store[name]; !exists {
		return fmt.Errorf("%w: archive %s not found", ErrNotFound, name)
	}

	delete(store, name)
	delete(b.archivedEventsStore(region), name)

	return nil
}

// DescribeArchive returns a single archive by name.
func (b *InMemoryBackend) DescribeArchive(ctx context.Context, name string) (*Archive, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: ArchiveName is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("DescribeArchive")
	defer b.mu.RUnlock()

	archive, exists := b.archivesStore(region)[name]
	if !exists {
		return nil, fmt.Errorf("%w: archive %s not found", ErrNotFound, name)
	}

	cp := *archive

	return &cp, nil
}

// ListArchives returns archives optionally filtered by name prefix, with pagination.
func (b *InMemoryBackend) ListArchives(ctx context.Context, namePrefix, nextToken string) ([]Archive, string, error) {
	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("ListArchives")
	defer b.mu.RUnlock()

	store := b.archivesStore(region)
	all := make([]Archive, 0, len(store))
	for _, a := range store {
		if namePrefix == "" || strings.HasPrefix(a.ArchiveName, namePrefix) {
			all = append(all, *a)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ArchiveName < all[j].ArchiveName })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// UpdateArchive updates an existing archive.
func (b *InMemoryBackend) UpdateArchive(ctx context.Context, input UpdateArchiveInput) (*Archive, error) {
	if input.ArchiveName == "" {
		return nil, fmt.Errorf("%w: ArchiveName is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("UpdateArchive")
	defer b.mu.Unlock()

	archive, exists := b.archivesStore(region)[input.ArchiveName]
	if !exists {
		return nil, fmt.Errorf("%w: archive %s not found", ErrNotFound, input.ArchiveName)
	}

	if input.Description != "" {
		archive.Description = input.Description
	}
	if input.EventPattern != "" {
		archive.EventPattern = input.EventPattern
	}
	if input.RetentionDays >= 0 {
		archive.RetentionDays = input.RetentionDays
	}

	cp := *archive

	return &cp, nil
}

// DeleteConnection deletes a connection.
func (b *InMemoryBackend) DeleteConnection(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("DeleteConnection")
	defer b.mu.Unlock()

	store := b.connectionsStore(region)
	if _, exists := store[name]; !exists {
		return fmt.Errorf("%w: connection %s not found", ErrNotFound, name)
	}

	delete(store, name)

	return nil
}

// DescribeConnection returns a single connection by name.
func (b *InMemoryBackend) DescribeConnection(ctx context.Context, name string) (*Connection, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("DescribeConnection")
	defer b.mu.RUnlock()

	conn, exists := b.connectionsStore(region)[name]
	if !exists {
		return nil, fmt.Errorf("%w: connection %s not found", ErrNotFound, name)
	}

	cp := *conn

	return &cp, nil
}

// ListConnections returns connections optionally filtered by name prefix, with pagination.
func (b *InMemoryBackend) ListConnections(ctx context.Context,
	namePrefix, nextToken string,
) ([]Connection, string, error) {
	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("ListConnections")
	defer b.mu.RUnlock()

	store := b.connectionsStore(region)
	all := make([]Connection, 0, len(store))
	for _, c := range store {
		if namePrefix == "" || strings.HasPrefix(c.Name, namePrefix) {
			all = append(all, *c)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// UpdateConnection updates an existing connection.
func (b *InMemoryBackend) UpdateConnection(ctx context.Context, input UpdateConnectionInput) (*Connection, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("UpdateConnection")
	defer b.mu.Unlock()

	conn, exists := b.connectionsStore(region)[input.Name]
	if !exists {
		return nil, fmt.Errorf("%w: connection %s not found", ErrNotFound, input.Name)
	}

	if input.Description != "" {
		conn.Description = input.Description
	}
	if input.AuthorizationType != "" {
		conn.AuthorizationType = input.AuthorizationType
	}
	if input.AuthParameters != nil {
		conn.AuthParameters = maskConnectionAuthParameters(input.AuthParameters)
	}
	conn.LastModifiedTime = time.Now()

	cp := *conn

	return &cp, nil
}

// DeleteEndpoint deletes an endpoint.
func (b *InMemoryBackend) DeleteEndpoint(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("DeleteEndpoint")
	defer b.mu.Unlock()

	store := b.endpointsStore(region)
	if _, exists := store[name]; !exists {
		return fmt.Errorf("%w: endpoint %s not found", ErrNotFound, name)
	}

	delete(store, name)

	return nil
}

// DescribeEndpoint returns a single endpoint by name.
func (b *InMemoryBackend) DescribeEndpoint(ctx context.Context, name string) (*Endpoint, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("DescribeEndpoint")
	defer b.mu.RUnlock()

	ep, exists := b.endpointsStore(region)[name]
	if !exists {
		return nil, fmt.Errorf("%w: endpoint %s not found", ErrNotFound, name)
	}

	cp := *ep

	return &cp, nil
}

// ListEndpoints returns endpoints optionally filtered by name prefix, with pagination.
func (b *InMemoryBackend) ListEndpoints(ctx context.Context, namePrefix, nextToken string) ([]Endpoint, string, error) {
	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("ListEndpoints")
	defer b.mu.RUnlock()

	store := b.endpointsStore(region)
	all := make([]Endpoint, 0, len(store))
	for _, ep := range store {
		if namePrefix == "" || strings.HasPrefix(ep.Name, namePrefix) {
			all = append(all, *ep)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// UpdateEndpoint updates an existing endpoint.
func (b *InMemoryBackend) UpdateEndpoint(ctx context.Context, input UpdateEndpointInput) (*Endpoint, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("UpdateEndpoint")
	defer b.mu.Unlock()

	ep, exists := b.endpointsStore(region)[input.Name]
	if !exists {
		return nil, fmt.Errorf("%w: endpoint %s not found", ErrNotFound, input.Name)
	}

	if input.Description != "" {
		ep.Description = input.Description
	}
	if input.RoleArn != "" {
		ep.RoleArn = input.RoleArn
	}
	if input.RoutingConfig != nil {
		ep.RoutingConfig = input.RoutingConfig
	}
	if input.ReplicationConfig != nil {
		ep.ReplicationConfig = input.ReplicationConfig
	}
	if len(input.EventBuses) > 0 {
		ep.EventBuses = input.EventBuses
	}
	ep.LastModifiedTime = time.Now()

	cp := *ep

	return &cp, nil
}

// DescribeAPIDestination returns a single API destination by name.
func (b *InMemoryBackend) DescribeAPIDestination(ctx context.Context, name string) (*APIDestination, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("DescribeAPIDestination")
	defer b.mu.RUnlock()

	dst, exists := b.apiDestinationsStore(region)[name]
	if !exists {
		return nil, fmt.Errorf("%w: API destination %s not found", ErrNotFound, name)
	}

	cp := *dst

	return &cp, nil
}

// ListAPIDestinations returns API destinations optionally filtered by name prefix, with pagination.
func (b *InMemoryBackend) ListAPIDestinations(ctx context.Context,
	namePrefix, nextToken string,
) ([]APIDestination, string, error) {
	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("ListAPIDestinations")
	defer b.mu.RUnlock()

	store := b.apiDestinationsStore(region)
	all := make([]APIDestination, 0, len(store))
	for _, d := range store {
		if namePrefix == "" || strings.HasPrefix(d.Name, namePrefix) {
			all = append(all, *d)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// UpdateAPIDestination updates an existing API destination.
func (b *InMemoryBackend) UpdateAPIDestination(ctx context.Context,
	input UpdateAPIDestinationInput,
) (*APIDestination, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("UpdateAPIDestination")
	defer b.mu.Unlock()

	dst, exists := b.apiDestinationsStore(region)[input.Name]
	if !exists {
		return nil, fmt.Errorf("%w: API destination %s not found", ErrNotFound, input.Name)
	}

	if input.ConnectionArn != "" {
		dst.ConnectionArn = input.ConnectionArn
	}
	if input.Description != "" {
		dst.Description = input.Description
	}
	if input.HTTPMethod != "" {
		dst.HTTPMethod = input.HTTPMethod
	}
	if input.InvocationEndpoint != "" {
		dst.InvocationEndpoint = input.InvocationEndpoint
	}
	if input.InvocationRateLimitPerSecond > 0 {
		dst.InvocationRateLimitPerSecond = input.InvocationRateLimitPerSecond
	}
	dst.LastModifiedTime = time.Now()

	cp := *dst

	return &cp, nil
}

// DescribeEventSource returns a single event source by name.
func (b *InMemoryBackend) DescribeEventSource(ctx context.Context, name string) (*EventSource, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("DescribeEventSource")
	defer b.mu.RUnlock()

	src, exists := b.eventSourcesStore(region)[name]
	if !exists {
		return nil, fmt.Errorf("%w: event source %s not found", ErrNotFound, name)
	}

	cp := *src

	return &cp, nil
}

// ListEventSources returns event sources optionally filtered by name prefix, with pagination.
func (b *InMemoryBackend) ListEventSources(ctx context.Context,
	namePrefix, nextToken string,
) ([]EventSource, string, error) {
	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("ListEventSources")
	defer b.mu.RUnlock()

	store := b.eventSourcesStore(region)
	all := make([]EventSource, 0, len(store))
	for _, s := range store {
		if namePrefix == "" || strings.HasPrefix(s.Name, namePrefix) {
			all = append(all, *s)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// DescribePartnerEventSource returns a single partner event source by name.
func (b *InMemoryBackend) DescribePartnerEventSource(ctx context.Context, name string) (*PartnerEventSource, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("DescribePartnerEventSource")
	defer b.mu.RUnlock()

	src, exists := b.partnerSourcesStore(region)[name]
	if !exists {
		return nil, fmt.Errorf("%w: partner event source %s not found", ErrNotFound, name)
	}

	cp := *src

	return &cp, nil
}

// DeletePartnerEventSource deletes a partner event source.
func (b *InMemoryBackend) DeletePartnerEventSource(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("DeletePartnerEventSource")
	defer b.mu.Unlock()

	store := b.partnerSourcesStore(region)
	if _, exists := store[name]; !exists {
		return fmt.Errorf("%w: partner event source %s not found", ErrNotFound, name)
	}

	delete(store, name)

	return nil
}

// ListPartnerEventSources returns partner event sources optionally filtered by name prefix.
func (b *InMemoryBackend) ListPartnerEventSources(ctx context.Context,
	namePrefix, nextToken string,
) ([]PartnerEventSource, string, error) {
	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("ListPartnerEventSources")
	defer b.mu.RUnlock()

	store := b.partnerSourcesStore(region)
	all := make([]PartnerEventSource, 0, len(store))
	for _, s := range store {
		if namePrefix == "" || strings.HasPrefix(s.Name, namePrefix) {
			all = append(all, *s)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// PutPartnerEvents records partner events (same as PutEvents but intended for partner sources).
func (b *InMemoryBackend) PutPartnerEvents(ctx context.Context, entries []EventEntry) []EventResultEntry {
	return b.PutEvents(ctx, entries)
}

// DescribeReplay returns a single replay by name.
func (b *InMemoryBackend) DescribeReplay(ctx context.Context, name string) (*Replay, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: ReplayName is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("DescribeReplay")
	defer b.mu.RUnlock()

	replay, exists := b.replaysStore(region)[name]
	if !exists {
		return nil, fmt.Errorf("%w: replay %s not found", ErrNotFound, name)
	}

	cp := *replay

	return &cp, nil
}

// ListReplays returns replays optionally filtered by name prefix, with pagination.
func (b *InMemoryBackend) ListReplays(ctx context.Context, namePrefix, nextToken string) ([]Replay, string, error) {
	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("ListReplays")
	defer b.mu.RUnlock()

	store := b.replaysStore(region)
	all := make([]Replay, 0, len(store))
	for _, r := range store {
		if namePrefix == "" || strings.HasPrefix(r.ReplayName, namePrefix) {
			all = append(all, *r)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ReplayName < all[j].ReplayName })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// StartReplay creates a new replay in the STARTING state.
func (b *InMemoryBackend) StartReplay(ctx context.Context, input StartReplayInput) (*Replay, error) {
	if input.ReplayName == "" {
		return nil, fmt.Errorf("%w: ReplayName is required", ErrInvalidParameter)
	}

	if input.EventSourceArn == "" {
		return nil, fmt.Errorf("%w: EventSourceArn is required", ErrInvalidParameter)
	}

	if !input.EventStartTime.IsZero() && !input.EventEndTime.IsZero() &&
		!input.EventStartTime.Before(input.EventEndTime) {
		return nil, fmt.Errorf(
			"%w: EventStartTime must be before EventEndTime",
			ErrInvalidParameter,
		)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("StartReplay")

	replays := b.replaysStore(region)
	if _, exists := replays[input.ReplayName]; exists {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: replay %s already exists", ErrAlreadyExists, input.ReplayName)
	}

	// Validate destination ARN points to a known event bus.
	if input.Destination != nil && input.Destination.Arn != "" {
		found := false
		for _, bus := range b.busesStore(region) {
			if bus.Arn == input.Destination.Arn {
				found = true

				break
			}
		}
		if !found {
			b.mu.Unlock()

			return nil, fmt.Errorf(
				"%w: destination ARN %s does not match any event bus",
				ErrInvalidParameter,
				input.Destination.Arn,
			)
		}
	}

	// Find the archive by ARN (EventSourceArn points to an archive ARN).
	var archiveName string
	var archivePattern string
	for name, archive := range b.archivesStore(region) {
		if archive.ArchiveArn == input.EventSourceArn {
			archiveName = name
			archivePattern = archive.EventPattern

			break
		}
	}

	replay := &Replay{
		EventSourceArn:  input.EventSourceArn,
		EventStartTime:  input.EventStartTime,
		EventEndTime:    input.EventEndTime,
		ReplayArn:       b.replayARN(input.ReplayName),
		ReplayName:      input.ReplayName,
		ReplayStartTime: time.Now(),
		State:           replayStateStarting,
		StateReason:     input.Description,
	}
	replays[input.ReplayName] = replay

	// Collect archived events to replay filtered by time window and event pattern.
	eventsToReplay := b.filterArchivedEvents(
		region,
		archiveName,
		archivePattern,
		input.EventStartTime,
		input.EventEndTime,
	)

	dt := b.deliveryTargets
	workerSem := b.workerSem
	svcCtx := b.ctx
	delivTimeout := b.deliveryTimeout
	cp := *replay
	b.mu.Unlock()

	// Deliver archived events asynchronously and mark the replay complete.
	if !b.closing.Load() {
		b.scheduleReplayWorker(svcCtx, region, workerSem, input.ReplayName, eventsToReplay, dt, delivTimeout)
	}

	return &cp, nil
}

// filterArchivedEvents returns archived events for the named archive filtered by
// time window [startTime, endTime) and optional event pattern.
// Must be called with b.mu held for reading.
func (b *InMemoryBackend) filterArchivedEvents(
	region, archiveName, pattern string,
	startTime, endTime time.Time,
) []EventEntry {
	if archiveName == "" {
		return nil
	}

	raw := b.archivedEventsStore(region)[archiveName]
	if len(raw) == 0 {
		return nil
	}

	result := make([]EventEntry, 0, len(raw))
	for _, e := range raw {
		t := time.Now()
		if e.Time != nil {
			t = *e.Time
		}
		if !startTime.IsZero() && t.Before(startTime) {
			continue
		}
		if !endTime.IsZero() && !t.Before(endTime) {
			continue
		}
		if pattern != "" {
			envelope := buildEventEnvelope(e)
			if !matchPattern(pattern, envelope) {
				continue
			}
		}
		result = append(result, e)
	}

	return result
}

// scheduleReplayWorker launches a background goroutine that delivers archived events
// and then marks the replay COMPLETED. Extracted to reduce cognitive complexity of StartReplay.
func (b *InMemoryBackend) scheduleReplayWorker(
	ctx context.Context,
	region string,
	workerSem chan struct{},
	replayName string,
	eventsToReplay []EventEntry,
	dt *DeliveryTargets,
	delivTimeout time.Duration,
) {
	b.wg.Go(func() {
		select {
		case workerSem <- struct{}{}:
			defer func() { <-workerSem }()
		case <-ctx.Done():
			return
		}

		if dt != nil && len(eventsToReplay) > 0 {
			b.deliverEvents(ctx, region, eventsToReplay, *dt, delivTimeout)
		}

		b.mu.Lock("StartReplay-complete")
		if r, ok := b.replaysStore(region)[replayName]; ok && r.State == replayStateStarting {
			r.State = "COMPLETED"
			r.ReplayEndTime = time.Now()
		}
		b.mu.Unlock()
	})
}

// ListRuleNamesByTarget returns rule names that have a target matching the given ARN.
func (b *InMemoryBackend) ListRuleNamesByTarget(ctx context.Context,
	targetARN, eventBusName, nextToken string,
) ([]string, string, error) {
	if eventBusName == "" {
		eventBusName = defaultEventBusName
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("ListRuleNamesByTarget")
	defer b.mu.RUnlock()

	// Within a region-scoped target map, keys have format: "busName/ruleName".
	prefix := ebBusKey(eventBusName) + "/"
	var names []string
	for targetKey, tMap := range b.targetsStore(region) {
		if !strings.HasPrefix(targetKey, prefix) {
			continue
		}
		for _, t := range tMap {
			if t.Arn == targetARN {
				names = append(names, strings.TrimPrefix(targetKey, prefix))

				break
			}
		}
	}

	sort.Strings(names)

	page, outToken := paginate(names, nextToken)

	return page, outToken, nil
}

// TestEventPattern tests an event pattern against an event JSON string.
func (b *InMemoryBackend) TestEventPattern(
	ctx context.Context, //nolint:revive // existing issue.
	pattern, event string,
) (bool, error) {
	if pattern == "" {
		return false, fmt.Errorf("%w: EventPattern is required", ErrInvalidParameter)
	}

	if event == "" {
		return false, fmt.Errorf("%w: Event is required", ErrInvalidParameter)
	}

	if !isValidJSON(event) {
		return false, fmt.Errorf("%w: Event must be valid JSON", ErrInvalidParameter)
	}

	compiled, err := b.getOrCompilePattern(pattern)
	if err != nil {
		return false, fmt.Errorf("%w: EventPattern is not valid JSON", ErrInvalidParameter)
	}

	return matchCompiledPattern(compiled, event), nil
}

// UpdateEventBus updates an existing event bus description.
func (b *InMemoryBackend) UpdateEventBus(ctx context.Context, input UpdateEventBusInput) (*EventBus, error) {
	busName := input.Name
	if busName == "" {
		busName = defaultEventBusName
	}

	region := getRegionFromContext(ctx, b.region)
	busKey := ebBusKey(busName)

	b.mu.Lock("UpdateEventBus")
	defer b.mu.Unlock()

	bus, exists := b.busesStore(region)[busKey]
	if !exists {
		return nil, fmt.Errorf("%w: event bus %s not found", ErrEventBusNotFound, busName)
	}

	bus.Description = input.Description

	cp := *bus

	return &cp, nil
}

// PutPermission adds or replaces a resource-based policy statement on an event bus.
func (b *InMemoryBackend) PutPermission(ctx context.Context, input PutPermissionInput) error {
	busName := input.EventBusName
	if busName == "" {
		busName = defaultEventBusName
	}

	region := getRegionFromContext(ctx, b.region)
	busKey := ebBusKey(busName)

	b.mu.Lock("PutPermission")
	defer b.mu.Unlock()

	if _, exists := b.busesStore(region)[busKey]; !exists {
		return fmt.Errorf("%w: event bus %s not found", ErrEventBusNotFound, busName)
	}

	policies := b.busePoliciesStore(region)
	policy := policies[busKey]
	if policy == nil {
		policy = &EventBusPolicy{Statements: make(map[string]*EventBusPolicyStatement)}
		policies[busKey] = policy
	}

	// If a raw Policy JSON is provided it replaces the whole policy.
	if input.Policy != "" {
		var stmts []EventBusPolicyStatement
		if err := json.Unmarshal([]byte(input.Policy), &stmts); err == nil {
			policy.Statements = make(map[string]*EventBusPolicyStatement, len(stmts))
			for i := range stmts {
				s := stmts[i]
				policy.Statements[s.Sid] = &s
			}
		}

		return nil
	}

	if input.StatementID == "" {
		return fmt.Errorf("%w: StatementId is required", ErrInvalidParameter)
	}

	stmt := &EventBusPolicyStatement{
		Sid:       input.StatementID,
		Effect:    "Allow",
		Action:    input.Action,
		Principal: input.Principal,
	}
	policy.Statements[input.StatementID] = stmt

	return nil
}

// RemovePermission removes a resource-based policy statement from an event bus.
func (b *InMemoryBackend) RemovePermission(ctx context.Context, input RemovePermissionInput) error {
	busName := input.EventBusName
	if busName == "" {
		busName = defaultEventBusName
	}

	region := getRegionFromContext(ctx, b.region)
	busKey := ebBusKey(busName)

	b.mu.Lock("RemovePermission")
	defer b.mu.Unlock()

	if _, exists := b.busesStore(region)[busKey]; !exists {
		return fmt.Errorf("%w: event bus %s not found", ErrEventBusNotFound, busName)
	}

	policies := b.busePoliciesStore(region)
	if input.RemoveAllPermissions {
		delete(policies, busKey)

		return nil
	}

	policy := policies[busKey]
	if policy == nil {
		return nil
	}
	delete(policy.Statements, input.StatementID)

	return nil
}

// GetEventBusPolicy returns the resource-based policy for an event bus as JSON.
func (b *InMemoryBackend) GetEventBusPolicy(ctx context.Context, eventBusName string) (string, error) {
	busName := eventBusName
	if busName == "" {
		busName = defaultEventBusName
	}

	region := getRegionFromContext(ctx, b.region)
	busKey := ebBusKey(busName)

	b.mu.RLock("GetEventBusPolicy")
	defer b.mu.RUnlock()

	if _, exists := b.busesStore(region)[busKey]; !exists {
		return "", fmt.Errorf("%w: event bus %s not found", ErrEventBusNotFound, busName)
	}

	policy := b.busePoliciesStore(region)[busKey]
	if policy == nil || len(policy.Statements) == 0 {
		return "", nil
	}

	stmts := make([]*EventBusPolicyStatement, 0, len(policy.Statements))
	for _, s := range policy.Statements {
		stmts = append(stmts, s)
	}
	data, err := json.Marshal(stmts)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// PutEventBusPolicy replaces the resource-based policy on an event bus with raw JSON.
func (b *InMemoryBackend) PutEventBusPolicy(ctx context.Context, input PutEventBusPolicyInput) error {
	busName := input.EventBusName
	if busName == "" {
		busName = defaultEventBusName
	}

	region := getRegionFromContext(ctx, b.region)
	busKey := ebBusKey(busName)

	b.mu.Lock("PutEventBusPolicy")
	defer b.mu.Unlock()

	if _, exists := b.busesStore(region)[busKey]; !exists {
		return fmt.Errorf("%w: event bus %s not found", ErrEventBusNotFound, busName)
	}

	policies := b.busePoliciesStore(region)
	if input.Policy == "" {
		delete(policies, busKey)

		return nil
	}

	var stmts []EventBusPolicyStatement
	if err := json.Unmarshal([]byte(input.Policy), &stmts); err != nil {
		return fmt.Errorf("%w: Policy must be valid JSON: %w", ErrInvalidParameter, err)
	}

	policy := &EventBusPolicy{Statements: make(map[string]*EventBusPolicyStatement, len(stmts))}
	for i := range stmts {
		s := stmts[i]
		policy.Statements[s.Sid] = &s
	}
	policies[busKey] = policy

	return nil
}

// pipeARN builds an ARN for an EventBridge Pipe.
func (b *InMemoryBackend) pipeARN(name string) string {
	return arn.Build("events", b.region, b.accountID, "pipe/"+name)
}

// CreatePipe creates a new EventBridge Pipe.
func (b *InMemoryBackend) CreatePipe(
	ctx context.Context, //nolint:revive // existing issue.
	input CreatePipeInput,
) (*Pipe, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}
	if input.SourceArn == "" {
		return nil, fmt.Errorf("%w: SourceArn is required", ErrInvalidParameter)
	}
	if input.TargetArn == "" {
		return nil, fmt.Errorf("%w: TargetArn is required", ErrInvalidParameter)
	}
	if input.RoleArn == "" {
		return nil, fmt.Errorf("%w: RoleArn is required", ErrInvalidParameter)
	}

	desiredState := input.DesiredState
	if desiredState == "" {
		desiredState = "RUNNING"
	}

	b.mu.Lock("CreatePipe")
	defer b.mu.Unlock()

	if _, exists := b.pipes[input.Name]; exists {
		return nil, fmt.Errorf("%w: pipe %s already exists", ErrAlreadyExists, input.Name)
	}

	now := time.Now()
	pipe := &Pipe{
		Arn:              b.pipeARN(input.Name),
		Name:             input.Name,
		Description:      input.Description,
		DesiredState:     desiredState,
		CurrentState:     "CREATING",
		SourceArn:        input.SourceArn,
		TargetArn:        input.TargetArn,
		RoleArn:          input.RoleArn,
		EnrichmentArn:    input.EnrichmentArn,
		CreationTime:     now,
		LastModifiedTime: now,
	}
	b.pipes[input.Name] = pipe

	cp := *pipe
	// Transition CREATING → RUNNING immediately (in-process simulation).
	pipe.CurrentState = desiredState

	return &cp, nil
}

// DeletePipe removes an EventBridge Pipe.
func (b *InMemoryBackend) DeletePipe(ctx context.Context, name string) error { //nolint:revive // existing issue.
	if name == "" {
		return fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeletePipe")
	defer b.mu.Unlock()

	pipe, exists := b.pipes[name]
	if !exists {
		return fmt.Errorf("%w: pipe %s not found", ErrNotFound, name)
	}

	pipe.CurrentState = "DELETING"
	delete(b.pipes, name)

	return nil
}

// DescribePipe returns a single EventBridge Pipe by name.
func (b *InMemoryBackend) DescribePipe(
	ctx context.Context, //nolint:revive // existing issue.
	name string,
) (*Pipe, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribePipe")
	defer b.mu.RUnlock()

	pipe, exists := b.pipes[name]
	if !exists {
		return nil, fmt.Errorf("%w: pipe %s not found", ErrNotFound, name)
	}

	cp := *pipe

	return &cp, nil
}

// ListPipes returns EventBridge Pipes optionally filtered by name prefix, with pagination.
func (b *InMemoryBackend) ListPipes(
	ctx context.Context, //nolint:revive // existing issue.
	namePrefix, nextToken string,
) ([]Pipe, string, error) {
	b.mu.RLock("ListPipes")
	defer b.mu.RUnlock()

	all := make([]Pipe, 0, len(b.pipes))
	for _, p := range b.pipes {
		if namePrefix == "" || strings.HasPrefix(p.Name, namePrefix) {
			all = append(all, *p)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// UpdatePipe updates an existing EventBridge Pipe.
func (b *InMemoryBackend) UpdatePipe(
	ctx context.Context, //nolint:revive // existing issue.
	input UpdatePipeInput,
) (*Pipe, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdatePipe")
	defer b.mu.Unlock()

	pipe, exists := b.pipes[input.Name]
	if !exists {
		return nil, fmt.Errorf("%w: pipe %s not found", ErrNotFound, input.Name)
	}

	if input.Description != "" {
		pipe.Description = input.Description
	}
	if input.RoleArn != "" {
		pipe.RoleArn = input.RoleArn
	}
	if input.TargetArn != "" {
		pipe.TargetArn = input.TargetArn
	}
	if input.EnrichmentArn != "" {
		pipe.EnrichmentArn = input.EnrichmentArn
	}
	if input.DesiredState != "" {
		pipe.DesiredState = input.DesiredState
		pipe.CurrentState = input.DesiredState
	}
	pipe.LastModifiedTime = time.Now()

	cp := *pipe

	return &cp, nil
}

// captureEventInArchives stores the entry in any archive whose EventSourceArn
// matches the event bus ARN and whose EventPattern matches the event.
// Must be called with b.mu held for writing.
func (b *InMemoryBackend) captureEventInArchives(region string, entry EventEntry, busName string) {
	busARN := b.busARN(region, busName)
	envelope := buildEventEnvelope(entry)
	archivedEvents := b.archivedEventsStore(region)
	for _, archive := range b.archivesStore(region) {
		if archive.EventSourceArn != busARN {
			continue
		}
		if archive.EventPattern == "" || matchPattern(archive.EventPattern, envelope) {
			archivedEvents[archive.ArchiveName] = append(
				archivedEvents[archive.ArchiveName],
				entry,
			)
			archive.EventCount++
		}
	}
}

// AddEventSourceInternal adds an event source directly for testing.
func (b *InMemoryBackend) AddEventSourceInternal(src *EventSource) {
	b.mu.Lock("AddEventSourceInternal")
	defer b.mu.Unlock()

	cp := *src
	b.eventSourcesStore(b.region)[src.Name] = &cp
}

// AddReplayInternal adds a replay directly for testing.
func (b *InMemoryBackend) AddReplayInternal(replay *Replay) {
	b.mu.Lock("AddReplayInternal")
	defer b.mu.Unlock()

	if replay.ReplayArn == "" {
		replay.ReplayArn = b.replayARN(replay.ReplayName)
	}

	cp := *replay
	b.replaysStore(b.region)[replay.ReplayName] = &cp
}

// AddAPIDestinationInternal adds an API destination directly for testing.
func (b *InMemoryBackend) AddAPIDestinationInternal(dst *APIDestination) {
	b.mu.Lock("AddAPIDestinationInternal")
	defer b.mu.Unlock()

	cp := *dst
	b.apiDestinationsStore(b.region)[dst.Name] = &cp
}

// AddArchiveInternal adds an archive directly for testing.
func (b *InMemoryBackend) AddArchiveInternal(archive *Archive) {
	b.mu.Lock("AddArchiveInternal")
	defer b.mu.Unlock()

	cp := *archive
	b.archivesStore(b.region)[archive.ArchiveName] = &cp
}

// AddConnectionInternal adds a connection directly for testing.
func (b *InMemoryBackend) AddConnectionInternal(conn *Connection) {
	b.mu.Lock("AddConnectionInternal")
	defer b.mu.Unlock()

	cp := *conn
	b.connectionsStore(b.region)[conn.Name] = &cp
}

// AddEndpointInternal adds an endpoint directly for testing.
func (b *InMemoryBackend) AddEndpointInternal(ep *Endpoint) {
	b.mu.Lock("AddEndpointInternal")
	defer b.mu.Unlock()

	cp := *ep
	b.endpointsStore(b.region)[ep.Name] = &cp
}

// AddPartnerSourceInternal adds a partner event source directly for testing.
func (b *InMemoryBackend) AddPartnerSourceInternal(src *PartnerEventSource) {
	b.mu.Lock("AddPartnerSourceInternal")
	defer b.mu.Unlock()

	cp := *src
	b.partnerSourcesStore(b.region)[src.Name] = &cp
}

// isValidHTTPMethod reports whether method is a supported API Destination HTTP method.
func isValidHTTPMethod(method string) bool {
	validMethods := map[string]struct{}{
		"GET":     {},
		"HEAD":    {},
		"POST":    {},
		"OPTIONS": {},
		"PUT":     {},
		"DELETE":  {},
		"PATCH":   {},
	}
	_, ok := validMethods[strings.ToUpper(method)]

	return ok
}

// isValidConnectionAuthType reports whether authType is a valid connection authorization type.
func isValidConnectionAuthType(authType string) bool {
	validAuthTypes := map[string]struct{}{
		"API_KEY":                  {},
		"BASIC":                    {},
		"OAUTH_CLIENT_CREDENTIALS": {},
	}
	_, ok := validAuthTypes[authType]

	return ok
}

// isValidJSON reports whether s is valid JSON.
func isValidJSON(s string) bool {
	var v any

	return json.Unmarshal([]byte(s), &v) == nil
}

// putEventsEntryBytes returns the byte size of an event entry as it counts
// against the 256 KiB PutEvents request cap. Per AWS documentation, the
// counted fields are Source, DetailType, Detail, Time, and each Resource ARN
// (14 bytes for Time when present). EventBusName is not counted.
func putEventsEntryBytes(e EventEntry) int {
	const timeBytes = 14

	total := len(e.Source) + len(e.DetailType) + len(e.Detail)
	if e.Time != nil {
		total += timeBytes
	}

	for _, r := range e.Resources {
		total += len(r)
	}

	return total
}

// maskConnectionAuthParameters returns a copy of the auth parameters with
// secret values redacted, matching AWS behaviour where sensitive credentials
// are never returned in plaintext from Describe/List operations.
func maskConnectionAuthParameters(p *ConnectionAuthParameters) *ConnectionAuthParameters {
	if p == nil {
		return nil
	}

	masked := &ConnectionAuthParameters{}

	if p.BasicAuthParameters != nil {
		masked.BasicAuthParameters = &ConnectionBasicAuthParameters{
			Username: p.BasicAuthParameters.Username,
			// Password is intentionally omitted (masked).
		}
	}

	if p.APIKeyAuthParameters != nil {
		masked.APIKeyAuthParameters = &ConnectionAPIKeyAuthParameters{
			APIKeyName: p.APIKeyAuthParameters.APIKeyName,
			// APIKeyValue is intentionally omitted (masked).
		}
	}

	if p.OAuthParameters != nil {
		op := &ConnectionOAuthParameters{
			AuthorizationEndpoint: p.OAuthParameters.AuthorizationEndpoint,
			HTTPMethod:            p.OAuthParameters.HTTPMethod,
		}
		if p.OAuthParameters.ClientParameters != nil {
			op.ClientParameters = &ConnectionOAuthClientParameters{
				ClientID: p.OAuthParameters.ClientParameters.ClientID,
				// ClientSecret is intentionally omitted (masked).
			}
		}
		if p.OAuthParameters.OAuthHTTPParameters != nil {
			op.OAuthHTTPParameters = maskHTTPParameters(p.OAuthParameters.OAuthHTTPParameters)
		}
		masked.OAuthParameters = op
	}

	if p.InvocationHTTPParameters != nil {
		masked.InvocationHTTPParameters = maskHTTPParameters(p.InvocationHTTPParameters)
	}

	return masked
}

// maskHTTPParameters returns a copy of ConnectionHTTPParameters with secret
// values marked as IsValueSecret=true and Value cleared.
func maskHTTPParameters(p *ConnectionHTTPParameters) *ConnectionHTTPParameters {
	if p == nil {
		return nil
	}

	m := &ConnectionHTTPParameters{}

	for _, bp := range p.BodyParameters {
		mp := ConnectionBodyParameter{Key: bp.Key, IsValueSecret: bp.IsValueSecret}
		if !bp.IsValueSecret {
			mp.Value = bp.Value
		}
		m.BodyParameters = append(m.BodyParameters, mp)
	}

	for _, hp := range p.HeaderParameters {
		mp := ConnectionHeaderParameter{Key: hp.Key, IsValueSecret: hp.IsValueSecret}
		if !hp.IsValueSecret {
			mp.Value = hp.Value
		}
		m.HeaderParameters = append(m.HeaderParameters, mp)
	}

	for _, qp := range p.QueryStringParameters {
		mp := ConnectionQueryStringParameter{Key: qp.Key, IsValueSecret: qp.IsValueSecret}
		if !qp.IsValueSecret {
			mp.Value = qp.Value
		}
		m.QueryStringParameters = append(m.QueryStringParameters, mp)
	}

	return m
}

// ---------------------------------------------------------------------------
// Schema Registry backend methods
// ---------------------------------------------------------------------------

const (
	defaultSchemaVersion = "1"

	// schemaTypeOpenAPI3 and schemaTypeJSONSchemaDraft4 are the only valid schema types AWS accepts.
	schemaTypeOpenAPI3         = "OpenApi3"
	schemaTypeJSONSchemaDraft4 = "JSONSchemaDraft4"

	// builtinRegistryAWSEvents and builtinRegistryDiscoveredSchemas are AWS-managed registries
	// that cannot be created or deleted by users.
	builtinRegistryAWSEvents         = "aws.events"
	builtinRegistryDiscoveredSchemas = "discovered-schemas"
)

// isBuiltinRegistry reports whether name is an AWS-managed registry.
func isBuiltinRegistry(name string) bool {
	return name == builtinRegistryAWSEvents || name == builtinRegistryDiscoveredSchemas
}

func (b *InMemoryBackend) registryARN(name string) string {
	return arn.Build("schemas", b.region, b.accountID, "registry/"+name)
}

func (b *InMemoryBackend) schemaARN(registryName, schemaName string) string {
	return arn.Build("schemas", b.region, b.accountID, "schema/"+registryName+"/"+schemaName)
}

func (b *InMemoryBackend) schemaVersionKey(registryName, schemaName string) string {
	return registryName + "/" + schemaName
}

func (b *InMemoryBackend) codeBindingKey(registryName, schemaName, language string) string {
	return registryName + "/" + schemaName + "/" + language
}

// CreateRegistry creates a new schema registry.
func (b *InMemoryBackend) CreateRegistry(
	ctx context.Context, //nolint:revive // existing issue.
	input CreateRegistryInput,
) (*SchemaRegistry, error) {
	if input.RegistryName == "" {
		return nil, fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	if isBuiltinRegistry(input.RegistryName) {
		return nil, fmt.Errorf(
			"%w: cannot create registry with reserved name %s",
			ErrForbiddenOperation,
			input.RegistryName,
		)
	}

	b.mu.Lock("CreateRegistry")
	defer b.mu.Unlock()

	if _, exists := b.registries[input.RegistryName]; exists {
		return nil, fmt.Errorf(
			"%w: registry %s already exists",
			ErrAlreadyExists,
			input.RegistryName,
		)
	}

	reg := &SchemaRegistry{
		RegistryArn:  b.registryARN(input.RegistryName),
		RegistryName: input.RegistryName,
		Description:  input.Description,
		Tags:         input.Tags,
	}
	b.registries[input.RegistryName] = reg

	cp := *reg

	return &cp, nil
}

// DeleteRegistry deletes a registry and all its schemas and versions.
func (b *InMemoryBackend) DeleteRegistry(
	ctx context.Context, //nolint:revive // existing issue.
	registryName string,
) error {
	if registryName == "" {
		return fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	if isBuiltinRegistry(registryName) {
		return fmt.Errorf(
			"%w: cannot delete built-in registry %s",
			ErrForbiddenOperation,
			registryName,
		)
	}

	b.mu.Lock("DeleteRegistry")
	defer b.mu.Unlock()

	if _, exists := b.registries[registryName]; !exists {
		return fmt.Errorf("%w: registry %s not found", ErrNotFound, registryName)
	}

	delete(b.registries, registryName)
	delete(b.schemas, registryName)

	// Remove all version and code binding records for this registry's schemas.
	for key := range b.schemaVersions {
		if strings.HasPrefix(key, registryName+"/") {
			delete(b.schemaVersions, key)
		}
	}

	for key := range b.codeBindings {
		if strings.HasPrefix(key, registryName+"/") {
			delete(b.codeBindings, key)
		}
	}

	return nil
}

// DescribeRegistry returns a single schema registry.
func (b *InMemoryBackend) DescribeRegistry(
	ctx context.Context, //nolint:revive // existing issue.
	registryName string,
) (*SchemaRegistry, error) {
	if registryName == "" {
		return nil, fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeRegistry")
	defer b.mu.RUnlock()

	reg, exists := b.registries[registryName]
	if !exists {
		return nil, fmt.Errorf("%w: registry %s not found", ErrNotFound, registryName)
	}

	cp := *reg

	return &cp, nil
}

// ListRegistries returns schema registries optionally filtered by name prefix.
func (b *InMemoryBackend) ListRegistries(ctx context.Context, //nolint:revive // existing issue.
	namePrefix, nextToken string,
) ([]SchemaRegistry, string, error) {
	b.mu.RLock("ListRegistries")
	defer b.mu.RUnlock()

	all := make([]SchemaRegistry, 0, len(b.registries))
	for _, reg := range b.registries {
		if namePrefix == "" || strings.HasPrefix(reg.RegistryName, namePrefix) {
			all = append(all, *reg)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].RegistryName < all[j].RegistryName })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// UpdateRegistry updates an existing schema registry description.
func (b *InMemoryBackend) UpdateRegistry(
	ctx context.Context, //nolint:revive // existing issue.
	input UpdateRegistryInput,
) (*SchemaRegistry, error) {
	if input.RegistryName == "" {
		return nil, fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateRegistry")
	defer b.mu.Unlock()

	reg, exists := b.registries[input.RegistryName]
	if !exists {
		return nil, fmt.Errorf("%w: registry %s not found", ErrNotFound, input.RegistryName)
	}

	reg.Description = input.Description

	cp := *reg

	return &cp, nil
}

// CreateSchema creates a new schema (version "1") within a registry.
func (b *InMemoryBackend) CreateSchema(
	ctx context.Context, //nolint:revive // existing issue.
	input CreateSchemaInput,
) (*Schema, error) {
	if input.RegistryName == "" {
		return nil, fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	if input.SchemaName == "" {
		return nil, fmt.Errorf("%w: SchemaName is required", ErrInvalidParameter)
	}

	if input.Type == "" {
		return nil, fmt.Errorf("%w: Type is required", ErrInvalidParameter)
	}

	if input.Type != schemaTypeOpenAPI3 && input.Type != schemaTypeJSONSchemaDraft4 {
		return nil, fmt.Errorf(
			"%w: Type must be %s or %s, got %s",
			ErrInvalidParameter,
			schemaTypeOpenAPI3,
			schemaTypeJSONSchemaDraft4,
			input.Type,
		)
	}

	if input.Content == "" {
		return nil, fmt.Errorf("%w: Content is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateSchema")
	defer b.mu.Unlock()

	if _, exists := b.registries[input.RegistryName]; !exists {
		return nil, fmt.Errorf("%w: registry %s not found", ErrNotFound, input.RegistryName)
	}

	if b.schemas[input.RegistryName] == nil {
		b.schemas[input.RegistryName] = make(map[string]*Schema)
	}

	if _, exists := b.schemas[input.RegistryName][input.SchemaName]; exists {
		return nil, fmt.Errorf(
			"%w: schema %s already exists in registry %s",
			ErrAlreadyExists,
			input.SchemaName,
			input.RegistryName,
		)
	}

	now := time.Now()
	schema := &Schema{
		SchemaArn:          b.schemaARN(input.RegistryName, input.SchemaName),
		SchemaName:         input.SchemaName,
		SchemaVersion:      defaultSchemaVersion,
		RegistryName:       input.RegistryName,
		Description:        input.Description,
		Type:               input.Type,
		Content:            input.Content,
		LastModified:       now,
		VersionCreatedDate: now,
		Tags:               input.Tags,
	}
	b.schemas[input.RegistryName][input.SchemaName] = schema

	// Record version 1.
	versionKey := b.schemaVersionKey(input.RegistryName, input.SchemaName)
	sv := &SchemaVersion{
		SchemaArn:     schema.SchemaArn,
		SchemaName:    input.SchemaName,
		SchemaVersion: defaultSchemaVersion,
		RegistryName:  input.RegistryName,
		Type:          input.Type,
		Content:       input.Content,
		CreatedDate:   now,
	}
	b.schemaVersions[versionKey] = []*SchemaVersion{sv}

	cp := *schema

	return &cp, nil
}

// DeleteSchema deletes a schema and all its versions.
func (b *InMemoryBackend) DeleteSchema(
	ctx context.Context, //nolint:revive // existing issue.
	registryName, schemaName string,
) error {
	if registryName == "" {
		return fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	if schemaName == "" {
		return fmt.Errorf("%w: SchemaName is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteSchema")
	defer b.mu.Unlock()

	if _, exists := b.registries[registryName]; !exists {
		return fmt.Errorf("%w: registry %s not found", ErrNotFound, registryName)
	}

	if b.schemas[registryName] == nil || b.schemas[registryName][schemaName] == nil {
		return fmt.Errorf(
			"%w: schema %s not found in registry %s",
			ErrNotFound,
			schemaName,
			registryName,
		)
	}

	delete(b.schemas[registryName], schemaName)

	versionKey := b.schemaVersionKey(registryName, schemaName)
	delete(b.schemaVersions, versionKey)

	// Remove all code bindings for this schema.
	for key := range b.codeBindings {
		if strings.HasPrefix(key, registryName+"/"+schemaName+"/") {
			delete(b.codeBindings, key)
		}
	}

	return nil
}

// DescribeSchema returns the current (or requested version of) a schema.
func (b *InMemoryBackend) DescribeSchema(ctx context.Context, //nolint:revive // existing issue.
	registryName, schemaName, schemaVersion string,
) (*Schema, error) {
	if registryName == "" {
		return nil, fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	if schemaName == "" {
		return nil, fmt.Errorf("%w: SchemaName is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeSchema")
	defer b.mu.RUnlock()

	if _, exists := b.registries[registryName]; !exists {
		return nil, fmt.Errorf("%w: registry %s not found", ErrNotFound, registryName)
	}

	if b.schemas[registryName] == nil || b.schemas[registryName][schemaName] == nil {
		return nil, fmt.Errorf(
			"%w: schema %s not found in registry %s",
			ErrNotFound,
			schemaName,
			registryName,
		)
	}

	schema := b.schemas[registryName][schemaName]

	// If a specific version is requested, fetch that version's content.
	if schemaVersion != "" && schemaVersion != schema.SchemaVersion {
		versionKey := b.schemaVersionKey(registryName, schemaName)
		for _, sv := range b.schemaVersions[versionKey] {
			if sv.SchemaVersion == schemaVersion {
				cp := *schema
				cp.SchemaVersion = sv.SchemaVersion
				cp.Content = sv.Content
				cp.Type = sv.Type
				cp.VersionCreatedDate = sv.CreatedDate

				return &cp, nil
			}
		}

		return nil, fmt.Errorf("%w: schema version %s not found", ErrNotFound, schemaVersion)
	}

	cp := *schema

	return &cp, nil
}

// ListSchemas returns schemas in a registry optionally filtered by name prefix.
func (b *InMemoryBackend) ListSchemas(ctx context.Context, //nolint:revive // existing issue.
	registryName, namePrefix, nextToken string,
) ([]Schema, string, error) {
	if registryName == "" {
		return nil, "", fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	b.mu.RLock("ListSchemas")
	defer b.mu.RUnlock()

	if _, exists := b.registries[registryName]; !exists {
		return nil, "", fmt.Errorf("%w: registry %s not found", ErrNotFound, registryName)
	}

	all := make([]Schema, 0, len(b.schemas[registryName]))
	for _, s := range b.schemas[registryName] {
		if namePrefix == "" || strings.HasPrefix(s.SchemaName, namePrefix) {
			all = append(all, *s)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].SchemaName < all[j].SchemaName })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// SearchSchemas searches schemas in a registry by keyword match against schema name or content.
func (b *InMemoryBackend) SearchSchemas(ctx context.Context, //nolint:revive // existing issue.
	registryName, keywords, nextToken string,
) ([]Schema, string, error) {
	if registryName == "" {
		return nil, "", fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	b.mu.RLock("SearchSchemas")
	defer b.mu.RUnlock()

	if _, exists := b.registries[registryName]; !exists {
		return nil, "", fmt.Errorf("%w: registry %s not found", ErrNotFound, registryName)
	}

	all := make([]Schema, 0)
	lower := strings.ToLower(keywords)

	for _, s := range b.schemas[registryName] {
		if keywords == "" ||
			strings.Contains(strings.ToLower(s.SchemaName), lower) ||
			strings.Contains(strings.ToLower(s.Content), lower) {
			all = append(all, *s)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].SchemaName < all[j].SchemaName })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// UpdateSchema creates a new version of an existing schema.
func (b *InMemoryBackend) UpdateSchema(
	ctx context.Context, //nolint:revive // existing issue.
	input UpdateSchemaInput,
) (*Schema, error) {
	if input.RegistryName == "" {
		return nil, fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	if input.SchemaName == "" {
		return nil, fmt.Errorf("%w: SchemaName is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateSchema")
	defer b.mu.Unlock()

	if _, exists := b.registries[input.RegistryName]; !exists {
		return nil, fmt.Errorf("%w: registry %s not found", ErrNotFound, input.RegistryName)
	}

	if b.schemas[input.RegistryName] == nil ||
		b.schemas[input.RegistryName][input.SchemaName] == nil {
		return nil, fmt.Errorf(
			"%w: schema %s not found in registry %s",
			ErrNotFound,
			input.SchemaName,
			input.RegistryName,
		)
	}

	schema := b.schemas[input.RegistryName][input.SchemaName]

	now := time.Now()

	versionKey := b.schemaVersionKey(input.RegistryName, input.SchemaName)
	currentVersions := b.schemaVersions[versionKey]
	newVersionNum := strconv.Itoa(len(currentVersions) + 1)

	// Apply updates.
	if input.Content != "" {
		schema.Content = input.Content
	}

	if input.Type != "" {
		schema.Type = input.Type
	}

	if input.Description != "" {
		schema.Description = input.Description
	}

	schema.SchemaVersion = newVersionNum
	schema.LastModified = now
	schema.VersionCreatedDate = now

	sv := &SchemaVersion{
		SchemaArn:     schema.SchemaArn,
		SchemaName:    input.SchemaName,
		SchemaVersion: newVersionNum,
		RegistryName:  input.RegistryName,
		Type:          schema.Type,
		Content:       schema.Content,
		CreatedDate:   now,
	}
	b.schemaVersions[versionKey] = append(currentVersions, sv)

	cp := *schema

	return &cp, nil
}

// ListSchemaVersions returns all versions of a schema.
func (b *InMemoryBackend) ListSchemaVersions(ctx context.Context, //nolint:revive // existing issue.
	registryName, schemaName, nextToken string,
) ([]SchemaVersion, string, error) {
	if registryName == "" {
		return nil, "", fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	if schemaName == "" {
		return nil, "", fmt.Errorf("%w: SchemaName is required", ErrInvalidParameter)
	}

	b.mu.RLock("ListSchemaVersions")
	defer b.mu.RUnlock()

	if _, exists := b.registries[registryName]; !exists {
		return nil, "", fmt.Errorf("%w: registry %s not found", ErrNotFound, registryName)
	}

	if b.schemas[registryName] == nil || b.schemas[registryName][schemaName] == nil {
		return nil, "", fmt.Errorf(
			"%w: schema %s not found in registry %s",
			ErrNotFound,
			schemaName,
			registryName,
		)
	}

	versionKey := b.schemaVersionKey(registryName, schemaName)
	raw := b.schemaVersions[versionKey]
	all := make([]SchemaVersion, len(raw))
	for i, sv := range raw {
		all[i] = *sv
	}

	// Versions are stored in insertion order (ascending version number).
	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// DescribeSchemaVersion returns a specific schema version.
func (b *InMemoryBackend) DescribeSchemaVersion(ctx context.Context, //nolint:revive // existing issue.
	registryName, schemaName, schemaVersion string,
) (*SchemaVersion, error) {
	if registryName == "" {
		return nil, fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	if schemaName == "" {
		return nil, fmt.Errorf("%w: SchemaName is required", ErrInvalidParameter)
	}

	if schemaVersion == "" {
		return nil, fmt.Errorf("%w: SchemaVersion is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeSchemaVersion")
	defer b.mu.RUnlock()

	versionKey := b.schemaVersionKey(registryName, schemaName)
	for _, sv := range b.schemaVersions[versionKey] {
		if sv.SchemaVersion == schemaVersion {
			cp := *sv

			return &cp, nil
		}
	}

	return nil, fmt.Errorf(
		"%w: schema version %s not found for %s/%s",
		ErrNotFound,
		schemaVersion,
		registryName,
		schemaName,
	)
}

// DeleteSchemaVersion deletes a specific version of a schema.
// AWS rejects deletion of the last remaining version (BadRequestException).
func (b *InMemoryBackend) DeleteSchemaVersion(ctx context.Context, //nolint:revive // existing issue.
	registryName, schemaName, schemaVersion string,
) error {
	if registryName == "" {
		return fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	if schemaName == "" {
		return fmt.Errorf("%w: SchemaName is required", ErrInvalidParameter)
	}

	if schemaVersion == "" {
		return fmt.Errorf("%w: SchemaVersion is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteSchemaVersion")
	defer b.mu.Unlock()

	versionKey := b.schemaVersionKey(registryName, schemaName)
	versions := b.schemaVersions[versionKey]

	idx := -1
	for i, sv := range versions {
		if sv.SchemaVersion == schemaVersion {
			idx = i

			break
		}
	}

	if idx < 0 {
		return fmt.Errorf(
			"%w: schema version %s not found for %s/%s",
			ErrNotFound,
			schemaVersion,
			registryName,
			schemaName,
		)
	}

	// AWS rejects deletion of the last remaining schema version.
	if len(versions) == 1 {
		return fmt.Errorf(
			"%w: cannot delete the last remaining version of schema %s",
			ErrInvalidParameter,
			schemaName,
		)
	}

	b.schemaVersions[versionKey] = append(versions[:idx], versions[idx+1:]...)

	// If the deleted version was the latest, update the parent schema pointer.
	b.maybeUpdateSchemaAfterVersionDelete(registryName, schemaName, schemaVersion, versionKey)

	return nil
}

// maybeUpdateSchemaAfterVersionDelete updates the parent schema's version pointer when
// the deleted version was the schema's current latest.
func (b *InMemoryBackend) maybeUpdateSchemaAfterVersionDelete(
	registryName, schemaName, schemaVersion, versionKey string,
) {
	if b.schemas[registryName] == nil {
		return
	}

	schema, ok := b.schemas[registryName][schemaName]
	if !ok || schema.SchemaVersion != schemaVersion {
		return
	}

	remaining := b.schemaVersions[versionKey]
	if len(remaining) == 0 {
		return
	}

	latest := remaining[len(remaining)-1]
	schema.SchemaVersion = latest.SchemaVersion
	schema.Content = latest.Content
	schema.Type = latest.Type
	schema.VersionCreatedDate = latest.CreatedDate
}

// GetDiscoveredSchema generates a schema skeleton from one or more event JSON strings.
// Returns a minimal OpenApi3 schema template (real schema inference is out of scope).
func (b *InMemoryBackend) GetDiscoveredSchema(
	ctx context.Context, //nolint:revive // existing issue.
	input GetDiscoveredSchemaInput,
) (string, error) {
	if len(input.Events) == 0 {
		return "", fmt.Errorf("%w: at least one event is required", ErrInvalidParameter)
	}

	if input.Type == "" {
		return "", fmt.Errorf("%w: Type is required", ErrInvalidParameter)
	}

	// Return a minimal discoverable schema stub. AWS generates a full schema from
	// the event payload; for in-process emulation a minimal valid skeleton suffices.
	stub := `{"openapi":"3.0.0","info":{"title":"DiscoveredSchema","version":"1.0"},"paths":{}}`

	return stub, nil
}

// PutCodeBinding triggers code binding generation for a schema version.
func (b *InMemoryBackend) PutCodeBinding(
	ctx context.Context, //nolint:revive // existing issue.
	input PutCodeBindingInput,
) (*CodeBinding, error) {
	if input.RegistryName == "" {
		return nil, fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	if input.SchemaName == "" {
		return nil, fmt.Errorf("%w: SchemaName is required", ErrInvalidParameter)
	}

	if input.Language == "" {
		return nil, fmt.Errorf("%w: Language is required", ErrInvalidParameter)
	}

	b.mu.Lock("PutCodeBinding")
	defer b.mu.Unlock()

	if _, exists := b.registries[input.RegistryName]; !exists {
		return nil, fmt.Errorf("%w: registry %s not found", ErrNotFound, input.RegistryName)
	}

	if b.schemas[input.RegistryName] == nil ||
		b.schemas[input.RegistryName][input.SchemaName] == nil {
		return nil, fmt.Errorf(
			"%w: schema %s not found in registry %s",
			ErrNotFound,
			input.SchemaName,
			input.RegistryName,
		)
	}

	schema := b.schemas[input.RegistryName][input.SchemaName]

	schemaVer := input.SchemaVersion
	if schemaVer == "" {
		schemaVer = schema.SchemaVersion
	}

	now := time.Now()
	binding := &CodeBinding{
		CreationDate:  now,
		LastModified:  now,
		Language:      input.Language,
		SchemaVersion: schemaVer,
		Status:        "CREATE_COMPLETE",
	}

	key := b.codeBindingKey(input.RegistryName, input.SchemaName, input.Language)
	b.codeBindings[key] = binding

	cp := *binding

	return &cp, nil
}

// DescribeCodeBinding returns the status of a code binding.
func (b *InMemoryBackend) DescribeCodeBinding(ctx context.Context, //nolint:revive // existing issue.
	input DescribeCodeBindingInput,
) (*CodeBinding, error) {
	if input.RegistryName == "" {
		return nil, fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	if input.SchemaName == "" {
		return nil, fmt.Errorf("%w: SchemaName is required", ErrInvalidParameter)
	}

	if input.Language == "" {
		return nil, fmt.Errorf("%w: Language is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeCodeBinding")
	defer b.mu.RUnlock()

	key := b.codeBindingKey(input.RegistryName, input.SchemaName, input.Language)
	binding, exists := b.codeBindings[key]
	if !exists {
		return nil, fmt.Errorf("%w: code binding for %s/%s language=%s not found",
			ErrNotFound, input.RegistryName, input.SchemaName, input.Language)
	}

	cp := *binding

	return &cp, nil
}

// ListCodeBindings returns all code bindings for a given schema (optionally filtered by version).
func (b *InMemoryBackend) ListCodeBindings(ctx context.Context, //nolint:revive // existing issue.
	input ListCodeBindingsInput,
) ([]CodeBinding, string, error) {
	if input.RegistryName == "" {
		return nil, "", fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	if input.SchemaName == "" {
		return nil, "", fmt.Errorf("%w: SchemaName is required", ErrInvalidParameter)
	}

	b.mu.RLock("ListCodeBindings")
	defer b.mu.RUnlock()

	prefix := input.RegistryName + "/" + input.SchemaName + "/"
	all := make([]CodeBinding, 0)

	for key, cb := range b.codeBindings {
		if !strings.HasPrefix(key, prefix) {
			continue
		}

		if input.SchemaVersion != "" && cb.SchemaVersion != input.SchemaVersion {
			continue
		}

		all = append(all, *cb)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Language < all[j].Language })

	page, outToken := paginate(all, input.NextToken)

	return page, outToken, nil
}

// GetCodeBindingSource returns placeholder source code for a generated code binding.
// Real source generation is out of scope for in-process emulation.
func (b *InMemoryBackend) GetCodeBindingSource(ctx context.Context, //nolint:revive // existing issue.
	registryName, schemaName, language, schemaVersion string,
) (string, error) {
	if registryName == "" {
		return "", fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	if schemaName == "" {
		return "", fmt.Errorf("%w: SchemaName is required", ErrInvalidParameter)
	}

	if language == "" {
		return "", fmt.Errorf("%w: Language is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetCodeBindingSource")
	defer b.mu.RUnlock()

	key := b.codeBindingKey(registryName, schemaName, language)
	if _, exists := b.codeBindings[key]; !exists {
		return "", fmt.Errorf("%w: code binding for %s/%s language=%s not found",
			ErrNotFound, registryName, schemaName, language)
	}

	// Return a minimal placeholder; real codegen is AWS-side only.
	src := fmt.Sprintf("// Generated code binding for %s/%s (%s)\n// Schema version: %s\n",
		registryName, schemaName, language, schemaVersion)

	return src, nil
}
