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
	CreateEventBus(name, description string) (*EventBus, error)
	DeleteEventBus(name string) error
	ListEventBuses(namePrefix, nextToken string) ([]EventBus, string, error)
	DescribeEventBus(name string) (*EventBus, error)
	PutRule(input PutRuleInput) (*Rule, error)
	DeleteRule(name, eventBusName string) error
	ListRules(eventBusName, namePrefix, nextToken string) ([]Rule, string, error)
	DescribeRule(name, eventBusName string) (*Rule, error)
	EnableRule(name, eventBusName string) error
	DisableRule(name, eventBusName string) error
	PutTargets(ruleName, eventBusName string, targets []Target) ([]FailedEntry, error)
	RemoveTargets(ruleName, eventBusName string, ids []string) ([]FailedEntry, error)
	ListTargetsByRule(ruleName, eventBusName, nextToken string) ([]Target, string, error)
	PutEvents(entries []EventEntry) []EventResultEntry
	GetEventLog() []EventLogEntry
	ActivateEventSource(name string) error
	DeactivateEventSource(name string) error
	CreatePartnerEventSource(name, account string) (*PartnerEventSource, error)
	CancelReplay(replayName string) (*Replay, error)
	CreateAPIDestination(input CreateAPIDestinationInput) (*APIDestination, error)
	CreateArchive(input CreateArchiveInput) (*Archive, error)
	CreateConnection(input CreateConnectionInput) (*Connection, error)
	CreateEndpoint(input CreateEndpointInput) (*Endpoint, error)
	DeauthorizeConnection(name string) (*Connection, error)
	DeleteAPIDestination(name string) error
	DeleteArchive(name string) error
	DescribeArchive(name string) (*Archive, error)
	ListArchives(namePrefix, nextToken string) ([]Archive, string, error)
	UpdateArchive(input UpdateArchiveInput) (*Archive, error)
	DeleteConnection(name string) error
	DescribeConnection(name string) (*Connection, error)
	ListConnections(namePrefix, nextToken string) ([]Connection, string, error)
	UpdateConnection(input UpdateConnectionInput) (*Connection, error)
	DeleteEndpoint(name string) error
	DescribeEndpoint(name string) (*Endpoint, error)
	ListEndpoints(namePrefix, nextToken string) ([]Endpoint, string, error)
	UpdateEndpoint(input UpdateEndpointInput) (*Endpoint, error)
	DescribeAPIDestination(name string) (*APIDestination, error)
	ListAPIDestinations(namePrefix, nextToken string) ([]APIDestination, string, error)
	UpdateAPIDestination(input UpdateAPIDestinationInput) (*APIDestination, error)
	DescribeEventSource(name string) (*EventSource, error)
	ListEventSources(namePrefix, nextToken string) ([]EventSource, string, error)
	DescribePartnerEventSource(name string) (*PartnerEventSource, error)
	DeletePartnerEventSource(name string) error
	ListPartnerEventSources(namePrefix, nextToken string) ([]PartnerEventSource, string, error)
	PutPartnerEvents(entries []EventEntry) []EventResultEntry
	DescribeReplay(name string) (*Replay, error)
	ListReplays(namePrefix, nextToken string) ([]Replay, string, error)
	StartReplay(input StartReplayInput) (*Replay, error)
	ListRuleNamesByTarget(targetARN, eventBusName, nextToken string) ([]string, string, error)
	TestEventPattern(pattern, event string) (bool, error)
	UpdateEventBus(input UpdateEventBusInput) (*EventBus, error)
	PutPermission(input PutPermissionInput) error
	RemovePermission(input RemovePermissionInput) error
	GetEventBusPolicy(eventBusName string) (string, error)
	PutEventBusPolicy(input PutEventBusPolicyInput) error
	CreatePipe(input CreatePipeInput) (*Pipe, error)
	DeletePipe(name string) error
	DescribePipe(name string) (*Pipe, error)
	ListPipes(namePrefix, nextToken string) ([]Pipe, string, error)
	UpdatePipe(input UpdatePipeInput) (*Pipe, error)
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	ctx             context.Context
	mu              *lockmetrics.RWMutex
	connections     map[string]*Connection
	rules           map[string]map[string]*Rule
	targets         map[string]map[string]*Target
	eventSources    map[string]*EventSource
	replays         map[string]*Replay
	apiDestinations map[string]*APIDestination
	cancel          context.CancelFunc
	deliveryTargets *DeliveryTargets
	endpoints       map[string]*Endpoint
	buses           map[string]*EventBus
	partnerSources  map[string]*PartnerEventSource
	archives        map[string]*Archive
	archivedEvents  map[string][]EventEntry
	busePolicies    map[string]*EventBusPolicy
	pipes           map[string]*Pipe
	workerSem       chan struct{}
	ruleIndex       map[string]map[ruleIndexKey]map[string]*Rule
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
func NewInMemoryBackendWithContext(svcCtx context.Context, accountID, region string) *InMemoryBackend {
	if svcCtx == nil {
		svcCtx = context.Background()
	}

	ctx, cancel := context.WithCancel(svcCtx)
	b := &InMemoryBackend{
		accountID:       accountID,
		region:          region,
		buses:           make(map[string]*EventBus),
		rules:           make(map[string]map[string]*Rule),
		targets:         make(map[string]map[string]*Target),
		eventSources:    make(map[string]*EventSource),
		replays:         make(map[string]*Replay),
		apiDestinations: make(map[string]*APIDestination),
		archives:        make(map[string]*Archive),
		archivedEvents:  make(map[string][]EventEntry),
		connections:     make(map[string]*Connection),
		endpoints:       make(map[string]*Endpoint),
		partnerSources:  make(map[string]*PartnerEventSource),
		busePolicies:    make(map[string]*EventBusPolicy),
		pipes:           make(map[string]*Pipe),
		deliveryTargets: &DeliveryTargets{},
		mu:              lockmetrics.New("eventbridge"),
		ctx:             ctx,
		cancel:          cancel,
		workerSem:       make(chan struct{}, defaultDeliveryWorkers),
		shutdownTimeout: defaultShutdownTimeout,
		deliveryTimeout: defaultDeliveryTimeout,
		ruleIndex:       make(map[string]map[ruleIndexKey]map[string]*Rule),
	}
	// Create the default event bus.
	b.buses[defaultEventBusName] = &EventBus{
		Name:        defaultEventBusName,
		Arn:         b.busARN(defaultEventBusName),
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

func (b *InMemoryBackend) busARN(name string) string {
	return arn.Build("events", b.region, b.accountID, "event-bus/"+name)
}

func (b *InMemoryBackend) ruleARN(busName, ruleName string) string {
	return arn.Build("events", b.region, b.accountID, "rule/"+busName+"/"+ruleName)
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

func (b *InMemoryBackend) targetKey(busName, ruleName string) string {
	return busName + "/" + ruleName
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

func (b *InMemoryBackend) addRuleToIndex(rule *Rule) {
	if rule.compiledPattern == nil && rule.EventPattern == "" {
		return
	}
	indexes := b.ruleIndex[rule.EventBusName]
	if indexes == nil {
		indexes = make(map[ruleIndexKey]map[string]*Rule)
		b.ruleIndex[rule.EventBusName] = indexes
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

func (b *InMemoryBackend) removeRuleFromIndex(rule *Rule) {
	indexes := b.ruleIndex[rule.EventBusName]
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
		delete(b.ruleIndex, rule.EventBusName)
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
func (b *InMemoryBackend) CreateEventBus(name, description string) (*EventBus, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	if len(name) > maxEventBusNameLength {
		return nil, fmt.Errorf("%w: Name must be %d characters or fewer", ErrInvalidParameter, maxEventBusNameLength)
	}

	if strings.HasPrefix(name, "aws.") {
		return nil, fmt.Errorf("%w: Event bus name cannot start with the reserved prefix \"aws.\"", ErrInvalidParameter)
	}

	b.mu.Lock("CreateEventBus")
	defer b.mu.Unlock()

	if _, exists := b.buses[name]; exists {
		return nil, fmt.Errorf("%w: Event bus %s already exists", ErrEventBusAlreadyExists, name)
	}

	// Count custom buses (exclude the default bus against the limit).
	customBusCount := 0
	for busName := range b.buses {
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
		Arn:         b.busARN(name),
		Description: description,
		CreatedTime: time.Now(),
	}
	b.buses[name] = bus

	return bus, nil
}

// DeleteEventBus deletes an event bus by name (default bus cannot be deleted).
// It also removes all rules and targets associated with the bus.
func (b *InMemoryBackend) DeleteEventBus(name string) error {
	if name == defaultEventBusName {
		return fmt.Errorf("%w: cannot delete the default event bus", ErrCannotDeleteDefaultBus)
	}

	b.mu.Lock("DeleteEventBus")
	defer b.mu.Unlock()

	if _, exists := b.buses[name]; !exists {
		return fmt.Errorf("%w: Event bus %s not found", ErrEventBusNotFound, name)
	}

	delete(b.buses, name)
	delete(b.ruleIndex, name)

	// Clean up all rules for this bus.
	if busRules, ok := b.rules[name]; ok {
		for ruleName := range busRules {
			delete(b.targets, b.targetKey(name, ruleName))
		}

		delete(b.rules, name)
	}

	return nil
}

// ListEventBuses returns event buses optionally filtered by name prefix, with pagination.
func (b *InMemoryBackend) ListEventBuses(namePrefix, nextToken string) ([]EventBus, string, error) {
	b.mu.RLock("ListEventBuses")
	defer b.mu.RUnlock()

	all := make([]EventBus, 0, len(b.buses))
	for _, bus := range b.buses {
		if namePrefix == "" || strings.HasPrefix(bus.Name, namePrefix) {
			all = append(all, *bus)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// DescribeEventBus returns details for a single event bus.
func (b *InMemoryBackend) DescribeEventBus(name string) (*EventBus, error) {
	if name == "" {
		name = defaultEventBusName
	}

	b.mu.RLock("DescribeEventBus")
	defer b.mu.RUnlock()

	bus, exists := b.buses[name]
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
		return fmt.Errorf("%w: Name must not exceed %d characters", ErrInvalidParameter, maxRuleNameLength)
	}

	if input.EventPattern != "" && input.ScheduleExpression != "" {
		return fmt.Errorf("%w: ScheduleExpression and EventPattern are mutually exclusive", ErrInvalidParameter)
	}

	if input.EventPattern == "" && input.ScheduleExpression == "" {
		return fmt.Errorf("%w: either EventPattern or ScheduleExpression must be provided", ErrInvalidParameter)
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
func (b *InMemoryBackend) PutRule(input PutRuleInput) (*Rule, error) {
	if err := validatePutRuleInput(input); err != nil {
		return nil, err
	}

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

	b.mu.Lock("PutRule")
	defer b.mu.Unlock()

	if _, exists := b.buses[busName]; !exists {
		return nil, fmt.Errorf("%w: Event bus %s not found", ErrEventBusNotFound, busName)
	}

	state := input.State
	if state == "" {
		state = ruleStateEnabled
	}

	if b.rules[busName] == nil {
		b.rules[busName] = make(map[string]*Rule)
	}

	// Enforce per-bus rule limit only for new rules (not updates).
	if _, exists := b.rules[busName][input.Name]; !exists {
		if len(b.rules[busName]) >= maxRulesPerBus {
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
		Arn:                b.ruleARN(busName, input.Name),
		EventBusName:       busName,
		EventPattern:       input.EventPattern,
		State:              state,
		Description:        input.Description,
		ScheduleExpression: input.ScheduleExpression,
		RoleArn:            input.RoleArn,
		ManagedBy:          input.ManagedBy,
		compiledPattern:    compiled,
	}

	if existing, exists := b.rules[busName][input.Name]; exists {
		b.removeRuleFromIndex(existing)
	}
	b.rules[busName][input.Name] = rule
	b.addRuleToIndex(rule)

	return rule, nil
}

// DeleteRule removes a rule from an event bus.
func (b *InMemoryBackend) DeleteRule(name, eventBusName string) error {
	if eventBusName == "" {
		eventBusName = defaultEventBusName
	}

	b.mu.Lock("DeleteRule")
	defer b.mu.Unlock()

	busRules, exists := b.rules[eventBusName]
	if !exists {
		return fmt.Errorf("%w: Rule %s not found", ErrRuleNotFound, name)
	}

	rule, ruleExists := busRules[name]
	if !ruleExists {
		return fmt.Errorf("%w: Rule %s not found", ErrRuleNotFound, name)
	}

	b.removeRuleFromIndex(rule)
	delete(busRules, name)
	// Also remove targets for this rule.
	delete(b.targets, b.targetKey(eventBusName, name))

	return nil
}

// ListRules returns rules for an event bus optionally filtered by name prefix.
func (b *InMemoryBackend) ListRules(eventBusName, namePrefix, nextToken string) ([]Rule, string, error) {
	if eventBusName == "" {
		eventBusName = defaultEventBusName
	}

	b.mu.RLock("ListRules")
	defer b.mu.RUnlock()

	busRules := b.rules[eventBusName]
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
func (b *InMemoryBackend) DescribeRule(name, eventBusName string) (*Rule, error) {
	if eventBusName == "" {
		eventBusName = defaultEventBusName
	}

	b.mu.RLock("DescribeRule")
	defer b.mu.RUnlock()

	busRules, exists := b.rules[eventBusName]
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
func (b *InMemoryBackend) EnableRule(name, eventBusName string) error {
	return b.setRuleState(name, eventBusName, ruleStateEnabled)
}

// DisableRule sets a rule's state to DISABLED.
func (b *InMemoryBackend) DisableRule(name, eventBusName string) error {
	return b.setRuleState(name, eventBusName, ruleStateDisabled)
}

func (b *InMemoryBackend) setRuleState(name, eventBusName, state string) error {
	if eventBusName == "" {
		eventBusName = defaultEventBusName
	}

	b.mu.Lock("setRuleState")
	defer b.mu.Unlock()

	busRules, exists := b.rules[eventBusName]
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
func (b *InMemoryBackend) PutTargets(ruleName, eventBusName string, targets []Target) ([]FailedEntry, error) {
	if eventBusName == "" {
		eventBusName = defaultEventBusName
	}

	if len(targets) == 0 {
		return nil, fmt.Errorf("%w: at least one target is required", ErrInvalidParameter)
	}

	b.mu.Lock("PutTargets")
	defer b.mu.Unlock()

	busRules, exists := b.rules[eventBusName]
	if !exists {
		return nil, fmt.Errorf("%w: Rule %s not found", ErrRuleNotFound, ruleName)
	}

	if _, ruleExists := busRules[ruleName]; !ruleExists {
		return nil, fmt.Errorf("%w: Rule %s not found", ErrRuleNotFound, ruleName)
	}

	key := b.targetKey(eventBusName, ruleName)
	if b.targets[key] == nil {
		b.targets[key] = make(map[string]*Target)
	}

	// Reject if adding these targets would exceed the per-rule limit.
	if len(b.targets[key])+len(targets) > maxTargetsPerRule {
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
		b.targets[key][t.ID] = &cp
	}

	return failed, nil
}

// RemoveTargets removes targets from a rule by their IDs.
func (b *InMemoryBackend) RemoveTargets(ruleName, eventBusName string, ids []string) ([]FailedEntry, error) {
	if eventBusName == "" {
		eventBusName = defaultEventBusName
	}

	b.mu.Lock("RemoveTargets")
	defer b.mu.Unlock()

	key := b.targetKey(eventBusName, ruleName)
	ruleTargets := b.targets[key]

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
func (b *InMemoryBackend) ListTargetsByRule(ruleName, eventBusName, nextToken string) ([]Target, string, error) {
	if eventBusName == "" {
		eventBusName = defaultEventBusName
	}

	b.mu.RLock("ListTargetsByRule")
	defer b.mu.RUnlock()

	key := b.targetKey(eventBusName, ruleName)
	ruleTargets := b.targets[key]
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
func (b *InMemoryBackend) PutEvents(entries []EventEntry) []EventResultEntry {
	const maxBatchBytes = 256 * 1024

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
		b.captureEventInArchives(entry, busName)
		accepted = append(accepted, entry)
		results = append(results, EventResultEntry{EventID: eventID})
	}

	dt := b.deliveryTargets
	workerSem := b.workerSem
	ctx := b.ctx
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
			case <-ctx.Done():
				return
			}
			b.deliverEvents(ctx, entriesCopy, dtCopy, delivTimeout)
		})
	}

	return results
}

// GetEventLog returns a copy of the current event log.
func (b *InMemoryBackend) GetEventLog() []EventLogEntry {
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

	b.buses = make(map[string]*EventBus)
	b.rules = make(map[string]map[string]*Rule)
	b.targets = make(map[string]map[string]*Target)
	b.eventLog = nil
	b.eventSources = make(map[string]*EventSource)
	b.replays = make(map[string]*Replay)
	b.apiDestinations = make(map[string]*APIDestination)
	b.archives = make(map[string]*Archive)
	b.archivedEvents = make(map[string][]EventEntry)
	b.connections = make(map[string]*Connection)
	b.endpoints = make(map[string]*Endpoint)
	b.partnerSources = make(map[string]*PartnerEventSource)
	b.busePolicies = make(map[string]*EventBusPolicy)
	b.pipes = make(map[string]*Pipe)
	b.ruleIndex = make(map[string]map[ruleIndexKey]map[string]*Rule)
	b.patternCache = sync.Map{}

	// Re-create the default event bus so it is always available after reset.
	b.buses[defaultEventBusName] = &EventBus{
		Name:        defaultEventBusName,
		Arn:         b.busARN(defaultEventBusName),
		CreatedTime: time.Now(),
	}
}

// ActivateEventSource activates a partner event source.
func (b *InMemoryBackend) ActivateEventSource(name string) error {
	if name == "" {
		return fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("ActivateEventSource")
	defer b.mu.Unlock()

	src, exists := b.eventSources[name]
	if !exists {
		return fmt.Errorf("%w: event source %s not found", ErrNotFound, name)
	}

	src.State = stateActive

	return nil
}

// DeactivateEventSource deactivates a partner event source.
func (b *InMemoryBackend) DeactivateEventSource(name string) error {
	if name == "" {
		return fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeactivateEventSource")
	defer b.mu.Unlock()

	src, exists := b.eventSources[name]
	if !exists {
		return fmt.Errorf("%w: event source %s not found", ErrNotFound, name)
	}

	src.State = "INACTIVE"

	return nil
}

// CreatePartnerEventSource creates a new partner event source.
func (b *InMemoryBackend) CreatePartnerEventSource(name, account string) (*PartnerEventSource, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreatePartnerEventSource")
	defer b.mu.Unlock()

	if _, exists := b.partnerSources[name]; exists {
		return nil, fmt.Errorf("%w: partner event source %s already exists", ErrAlreadyExists, name)
	}

	src := &PartnerEventSource{
		Arn:     b.partnerSourceARN(name),
		Name:    name,
		Account: account,
	}
	b.partnerSources[name] = src

	cp := *src

	return &cp, nil
}

// CancelReplay cancels a running or starting replay.
func (b *InMemoryBackend) CancelReplay(replayName string) (*Replay, error) {
	if replayName == "" {
		return nil, fmt.Errorf("%w: ReplayName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CancelReplay")
	defer b.mu.Unlock()

	replay, exists := b.replays[replayName]
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
func (b *InMemoryBackend) CreateAPIDestination(input CreateAPIDestinationInput) (*APIDestination, error) {
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

	b.mu.Lock("CreateAPIDestination")
	defer b.mu.Unlock()

	if _, exists := b.apiDestinations[input.Name]; exists {
		return nil, fmt.Errorf("%w: API destination %s already exists", ErrAlreadyExists, input.Name)
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
	b.apiDestinations[input.Name] = dst

	cp := *dst

	return &cp, nil
}

// CreateArchive creates a new event archive.
func (b *InMemoryBackend) CreateArchive(input CreateArchiveInput) (*Archive, error) {
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
		return nil, fmt.Errorf("%w: RetentionDays must be 0 (indefinite) or a positive integer", ErrInvalidParameter)
	}

	b.mu.Lock("CreateArchive")
	defer b.mu.Unlock()

	if _, exists := b.archives[input.ArchiveName]; exists {
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
	b.archives[input.ArchiveName] = archive

	cp := *archive

	return &cp, nil
}

// CreateConnection creates a new connection.
func (b *InMemoryBackend) CreateConnection(input CreateConnectionInput) (*Connection, error) {
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

	b.mu.Lock("CreateConnection")
	defer b.mu.Unlock()

	if _, exists := b.connections[input.Name]; exists {
		return nil, fmt.Errorf("%w: connection %s already exists", ErrAlreadyExists, input.Name)
	}

	now := time.Now()
	conn := &Connection{
		ConnectionArn:     b.connectionARN(input.Name),
		AuthorizationType: input.AuthorizationType,
		ConnectionState:   "AUTHORIZED",
		CreationTime:      now,
		Description:       input.Description,
		LastModifiedTime:  now,
		Name:              input.Name,
	}
	b.connections[input.Name] = conn

	cp := *conn

	return &cp, nil
}

// CreateEndpoint creates a new global endpoint.
func (b *InMemoryBackend) CreateEndpoint(input CreateEndpointInput) (*Endpoint, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateEndpoint")
	defer b.mu.Unlock()

	if _, exists := b.endpoints[input.Name]; exists {
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
	b.endpoints[input.Name] = ep

	cp := *ep

	return &cp, nil
}

// DeauthorizeConnection deauthorizes a connection.
func (b *InMemoryBackend) DeauthorizeConnection(name string) (*Connection, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeauthorizeConnection")
	defer b.mu.Unlock()

	conn, exists := b.connections[name]
	if !exists {
		return nil, fmt.Errorf("%w: connection %s not found", ErrNotFound, name)
	}

	conn.ConnectionState = "DEAUTHORIZED"

	cp := *conn

	return &cp, nil
}

// DeleteAPIDestination deletes an API destination.
func (b *InMemoryBackend) DeleteAPIDestination(name string) error {
	if name == "" {
		return fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteAPIDestination")
	defer b.mu.Unlock()

	if _, exists := b.apiDestinations[name]; !exists {
		return fmt.Errorf("%w: API destination %s not found", ErrNotFound, name)
	}

	delete(b.apiDestinations, name)

	return nil
}

// DeleteArchive deletes an archive.
func (b *InMemoryBackend) DeleteArchive(name string) error {
	if name == "" {
		return fmt.Errorf("%w: ArchiveName is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteArchive")
	defer b.mu.Unlock()

	if _, exists := b.archives[name]; !exists {
		return fmt.Errorf("%w: archive %s not found", ErrNotFound, name)
	}

	delete(b.archives, name)

	return nil
}

// DescribeArchive returns a single archive by name.
func (b *InMemoryBackend) DescribeArchive(name string) (*Archive, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: ArchiveName is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeArchive")
	defer b.mu.RUnlock()

	archive, exists := b.archives[name]
	if !exists {
		return nil, fmt.Errorf("%w: archive %s not found", ErrNotFound, name)
	}

	cp := *archive

	return &cp, nil
}

// ListArchives returns archives optionally filtered by name prefix, with pagination.
func (b *InMemoryBackend) ListArchives(namePrefix, nextToken string) ([]Archive, string, error) {
	b.mu.RLock("ListArchives")
	defer b.mu.RUnlock()

	all := make([]Archive, 0, len(b.archives))
	for _, a := range b.archives {
		if namePrefix == "" || strings.HasPrefix(a.ArchiveName, namePrefix) {
			all = append(all, *a)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ArchiveName < all[j].ArchiveName })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// UpdateArchive updates an existing archive.
func (b *InMemoryBackend) UpdateArchive(input UpdateArchiveInput) (*Archive, error) {
	if input.ArchiveName == "" {
		return nil, fmt.Errorf("%w: ArchiveName is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateArchive")
	defer b.mu.Unlock()

	archive, exists := b.archives[input.ArchiveName]
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
func (b *InMemoryBackend) DeleteConnection(name string) error {
	if name == "" {
		return fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteConnection")
	defer b.mu.Unlock()

	if _, exists := b.connections[name]; !exists {
		return fmt.Errorf("%w: connection %s not found", ErrNotFound, name)
	}

	delete(b.connections, name)

	return nil
}

// DescribeConnection returns a single connection by name.
func (b *InMemoryBackend) DescribeConnection(name string) (*Connection, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeConnection")
	defer b.mu.RUnlock()

	conn, exists := b.connections[name]
	if !exists {
		return nil, fmt.Errorf("%w: connection %s not found", ErrNotFound, name)
	}

	cp := *conn

	return &cp, nil
}

// ListConnections returns connections optionally filtered by name prefix, with pagination.
func (b *InMemoryBackend) ListConnections(namePrefix, nextToken string) ([]Connection, string, error) {
	b.mu.RLock("ListConnections")
	defer b.mu.RUnlock()

	all := make([]Connection, 0, len(b.connections))
	for _, c := range b.connections {
		if namePrefix == "" || strings.HasPrefix(c.Name, namePrefix) {
			all = append(all, *c)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// UpdateConnection updates an existing connection.
func (b *InMemoryBackend) UpdateConnection(input UpdateConnectionInput) (*Connection, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateConnection")
	defer b.mu.Unlock()

	conn, exists := b.connections[input.Name]
	if !exists {
		return nil, fmt.Errorf("%w: connection %s not found", ErrNotFound, input.Name)
	}

	if input.Description != "" {
		conn.Description = input.Description
	}
	if input.AuthorizationType != "" {
		conn.AuthorizationType = input.AuthorizationType
	}
	conn.LastModifiedTime = time.Now()

	cp := *conn

	return &cp, nil
}

// DeleteEndpoint deletes an endpoint.
func (b *InMemoryBackend) DeleteEndpoint(name string) error {
	if name == "" {
		return fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteEndpoint")
	defer b.mu.Unlock()

	if _, exists := b.endpoints[name]; !exists {
		return fmt.Errorf("%w: endpoint %s not found", ErrNotFound, name)
	}

	delete(b.endpoints, name)

	return nil
}

// DescribeEndpoint returns a single endpoint by name.
func (b *InMemoryBackend) DescribeEndpoint(name string) (*Endpoint, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeEndpoint")
	defer b.mu.RUnlock()

	ep, exists := b.endpoints[name]
	if !exists {
		return nil, fmt.Errorf("%w: endpoint %s not found", ErrNotFound, name)
	}

	cp := *ep

	return &cp, nil
}

// ListEndpoints returns endpoints optionally filtered by name prefix, with pagination.
func (b *InMemoryBackend) ListEndpoints(namePrefix, nextToken string) ([]Endpoint, string, error) {
	b.mu.RLock("ListEndpoints")
	defer b.mu.RUnlock()

	all := make([]Endpoint, 0, len(b.endpoints))
	for _, ep := range b.endpoints {
		if namePrefix == "" || strings.HasPrefix(ep.Name, namePrefix) {
			all = append(all, *ep)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// UpdateEndpoint updates an existing endpoint.
func (b *InMemoryBackend) UpdateEndpoint(input UpdateEndpointInput) (*Endpoint, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateEndpoint")
	defer b.mu.Unlock()

	ep, exists := b.endpoints[input.Name]
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
func (b *InMemoryBackend) DescribeAPIDestination(name string) (*APIDestination, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeAPIDestination")
	defer b.mu.RUnlock()

	dst, exists := b.apiDestinations[name]
	if !exists {
		return nil, fmt.Errorf("%w: API destination %s not found", ErrNotFound, name)
	}

	cp := *dst

	return &cp, nil
}

// ListAPIDestinations returns API destinations optionally filtered by name prefix, with pagination.
func (b *InMemoryBackend) ListAPIDestinations(namePrefix, nextToken string) ([]APIDestination, string, error) {
	b.mu.RLock("ListAPIDestinations")
	defer b.mu.RUnlock()

	all := make([]APIDestination, 0, len(b.apiDestinations))
	for _, d := range b.apiDestinations {
		if namePrefix == "" || strings.HasPrefix(d.Name, namePrefix) {
			all = append(all, *d)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// UpdateAPIDestination updates an existing API destination.
func (b *InMemoryBackend) UpdateAPIDestination(input UpdateAPIDestinationInput) (*APIDestination, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateAPIDestination")
	defer b.mu.Unlock()

	dst, exists := b.apiDestinations[input.Name]
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
func (b *InMemoryBackend) DescribeEventSource(name string) (*EventSource, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeEventSource")
	defer b.mu.RUnlock()

	src, exists := b.eventSources[name]
	if !exists {
		return nil, fmt.Errorf("%w: event source %s not found", ErrNotFound, name)
	}

	cp := *src

	return &cp, nil
}

// ListEventSources returns event sources optionally filtered by name prefix, with pagination.
func (b *InMemoryBackend) ListEventSources(namePrefix, nextToken string) ([]EventSource, string, error) {
	b.mu.RLock("ListEventSources")
	defer b.mu.RUnlock()

	all := make([]EventSource, 0, len(b.eventSources))
	for _, s := range b.eventSources {
		if namePrefix == "" || strings.HasPrefix(s.Name, namePrefix) {
			all = append(all, *s)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// DescribePartnerEventSource returns a single partner event source by name.
func (b *InMemoryBackend) DescribePartnerEventSource(name string) (*PartnerEventSource, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribePartnerEventSource")
	defer b.mu.RUnlock()

	src, exists := b.partnerSources[name]
	if !exists {
		return nil, fmt.Errorf("%w: partner event source %s not found", ErrNotFound, name)
	}

	cp := *src

	return &cp, nil
}

// DeletePartnerEventSource deletes a partner event source.
func (b *InMemoryBackend) DeletePartnerEventSource(name string) error {
	if name == "" {
		return fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeletePartnerEventSource")
	defer b.mu.Unlock()

	if _, exists := b.partnerSources[name]; !exists {
		return fmt.Errorf("%w: partner event source %s not found", ErrNotFound, name)
	}

	delete(b.partnerSources, name)

	return nil
}

// ListPartnerEventSources returns partner event sources optionally filtered by name prefix.
func (b *InMemoryBackend) ListPartnerEventSources(namePrefix, nextToken string) ([]PartnerEventSource, string, error) {
	b.mu.RLock("ListPartnerEventSources")
	defer b.mu.RUnlock()

	all := make([]PartnerEventSource, 0, len(b.partnerSources))
	for _, s := range b.partnerSources {
		if namePrefix == "" || strings.HasPrefix(s.Name, namePrefix) {
			all = append(all, *s)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// PutPartnerEvents records partner events (same as PutEvents but intended for partner sources).
func (b *InMemoryBackend) PutPartnerEvents(entries []EventEntry) []EventResultEntry {
	return b.PutEvents(entries)
}

// DescribeReplay returns a single replay by name.
func (b *InMemoryBackend) DescribeReplay(name string) (*Replay, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: ReplayName is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeReplay")
	defer b.mu.RUnlock()

	replay, exists := b.replays[name]
	if !exists {
		return nil, fmt.Errorf("%w: replay %s not found", ErrNotFound, name)
	}

	cp := *replay

	return &cp, nil
}

// ListReplays returns replays optionally filtered by name prefix, with pagination.
func (b *InMemoryBackend) ListReplays(namePrefix, nextToken string) ([]Replay, string, error) {
	b.mu.RLock("ListReplays")
	defer b.mu.RUnlock()

	all := make([]Replay, 0, len(b.replays))
	for _, r := range b.replays {
		if namePrefix == "" || strings.HasPrefix(r.ReplayName, namePrefix) {
			all = append(all, *r)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ReplayName < all[j].ReplayName })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// StartReplay creates a new replay in the STARTING state.
func (b *InMemoryBackend) StartReplay(input StartReplayInput) (*Replay, error) {
	if input.ReplayName == "" {
		return nil, fmt.Errorf("%w: ReplayName is required", ErrInvalidParameter)
	}

	if input.EventSourceArn == "" {
		return nil, fmt.Errorf("%w: EventSourceArn is required", ErrInvalidParameter)
	}

	if !input.EventStartTime.IsZero() && !input.EventEndTime.IsZero() &&
		!input.EventStartTime.Before(input.EventEndTime) {
		return nil, fmt.Errorf("%w: EventStartTime must be before EventEndTime", ErrInvalidParameter)
	}

	b.mu.Lock("StartReplay")

	if _, exists := b.replays[input.ReplayName]; exists {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: replay %s already exists", ErrAlreadyExists, input.ReplayName)
	}

	// Validate destination ARN points to a known event bus.
	if input.Destination != nil && input.Destination.Arn != "" {
		found := false
		for _, bus := range b.buses {
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
	for name, archive := range b.archives {
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
	b.replays[input.ReplayName] = replay

	// Collect archived events to replay filtered by time window and event pattern.
	eventsToReplay := b.filterArchivedEvents(archiveName, archivePattern, input.EventStartTime, input.EventEndTime)

	dt := b.deliveryTargets
	workerSem := b.workerSem
	ctx := b.ctx
	delivTimeout := b.deliveryTimeout
	cp := *replay
	b.mu.Unlock()

	// Deliver archived events asynchronously and mark the replay complete.
	if !b.closing.Load() {
		b.scheduleReplayWorker(ctx, workerSem, input.ReplayName, eventsToReplay, dt, delivTimeout)
	}

	return &cp, nil
}

// filterArchivedEvents returns archived events for the named archive filtered by
// time window [startTime, endTime) and optional event pattern.
// Must be called with b.mu held for reading.
func (b *InMemoryBackend) filterArchivedEvents(
	archiveName, pattern string,
	startTime, endTime time.Time,
) []EventEntry {
	if archiveName == "" {
		return nil
	}

	raw := b.archivedEvents[archiveName]
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
			b.deliverEvents(ctx, eventsToReplay, *dt, delivTimeout)
		}

		b.mu.Lock("StartReplay-complete")
		if r, ok := b.replays[replayName]; ok && r.State == replayStateStarting {
			r.State = "COMPLETED"
			r.ReplayEndTime = time.Now()
		}
		b.mu.Unlock()
	})
}

// ListRuleNamesByTarget returns rule names that have a target matching the given ARN.
func (b *InMemoryBackend) ListRuleNamesByTarget(targetARN, eventBusName, nextToken string) ([]string, string, error) {
	if eventBusName == "" {
		eventBusName = defaultEventBusName
	}

	b.mu.RLock("ListRuleNamesByTarget")
	defer b.mu.RUnlock()

	var names []string
	for ruleName, tMap := range b.targets {
		prefix := eventBusName + "/"
		if !strings.HasPrefix(ruleName, prefix) {
			continue
		}
		for _, t := range tMap {
			if t.Arn == targetARN {
				names = append(names, strings.TrimPrefix(ruleName, prefix))

				break
			}
		}
	}

	sort.Strings(names)

	page, outToken := paginate(names, nextToken)

	return page, outToken, nil
}

// TestEventPattern tests an event pattern against an event JSON string.
func (b *InMemoryBackend) TestEventPattern(pattern, event string) (bool, error) {
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
func (b *InMemoryBackend) UpdateEventBus(input UpdateEventBusInput) (*EventBus, error) {
	busName := input.Name
	if busName == "" {
		busName = defaultEventBusName
	}

	b.mu.Lock("UpdateEventBus")
	defer b.mu.Unlock()

	bus, exists := b.buses[busName]
	if !exists {
		return nil, fmt.Errorf("%w: event bus %s not found", ErrEventBusNotFound, busName)
	}

	bus.Description = input.Description

	cp := *bus

	return &cp, nil
}

// PutPermission adds or replaces a resource-based policy statement on an event bus.
func (b *InMemoryBackend) PutPermission(input PutPermissionInput) error {
	busName := input.EventBusName
	if busName == "" {
		busName = defaultEventBusName
	}

	b.mu.Lock("PutPermission")
	defer b.mu.Unlock()

	if _, exists := b.buses[busName]; !exists {
		return fmt.Errorf("%w: event bus %s not found", ErrEventBusNotFound, busName)
	}

	policy := b.busePolicies[busName]
	if policy == nil {
		policy = &EventBusPolicy{Statements: make(map[string]*EventBusPolicyStatement)}
		b.busePolicies[busName] = policy
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
func (b *InMemoryBackend) RemovePermission(input RemovePermissionInput) error {
	busName := input.EventBusName
	if busName == "" {
		busName = defaultEventBusName
	}

	b.mu.Lock("RemovePermission")
	defer b.mu.Unlock()

	if _, exists := b.buses[busName]; !exists {
		return fmt.Errorf("%w: event bus %s not found", ErrEventBusNotFound, busName)
	}

	if input.RemoveAllPermissions {
		delete(b.busePolicies, busName)

		return nil
	}

	policy := b.busePolicies[busName]
	if policy == nil {
		return nil
	}
	delete(policy.Statements, input.StatementID)

	return nil
}

// GetEventBusPolicy returns the resource-based policy for an event bus as JSON.
func (b *InMemoryBackend) GetEventBusPolicy(eventBusName string) (string, error) {
	busName := eventBusName
	if busName == "" {
		busName = defaultEventBusName
	}

	b.mu.RLock("GetEventBusPolicy")
	defer b.mu.RUnlock()

	if _, exists := b.buses[busName]; !exists {
		return "", fmt.Errorf("%w: event bus %s not found", ErrEventBusNotFound, busName)
	}

	policy := b.busePolicies[busName]
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
func (b *InMemoryBackend) PutEventBusPolicy(input PutEventBusPolicyInput) error {
	busName := input.EventBusName
	if busName == "" {
		busName = defaultEventBusName
	}

	b.mu.Lock("PutEventBusPolicy")
	defer b.mu.Unlock()

	if _, exists := b.buses[busName]; !exists {
		return fmt.Errorf("%w: event bus %s not found", ErrEventBusNotFound, busName)
	}

	if input.Policy == "" {
		delete(b.busePolicies, busName)

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
	b.busePolicies[busName] = policy

	return nil
}

// pipeARN builds an ARN for an EventBridge Pipe.
func (b *InMemoryBackend) pipeARN(name string) string {
	return arn.Build("events", b.region, b.accountID, "pipe/"+name)
}

// CreatePipe creates a new EventBridge Pipe.
func (b *InMemoryBackend) CreatePipe(input CreatePipeInput) (*Pipe, error) {
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
func (b *InMemoryBackend) DeletePipe(name string) error {
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
func (b *InMemoryBackend) DescribePipe(name string) (*Pipe, error) {
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
func (b *InMemoryBackend) ListPipes(namePrefix, nextToken string) ([]Pipe, string, error) {
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
func (b *InMemoryBackend) UpdatePipe(input UpdatePipeInput) (*Pipe, error) {
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
func (b *InMemoryBackend) captureEventInArchives(entry EventEntry, busName string) {
	busARN := b.busARN(busName)
	envelope := buildEventEnvelope(entry)
	for _, archive := range b.archives {
		if archive.EventSourceArn != busARN {
			continue
		}
		if archive.EventPattern == "" || matchPattern(archive.EventPattern, envelope) {
			b.archivedEvents[archive.ArchiveName] = append(b.archivedEvents[archive.ArchiveName], entry)
			archive.EventCount++
		}
	}
}

// AddEventSourceInternal adds an event source directly for testing.
func (b *InMemoryBackend) AddEventSourceInternal(src *EventSource) {
	b.mu.Lock("AddEventSourceInternal")
	defer b.mu.Unlock()

	cp := *src
	b.eventSources[src.Name] = &cp
}

// AddReplayInternal adds a replay directly for testing.
func (b *InMemoryBackend) AddReplayInternal(replay *Replay) {
	b.mu.Lock("AddReplayInternal")
	defer b.mu.Unlock()

	if replay.ReplayArn == "" {
		replay.ReplayArn = b.replayARN(replay.ReplayName)
	}

	cp := *replay
	b.replays[replay.ReplayName] = &cp
}

// AddAPIDestinationInternal adds an API destination directly for testing.
func (b *InMemoryBackend) AddAPIDestinationInternal(dst *APIDestination) {
	b.mu.Lock("AddAPIDestinationInternal")
	defer b.mu.Unlock()

	cp := *dst
	b.apiDestinations[dst.Name] = &cp
}

// AddArchiveInternal adds an archive directly for testing.
func (b *InMemoryBackend) AddArchiveInternal(archive *Archive) {
	b.mu.Lock("AddArchiveInternal")
	defer b.mu.Unlock()

	cp := *archive
	b.archives[archive.ArchiveName] = &cp
}

// AddConnectionInternal adds a connection directly for testing.
func (b *InMemoryBackend) AddConnectionInternal(conn *Connection) {
	b.mu.Lock("AddConnectionInternal")
	defer b.mu.Unlock()

	cp := *conn
	b.connections[conn.Name] = &cp
}

// AddEndpointInternal adds an endpoint directly for testing.
func (b *InMemoryBackend) AddEndpointInternal(ep *Endpoint) {
	b.mu.Lock("AddEndpointInternal")
	defer b.mu.Unlock()

	cp := *ep
	b.endpoints[ep.Name] = &cp
}

// AddPartnerSourceInternal adds a partner event source directly for testing.
func (b *InMemoryBackend) AddPartnerSourceInternal(src *PartnerEventSource) {
	b.mu.Lock("AddPartnerSourceInternal")
	defer b.mu.Unlock()

	cp := *src
	b.partnerSources[src.Name] = &cp
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
