package eventbridge

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
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
)

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
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	ctx             context.Context
	deliveryTargets *DeliveryTargets
	buses           map[string]*EventBus
	rules           map[string]map[string]*Rule
	targets         map[string]map[string]*Target
	eventSources    map[string]*EventSource
	replays         map[string]*Replay
	apiDestinations map[string]*APIDestination
	archives        map[string]*Archive
	connections     map[string]*Connection
	endpoints       map[string]*Endpoint
	partnerSources  map[string]*PartnerEventSource
	mu              *lockmetrics.RWMutex
	cancel          context.CancelFunc
	workerSem       chan struct{}
	accountID       string
	region          string
	eventLog        []EventLogEntry
	wg              sync.WaitGroup
	closing         atomic.Bool
	shutdownTimeout time.Duration
	deliveryTimeout time.Duration
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

	ctx, cancel := context.WithCancel(svcCtx) //nolint:gosec // cancel is retained on backend for shutdown cleanup
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
		connections:     make(map[string]*Connection),
		endpoints:       make(map[string]*Endpoint),
		partnerSources:  make(map[string]*PartnerEventSource),
		deliveryTargets: &DeliveryTargets{},
		mu:              lockmetrics.New("eventbridge"),
		ctx:             ctx,
		cancel:          cancel,
		workerSem:       make(chan struct{}, defaultDeliveryWorkers),
		shutdownTimeout: defaultShutdownTimeout,
		deliveryTimeout: defaultDeliveryTimeout,
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

	select {
	case <-done:
	case <-time.After(timeout):
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

// CreateEventBus creates a new event bus.
func (b *InMemoryBackend) CreateEventBus(name, description string) (*EventBus, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateEventBus")
	defer b.mu.Unlock()

	if _, exists := b.buses[name]; exists {
		return nil, fmt.Errorf("%w: Event bus %s already exists", ErrEventBusAlreadyExists, name)
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

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []EventBus{}, "", nil
	}

	const defaultLimit = 100
	end := startIdx + defaultLimit
	var outToken string
	if end < len(all) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken, nil
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

// PutRule creates or updates a rule on an event bus.
func (b *InMemoryBackend) PutRule(input PutRuleInput) (*Rule, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	busName := input.EventBusName
	if busName == "" {
		busName = defaultEventBusName
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

	rule := &Rule{
		Name:               input.Name,
		Arn:                b.ruleARN(busName, input.Name),
		EventBusName:       busName,
		EventPattern:       input.EventPattern,
		State:              state,
		Description:        input.Description,
		ScheduleExpression: input.ScheduleExpression,
		RoleArn:            input.RoleArn,
	}
	b.rules[busName][input.Name] = rule

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

	if _, ruleExists := busRules[name]; !ruleExists {
		return fmt.Errorf("%w: Rule %s not found", ErrRuleNotFound, name)
	}

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

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []Rule{}, "", nil
	}

	const defaultLimit = 100
	end := startIdx + defaultLimit
	var outToken string
	if end < len(all) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken, nil
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

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []Target{}, "", nil
	}

	const defaultLimit = 100
	end := startIdx + defaultLimit
	var outToken string
	if end < len(all) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken, nil
}

// PutEvents records events in the event log and returns result entries.
func (b *InMemoryBackend) PutEvents(entries []EventEntry) []EventResultEntry {
	b.mu.Lock("PutEvents")

	results := make([]EventResultEntry, 0, len(entries))
	for _, entry := range entries {
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
	if dt != nil && !b.closing.Load() {
		entriesCopy := make([]EventEntry, len(entries))
		copy(entriesCopy, entries)
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
	b.connections = make(map[string]*Connection)
	b.endpoints = make(map[string]*Endpoint)
	b.partnerSources = make(map[string]*PartnerEventSource)

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

	src.State = "ACTIVE"

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

	if replay.State != "RUNNING" && replay.State != "STARTING" {
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

	b.mu.Lock("CreateAPIDestination")
	defer b.mu.Unlock()

	if _, exists := b.apiDestinations[input.Name]; exists {
		return nil, fmt.Errorf("%w: API destination %s already exists", ErrAlreadyExists, input.Name)
	}

	now := time.Now()
	dst := &APIDestination{
		APIDestinationArn:            b.apiDestinationARN(input.Name),
		APIDestinationState:          "ACTIVE",
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

	if input.EventSourceArn == "" {
		return nil, fmt.Errorf("%w: EventSourceArn is required", ErrInvalidParameter)
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
		State:             "ACTIVE",
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
