package eventbridge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	ErrEventBusNotFound       = errors.New("ResourceNotFoundException")
	ErrEventBusAlreadyExists  = errors.New("ResourceAlreadyExistsException")
	ErrRuleNotFound           = errors.New("ResourceNotFoundException")
	ErrCannotDeleteDefaultBus = errors.New("IllegalArgumentException")
	ErrInvalidParameter       = errors.New("InvalidParameterException")
)

const (
	defaultEventBusName = "default"
	maxEventLogSize     = 1000
	ruleStateEnabled    = "ENABLED"
	ruleStateDisabled   = "DISABLED"
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
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	logger          *slog.Logger
	deliveryTargets *DeliveryTargets
	buses           map[string]*EventBus
	rules           map[string]map[string]*Rule
	targets         map[string]map[string]*Target
	accountID       string
	region          string
	eventLog        []EventLogEntry
	mu              sync.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend with default configuration.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig("000000000000", "us-east-1")
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with given account and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		accountID:       accountID,
		region:          region,
		buses:           make(map[string]*EventBus),
		rules:           make(map[string]map[string]*Rule),
		targets:         make(map[string]map[string]*Target),
		logger:          slog.Default(),
		deliveryTargets: &DeliveryTargets{},
	}
	// Create the default event bus.
	b.buses[defaultEventBusName] = &EventBus{
		Name:        defaultEventBusName,
		Arn:         b.busARN(defaultEventBusName),
		CreatedTime: time.Now(),
	}

	return b
}

// SetLogger sets the logger for the backend.
func (b *InMemoryBackend) SetLogger(log *slog.Logger) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.logger = log
}

// SetDeliveryTargets configures the service references used for fan-out delivery.
func (b *InMemoryBackend) SetDeliveryTargets(dt *DeliveryTargets) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deliveryTargets = dt
}

func (b *InMemoryBackend) busARN(name string) string {
	return fmt.Sprintf("arn:aws:events:%s:%s:event-bus/%s", b.region, b.accountID, name)
}

func (b *InMemoryBackend) ruleARN(busName, ruleName string) string {
	return fmt.Sprintf("arn:aws:events:%s:%s:rule/%s/%s", b.region, b.accountID, busName, ruleName)
}

func (b *InMemoryBackend) targetKey(busName, ruleName string) string {
	return busName + "/" + ruleName
}

// CreateEventBus creates a new event bus.
func (b *InMemoryBackend) CreateEventBus(name, description string) (*EventBus, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock()
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

	b.mu.Lock()
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
	b.mu.RLock()
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

	b.mu.RLock()
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

	b.mu.Lock()
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

	b.mu.Lock()
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

	b.mu.RLock()
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

	b.mu.RLock()
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

	b.mu.Lock()
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

	b.mu.Lock()
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

	b.mu.Lock()
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

	b.mu.RLock()
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
	b.mu.Lock()

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
	b.mu.Unlock()

	// Trigger async fan-out delivery after releasing the lock.
	if dt != nil {
		entriesCopy := make([]EventEntry, len(entries))
		copy(entriesCopy, entries)
		go b.deliverEvents(context.Background(), entriesCopy, *dt)
	}

	return results
}

// GetEventLog returns a copy of the current event log.
func (b *InMemoryBackend) GetEventLog() []EventLogEntry {
	b.mu.RLock()
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
