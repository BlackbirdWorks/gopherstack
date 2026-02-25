package stepfunctions

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"
)

var (
	ErrStateMachineAlreadyExists = errors.New("StateMachineAlreadyExists")
	ErrStateMachineDoesNotExist  = errors.New("StateMachineDoesNotExist")
	ErrExecutionAlreadyExists    = errors.New("ExecutionAlreadyExists")
	ErrExecutionDoesNotExist     = errors.New("ExecutionDoesNotExist")
)

// executionSucceededEventID is the history event ID for ExecutionSucceeded.
const executionSucceededEventID = 2

// StorageBackend is the interface for a Step Functions in-memory store.
type StorageBackend interface {
	CreateStateMachine(name, definition, roleArn, smType string) (*StateMachine, error)
	DeleteStateMachine(arn string) error
	ListStateMachines(nextToken string, maxResults int) ([]StateMachine, string, error)
	DescribeStateMachine(arn string) (*StateMachine, error)
	StartExecution(stateMachineArn, name, input string) (*Execution, error)
	StopExecution(executionArn, errCode, cause string) error
	DescribeExecution(executionArn string) (*Execution, error)
	ListExecutions(stateMachineArn, statusFilter, nextToken string, maxResults int) ([]Execution, string, error)
	GetExecutionHistory(
		executionArn, nextToken string,
		maxResults int,
		reverseOrder bool,
	) ([]HistoryEvent, string, error)
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	stateMachines map[string]*StateMachine   // key = stateMachineArn
	executions    map[string]*Execution      // key = executionArn
	history       map[string][]*HistoryEvent // key = executionArn
	accountID     string
	region        string
	mu            sync.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend with default configuration.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig("000000000000", "us-east-1")
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with given account and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		accountID:     accountID,
		region:        region,
		stateMachines: make(map[string]*StateMachine),
		executions:    make(map[string]*Execution),
		history:       make(map[string][]*HistoryEvent),
	}
}

func (b *InMemoryBackend) smARN(name string) string {
	return fmt.Sprintf("arn:aws:states:%s:%s:stateMachine:%s", b.region, b.accountID, name)
}

func (b *InMemoryBackend) execARN(smName, execName string) string {
	return fmt.Sprintf("arn:aws:states:%s:%s:execution:%s:%s", b.region, b.accountID, smName, execName)
}

// CreateStateMachine creates and stores a new state machine.
func (b *InMemoryBackend) CreateStateMachine(name, definition, roleArn, smType string) (*StateMachine, error) {
	if smType == "" {
		smType = "STANDARD"
	}

	arn := b.smARN(name)

	b.mu.Lock()
	defer b.mu.Unlock()

	for _, sm := range b.stateMachines {
		if sm.Name == name && sm.Status != "DELETING" {
			return nil, fmt.Errorf("%w: %s", ErrStateMachineAlreadyExists, name)
		}
	}

	sm := &StateMachine{
		CreationDate:    float64(time.Now().Unix()),
		Name:            name,
		StateMachineArn: arn,
		Type:            smType,
		Status:          "ACTIVE",
		Definition:      definition,
		RoleArn:         roleArn,
	}
	b.stateMachines[arn] = sm

	return sm, nil
}

// DeleteStateMachine marks a state machine as DELETING then removes it.
func (b *InMemoryBackend) DeleteStateMachine(arn string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	sm, exists := b.stateMachines[arn]
	if !exists {
		return fmt.Errorf("%w: %s", ErrStateMachineDoesNotExist, arn)
	}

	sm.Status = "DELETING"
	delete(b.stateMachines, arn)

	return nil
}

// ListStateMachines returns state machines with optional pagination.
func (b *InMemoryBackend) ListStateMachines(nextToken string, maxResults int) ([]StateMachine, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	all := make([]StateMachine, 0, len(b.stateMachines))
	for _, sm := range b.stateMachines {
		all = append(all, *sm)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	sms, token := paginate(all, nextToken, maxResults)

	return sms, token, nil
}

// DescribeStateMachine returns details for a single state machine.
func (b *InMemoryBackend) DescribeStateMachine(arn string) (*StateMachine, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	sm, exists := b.stateMachines[arn]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrStateMachineDoesNotExist, arn)
	}

	cp := *sm

	return &cp, nil
}

// StartExecution creates an execution and immediately marks it SUCCEEDED (stub).
func (b *InMemoryBackend) StartExecution(stateMachineArn, name, input string) (*Execution, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	sm, exists := b.stateMachines[stateMachineArn]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrStateMachineDoesNotExist, stateMachineArn)
	}

	execArn := b.execARN(sm.Name, name)
	if _, alreadyExists := b.executions[execArn]; alreadyExists {
		return nil, fmt.Errorf("%w: %s", ErrExecutionAlreadyExists, name)
	}

	now := float64(time.Now().Unix())
	exec := &Execution{
		StartDate:       now,
		ExecutionArn:    execArn,
		StateMachineArn: stateMachineArn,
		Name:            name,
		Status:          "RUNNING",
		Input:           input,
	}
	b.executions[execArn] = exec

	b.history[execArn] = []*HistoryEvent{
		{Timestamp: now, Type: "ExecutionStarted", ID: 1, PreviousEventID: 0},
	}

	// Auto-succeed: mark as SUCCEEDED with pass-through output.
	stopDate := now
	exec.StopDate = &stopDate
	exec.Status = "SUCCEEDED"
	exec.Output = input
	b.history[execArn] = append(b.history[execArn], &HistoryEvent{
		Timestamp: now, Type: "ExecutionSucceeded", ID: executionSucceededEventID, PreviousEventID: 1,
	})

	return exec, nil
}

// StopExecution marks an execution as ABORTED.
func (b *InMemoryBackend) StopExecution(executionArn, errCode, cause string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	exec, exists := b.executions[executionArn]
	if !exists {
		return fmt.Errorf("%w: %s", ErrExecutionDoesNotExist, executionArn)
	}

	now := float64(time.Now().Unix())
	exec.Status = "ABORTED"
	exec.StopDate = &now
	exec.Error = errCode
	exec.Cause = cause

	nextID := int64(len(b.history[executionArn]) + 1)
	b.history[executionArn] = append(b.history[executionArn], &HistoryEvent{
		Timestamp: now, Type: "ExecutionAborted", ID: nextID, PreviousEventID: nextID - 1,
	})

	return nil
}

// DescribeExecution returns details for a single execution.
func (b *InMemoryBackend) DescribeExecution(executionArn string) (*Execution, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	exec, exists := b.executions[executionArn]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrExecutionDoesNotExist, executionArn)
	}

	cp := *exec

	return &cp, nil
}

// ListExecutions returns executions for a state machine with optional pagination.
func (b *InMemoryBackend) ListExecutions(
	stateMachineArn, statusFilter, nextToken string, maxResults int,
) ([]Execution, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	all := make([]Execution, 0)
	for _, exec := range b.executions {
		if exec.StateMachineArn != stateMachineArn {
			continue
		}
		if statusFilter != "" && exec.Status != statusFilter {
			continue
		}
		all = append(all, *exec)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	execs, token := paginate(all, nextToken, maxResults)

	return execs, token, nil
}

// GetExecutionHistory returns history events for an execution.
func (b *InMemoryBackend) GetExecutionHistory(
	executionArn, nextToken string, maxResults int, reverseOrder bool,
) ([]HistoryEvent, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, exists := b.executions[executionArn]; !exists {
		return nil, "", fmt.Errorf("%w: %s", ErrExecutionDoesNotExist, executionArn)
	}

	raw := b.history[executionArn]
	all := make([]HistoryEvent, 0, len(raw))
	for _, e := range raw {
		all = append(all, *e)
	}

	if reverseOrder {
		sort.Slice(all, func(i, j int) bool { return all[i].ID > all[j].ID })
	}

	events, token := paginate(all, nextToken, maxResults)

	return events, token, nil
}

// paginate applies token-based pagination to a sorted slice.
func paginate[T any](all []T, nextToken string, maxResults int) ([]T, string) {
	const defaultLimit = 100

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []T{}, ""
	}

	limit := defaultLimit
	if maxResults > 0 {
		limit = maxResults
	}

	end := startIdx + limit

	var outToken string
	if end < len(all) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken
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
