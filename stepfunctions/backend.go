package stepfunctions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"time"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/stepfunctions/asl"
)

var (
	ErrStateMachineAlreadyExists = errors.New("StateMachineAlreadyExists")
	ErrStateMachineDoesNotExist  = errors.New("StateMachineDoesNotExist")
	ErrExecutionAlreadyExists    = errors.New("ExecutionAlreadyExists")
	ErrExecutionDoesNotExist     = errors.New("ExecutionDoesNotExist")
)

const (
	executionStartedEventID   = int64(1)
	executionSucceededEventID = int64(2)
)

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
	lambdaInvoker asl.LambdaInvoker
	logger        *slog.Logger
	accountID     string
	region        string
	mu            *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend with default configuration.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion)
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with given account and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		accountID:     accountID,
		region:        region,
		stateMachines: make(map[string]*StateMachine),
		executions:    make(map[string]*Execution),
		history:       make(map[string][]*HistoryEvent),
		logger:        slog.Default(),
		mu: lockmetrics.New("stepfunctions"),
	}
}

// SetLambdaInvoker configures the Lambda invoker for Task states.
func (b *InMemoryBackend) SetLambdaInvoker(invoker asl.LambdaInvoker) {
	b.mu.Lock("SetLambdaInvoker")
	defer b.mu.Unlock()
	b.lambdaInvoker = invoker
}

// SetLogger sets the logger for the backend.
func (b *InMemoryBackend) SetLogger(log *slog.Logger) {
	b.mu.Lock("SetLogger")
	defer b.mu.Unlock()
	b.logger = log
}

func (b *InMemoryBackend) smARN(name string) string {
	return arn.Build("states", b.region, b.accountID, "stateMachine:"+name)
}

func (b *InMemoryBackend) execARN(smName, execName string) string {
	return arn.Build("states", b.region, b.accountID, "execution:"+smName+":"+execName)
}

// CreateStateMachine creates and stores a new state machine.
func (b *InMemoryBackend) CreateStateMachine(name, definition, roleArn, smType string) (*StateMachine, error) {
	if smType == "" {
		smType = "STANDARD"
	}

	arn := b.smARN(name)

	b.mu.Lock("CreateStateMachine")
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
	b.mu.Lock("DeleteStateMachine")
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
	b.mu.RLock("ListStateMachines")
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
	b.mu.RLock("DescribeStateMachine")
	defer b.mu.RUnlock()

	sm, exists := b.stateMachines[arn]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrStateMachineDoesNotExist, arn)
	}

	cp := *sm

	return &cp, nil
}

// StartExecution creates an execution and runs the ASL interpreter.
// If the state machine definition is a valid ASL, the interpreter runs asynchronously.
// If the definition cannot be parsed, execution completes synchronously with pass-through output.
func (b *InMemoryBackend) StartExecution(stateMachineArn, name, input string) (*Execution, error) {
	b.mu.Lock("StartExecution")

	sm, exists := b.stateMachines[stateMachineArn]
	if !exists {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: %s", ErrStateMachineDoesNotExist, stateMachineArn)
	}

	execArn := b.execARN(sm.Name, name)
	if _, alreadyExists := b.executions[execArn]; alreadyExists {
		b.mu.Unlock()

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
		{Timestamp: now, Type: "ExecutionStarted", ID: executionStartedEventID, PreviousEventID: 0},
	}

	definition := sm.Definition
	lambdaInvoker := b.lambdaInvoker

	// Try to parse the definition to decide whether to run async.
	parsedSM, parseErr := asl.Parse(definition)
	if parseErr != nil {
		// Invalid definition: fall back to synchronous pass-through for backward compatibility.
		// Intentionally returning nil error — the execution still "succeeds" as a no-op.
		stopDate := now
		exec.StopDate = &stopDate
		exec.Status = "SUCCEEDED"
		exec.Output = input
		b.history[execArn] = append(b.history[execArn], &HistoryEvent{
			Timestamp:       now,
			Type:            "ExecutionSucceeded",
			ID:              executionSucceededEventID,
			PreviousEventID: executionStartedEventID,
		})
		b.mu.Unlock()

		return exec, nil //nolint:nilerr // parseErr is an expected condition; caller gets a valid execution
	}

	b.mu.Unlock()

	// Run the ASL interpreter asynchronously for valid state machine definitions.
	go b.runParsedExecution(context.Background(), execArn, parsedSM, input, lambdaInvoker)

	return exec, nil
}

// historyRecorder adapts InMemoryBackend to the asl.HistoryRecorder interface.
type historyRecorder struct {
	backend *InMemoryBackend
}

func (r *historyRecorder) RecordStateEntered(execARN, stateName, stateType string, _ any) {
	r.backend.mu.Lock("RecordStateEntered")
	defer r.backend.mu.Unlock()

	events := r.backend.history[execARN]
	nextID := int64(len(events) + 1)
	r.backend.history[execARN] = append(events, &HistoryEvent{
		Timestamp:       float64(time.Now().Unix()),
		Type:            "TaskStateEntered",
		ID:              nextID,
		PreviousEventID: nextID - 1,
		StateEnteredEventDetails: &StateEnteredEventDetails{
			Name: stateName + "(" + stateType + ")",
		},
	})
}

func (r *historyRecorder) RecordStateExited(execARN, stateName, stateType string, _ any) {
	r.backend.mu.Lock("RecordStateExited")
	defer r.backend.mu.Unlock()

	events := r.backend.history[execARN]
	nextID := int64(len(events) + 1)
	r.backend.history[execARN] = append(events, &HistoryEvent{
		Timestamp:       float64(time.Now().Unix()),
		Type:            "TaskStateExited",
		ID:              nextID,
		PreviousEventID: nextID - 1,
		StateExitedEventDetails: &StateExitedEventDetails{
			Name: stateName + "(" + stateType + ")",
		},
	})
}

func (r *historyRecorder) RecordTaskScheduled(execARN, _ /* stateName */, _ /* resource */ string) {
	r.backend.mu.Lock("RecordTaskScheduled")
	defer r.backend.mu.Unlock()

	events := r.backend.history[execARN]
	nextID := int64(len(events) + 1)
	r.backend.history[execARN] = append(events, &HistoryEvent{
		Timestamp:       float64(time.Now().Unix()),
		Type:            "TaskScheduled",
		ID:              nextID,
		PreviousEventID: nextID - 1,
	})
}

func (r *historyRecorder) RecordTaskSucceeded(execARN, _ /* stateName */ string, _ any) {
	r.backend.mu.Lock("RecordTaskSucceeded")
	defer r.backend.mu.Unlock()

	events := r.backend.history[execARN]
	nextID := int64(len(events) + 1)
	r.backend.history[execARN] = append(events, &HistoryEvent{
		Timestamp:       float64(time.Now().Unix()),
		Type:            "TaskSucceeded",
		ID:              nextID,
		PreviousEventID: nextID - 1,
	})
}

func (r *historyRecorder) RecordTaskFailed(execARN, _ /* stateName */, _ /* errCode */, _ /* cause */ string) {
	r.backend.mu.Lock("RecordTaskFailed")
	defer r.backend.mu.Unlock()

	events := r.backend.history[execARN]
	nextID := int64(len(events) + 1)
	r.backend.history[execARN] = append(events, &HistoryEvent{
		Timestamp:       float64(time.Now().Unix()),
		Type:            "TaskFailed",
		ID:              nextID,
		PreviousEventID: nextID - 1,
	})
}

// runParsedExecution runs the ASL interpreter for a pre-parsed state machine and updates the execution record.
func (b *InMemoryBackend) runParsedExecution(
	ctx context.Context,
	execARN string,
	sm *asl.StateMachine,
	input string,
	lambdaInvoker asl.LambdaInvoker,
) {
	rec := &historyRecorder{backend: b}
	executor := asl.NewExecutor(sm, lambdaInvoker, rec)
	result, execErr := executor.Execute(ctx, execARN, input)

	b.mu.Lock("runParsedExecution")
	defer b.mu.Unlock()

	exec := b.executions[execARN]
	if exec == nil {
		return
	}

	now := float64(time.Now().Unix())
	exec.StopDate = &now
	events := b.history[execARN]
	nextID := int64(len(events) + 1)

	if execErr != nil {
		exec.Status = "FAILED"
		exec.Error = execErr.Error()
		b.history[execARN] = append(events, &HistoryEvent{
			Timestamp: now, Type: "ExecutionFailed", ID: nextID, PreviousEventID: nextID - 1,
		})

		return
	}

	if result.Error != "" {
		exec.Status = "FAILED"
		exec.Error = result.Error
		exec.Cause = result.Cause
		b.history[execARN] = append(events, &HistoryEvent{
			Timestamp: now, Type: "ExecutionFailed", ID: nextID, PreviousEventID: nextID - 1,
		})

		return
	}

	outputBytes, _ := json.Marshal(result.Output)
	exec.Status = "SUCCEEDED"
	exec.Output = string(outputBytes)
	b.history[execARN] = append(events, &HistoryEvent{
		Timestamp: now, Type: "ExecutionSucceeded", ID: nextID, PreviousEventID: nextID - 1,
	})
}

// StopExecution marks an execution as ABORTED.
func (b *InMemoryBackend) StopExecution(executionArn, errCode, cause string) error {
	b.mu.Lock("StopExecution")
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
	b.mu.RLock("DescribeExecution")
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
	b.mu.RLock("ListExecutions")
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
	b.mu.RLock("GetExecutionHistory")
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
