package stepfunctions

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/base64"
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
	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

var (
	ErrStateMachineAlreadyExists = errors.New("StateMachineAlreadyExists")
	ErrStateMachineDoesNotExist  = errors.New("StateMachineDoesNotExist")
	ErrExecutionAlreadyExists    = errors.New("ExecutionAlreadyExists")
	ErrExecutionDoesNotExist     = errors.New("ExecutionDoesNotExist")
	ErrInvalidDefinition         = errors.New("InvalidDefinition")
	ErrInvalidExecutionType      = errors.New("InvalidExecutionType")
	ErrActivityAlreadyExists     = errors.New("ActivityAlreadyExists")
	ErrActivityDoesNotExist      = errors.New("ActivityDoesNotExist")
	ErrTaskTokenNotFound         = errors.New("TaskTokenNotFound")
	ErrActivityTaskFailed        = errors.New("ActivityTaskFailed")
)

const (
	executionStartedEventID   = int64(1)
	executionSucceededEventID = int64(2)
	maxHistoryEvents          = 25000
	maxPendingActivityTasks   = 1000
	activityPollTimeout       = 60 * time.Second
	activityTokenBytes        = 32

	statusRunning   = "RUNNING"
	statusSucceeded = "SUCCEEDED"
	statusFailed    = "FAILED"
	statusAborted   = "ABORTED"
	statusActive    = "ACTIVE"
	statusDeleting  = "DELETING"
)

// StorageBackend is the interface for a Step Functions in-memory store.
type StorageBackend interface {
	CreateStateMachine(name, definition, roleArn, smType string) (*StateMachine, error)
	DeleteStateMachine(arn string) error
	ListStateMachines(nextToken string, maxResults int) ([]StateMachine, string, error)
	DescribeStateMachine(arn string) (*StateMachine, error)
	UpdateStateMachine(arn, definition, roleArn string) (float64, error)
	StartExecution(stateMachineArn, name, input string) (*Execution, error)
	StartSyncExecution(stateMachineArn, name, input string) (*SyncExecutionResult, error)
	StopExecution(executionArn, errCode, cause string) error
	DescribeExecution(executionArn string) (*Execution, error)
	ListExecutions(stateMachineArn, statusFilter, nextToken string, maxResults int) ([]Execution, string, error)
	GetExecutionHistory(
		executionArn, nextToken string,
		maxResults int,
		reverseOrder bool,
	) ([]HistoryEvent, string, error)
	CreateActivity(name string) (*Activity, error)
	DeleteActivity(activityArn string) error
	DescribeActivity(activityArn string) (*Activity, error)
	ListActivities(nextToken string, maxResults int) ([]Activity, string, error)
	GetActivityTask(ctx context.Context, activityArn, workerName string) (*ActivityTask, error)
	SendTaskSuccess(taskToken, output string) error
	SendTaskFailure(taskToken, errCode, cause string) error
	SendTaskHeartbeat(taskToken string) error
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	lambdaInvoker  asl.LambdaInvoker
	sqsIntegration asl.SQSIntegration
	snsIntegration asl.SNSIntegration
	ddbIntegration asl.DynamoDBIntegration
	stateMachines  map[string]*StateMachine
	executions     map[string]*Execution
	history        map[string][]*HistoryEvent
	// nameIndex maps state machine name → ARN for O(1) duplicate detection.
	nameIndex map[string]string
	// smExecutions maps state machine ARN → execution ARNs for O(1) scoped listing.
	smExecutions map[string][]string
	// cancelFns holds the cancel function for each running execution goroutine.
	cancelFns map[string]context.CancelFunc
	// deletedExecs is a tombstone set for executions removed by DeleteStateMachine.
	// historyRecorder and runParsedExecution skip writes for tombstoned ARNs.
	deletedExecs      map[string]bool
	activities        map[string]*Activity
	activityNameIndex map[string]string
	// pendingTaskQueues maps activity ARN → buffered channel of pending tasks.
	pendingTaskQueues map[string]chan *activityTaskEntry
	// tasksByToken maps task token → task entry for SendTaskSuccess/Failure.
	tasksByToken map[string]*activityTaskEntry
	logger       *slog.Logger
	mu           *lockmetrics.RWMutex
	// svcCtx is the service lifecycle context. Execution goroutines derive their
	// contexts from it so that all active executions are cancelled on server shutdown.
	svcCtx    context.Context
	accountID string
	region    string
}

// activityTaskEntry holds a pending activity task and its result channel.
type activityTaskEntry struct {
	activityArn string
	resultCh    chan activityTaskResult
	taskToken   string
	input       string
}

// activityTaskResult holds the result of an activity task.
type activityTaskResult struct {
	output    string
	errCode   string
	cause     string
	succeeded bool
}

// NewInMemoryBackend creates a new InMemoryBackend with default configuration.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion)
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with given account and region.
// Use NewInMemoryBackendWithContext to bind execution goroutines to a parent context.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	return newInMemoryBackend(context.Background(), accountID, region)
}

// NewInMemoryBackendWithContext creates a new InMemoryBackend whose execution goroutines
// derive their contexts from svcCtx. When svcCtx is cancelled (e.g. on server shutdown),
// all running executions are also cancelled.
// If svcCtx is nil, [context.Background] is used.
func NewInMemoryBackendWithContext(svcCtx context.Context, accountID, region string) *InMemoryBackend {
	return newInMemoryBackend(svcCtx, accountID, region)
}

func newInMemoryBackend(svcCtx context.Context, accountID, region string) *InMemoryBackend {
	if svcCtx == nil {
		svcCtx = context.Background()
	}

	return &InMemoryBackend{
		accountID:         accountID,
		region:            region,
		svcCtx:            svcCtx,
		stateMachines:     make(map[string]*StateMachine),
		executions:        make(map[string]*Execution),
		history:           make(map[string][]*HistoryEvent),
		nameIndex:         make(map[string]string),
		smExecutions:      make(map[string][]string),
		cancelFns:         make(map[string]context.CancelFunc),
		deletedExecs:      make(map[string]bool),
		activities:        make(map[string]*Activity),
		activityNameIndex: make(map[string]string),
		pendingTaskQueues: make(map[string]chan *activityTaskEntry),
		tasksByToken:      make(map[string]*activityTaskEntry),
		logger:            slog.Default(),
		mu:                lockmetrics.New("stepfunctions"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string {
	return b.region
}

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string {
	return b.accountID
}

// Destroy cancels all running execution goroutines and releases resources.
func (b *InMemoryBackend) Destroy() {
	b.mu.Lock("Destroy")
	defer b.mu.Unlock()

	for execARN, cancel := range b.cancelFns {
		cancel()
		delete(b.cancelFns, execARN)
	}

	// Close activity task queues to unblock any waiting GetActivityTask callers.
	for activityArn, queue := range b.pendingTaskQueues {
		close(queue)
		delete(b.pendingTaskQueues, activityArn)
	}
}

// SetLambdaInvoker configures the Lambda invoker for Task states.
func (b *InMemoryBackend) SetLambdaInvoker(invoker asl.LambdaInvoker) {
	b.mu.Lock("SetLambdaInvoker")
	defer b.mu.Unlock()
	b.lambdaInvoker = invoker
}

// SetSQSIntegration configures the SQS integration for Task states.
func (b *InMemoryBackend) SetSQSIntegration(sqs asl.SQSIntegration) {
	b.mu.Lock("SetSQSIntegration")
	defer b.mu.Unlock()
	b.sqsIntegration = sqs
}

// SetSNSIntegration configures the SNS integration for Task states.
func (b *InMemoryBackend) SetSNSIntegration(sns asl.SNSIntegration) {
	b.mu.Lock("SetSNSIntegration")
	defer b.mu.Unlock()
	b.snsIntegration = sns
}

// SetDynamoDBIntegration configures the DynamoDB integration for Task states.
func (b *InMemoryBackend) SetDynamoDBIntegration(ddb asl.DynamoDBIntegration) {
	b.mu.Lock("SetDynamoDBIntegration")
	defer b.mu.Unlock()
	b.ddbIntegration = ddb
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

func (b *InMemoryBackend) activityARN(name string) string {
	return arn.Build("states", b.region, b.accountID, "activity:"+name)
}

// CreateStateMachine creates and stores a new state machine.
func (b *InMemoryBackend) CreateStateMachine(name, definition, roleArn, smType string) (*StateMachine, error) {
	if smType == "" {
		smType = "STANDARD"
	}

	// Validate the definition before storing.
	if _, err := asl.Parse(definition); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidDefinition, err)
	}

	arn := b.smARN(name)

	b.mu.Lock("CreateStateMachine")
	defer b.mu.Unlock()

	if existingARN, exists := b.nameIndex[name]; exists {
		if sm := b.stateMachines[existingARN]; sm != nil && sm.Status != statusDeleting {
			return nil, fmt.Errorf("%w: %s", ErrStateMachineAlreadyExists, name)
		}
	}

	sm := &StateMachine{
		CreationDate:    float64(time.Now().Unix()),
		Name:            name,
		StateMachineArn: arn,
		Type:            smType,
		Status:          statusActive,
		Definition:      definition,
		RoleArn:         roleArn,
	}
	b.stateMachines[arn] = sm
	b.nameIndex[name] = arn

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

	sm.Status = statusDeleting
	delete(b.stateMachines, arn)
	delete(b.nameIndex, sm.Name)

	// Cancel running goroutines and clean up all executions and history for this SM.
	for _, execARN := range b.smExecutions[arn] {
		if cancelFn, ok := b.cancelFns[execARN]; ok {
			cancelFn()
			delete(b.cancelFns, execARN)
			// Only tombstone executions whose goroutines are still running; completed
			// executions have already cleaned up their own tombstones.
			b.deletedExecs[execARN] = true
		}

		delete(b.executions, execARN)
		delete(b.history, execARN)
	}

	delete(b.smExecutions, arn)

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

// UpdateStateMachine updates a state machine's definition and/or roleArn.
// It returns the update timestamp (Unix epoch seconds).
func (b *InMemoryBackend) UpdateStateMachine(smARN, definition, roleArn string) (float64, error) {
	// Validate the new definition before acquiring the lock.
	if definition != "" {
		if _, err := asl.Parse(definition); err != nil {
			return 0, fmt.Errorf("%w: %w", ErrInvalidDefinition, err)
		}
	}

	b.mu.Lock("UpdateStateMachine")
	defer b.mu.Unlock()

	sm, exists := b.stateMachines[smARN]
	if !exists {
		return 0, fmt.Errorf("%w: %s", ErrStateMachineDoesNotExist, smARN)
	}

	if definition != "" {
		sm.Definition = definition
	}

	if roleArn != "" {
		sm.RoleArn = roleArn
	}

	sm.UpdatedDate = float64(time.Now().Unix())

	return sm.UpdatedDate, nil
}

// StartSyncExecution executes an EXPRESS state machine synchronously and returns the result.
func (b *InMemoryBackend) StartSyncExecution(stateMachineArn, name, input string) (*SyncExecutionResult, error) {
	b.mu.RLock("StartSyncExecution")
	sm, exists := b.stateMachines[stateMachineArn]
	if !exists {
		b.mu.RUnlock()

		return nil, fmt.Errorf("%w: %s", ErrStateMachineDoesNotExist, stateMachineArn)
	}

	if sm.Type != "EXPRESS" {
		b.mu.RUnlock()

		return nil, fmt.Errorf("%w: sync execution requires EXPRESS state machine", ErrInvalidExecutionType)
	}

	smName := sm.Name
	definition := sm.Definition
	lambdaInvoker := b.lambdaInvoker
	sqsIntegration := b.sqsIntegration
	snsIntegration := b.snsIntegration
	ddbIntegration := b.ddbIntegration
	b.mu.RUnlock()

	parsedSM, parseErr := asl.Parse(definition)
	if parseErr != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidDefinition, parseErr)
	}

	if name == "" {
		name = fmt.Sprintf("sync-%d", time.Now().UnixNano())
	}

	startDate := float64(time.Now().Unix())
	execARN := b.execARN(smName, name)

	// Run synchronously with nil history recorder (sync executions are ephemeral).
	executor := asl.NewExecutor(parsedSM, lambdaInvoker, nil)
	executor.SetSQSIntegration(sqsIntegration)
	executor.SetSNSIntegration(snsIntegration)
	executor.SetDynamoDBIntegration(ddbIntegration)
	executor.SetActivityInvoker(b)

	result, execErr := executor.Execute(b.svcCtx, execARN, input)

	stopDate := float64(time.Now().Unix())

	syncResult := &SyncExecutionResult{
		StartDate:       startDate,
		StopDate:        stopDate,
		ExecutionArn:    execARN,
		StateMachineArn: stateMachineArn,
		Name:            name,
		Input:           input,
	}

	if execErr != nil {
		syncResult.Status = statusFailed
		syncResult.Error = execErr.Error()

		return syncResult, nil //nolint:nilerr // execution errors are encoded in the result; no Go error is returned
	}

	if result.Error != "" {
		syncResult.Status = statusFailed
		syncResult.Error = result.Error
		syncResult.Cause = result.Cause

		return syncResult, nil
	}

	outputBytes, _ := json.Marshal(result.Output)
	syncResult.Status = statusSucceeded
	syncResult.Output = string(outputBytes)

	return syncResult, nil
}

// StartExecution creates an execution and runs the ASL interpreter asynchronously.
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

	// Parse the definition before inserting any state, so a bad definition never
	// leaves an orphaned RUNNING execution in the store.
	definition := sm.Definition
	parsedSM, parseErr := asl.Parse(definition)
	if parseErr != nil {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: %w", ErrInvalidDefinition, parseErr)
	}

	now := float64(time.Now().Unix())
	exec := &Execution{
		StartDate:       now,
		ExecutionArn:    execArn,
		StateMachineArn: stateMachineArn,
		Name:            name,
		Status:          statusRunning,
		Input:           input,
	}
	b.executions[execArn] = exec

	b.history[execArn] = []*HistoryEvent{
		{Timestamp: now, Type: "ExecutionStarted", ID: executionStartedEventID, PreviousEventID: 0},
	}

	lambdaInvoker := b.lambdaInvoker
	sqsIntegration := b.sqsIntegration
	snsIntegration := b.snsIntegration
	ddbIntegration := b.ddbIntegration

	// Register the execution in the SM→executions index and store a cancel fn
	// so StopExecution and DeleteStateMachine can cancel the goroutine.
	// The context is derived from b.svcCtx so that all active executions are
	// also cancelled when the server shuts down.

	ctx, cancel := context.WithCancel(b.svcCtx) //nolint:gosec // cancel is stored for StopExecution/DeleteStateMachine
	b.cancelFns[execArn] = cancel
	b.smExecutions[stateMachineArn] = append(b.smExecutions[stateMachineArn], execArn)

	var activityInvoker asl.ActivityInvoker = b

	b.mu.Unlock()

	// Run the ASL interpreter asynchronously.
	go b.runParsedExecution(
		ctx, execArn, parsedSM, input,
		lambdaInvoker, sqsIntegration, snsIntegration, ddbIntegration, activityInvoker,
	)

	return exec, nil
}

// historyRecorder adapts InMemoryBackend to the asl.HistoryRecorder interface.
type historyRecorder struct {
	backend *InMemoryBackend
}

// stateEnteredEventType returns the AWS event type name for the state-entered event
// for each state type.
func stateEnteredEventType(stateType string) string {
	switch stateType {
	case "Task":
		return "TaskStateEntered"
	case "Pass":
		return "PassStateEntered"
	case "Choice":
		return "ChoiceStateEntered"
	case "Wait":
		return "WaitStateEntered"
	case "Succeed":
		return "SucceedStateEntered"
	case "Fail":
		return "FailStateEntered"
	case "Parallel":
		return "ParallelStateEntered"
	case "Map":
		return "MapStateEntered"
	default:
		return stateType + "StateEntered"
	}
}

// stateExitedEventType returns the AWS event type name for the state-exited event
// for each state type.
func stateExitedEventType(stateType string) string {
	switch stateType {
	case "Task":
		return "TaskStateExited"
	case "Pass":
		return "PassStateExited"
	case "Choice":
		return "ChoiceStateExited"
	case "Wait":
		return "WaitStateExited"
	case "Succeed":
		return "SucceedStateExited"
	case "Parallel":
		return "ParallelStateExited"
	case "Map":
		return "MapStateExited"
	default:
		return stateType + "StateExited"
	}
}

func (r *historyRecorder) RecordStateEntered(execARN, stateName, stateType string, _ any) {
	r.backend.mu.Lock("RecordStateEntered")
	defer r.backend.mu.Unlock()

	if r.backend.deletedExecs[execARN] {
		return
	}

	events := r.backend.history[execARN]
	if len(events) >= maxHistoryEvents {
		return
	}

	nextID := int64(len(events) + 1)
	r.backend.history[execARN] = append(events, &HistoryEvent{
		Timestamp:       float64(time.Now().Unix()),
		Type:            stateEnteredEventType(stateType),
		ID:              nextID,
		PreviousEventID: nextID - 1,
		StateEnteredEventDetails: &StateEnteredEventDetails{
			Name: stateName,
		},
	})
}

func (r *historyRecorder) RecordStateExited(execARN, stateName, stateType string, _ any) {
	r.backend.mu.Lock("RecordStateExited")
	defer r.backend.mu.Unlock()

	if r.backend.deletedExecs[execARN] {
		return
	}

	events := r.backend.history[execARN]
	if len(events) >= maxHistoryEvents {
		return
	}

	nextID := int64(len(events) + 1)
	r.backend.history[execARN] = append(events, &HistoryEvent{
		Timestamp:       float64(time.Now().Unix()),
		Type:            stateExitedEventType(stateType),
		ID:              nextID,
		PreviousEventID: nextID - 1,
		StateExitedEventDetails: &StateExitedEventDetails{
			Name: stateName,
		},
	})
}

func (r *historyRecorder) RecordTaskScheduled(execARN, _ /* stateName */, _ /* resource */ string) {
	r.backend.mu.Lock("RecordTaskScheduled")
	defer r.backend.mu.Unlock()

	if r.backend.deletedExecs[execARN] {
		return
	}

	events := r.backend.history[execARN]
	if len(events) >= maxHistoryEvents {
		return
	}

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

	if r.backend.deletedExecs[execARN] {
		return
	}

	events := r.backend.history[execARN]
	if len(events) >= maxHistoryEvents {
		return
	}

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

	if r.backend.deletedExecs[execARN] {
		return
	}

	events := r.backend.history[execARN]
	if len(events) >= maxHistoryEvents {
		return
	}

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
	sqsIntegration asl.SQSIntegration,
	snsIntegration asl.SNSIntegration,
	ddbIntegration asl.DynamoDBIntegration,
	activityInvoker asl.ActivityInvoker,
) {
	rec := &historyRecorder{backend: b}
	executor := asl.NewExecutor(sm, lambdaInvoker, rec)
	executor.SetSQSIntegration(sqsIntegration)
	executor.SetSNSIntegration(snsIntegration)
	executor.SetDynamoDBIntegration(ddbIntegration)
	executor.SetActivityInvoker(activityInvoker)
	result, execErr := executor.Execute(ctx, execARN, input)

	b.mu.Lock("runParsedExecution")
	defer b.mu.Unlock()

	// Clean up the cancel function now that the goroutine has exited.
	delete(b.cancelFns, execARN)

	// If the execution was tombstoned by DeleteStateMachine, discard and exit.
	if b.deletedExecs[execARN] {
		delete(b.deletedExecs, execARN)

		return
	}

	exec := b.executions[execARN]
	if exec == nil {
		return
	}

	// If the execution was already moved to a terminal state (e.g., ABORTED via
	// StopExecution) while the background runner was in flight, do not overwrite it.
	if exec.Status != statusRunning {
		return
	}

	now := float64(time.Now().Unix())
	exec.StopDate = &now
	events := b.history[execARN]
	nextID := int64(len(events) + 1)

	if execErr != nil {
		exec.Status = statusFailed
		exec.Error = execErr.Error()
		b.history[execARN] = append(events, &HistoryEvent{
			Timestamp: now, Type: "ExecutionFailed", ID: nextID, PreviousEventID: nextID - 1,
		})

		return
	}

	if result.Error != "" {
		exec.Status = statusFailed
		exec.Error = result.Error
		exec.Cause = result.Cause
		b.history[execARN] = append(events, &HistoryEvent{
			Timestamp: now, Type: "ExecutionFailed", ID: nextID, PreviousEventID: nextID - 1,
		})

		return
	}

	outputBytes, _ := json.Marshal(result.Output)
	exec.Status = statusSucceeded
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
	exec.Status = statusAborted
	exec.StopDate = &now
	exec.Error = errCode
	exec.Cause = cause

	// Cancel the running goroutine for this execution.
	if cancelFn, ok := b.cancelFns[executionArn]; ok {
		cancelFn()
		delete(b.cancelFns, executionArn)
	}

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

	execARNs := b.smExecutions[stateMachineArn]
	all := make([]Execution, 0, len(execARNs))

	for _, execARN := range execARNs {
		exec := b.executions[execARN]
		if exec == nil {
			// Defensive guard: the smExecutions index should always be consistent
			// with b.executions, but skip any stale references just in case.
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

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
// Running executions are cancelled before state is cleared.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")

	// Cancel all running execution goroutines.
	for _, cancel := range b.cancelFns {
		cancel()
	}

	// Close activity task queues to unblock any waiting GetActivityTask callers.
	for _, queue := range b.pendingTaskQueues {
		close(queue)
	}

	// Tombstone all current execution ARNs so any in-flight goroutines that
	// exit after the reset see the tombstone and discard their state writes.
	newDeleted := make(map[string]bool, len(b.executions))
	for arn := range b.executions {
		newDeleted[arn] = true
	}

	b.stateMachines = make(map[string]*StateMachine)
	b.executions = make(map[string]*Execution)
	b.history = make(map[string][]*HistoryEvent)
	b.nameIndex = make(map[string]string)
	b.smExecutions = make(map[string][]string)
	b.cancelFns = make(map[string]context.CancelFunc)
	b.deletedExecs = newDeleted
	b.activities = make(map[string]*Activity)
	b.activityNameIndex = make(map[string]string)
	b.pendingTaskQueues = make(map[string]chan *activityTaskEntry)
	b.tasksByToken = make(map[string]*activityTaskEntry)

	b.mu.Unlock()
}

// CreateActivity creates a new activity resource.
func (b *InMemoryBackend) CreateActivity(name string) (*Activity, error) {
	actARN := b.activityARN(name)

	b.mu.Lock("CreateActivity")
	defer b.mu.Unlock()

	if _, exists := b.activityNameIndex[name]; exists {
		return nil, fmt.Errorf("%w: %s", ErrActivityAlreadyExists, name)
	}

	a := &Activity{
		Name:         name,
		ActivityArn:  actARN,
		CreationDate: float64(time.Now().Unix()),
	}
	b.activities[actARN] = a
	b.activityNameIndex[name] = actARN
	b.pendingTaskQueues[actARN] = make(chan *activityTaskEntry, maxPendingActivityTasks)

	cp := *a

	return &cp, nil
}

// DeleteActivity deletes an activity and closes its pending task queue.
func (b *InMemoryBackend) DeleteActivity(activityArn string) error {
	b.mu.Lock("DeleteActivity")
	defer b.mu.Unlock()

	a, exists := b.activities[activityArn]
	if !exists {
		return fmt.Errorf("%w: %s", ErrActivityDoesNotExist, activityArn)
	}

	delete(b.activities, activityArn)
	delete(b.activityNameIndex, a.Name)

	if queue, hasQueue := b.pendingTaskQueues[activityArn]; hasQueue {
		close(queue)
		delete(b.pendingTaskQueues, activityArn)
	}

	taskTokens := make([]string, 0)
	for taskToken, entry := range b.tasksByToken {
		if entry.activityArn == activityArn {
			taskTokens = append(taskTokens, taskToken)
		}
	}
	for _, taskToken := range taskTokens {
		delete(b.tasksByToken, taskToken)
	}

	return nil
}

// DescribeActivity returns activity details.
func (b *InMemoryBackend) DescribeActivity(activityArn string) (*Activity, error) {
	b.mu.RLock("DescribeActivity")
	defer b.mu.RUnlock()

	a, ok := b.activities[activityArn]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrActivityDoesNotExist, activityArn)
	}

	cp := *a

	return &cp, nil
}

// ListActivities returns all activities with pagination.
func (b *InMemoryBackend) ListActivities(nextToken string, maxResults int) ([]Activity, string, error) {
	b.mu.RLock("ListActivities")
	defer b.mu.RUnlock()

	all := make([]Activity, 0, len(b.activities))
	for _, a := range b.activities {
		all = append(all, *a)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	acts, token := paginate(all, nextToken, maxResults)

	return acts, token, nil
}

// GetActivityTask long-polls for a pending task (up to 60 seconds).
// Returns an empty ActivityTask (TaskToken="") if no task is available — AWS-compatible behavior.
func (b *InMemoryBackend) GetActivityTask(
	ctx context.Context,
	activityArn, _ /* workerName */ string,
) (*ActivityTask, error) {
	b.mu.RLock("GetActivityTask")
	queue, ok := b.pendingTaskQueues[activityArn]
	b.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrActivityDoesNotExist, activityArn)
	}

	pollCtx, cancel := context.WithTimeout(ctx, activityPollTimeout)
	defer cancel()

	select {
	case entry, open := <-queue:
		if !open || entry == nil {
			// Channel was closed (activity destroyed or backend reset).
			return &ActivityTask{}, nil
		}

		return &ActivityTask{TaskToken: entry.taskToken, Input: entry.input}, nil
	case <-pollCtx.Done():
		return &ActivityTask{}, nil
	}
}

// SendTaskSuccess signals successful completion of an activity task with output.
func (b *InMemoryBackend) SendTaskSuccess(taskToken, output string) error {
	b.mu.Lock("SendTaskSuccess")
	entry, ok := b.tasksByToken[taskToken]

	if !ok {
		b.mu.Unlock()

		return fmt.Errorf("%w: %s", ErrTaskTokenNotFound, taskToken)
	}

	delete(b.tasksByToken, taskToken)
	b.mu.Unlock()

	select {
	case entry.resultCh <- activityTaskResult{output: output, succeeded: true}:
	default:
	}

	return nil
}

// SendTaskFailure signals failure of an activity task.
func (b *InMemoryBackend) SendTaskFailure(taskToken, errCode, cause string) error {
	b.mu.Lock("SendTaskFailure")
	entry, ok := b.tasksByToken[taskToken]

	if !ok {
		b.mu.Unlock()

		return fmt.Errorf("%w: %s", ErrTaskTokenNotFound, taskToken)
	}

	delete(b.tasksByToken, taskToken)
	b.mu.Unlock()

	select {
	case entry.resultCh <- activityTaskResult{errCode: errCode, cause: cause, succeeded: false}:
	default:
	}

	return nil
}

// SendTaskHeartbeat resets the heartbeat timer for an activity task.
// In this implementation, it only validates that the task token is known.
func (b *InMemoryBackend) SendTaskHeartbeat(taskToken string) error {
	b.mu.RLock("SendTaskHeartbeat")
	_, ok := b.tasksByToken[taskToken]
	b.mu.RUnlock()

	if !ok {
		return fmt.Errorf("%w: %s", ErrTaskTokenNotFound, taskToken)
	}

	return nil
}

// InvokeActivity implements asl.ActivityInvoker.
// It enqueues a task for the activity and blocks until a worker calls
// SendTaskSuccess or SendTaskFailure, or the context is cancelled.
func (b *InMemoryBackend) InvokeActivity(ctx context.Context, activityArn, inputJSON string) (string, error) {
	tokenBytes := make([]byte, activityTokenBytes)
	if _, err := cryptorand.Read(tokenBytes); err != nil {
		return "", fmt.Errorf("generate task token: %w", err)
	}

	taskToken := base64.URLEncoding.EncodeToString(tokenBytes)

	entry := &activityTaskEntry{
		activityArn: activityArn,
		taskToken:   taskToken,
		input:       inputJSON,
		resultCh:    make(chan activityTaskResult, 1),
	}

	b.mu.Lock("InvokeActivity")
	queue, ok := b.pendingTaskQueues[activityArn]

	if !ok {
		b.mu.Unlock()

		return "", fmt.Errorf("%w: %s", ErrActivityDoesNotExist, activityArn)
	}

	b.tasksByToken[taskToken] = entry
	b.mu.Unlock()

	// Enqueue the task, respecting context cancellation if the queue is full.
	select {
	case queue <- entry:
	case <-ctx.Done():
		b.mu.Lock("InvokeActivity.cancel")
		delete(b.tasksByToken, taskToken)
		b.mu.Unlock()

		return "", ctx.Err()
	}

	// Wait for the worker to complete the task.
	select {
	case result := <-entry.resultCh:
		if result.succeeded {
			return result.output, nil
		}

		return "", fmt.Errorf("%w: %s", ErrActivityTaskFailed, result.errCode)
	case <-ctx.Done():
		b.mu.Lock("InvokeActivity.wait.cancel")
		delete(b.tasksByToken, taskToken)
		b.mu.Unlock()

		return "", ctx.Err()
	}
}
