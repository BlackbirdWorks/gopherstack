package stepfunctions

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

var (
	ErrStateMachineAlreadyExists       = errors.New("StateMachineAlreadyExists")
	ErrStateMachineDoesNotExist        = errors.New("StateMachineDoesNotExist")
	ErrStateMachineVersionDoesNotExist = errors.New("StateMachineVersionDoesNotExist")
	ErrStateMachineAliasAlreadyExists  = errors.New("StateMachineAliasAlreadyExists")
	ErrStateMachineAliasDoesNotExist   = errors.New("StateMachineAliasDoesNotExist")
	ErrExecutionAlreadyExists          = errors.New("ExecutionAlreadyExists")
	ErrExecutionDoesNotExist           = errors.New("ExecutionDoesNotExist")
	ErrExecutionNotRedrivable          = errors.New("ExecutionNotRedrivable")
	ErrInvalidDefinition               = errors.New("InvalidDefinition")
	ErrInvalidExecutionType            = errors.New("InvalidExecutionType")
	ErrInvalidRoleArn                  = errors.New("InvalidArn")
	ErrInvalidName                     = errors.New("InvalidName")
	ErrInvalidRoutingConfiguration     = errors.New("InvalidRoutingConfiguration")
	ErrTagPolicyViolation              = errors.New("TagPolicyViolation")
	ErrActivityAlreadyExists           = errors.New("ActivityAlreadyExists")
	ErrActivityDoesNotExist            = errors.New("ActivityDoesNotExist")
	ErrTaskTokenNotFound               = errors.New("TaskTokenNotFound")
	ErrTaskTokenAlreadyExists          = errors.New("TaskTokenAlreadyExists")
	ErrActivityTaskFailed              = errors.New("ActivityTaskFailed")
	ErrHeartbeatTimeout                = errors.New("States.HeartbeatTimeout")
	ErrInvalidExecutionInput           = errors.New("InvalidExecutionInput")
)

const (
	// maxExecutionInputBytes mirrors the AWS Step Functions hard limit on
	// StartExecution / StartSyncExecution input payload size (256 KiB).
	maxExecutionInputBytes    = 256 * 1024
	executionStartedEventID   = int64(1)
	executionSucceededEventID = int64(2)
	maxHistoryEvents          = 25000
	maxPendingActivityTasks   = 1000
	activityPollTimeout       = 60 * time.Second
	activityTokenBytes        = 32

	// maxExecutionNameLen is the AWS limit on execution name length.
	maxExecutionNameLen = 80
	// maxStateMachineNameLen is the AWS limit on state machine name length.
	maxStateMachineNameLen = 80
	// maxActivityNameLen is the AWS limit on activity name length.
	maxActivityNameLen = 80

	// executionPruneSweepThreshold is the number of stored executions above which
	// StartExecution opportunistically prunes finished executions that have aged
	// past the retention period. Keeps the execution map bounded even when the
	// background janitor is disabled.
	executionPruneSweepThreshold = 500

	statusRunning   = "RUNNING"
	statusSucceeded = "SUCCEEDED"
	statusFailed    = "FAILED"
	statusAborted   = "ABORTED"
	statusActive    = "ACTIVE"
	statusDeleting  = "DELETING"
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

// regionFromARN extracts the region component from an ARN string.
// ARN format: arn:{partition}:{service}:{region}:{account}:{resource}.
func regionFromARN(arnStr, fallback string) string {
	const arnRegionIdx = 3

	parts := strings.Split(arnStr, ":")
	if len(parts) > arnRegionIdx && parts[arnRegionIdx] != "" {
		return parts[arnRegionIdx]
	}

	return fallback
}

// StorageBackend is the interface for a Step Functions in-memory store.
type StorageBackend interface {
	CreateStateMachine(ctx context.Context, name, definition, roleArn, smType string) (*StateMachine, error)
	DeleteStateMachine(arn string) error
	ListStateMachines(ctx context.Context, nextToken string, maxResults int) ([]StateMachine, string, error)
	DescribeStateMachine(arn string) (*StateMachine, error)
	UpdateStateMachine(arn, definition, roleArn string) (float64, error)
	PublishStateMachineVersion(smARN, description, revisionID string) (*StateMachineVersion, error)
	DescribeStateMachineVersion(versionARN string) (*StateMachineVersion, error)
	DeleteStateMachineVersion(versionARN string) error
	ListStateMachineVersions(smARN, nextToken string, maxResults int) ([]StateMachineVersion, string, error)
	CreateStateMachineAlias(smARN, name, description string, routing []AliasRoutingConfig) (*StateMachineAlias, error)
	UpdateStateMachineAlias(aliasARN, description string, routing []AliasRoutingConfig) (*StateMachineAlias, error)
	DeleteStateMachineAlias(aliasARN string) error
	DescribeStateMachineAlias(aliasARN string) (*StateMachineAlias, error)
	ListStateMachineAliases(smARN, nextToken string, maxResults int) ([]StateMachineAlias, string, error)
	StartExecution(stateMachineArn, name, input string) (*Execution, error)
	StartSyncExecution(stateMachineArn, name, input string) (*SyncExecutionResult, error)
	StopExecution(executionArn, errCode, cause string) error
	RedriveExecution(executionARN string) (*Execution, error)
	DescribeExecution(executionArn string) (*Execution, error)
	DescribeStateMachineForExecution(executionARN string) (*StateMachine, error)
	ListExecutions(stateMachineArn, statusFilter, nextToken string, maxResults int) ([]Execution, string, error)
	GetExecutionHistory(
		executionArn, nextToken string,
		maxResults int,
		reverseOrder bool,
	) ([]HistoryEvent, string, error)
	CreateActivity(ctx context.Context, name string) (*Activity, error)
	DeleteActivity(activityArn string) error
	DescribeActivity(activityArn string) (*Activity, error)
	ListActivities(ctx context.Context, nextToken string, maxResults int) ([]Activity, string, error)
	GetActivityTask(ctx context.Context, activityArn, workerName string) (*ActivityTask, error)
	SendTaskSuccess(taskToken, output string) error
	SendTaskFailure(taskToken, errCode, cause string) error
	SendTaskHeartbeat(taskToken string) error
	SetStateMachineConfigurations(
		arn string,
		tracing *TracingConfiguration,
		logging *LoggingConfiguration,
		encryption *EncryptionConfiguration,
	) error
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
	// nameIndex maps region → name → ARN for O(1) duplicate detection per region.
	nameIndex map[string]map[string]string
	// smExecutions maps state machine ARN → execution ARNs for O(1) scoped listing.
	smExecutions map[string][]string
	// cancelFns holds the cancel function for each running execution goroutine.
	cancelFns map[string]context.CancelFunc
	// deletedExecs is a tombstone set for executions removed by DeleteStateMachine.
	// historyRecorder and runParsedExecution skip writes for tombstoned ARNs.
	deletedExecs      map[string]bool
	activities        map[string]*Activity
	activityNameIndex map[string]map[string]string
	// pendingTaskQueues maps activity ARN → buffered channel of pending tasks.
	pendingTaskQueues map[string]chan *activityTaskEntry
	// tasksByToken maps task token → task entry for SendTaskSuccess/Failure.
	tasksByToken map[string]*activityTaskEntry
	// versions maps version ARN → version for PublishStateMachineVersion.
	versions map[string]*StateMachineVersion
	// smVersions maps state machine ARN → ordered list of version ARNs.
	smVersions map[string][]string
	// aliases maps alias ARN → alias for CreateStateMachineAlias.
	aliases map[string]*StateMachineAlias
	// smAliases maps state machine ARN → list of alias ARNs.
	smAliases map[string][]string
	// executionDefinitions maps execution ARN → the SM definition that was active at start time.
	executionDefinitions map[string]string
	// historyTruncated tracks executions where the history cap has been reached
	// so we only emit a single warning per execution.
	historyTruncated map[string]bool
	// execHistoryMu holds per-execution mutexes so concurrent executions can
	// append history without contending on the global b.mu write lock.
	execHistoryMu sync.Map // execARN → *sync.Mutex
	logger        *slog.Logger
	mu            *lockmetrics.RWMutex
	// svcCtx is the service lifecycle context. Execution goroutines derive their
	// contexts from it so that all active executions are cancelled on server shutdown.
	svcCtx    context.Context
	accountID string
	region    string
	settings  Settings
}

// activityTaskEntry holds a pending activity task and its result channel.
type activityTaskEntry struct {
	// heartbeatTimer is reset on each SendTaskHeartbeat call. Nil if no heartbeat timeout.
	heartbeatTimer *time.Timer
	// heartbeatStop signals the heartbeat monitor to stop (on task completion).
	heartbeatStop chan struct{}
	resultCh      chan activityTaskResult
	activityArn   string
	taskToken     string
	input         string
	// heartbeatDuration is the original duration for resetting the heartbeat timer.
	heartbeatDuration time.Duration
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
		accountID:            accountID,
		region:               region,
		svcCtx:               svcCtx,
		stateMachines:        make(map[string]*StateMachine),
		executions:           make(map[string]*Execution),
		history:              make(map[string][]*HistoryEvent),
		nameIndex:            make(map[string]map[string]string),
		smExecutions:         make(map[string][]string),
		cancelFns:            make(map[string]context.CancelFunc),
		deletedExecs:         make(map[string]bool),
		activities:           make(map[string]*Activity),
		activityNameIndex:    make(map[string]map[string]string),
		pendingTaskQueues:    make(map[string]chan *activityTaskEntry),
		tasksByToken:         make(map[string]*activityTaskEntry),
		versions:             make(map[string]*StateMachineVersion),
		smVersions:           make(map[string][]string),
		aliases:              make(map[string]*StateMachineAlias),
		smAliases:            make(map[string][]string),
		executionDefinitions: make(map[string]string),
		historyTruncated:     make(map[string]bool),
		logger:               slog.Default(),
		mu:                   lockmetrics.New("stepfunctions"),
		settings:             DefaultSettings(),
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

// SetSettings updates the backend settings.
func (b *InMemoryBackend) SetSettings(s Settings) {
	b.mu.Lock("SetSettings")
	defer b.mu.Unlock()
	b.settings = s
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

func (b *InMemoryBackend) smARN(region, name string) string {
	return arn.Build("states", region, b.accountID, "stateMachine:"+name)
}

func (b *InMemoryBackend) execARN(stateMachineARN, smName, execName string) string {
	region := regionFromARN(stateMachineARN, b.region)

	return arn.Build("states", region, b.accountID, "execution:"+smName+":"+execName)
}

func (b *InMemoryBackend) activityARN(region, name string) string {
	return arn.Build("states", region, b.accountID, "activity:"+name)
}

func (b *InMemoryBackend) versionARN(stateMachineARN, smName string, version int) string {
	region := regionFromARN(stateMachineARN, b.region)

	return arn.Build("states", region, b.accountID, fmt.Sprintf("stateMachine:%s:%d", smName, version))
}

func (b *InMemoryBackend) aliasARN(stateMachineARN, smName, aliasName string) string {
	region := regionFromARN(stateMachineARN, b.region)

	return arn.Build("states", region, b.accountID, "stateMachine:"+smName+":"+aliasName)
}

// regionNameIndex lazily initialises and returns the name→ARN map for region.
// Caller must hold b.mu write lock.
func (b *InMemoryBackend) regionNameIndex(region string) map[string]string {
	if b.nameIndex[region] == nil {
		b.nameIndex[region] = make(map[string]string)
	}

	return b.nameIndex[region]
}

// regionActivityIndex lazily initialises and returns the name→ARN map for region.
// Caller must hold b.mu write lock.
func (b *InMemoryBackend) regionActivityIndex(region string) map[string]string {
	if b.activityNameIndex[region] == nil {
		b.activityNameIndex[region] = make(map[string]string)
	}

	return b.activityNameIndex[region]
}

// SetStateMachineConfigurations sets optional tracing, logging, and encryption configuration
// for a state machine. Any nil argument leaves the corresponding field unchanged.
func (b *InMemoryBackend) SetStateMachineConfigurations(
	arn string,
	tracing *TracingConfiguration,
	logging *LoggingConfiguration,
	encryption *EncryptionConfiguration,
) error {
	b.mu.Lock("SetStateMachineConfigurations")
	defer b.mu.Unlock()

	sm, ok := b.stateMachines[arn]
	if !ok || sm == nil {
		return fmt.Errorf("%w: %s", ErrStateMachineDoesNotExist, arn)
	}

	if tracing != nil {
		sm.TracingConfiguration = tracing
	}

	if logging != nil {
		sm.LoggingConfiguration = logging
	}

	if encryption != nil {
		sm.EncryptionConfiguration = encryption
	}

	return nil
}

// CreateStateMachine creates and stores a new state machine in the caller's region.
func (b *InMemoryBackend) CreateStateMachine(
	ctx context.Context,
	name, definition, roleArn, smType string,
) (*StateMachine, error) {
	if smType == "" {
		smType = "STANDARD"
	}

	if err := validateName(name, maxStateMachineNameLen); err != nil {
		return nil, err
	}

	if err := validateRoleARN(roleArn); err != nil {
		return nil, err
	}

	// Validate the definition before storing.
	if _, err := asl.Parse(definition); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidDefinition, err)
	}

	region := getRegionFromContext(ctx, b.region)
	smARN := b.smARN(region, name)

	b.mu.Lock("CreateStateMachine")
	defer b.mu.Unlock()

	nameIdx := b.regionNameIndex(region)
	if existingARN, exists := nameIdx[name]; exists {
		if sm := b.stateMachines[existingARN]; sm != nil && sm.Status != statusDeleting {
			// AWS idempotency: same name+definition+type+roleArn → return existing without error.
			if sm.Definition == definition && sm.Type == smType && sm.RoleArn == roleArn {
				cp := *sm

				return &cp, nil
			}

			return nil, fmt.Errorf("%w: %s", ErrStateMachineAlreadyExists, name)
		}
	}

	sm := &StateMachine{
		CreationDate:    float64(time.Now().Unix()),
		Name:            name,
		StateMachineArn: smARN,
		Type:            smType,
		Status:          statusActive,
		Definition:      definition,
		RoleArn:         roleArn,
	}
	b.stateMachines[smARN] = sm
	nameIdx[name] = smARN

	return sm, nil
}

// PruneExecutions removes executions and history older than the retention period.
func (b *InMemoryBackend) PruneExecutions(_ context.Context) int {
	retention := b.settings.ExecutionRetention
	if retention == 0 {
		retention = defaultExecutionRetention
	}

	cutoff := float64(time.Now().Add(-retention).Unix())

	b.mu.Lock("PruneExecutions")
	defer b.mu.Unlock()

	return b.pruneExecutionsLocked(cutoff)
}

// pruneExecutionsLocked removes finished executions older than cutoff (Unix seconds).
// Must be called with b.mu held for writing.
func (b *InMemoryBackend) pruneExecutionsLocked(cutoff float64) int {
	var toDelete []string
	for arn, exec := range b.executions {
		if exec.Status != statusRunning && exec.StopDate != nil && *exec.StopDate < cutoff {
			toDelete = append(toDelete, arn)
		}
	}

	for _, arn := range toDelete {
		delete(b.executions, arn)
		delete(b.history, arn)
		delete(b.executionDefinitions, arn)
		delete(b.historyTruncated, arn)
		b.execHistoryMu.Delete(arn)

		for smARN, execs := range b.smExecutions {
			for i, e := range execs {
				if e == arn {
					b.smExecutions[smARN] = append(execs[:i], execs[i+1:]...)

					break
				}
			}
		}
	}

	return len(toDelete)
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

	smRegion := regionFromARN(arn, b.region)
	delete(b.nameIndex[smRegion], sm.Name)

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
		delete(b.executionDefinitions, execARN)
		delete(b.historyTruncated, execARN)
		b.execHistoryMu.Delete(execARN)
	}

	delete(b.smExecutions, arn)

	// Remove all versions for this state machine.
	for _, vARN := range b.smVersions[arn] {
		delete(b.versions, vARN)
	}
	delete(b.smVersions, arn)

	// Remove all aliases for this state machine.
	for _, aARN := range b.smAliases[arn] {
		delete(b.aliases, aARN)
	}
	delete(b.smAliases, arn)

	return nil
}

// ListStateMachines returns state machines in the caller's region with optional pagination.
func (b *InMemoryBackend) ListStateMachines(
	ctx context.Context,
	nextToken string,
	maxResults int,
) ([]StateMachine, string, error) {
	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("ListStateMachines")
	defer b.mu.RUnlock()

	all := make([]StateMachine, 0, len(b.stateMachines))
	for _, sm := range b.stateMachines {
		if regionFromARN(sm.StateMachineArn, b.region) != region {
			continue
		}

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

	if roleArn != "" {
		if err := validateRoleARN(roleArn); err != nil {
			return 0, err
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
	if len(input) > maxExecutionInputBytes {
		return nil, fmt.Errorf("%w: input exceeds %d bytes", ErrInvalidExecutionInput, maxExecutionInputBytes)
	}

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

	const millisPerSecond = 1000.0
	startDate := float64(time.Now().UnixMilli()) / millisPerSecond
	execARN := b.execARN(stateMachineArn, smName, name)

	// Express Workflows must complete within 5 minutes per AWS spec.
	const expressSyncTimeout = 5 * time.Minute

	syncCtx, syncCancel := context.WithTimeout(b.svcCtx, expressSyncTimeout)
	defer syncCancel()

	// Run synchronously with nil history recorder (sync executions are ephemeral).
	executor := asl.NewExecutor(parsedSM, lambdaInvoker, nil)
	executor.SetSQSIntegration(sqsIntegration)
	executor.SetSNSIntegration(snsIntegration)
	executor.SetDynamoDBIntegration(ddbIntegration)
	executor.SetActivityInvoker(b)
	executor.SetTaskTokenCallbackInvoker(b)
	executor.SetExecutionContext(
		execARN,
		name,
		sm.RoleArn,
		time.Unix(int64(startDate), 0).UTC().Format(time.RFC3339),
		stateMachineArn,
		sm.Name,
	)

	result, execErr := executor.Execute(syncCtx, execARN, input)

	return finalizeSyncExecutionResult(execARN, stateMachineArn, name, input, startDate, result, execErr), nil
}

// finalizeSyncExecutionResult assembles the SyncExecutionResult based on the
// outcome of the synchronous executor invocation. Extracted to keep
// StartSyncExecution under the funlen threshold.
func finalizeSyncExecutionResult(
	execARN, stateMachineArn, name, input string,
	startDate float64,
	result *asl.ExecutionResult,
	execErr error,
) *SyncExecutionResult {
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
		if errors.Is(execErr, context.DeadlineExceeded) {
			syncResult.Status = "TIMED_OUT"
			syncResult.Error = "States.Timeout"
			syncResult.Cause = "Express Workflow exceeded the 5-minute maximum execution time"
		} else {
			syncResult.Status = statusFailed
			syncResult.Error = execErr.Error()
		}

		return syncResult
	}

	if result.Error != "" {
		syncResult.Status = statusFailed
		syncResult.Error = result.Error
		syncResult.Cause = result.Cause

		return syncResult
	}

	outputBytes, _ := json.Marshal(result.Output)
	syncResult.Status = statusSucceeded
	syncResult.Output = string(outputBytes)

	return syncResult
}

// StartExecution creates an execution and runs the ASL interpreter asynchronously.
func (b *InMemoryBackend) StartExecution(stateMachineArn, name, input string) (*Execution, error) {
	if len(input) > maxExecutionInputBytes {
		return nil, fmt.Errorf("%w: input exceeds %d bytes", ErrInvalidExecutionInput, maxExecutionInputBytes)
	}

	if name != "" {
		if err := validateName(name, maxExecutionNameLen); err != nil {
			return nil, err
		}
	}

	b.mu.Lock("StartExecution")

	// Opportunistically prune finished executions that have aged past the retention
	// period so the executions/history maps stay bounded when the janitor is off.
	if len(b.executions) >= executionPruneSweepThreshold {
		retention := b.settings.ExecutionRetention
		if retention == 0 {
			retention = defaultExecutionRetention
		}

		b.pruneExecutionsLocked(float64(time.Now().Add(-retention).Unix()))
	}

	sm, exists := b.stateMachines[stateMachineArn]
	if !exists {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: %s", ErrStateMachineDoesNotExist, stateMachineArn)
	}

	if sm.Type == "EXPRESS" {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: async execution requires STANDARD state machine", ErrInvalidExecutionType)
	}

	execArn := b.execARN(stateMachineArn, sm.Name, name)
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

	const millisPerSecond = 1000.0
	now := float64(time.Now().UnixMilli()) / millisPerSecond
	exec := &Execution{
		StartDate:       now,
		ExecutionArn:    execArn,
		StateMachineArn: stateMachineArn,
		Name:            name,
		Status:          statusRunning,
		Input:           input,
	}
	b.executions[execArn] = exec

	// Snapshot the definition at execution start time for DescribeStateMachineForExecution.
	b.executionDefinitions[execArn] = definition

	b.history[execArn] = []*HistoryEvent{
		{Timestamp: now, Type: "ExecutionStarted", ID: executionStartedEventID, PreviousEventID: 0},
	}
	b.execHistoryMu.Store(execArn, &sync.Mutex{})

	lambdaInvoker := b.lambdaInvoker
	sqsIntegration := b.sqsIntegration
	snsIntegration := b.snsIntegration
	ddbIntegration := b.ddbIntegration

	// Register the execution in the SM→executions index and store a cancel fn
	// so StopExecution and DeleteStateMachine can cancel the goroutine.
	// The context is derived from b.svcCtx so that all active executions are
	// also cancelled when the server shuts down.

	//nolint:gosec // cancel is stored in b.cancelFns for StopExecution/DeleteStateMachine
	ctx, cancel := context.WithCancel(b.svcCtx)
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

// applyExecutorContext populates the ASL executor's `$$` context object with
// data derived from the persisted execution and state machine records.
func (b *InMemoryBackend) applyExecutorContext(executor *asl.Executor, execARN string) {
	b.mu.RLock("applyExecutorContext")
	defer b.mu.RUnlock()

	exec, ok := b.executions[execARN]
	if !ok {
		return
	}

	sm, smOK := b.stateMachines[exec.StateMachineArn]
	if !smOK || sm == nil {
		executor.SetExecutionContext(
			exec.ExecutionArn,
			exec.Name,
			"",
			time.Unix(int64(exec.StartDate), 0).UTC().Format(time.RFC3339),
			exec.StateMachineArn,
			"",
		)

		return
	}

	executor.SetExecutionContext(
		exec.ExecutionArn,
		exec.Name,
		sm.RoleArn,
		time.Unix(int64(exec.StartDate), 0).UTC().Format(time.RFC3339),
		exec.StateMachineArn,
		sm.Name,
	)
}

func validateRoleARN(roleArn string) error {
	const arnParts = 6

	if roleArn == "" {
		return nil
	}

	if !strings.HasPrefix(roleArn, "arn:") {
		return fmt.Errorf("%w: roleArn must be an ARN", ErrInvalidRoleArn)
	}

	if strings.ContainsAny(roleArn, " \t\r\n") {
		return fmt.Errorf("%w: roleArn must not contain whitespace", ErrInvalidRoleArn)
	}

	parts := strings.Split(roleArn, ":")
	if len(parts) == arnParts {
		if parts[2] == "" || parts[5] == "" {
			return fmt.Errorf("%w: roleArn must include service and resource", ErrInvalidRoleArn)
		}
	}

	return nil
}

// namePattern is the AWS-allowed character set for state machine, execution, and activity names.
// AWS allows: letters, digits, and [-+/=_.@ ].
var namePattern = regexp.MustCompile(`^[-a-zA-Z0-9+/=_.@ ]+$`)

// validateName checks that a resource name meets AWS length and character constraints.
func validateName(name string, maxLen int) error {
	if name == "" || len(name) > maxLen {
		return fmt.Errorf("%w: name must be 1-%d characters", ErrInvalidName, maxLen)
	}

	if !namePattern.MatchString(name) {
		return fmt.Errorf(
			"%w: name must contain only letters, digits, and [-+/=_.@ ]",
			ErrInvalidName,
		)
	}

	return nil
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

// execHistoryLock returns the per-execution mutex for history writes, creating it if absent.
// Callers must NOT hold b.mu when calling this.
func (b *InMemoryBackend) execHistoryLock(execARN string) *sync.Mutex {
	v, _ := b.execHistoryMu.LoadOrStore(execARN, &sync.Mutex{})
	mu, _ := v.(*sync.Mutex)

	return mu
}

// appendHistory appends event to the per-execution history using a per-execution
// mutex so concurrent executions do not serialize on the global b.mu write lock.
func (b *InMemoryBackend) appendHistory(execARN string, event *HistoryEvent) {
	mu := b.execHistoryLock(execARN)
	mu.Lock()
	defer mu.Unlock()

	// Recheck tombstone under the global read lock so we don't write history
	// for an execution that DeleteStateMachine just removed.
	b.mu.RLock("appendHistory")
	defer b.mu.RUnlock()

	if b.deletedExecs[execARN] {
		return
	}

	events, ok := b.checkHistoryCapacity(execARN)
	if !ok {
		return
	}

	nextID := int64(len(events) + 1)
	event.ID = nextID
	event.PreviousEventID = nextID - 1
	b.history[execARN] = append(events, event)
}

func (r *historyRecorder) RecordStateEntered(execARN, stateName, stateType string, _ any) {
	r.backend.appendHistory(execARN, &HistoryEvent{
		Timestamp:                float64(time.Now().Unix()),
		Type:                     stateEnteredEventType(stateType),
		StateEnteredEventDetails: &StateEnteredEventDetails{Name: stateName},
	})
}

func (r *historyRecorder) RecordStateExited(execARN, stateName, stateType string, _ any) {
	r.backend.appendHistory(execARN, &HistoryEvent{
		Timestamp:               float64(time.Now().Unix()),
		Type:                    stateExitedEventType(stateType),
		StateExitedEventDetails: &StateExitedEventDetails{Name: stateName},
	})
}

func (r *historyRecorder) RecordTaskScheduled(execARN, _ /* stateName */, _ /* resource */ string) {
	r.backend.appendHistory(execARN, &HistoryEvent{
		Timestamp: float64(time.Now().Unix()),
		Type:      "TaskScheduled",
	})
}

func (r *historyRecorder) RecordTaskSucceeded(execARN, _ /* stateName */ string, _ any) {
	r.backend.appendHistory(execARN, &HistoryEvent{
		Timestamp: float64(time.Now().Unix()),
		Type:      "TaskSucceeded",
	})
}

func (r *historyRecorder) RecordTaskFailed(execARN, _ /* stateName */, _ /* errCode */, _ /* cause */ string) {
	r.backend.appendHistory(execARN, &HistoryEvent{
		Timestamp: float64(time.Now().Unix()),
		Type:      "TaskFailed",
	})
}

// checkHistoryCapacity returns the current event slice and whether there is
// room to append. On the first refusal per execution it logs a warning so that
// silent truncation is observable. Caller must hold b.mu read or write lock.
func (b *InMemoryBackend) checkHistoryCapacity(execARN string) ([]*HistoryEvent, bool) {
	events := b.history[execARN]
	if len(events) < maxHistoryEvents {
		return events, true
	}

	if !b.historyTruncated[execARN] {
		b.historyTruncated[execARN] = true
		b.logger.Warn(
			"stepfunctions: execution history truncated at maxHistoryEvents",
			"executionArn", execARN,
			"maxHistoryEvents", maxHistoryEvents,
		)
	}

	return events, false
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
	executor.SetTaskTokenCallbackInvoker(b)
	b.applyExecutorContext(executor, execARN)
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

// StopExecution marks a RUNNING execution as ABORTED.
// AWS behaviour: idempotent on already-terminal executions — returns success without mutation.
func (b *InMemoryBackend) StopExecution(executionArn, errCode, cause string) error {
	b.mu.Lock("StopExecution")
	defer b.mu.Unlock()

	exec, exists := b.executions[executionArn]
	if !exists {
		return fmt.Errorf("%w: %s", ErrExecutionDoesNotExist, executionArn)
	}

	// Already in a terminal state — no-op per AWS semantics.
	if exec.Status != statusRunning {
		return nil
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

	sort.Slice(all, func(i, j int) bool { return all[i].StartDate > all[j].StartDate })

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
	b.nameIndex = make(map[string]map[string]string)
	b.smExecutions = make(map[string][]string)
	b.cancelFns = make(map[string]context.CancelFunc)
	b.deletedExecs = newDeleted
	b.activities = make(map[string]*Activity)
	b.activityNameIndex = make(map[string]map[string]string)
	b.pendingTaskQueues = make(map[string]chan *activityTaskEntry)
	b.tasksByToken = make(map[string]*activityTaskEntry)
	b.versions = make(map[string]*StateMachineVersion)
	b.smVersions = make(map[string][]string)
	b.aliases = make(map[string]*StateMachineAlias)
	b.smAliases = make(map[string][]string)
	b.executionDefinitions = make(map[string]string)
	b.historyTruncated = make(map[string]bool)
	b.execHistoryMu = sync.Map{}

	b.mu.Unlock()
}

// CreateActivity creates a new activity resource in the caller's region.
func (b *InMemoryBackend) CreateActivity(ctx context.Context, name string) (*Activity, error) {
	if err := validateName(name, maxActivityNameLen); err != nil {
		return nil, err
	}

	region := getRegionFromContext(ctx, b.region)
	actARN := b.activityARN(region, name)

	b.mu.Lock("CreateActivity")
	defer b.mu.Unlock()

	actIdx := b.regionActivityIndex(region)
	if _, exists := actIdx[name]; exists {
		return nil, fmt.Errorf("%w: %s", ErrActivityAlreadyExists, name)
	}

	a := &Activity{
		Name:         name,
		ActivityArn:  actARN,
		CreationDate: float64(time.Now().Unix()),
	}
	b.activities[actARN] = a
	actIdx[name] = actARN
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

	actRegion := regionFromARN(activityArn, b.region)
	delete(b.activityNameIndex[actRegion], a.Name)

	if queue, hasQueue := b.pendingTaskQueues[activityArn]; hasQueue {
		close(queue)
		delete(b.pendingTaskQueues, activityArn)
	}

	taskTokens := make([]string, 0, len(b.tasksByToken))
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

// ListActivities returns activities in the caller's region with optional pagination.
func (b *InMemoryBackend) ListActivities(
	ctx context.Context,
	nextToken string,
	maxResults int,
) ([]Activity, string, error) {
	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("ListActivities")
	defer b.mu.RUnlock()

	all := make([]Activity, 0, len(b.activities))
	for _, a := range b.activities {
		if regionFromARN(a.ActivityArn, b.region) != region {
			continue
		}

		all = append(all, *a)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	acts, token := paginate(all, nextToken, maxResults)

	return acts, token, nil
}

// PublishStateMachineVersion creates an immutable snapshot version of a state machine.
func (b *InMemoryBackend) PublishStateMachineVersion(
	smARN, description, revisionID string,
) (*StateMachineVersion, error) {
	b.mu.Lock("PublishStateMachineVersion")
	defer b.mu.Unlock()

	sm, exists := b.stateMachines[smARN]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrStateMachineDoesNotExist, smARN)
	}

	versionNum := len(b.smVersions[smARN]) + 1
	vARN := b.versionARN(smARN, sm.Name, versionNum)

	v := &StateMachineVersion{
		StateMachineVersionArn: vARN,
		StateMachineArn:        smARN,
		Name:                   sm.Name,
		Definition:             sm.Definition,
		RoleArn:                sm.RoleArn,
		Type:                   sm.Type,
		Status:                 statusActive,
		Description:            description,
		RevisionID:             revisionID,
		CreationDate:           float64(time.Now().Unix()),
	}

	b.versions[vARN] = v
	b.smVersions[smARN] = append(b.smVersions[smARN], vARN)

	cp := *v

	return &cp, nil
}

// DescribeStateMachineVersion returns details for a specific version.
func (b *InMemoryBackend) DescribeStateMachineVersion(versionARN string) (*StateMachineVersion, error) {
	b.mu.RLock("DescribeStateMachineVersion")
	defer b.mu.RUnlock()

	v, exists := b.versions[versionARN]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrStateMachineVersionDoesNotExist, versionARN)
	}

	cp := *v

	return &cp, nil
}

// DeleteStateMachineVersion removes a specific version.
func (b *InMemoryBackend) DeleteStateMachineVersion(versionARN string) error {
	b.mu.Lock("DeleteStateMachineVersion")
	defer b.mu.Unlock()

	v, exists := b.versions[versionARN]
	if !exists {
		return fmt.Errorf("%w: %s", ErrStateMachineVersionDoesNotExist, versionARN)
	}

	delete(b.versions, versionARN)

	// Remove from the SM's version list.
	smARN := v.StateMachineArn
	versions := b.smVersions[smARN]
	updated := make([]string, 0, len(versions))
	for _, vv := range versions {
		if vv != versionARN {
			updated = append(updated, vv)
		}
	}
	b.smVersions[smARN] = updated

	return nil
}

// ListStateMachineVersions returns all versions for a state machine.
func (b *InMemoryBackend) ListStateMachineVersions(
	smARN, nextToken string, maxResults int,
) ([]StateMachineVersion, string, error) {
	b.mu.RLock("ListStateMachineVersions")
	defer b.mu.RUnlock()

	if _, exists := b.stateMachines[smARN]; !exists {
		return nil, "", fmt.Errorf("%w: %s", ErrStateMachineDoesNotExist, smARN)
	}

	vARNs := b.smVersions[smARN]
	all := make([]StateMachineVersion, 0, len(vARNs))
	for _, vARN := range vARNs {
		if v := b.versions[vARN]; v != nil {
			all = append(all, *v)
		}
	}

	// Return newest first.
	sort.Slice(all, func(i, j int) bool { return all[i].CreationDate > all[j].CreationDate })

	versions, token := paginate(all, nextToken, maxResults)

	return versions, token, nil
}

// validateRoutingConfig enforces AWS alias routing constraints:
// 1-2 entries, each weight 0-100, total weight = 100.
func validateRoutingConfig(routing []AliasRoutingConfig) error {
	if len(routing) == 0 || len(routing) > 2 {
		return fmt.Errorf("%w: routing configuration must have 1 or 2 entries", ErrInvalidRoutingConfiguration)
	}

	total := 0

	for _, r := range routing {
		if r.Weight < 0 || r.Weight > 100 {
			return fmt.Errorf("%w: each routing weight must be between 0 and 100", ErrInvalidRoutingConfiguration)
		}

		total += r.Weight
	}

	const totalWeight = 100
	if total != totalWeight {
		return fmt.Errorf("%w: routing weights must sum to 100, got %d", ErrInvalidRoutingConfiguration, total)
	}

	return nil
}

// CreateStateMachineAlias creates a named routing alias for one or more state machine versions.
func (b *InMemoryBackend) CreateStateMachineAlias(
	smARN, name, description string,
	routing []AliasRoutingConfig,
) (*StateMachineAlias, error) {
	if err := validateRoutingConfig(routing); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateStateMachineAlias")
	defer b.mu.Unlock()

	sm, exists := b.stateMachines[smARN]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrStateMachineDoesNotExist, smARN)
	}

	aARN := b.aliasARN(smARN, sm.Name, name)

	if _, already := b.aliases[aARN]; already {
		return nil, fmt.Errorf("%w: %s", ErrStateMachineAliasAlreadyExists, name)
	}

	now := float64(time.Now().Unix())
	alias := &StateMachineAlias{
		StateMachineAliasArn: aARN,
		Name:                 name,
		Description:          description,
		RoutingConfiguration: routing,
		CreationDate:         now,
	}

	b.aliases[aARN] = alias
	b.smAliases[smARN] = append(b.smAliases[smARN], aARN)

	cp := *alias

	return &cp, nil
}

// UpdateStateMachineAlias updates an alias's description and/or routing configuration.
func (b *InMemoryBackend) UpdateStateMachineAlias(
	aliasARN, description string,
	routing []AliasRoutingConfig,
) (*StateMachineAlias, error) {
	b.mu.Lock("UpdateStateMachineAlias")
	defer b.mu.Unlock()

	alias, exists := b.aliases[aliasARN]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrStateMachineAliasDoesNotExist, aliasARN)
	}

	if description != "" {
		alias.Description = description
	}

	if len(routing) > 0 {
		if err := validateRoutingConfig(routing); err != nil {
			return nil, err
		}

		alias.RoutingConfiguration = routing
	}

	alias.UpdatedDate = float64(time.Now().Unix())

	cp := *alias

	return &cp, nil
}

// DeleteStateMachineAlias removes a state machine alias.
func (b *InMemoryBackend) DeleteStateMachineAlias(aliasARN string) error {
	b.mu.Lock("DeleteStateMachineAlias")
	defer b.mu.Unlock()

	alias, exists := b.aliases[aliasARN]
	if !exists {
		return fmt.Errorf("%w: %s", ErrStateMachineAliasDoesNotExist, aliasARN)
	}

	// Remove from the SM's alias list — find parent SM ARN via alias's routing or by scanning smAliases.
	for smARN, aARNs := range b.smAliases {
		updated := make([]string, 0, len(aARNs))
		for _, aARN := range aARNs {
			if aARN != aliasARN {
				updated = append(updated, aARN)
			}
		}
		if len(updated) != len(aARNs) {
			b.smAliases[smARN] = updated
		}
	}

	_ = alias
	delete(b.aliases, aliasARN)

	return nil
}

// DescribeStateMachineAlias returns details for a state machine alias.
func (b *InMemoryBackend) DescribeStateMachineAlias(aliasARN string) (*StateMachineAlias, error) {
	b.mu.RLock("DescribeStateMachineAlias")
	defer b.mu.RUnlock()

	alias, exists := b.aliases[aliasARN]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrStateMachineAliasDoesNotExist, aliasARN)
	}

	cp := *alias

	return &cp, nil
}

// ListStateMachineAliases returns all aliases for a state machine.
func (b *InMemoryBackend) ListStateMachineAliases(
	smARN, nextToken string, maxResults int,
) ([]StateMachineAlias, string, error) {
	b.mu.RLock("ListStateMachineAliases")
	defer b.mu.RUnlock()

	if _, exists := b.stateMachines[smARN]; !exists {
		return nil, "", fmt.Errorf("%w: %s", ErrStateMachineDoesNotExist, smARN)
	}

	aARNs := b.smAliases[smARN]
	all := make([]StateMachineAlias, 0, len(aARNs))
	for _, aARN := range aARNs {
		if a := b.aliases[aARN]; a != nil {
			all = append(all, *a)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	aliases, token := paginate(all, nextToken, maxResults)

	return aliases, token, nil
}

// RedriveExecution re-runs a FAILED or ABORTED execution starting from its last known state.
// AWS Step Functions re-runs from the last state that was reached before failure.
// In this implementation we restart the entire execution with the original input (AWS parity for STANDARD executions).
func (b *InMemoryBackend) RedriveExecution(executionARN string) (*Execution, error) {
	b.mu.Lock("RedriveExecution")

	exec, exists := b.executions[executionARN]
	if !exists {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: %s", ErrExecutionDoesNotExist, executionARN)
	}

	if exec.Status != statusFailed && exec.Status != statusAborted {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: execution %s is in status %s; only FAILED or ABORTED executions can be redriven",
			ErrExecutionNotRedrivable, executionARN, exec.Status)
	}

	smARN := exec.StateMachineArn
	originalInput := exec.Input

	sm, smExists := b.stateMachines[smARN]
	if !smExists {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: state machine %s no longer exists", ErrStateMachineDoesNotExist, smARN)
	}

	definition := sm.Definition
	smName := sm.Name
	parsedSM, parseErr := asl.Parse(definition)
	if parseErr != nil {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: %w", ErrInvalidDefinition, parseErr)
	}

	// Reset the execution to RUNNING.
	now := float64(time.Now().Unix())
	exec.Status = statusRunning
	exec.Output = ""
	exec.Error = ""
	exec.Cause = ""
	exec.StopDate = nil
	exec.StartDate = now
	exec.RedriveCount++
	exec.RedriveDate = &now

	// Reset history.
	b.history[executionARN] = []*HistoryEvent{
		{Timestamp: now, Type: "ExecutionStarted", ID: executionStartedEventID, PreviousEventID: 0},
	}

	// Snapshot the (possibly-updated) definition.
	b.executionDefinitions[executionARN] = definition

	lambdaInvoker := b.lambdaInvoker
	sqsIntegration := b.sqsIntegration
	snsIntegration := b.snsIntegration
	ddbIntegration := b.ddbIntegration

	//nolint:gosec // cancel is stored in b.cancelFns for StopExecution/DeleteStateMachine
	ctx, cancel := context.WithCancel(b.svcCtx)
	b.cancelFns[executionARN] = cancel

	// Ensure execution is tracked under the SM.
	if !slices.Contains(b.smExecutions[smARN], executionARN) {
		b.smExecutions[smARN] = append(b.smExecutions[smARN], executionARN)
	}

	var activityInvoker asl.ActivityInvoker = b

	_ = smName
	b.mu.Unlock()

	go b.runParsedExecution(
		ctx, executionARN, parsedSM, originalInput,
		lambdaInvoker, sqsIntegration, snsIntegration, ddbIntegration, activityInvoker,
	)

	b.mu.RLock("RedriveExecution.result")
	cp := *b.executions[executionARN]
	b.mu.RUnlock()

	return &cp, nil
}

// DescribeStateMachineForExecution returns the state machine definition that was active
// when the given execution was started.
func (b *InMemoryBackend) DescribeStateMachineForExecution(executionARN string) (*StateMachine, error) {
	b.mu.RLock("DescribeStateMachineForExecution")
	defer b.mu.RUnlock()

	exec, exists := b.executions[executionARN]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrExecutionDoesNotExist, executionARN)
	}

	definition, hasSnapshot := b.executionDefinitions[executionARN]
	if !hasSnapshot {
		// Fall back to the current definition if no snapshot was taken (pre-snapshot executions).
		sm, smExists := b.stateMachines[exec.StateMachineArn]
		if !smExists {
			return nil, fmt.Errorf(
				"%w: state machine %s no longer exists", ErrStateMachineDoesNotExist, exec.StateMachineArn,
			)
		}

		cp := *sm

		return &cp, nil
	}

	sm, smExists := b.stateMachines[exec.StateMachineArn]
	if !smExists {
		// SM was deleted but execution still exists — return a synthetic SM with the snapshot.
		return &StateMachine{
			StateMachineArn: exec.StateMachineArn,
			Definition:      definition,
		}, nil
	}

	cp := *sm
	cp.Definition = definition

	return &cp, nil
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
func (b *InMemoryBackend) SendTaskHeartbeat(taskToken string) error {
	b.mu.RLock("SendTaskHeartbeat")
	entry, ok := b.tasksByToken[taskToken]
	b.mu.RUnlock()

	if !ok {
		return fmt.Errorf("%w: %s", ErrTaskTokenNotFound, taskToken)
	}

	if entry.heartbeatTimer != nil && entry.heartbeatDuration > 0 {
		if !entry.heartbeatTimer.Stop() {
			select {
			case <-entry.heartbeatTimer.C:
			default:
			}
		}

		entry.heartbeatTimer.Reset(entry.heartbeatDuration)
	}

	return nil
}

// WaitForTaskToken registers a callback token and blocks until terminal callback.
// It returns ErrTaskTokenAlreadyExists when token already exists, ErrHeartbeatTimeout
// when heartbeatSeconds elapses without heartbeat/success/failure, or ctx.Err() on cancellation.
func (b *InMemoryBackend) WaitForTaskToken(
	ctx context.Context,
	taskToken string,
	heartbeatSeconds int,
) (string, error) {
	entry := &activityTaskEntry{
		taskToken: taskToken,
		resultCh:  make(chan activityTaskResult, 1),
	}

	if heartbeatSeconds > 0 {
		entry.heartbeatDuration = time.Duration(heartbeatSeconds) * time.Second
		entry.heartbeatStop = make(chan struct{}, 1)
		entry.heartbeatTimer = time.NewTimer(entry.heartbeatDuration)
	}

	b.mu.Lock("WaitForTaskToken")
	if _, exists := b.tasksByToken[taskToken]; exists {
		b.mu.Unlock()

		if entry.heartbeatTimer != nil {
			entry.heartbeatTimer.Stop()
		}

		return "", fmt.Errorf("%w: %s", ErrTaskTokenAlreadyExists, taskToken)
	}

	b.tasksByToken[taskToken] = entry
	b.mu.Unlock()

	var heartbeatCh <-chan time.Time
	if entry.heartbeatTimer != nil {
		heartbeatCh = entry.heartbeatTimer.C
	}

	select {
	case result := <-entry.resultCh:
		if entry.heartbeatTimer != nil {
			entry.heartbeatTimer.Stop()
		}

		if result.succeeded {
			return result.output, nil
		}

		return "", fmt.Errorf("%w: %s", ErrActivityTaskFailed, result.errCode)
	case <-heartbeatCh:
		b.mu.Lock("WaitForTaskToken.heartbeat.timeout")
		delete(b.tasksByToken, taskToken)
		b.mu.Unlock()

		return "", ErrHeartbeatTimeout
	case <-ctx.Done():
		b.mu.Lock("WaitForTaskToken.wait.cancel")
		delete(b.tasksByToken, taskToken)
		b.mu.Unlock()

		if entry.heartbeatTimer != nil {
			entry.heartbeatTimer.Stop()
		}

		return "", ctx.Err()
	}
}

// InvokeActivity implements asl.ActivityInvoker.
// It enqueues a task for the activity and blocks until a worker calls
// SendTaskSuccess or SendTaskFailure, or the context is cancelled.
// If heartbeatSeconds > 0, the task fails with ErrHeartbeatTimeout if no
// SendTaskHeartbeat call arrives within the interval.
func (b *InMemoryBackend) InvokeActivity(
	ctx context.Context,
	activityArn, inputJSON string,
	heartbeatSeconds int,
) (string, error) {
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

	if heartbeatSeconds > 0 {
		entry.heartbeatDuration = time.Duration(heartbeatSeconds) * time.Second
		entry.heartbeatStop = make(chan struct{}, 1)
		entry.heartbeatTimer = time.NewTimer(entry.heartbeatDuration)
	}

	b.mu.Lock("InvokeActivity")
	queue, ok := b.pendingTaskQueues[activityArn]

	if !ok {
		b.mu.Unlock()

		if entry.heartbeatTimer != nil {
			entry.heartbeatTimer.Stop()
		}

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

		if entry.heartbeatTimer != nil {
			entry.heartbeatTimer.Stop()
		}

		return "", ctx.Err()
	}

	// Resolve heartbeat channel (nil if no timeout configured).
	var heartbeatCh <-chan time.Time
	if entry.heartbeatTimer != nil {
		heartbeatCh = entry.heartbeatTimer.C
	}

	// Wait for the worker to complete the task.
	select {
	case result := <-entry.resultCh:
		if entry.heartbeatTimer != nil {
			entry.heartbeatTimer.Stop()
		}

		if result.succeeded {
			return result.output, nil
		}

		return "", fmt.Errorf("%w: %s", ErrActivityTaskFailed, result.errCode)
	case <-heartbeatCh:
		b.mu.Lock("InvokeActivity.heartbeat.timeout")
		delete(b.tasksByToken, taskToken)
		b.mu.Unlock()

		return "", ErrHeartbeatTimeout
	case <-ctx.Done():
		b.mu.Lock("InvokeActivity.wait.cancel")
		delete(b.tasksByToken, taskToken)
		b.mu.Unlock()

		if entry.heartbeatTimer != nil {
			entry.heartbeatTimer.Stop()
		}

		return "", ctx.Err()
	}
}
