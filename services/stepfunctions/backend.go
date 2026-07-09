package stepfunctions

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
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
	ErrStateMachineTypeNotSupported    = errors.New("StateMachineTypeNotSupported")
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
	ErrValidation                      = errors.New("ValidationException")
	ErrMapRunDoesNotExist              = errors.New("MapRunDoesNotExist")
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
	CreateStateMachine(
		ctx context.Context,
		name, definition, roleArn, smType string,
	) (*StateMachine, error)
	DeleteStateMachine(arn string) error
	ListStateMachines(
		ctx context.Context,
		nextToken string,
		maxResults int,
	) ([]StateMachine, string, error)
	DescribeStateMachine(arn string) (*StateMachine, error)
	UpdateStateMachine(arn, definition, roleArn string) (float64, error)
	PublishStateMachineVersion(smARN, description, revisionID string) (*StateMachineVersion, error)
	DescribeStateMachineVersion(versionARN string) (*StateMachineVersion, error)
	DeleteStateMachineVersion(versionARN string) error
	ListStateMachineVersions(
		smARN, nextToken string,
		maxResults int,
	) ([]StateMachineVersion, string, error)
	CreateStateMachineAlias(
		smARN, name, description string,
		routing []AliasRoutingConfig,
	) (*StateMachineAlias, error)
	UpdateStateMachineAlias(
		aliasARN, description string,
		routing []AliasRoutingConfig,
	) (*StateMachineAlias, error)
	DeleteStateMachineAlias(aliasARN string) error
	DescribeStateMachineAlias(aliasARN string) (*StateMachineAlias, error)
	ListStateMachineAliases(
		smARN, nextToken string,
		maxResults int,
	) ([]StateMachineAlias, string, error)
	StartExecution(stateMachineArn, name, input string) (*Execution, error)
	StartSyncExecution(stateMachineArn, name, input string) (*SyncExecutionResult, error)
	StopExecution(executionArn, errCode, cause string) error
	RedriveExecution(executionARN string) (*Execution, error)
	DescribeExecution(executionArn string) (*Execution, error)
	DescribeStateMachineForExecution(executionARN string) (*StateMachine, error)
	ListExecutions(
		stateMachineArn, statusFilter, nextToken string,
		maxResults int,
	) ([]Execution, string, error)
	GetExecutionHistory(
		executionArn, nextToken string,
		maxResults int,
		reverseOrder bool,
	) ([]HistoryEvent, string, error)
	CreateActivity(ctx context.Context, name string) (*Activity, error)
	DeleteActivity(activityArn string) error
	DescribeActivity(activityArn string) (*Activity, error)
	ListActivities(
		ctx context.Context,
		nextToken string,
		maxResults int,
	) ([]Activity, string, error)
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
	DescribeMapRun(mapRunARN string) (*MapRun, error)
	UpdateMapRun(
		mapRunARN string,
		maxConcurrency int,
		toleratedFailureCount int,
		toleratedFailurePercentage float64,
	) (*MapRun, error)
	ListMapRuns(executionARN, nextToken string, maxResults int) ([]MapRun, string, error)
}

// InMemoryBackend implements StorageBackend using in-memory maps.
//
// Phase 3.3: the resource maps (stateMachines, executions, activities,
// versions, aliases, mapRuns) are *store.Table[T] registered once on
// registry -- see store_setup.go. Execution history moved from a separate
// map onto Execution.history (inline, unexported); see the doc comment on
// [Execution]. Fields whose key is not a pure, immutable function of the
// stored value's own fields (or that hold non-serializable runtime state:
// channels, cancel funcs, timers) remain plain maps -- see store_setup.go's
// package comment and each field's own comment below for why.
type InMemoryBackend struct {
	lambdaInvoker   asl.LambdaInvoker
	sqsIntegration  asl.SQSIntegration
	snsIntegration  asl.SNSIntegration
	ddbIntegration  asl.DynamoDBIntegration
	ecsIntegration  asl.ECSIntegration
	glueIntegration asl.GlueIntegration
	ebIntegration   asl.EventBridgeIntegration
	svcCtx          context.Context
	// tasksByToken maps task token → task entry for SendTaskSuccess/Failure.
	// Left as a plain map (not a store.Table): activityTaskEntry carries
	// live, non-serializable runtime state (timers, result channels) and is
	// never persisted -- an isolated in-flight token store, not an
	// AWS-visible resource collection.
	tasksByToken map[string]*activityTaskEntry
	// nameIndex maps region → name → ARN for O(1) duplicate detection per region.
	nameIndex map[string]map[string]string
	// cancelFns holds the cancel function for each running execution goroutine.
	cancelFns map[string]context.CancelFunc
	// deletedExecs is a tombstone set for executions removed by DeleteStateMachine.
	// historyRecorder and runParsedExecution skip writes for tombstoned ARNs.
	deletedExecs      map[string]bool
	activities        *store.Table[Activity]
	activityNameIndex map[string]map[string]string
	// pendingTaskQueues maps activity ARN → buffered channel of pending tasks.
	pendingTaskQueues map[string]chan *activityTaskEntry
	executions        *store.Table[Execution]
	// executionsByStateMachine groups executions by (immutable) StateMachineArn,
	// replacing the former smExecutions []string index map.
	executionsByStateMachine *store.Index[Execution]
	// versions maps version ARN → version for PublishStateMachineVersion.
	versions *store.Table[StateMachineVersion]
	// versionsByStateMachine groups versions by (immutable) StateMachineArn,
	// replacing the former smVersions []string index map.
	versionsByStateMachine *store.Index[StateMachineVersion]
	// aliases maps alias ARN → alias for CreateStateMachineAlias.
	aliases *store.Table[StateMachineAlias]
	// smAliases maps state machine ARN → list of alias ARNs. Left as a plain
	// map: StateMachineAlias carries no StateMachineArn field of its own (only
	// the composite StateMachineAliasArn), so it is not a pure function of the
	// value's own fields and cannot be a store.Index key -- see store_setup.go.
	smAliases map[string][]string
	// executionDefinitions maps execution ARN → the SM definition that was active at start time.
	executionDefinitions map[string]string
	// historyTruncated tracks executions where the history cap has been reached
	// so we only emit a single warning per execution.
	historyTruncated map[string]bool
	stateMachines    *store.Table[StateMachine]
	mu               *lockmetrics.RWMutex
	// registry lets Reset (via resetLocked) collapse the six resource tables'
	// lifecycle to one registry.ResetAll() call instead of hand-rolled
	// re-initialization of each map. See store_setup.go.
	registry *store.Registry
	// mapRuns stores MapRun records keyed by MapRun ARN.
	mapRuns *store.Table[MapRun]
	// mapRunsByExecution groups MapRuns by (immutable) ExecutionArn, replacing
	// the former execMapRuns []string index map.
	mapRunsByExecution *store.Index[MapRun]
	// smExecsByStatus maps smARN → status → []execARN for O(1) filtered listing.
	// Left as a plain map (not a store.Index): Execution.Status is mutated in
	// place by StopExecution/RedriveExecution/runParsedExecution, which would
	// make a status-keyed Index stale -- see store_setup.go.
	smExecsByStatus map[string]map[string][]string
	accountID       string
	region          string
	settings        Settings
	// historyMu protects each Execution's inline history slice and
	// b.historyTruncated for concurrent cross-execution writes.
	// Lock order: b.mu (read or write) must be acquired before historyMu.
	historyMu sync.RWMutex
}

// activityTaskEntry holds a pending activity task and its result channel.
// Field order is tuned for GC pointer-bitmap scan range (pointer fields first).
type activityTaskEntry struct {
	// heartbeatTimer is reset on each SendTaskHeartbeat call. Nil if no heartbeat timeout.
	heartbeatTimer *time.Timer
	// heartbeatStop signals the heartbeat monitor to stop (on task completion).
	heartbeatStop chan struct{}
	resultCh      chan activityTaskResult
	// createdAt records when the task entry was created, used for TTL-based eviction.
	// time.Time contains a pointer (loc), placed before strings to keep scan range minimal.
	createdAt   time.Time
	activityArn string
	taskToken   string
	input       string
	// heartbeatDuration is the original duration for resetting the heartbeat timer.
	// Non-pointer field placed last to reduce GC scan range.
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
func NewInMemoryBackendWithContext(
	svcCtx context.Context,
	accountID, region string,
) *InMemoryBackend {
	return newInMemoryBackend(svcCtx, accountID, region)
}

func newInMemoryBackend(svcCtx context.Context, accountID, region string) *InMemoryBackend {
	if svcCtx == nil {
		svcCtx = context.Background()
	}

	b := &InMemoryBackend{
		accountID:            accountID,
		region:               region,
		svcCtx:               svcCtx,
		nameIndex:            make(map[string]map[string]string),
		cancelFns:            make(map[string]context.CancelFunc),
		deletedExecs:         make(map[string]bool),
		activityNameIndex:    make(map[string]map[string]string),
		pendingTaskQueues:    make(map[string]chan *activityTaskEntry),
		tasksByToken:         make(map[string]*activityTaskEntry),
		smAliases:            make(map[string][]string),
		executionDefinitions: make(map[string]string),
		historyTruncated:     make(map[string]bool),
		mu:                   lockmetrics.New("stepfunctions"),
		registry:             store.NewRegistry(),
		settings:             DefaultSettings(),
		smExecsByStatus:      make(map[string]map[string][]string),
	}

	registerAllTables(b)

	return b
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

// SetECSIntegration configures the ECS integration.
func (b *InMemoryBackend) SetECSIntegration(ecs asl.ECSIntegration) {
	b.mu.Lock("SetECSIntegration")
	defer b.mu.Unlock()
	b.ecsIntegration = ecs
}

// SetGlueIntegration configures the Glue integration.
func (b *InMemoryBackend) SetGlueIntegration(glue asl.GlueIntegration) {
	b.mu.Lock("SetGlueIntegration")
	defer b.mu.Unlock()
	b.glueIntegration = glue
}

// SetEventBridgeIntegration configures the EventBridge integration.
func (b *InMemoryBackend) SetEventBridgeIntegration(eb asl.EventBridgeIntegration) {
	b.mu.Lock("SetEventBridgeIntegration")
	defer b.mu.Unlock()
	b.ebIntegration = eb
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

	return arn.Build(
		"states",
		region,
		b.accountID,
		fmt.Sprintf("stateMachine:%s:%d", smName, version),
	)
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

	sm, ok := b.stateMachines.Get(arn)
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
		if sm, _ := b.stateMachines.Get(existingARN); sm != nil && sm.Status != statusDeleting {
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
	b.stateMachines.Put(sm)
	nameIdx[name] = smARN

	return sm, nil
}

// SweepTaskTokens evicts task tokens that have exceeded the TTL without a worker response,
// signalling the blocked InvokeActivity goroutine so it is not leaked. Returns evicted count.
func (b *InMemoryBackend) SweepTaskTokens() int {
	ttl := b.settings.TaskTokenTTL
	if ttl == 0 {
		ttl = defaultTaskTokenTTL
	}

	cutoff := time.Now().Add(-ttl)

	// Phase 1: collect stale tokens under read lock.
	b.mu.RLock("SweepTaskTokens.scan")

	var staleTokens []string

	for token, entry := range b.tasksByToken {
		if !entry.createdAt.IsZero() && entry.createdAt.Before(cutoff) {
			staleTokens = append(staleTokens, token)
		}
	}

	b.mu.RUnlock()

	if len(staleTokens) == 0 {
		return 0
	}

	// Phase 2: delete under write lock, collecting entries for notification.
	b.mu.Lock("SweepTaskTokens.delete")

	var stale []*activityTaskEntry

	for _, token := range staleTokens {
		entry, ok := b.tasksByToken[token]
		if !ok {
			continue // deleted between phases
		}

		if entry.createdAt.IsZero() || !entry.createdAt.Before(cutoff) {
			continue // renewed between phases
		}

		stale = append(stale, entry)
		delete(b.tasksByToken, token)

		if entry.heartbeatTimer != nil {
			entry.heartbeatTimer.Stop()
		}
	}

	b.mu.Unlock()

	for _, entry := range stale {
		select {
		case entry.resultCh <- activityTaskResult{errCode: "TaskTimedOut", cause: "task token TTL exceeded"}:
		default:
		}
	}

	return len(stale)
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
	type execPruneInfo struct {
		smARN  string
		status string
	}

	var toDelete []string

	pruneInfos := make(map[string]execPruneInfo)

	for _, exec := range b.executions.All() {
		if exec.Status != statusRunning && exec.StopDate != nil && *exec.StopDate < cutoff {
			toDelete = append(toDelete, exec.ExecutionArn)
			pruneInfos[exec.ExecutionArn] = execPruneInfo{smARN: exec.StateMachineArn, status: exec.Status}
		}
	}

	for _, arn := range toDelete {
		// Removes the execution from the table and, via the executionsByStateMachine
		// index, from the former smExecutions bookkeeping too -- the execution's
		// inline history goes with it since there is no longer a separate map.
		b.executions.Delete(arn)
		delete(b.executionDefinitions, arn)
		delete(b.historyTruncated, arn)

		if info, ok := pruneInfos[arn]; ok {
			b.removeFromStatusBucket(info.smARN, info.status, arn)
		}
	}

	b.sweepOrphanedTombstonesLocked()

	return len(toDelete)
}

// sweepOrphanedTombstonesLocked removes deletedExecs entries whose goroutines
// have already exited (no longer in cancelFns). Normally a goroutine removes
// its own tombstone in runParsedExecution, but an unusual exit path (panic/
// recover) could leave tombstones behind indefinitely without this sweep.
// Caller must hold b.mu for writing.
func (b *InMemoryBackend) sweepOrphanedTombstonesLocked() {
	for execARN := range b.deletedExecs {
		if _, running := b.cancelFns[execARN]; !running {
			delete(b.deletedExecs, execARN)
		}
	}
}

// DeleteStateMachine marks a state machine as DELETING then removes it.
func (b *InMemoryBackend) DeleteStateMachine(arn string) error {
	b.mu.Lock("DeleteStateMachine")
	defer b.mu.Unlock()

	sm, exists := b.stateMachines.Get(arn)
	if !exists {
		return fmt.Errorf("%w: %s", ErrStateMachineDoesNotExist, arn)
	}

	sm.Status = statusDeleting
	b.stateMachines.Delete(arn)

	smRegion := regionFromARN(arn, b.region)
	delete(b.nameIndex[smRegion], sm.Name)

	// Cancel running goroutines and clean up all executions and history for this SM.
	// Cloned first: b.executions.Delete below mutates the executionsByStateMachine
	// index this slice is backed by, so iterating the live index result while
	// deleting from it would be unsafe.
	execs := slices.Clone(b.executionsByStateMachine.Get(arn))
	for _, exec := range execs {
		execARN := exec.ExecutionArn

		if cancelFn, ok := b.cancelFns[execARN]; ok {
			cancelFn()
			delete(b.cancelFns, execARN)
			// Only tombstone executions whose goroutines are still running; completed
			// executions have already cleaned up their own tombstones.
			b.deletedExecs[execARN] = true
		}

		// Removes the execution (and its inline history) from the table and,
		// via the index, from the former smExecutions bookkeeping too.
		b.executions.Delete(execARN)
		delete(b.executionDefinitions, execARN)
		delete(b.historyTruncated, execARN)
	}

	delete(b.smExecsByStatus, arn)

	// Remove all versions for this state machine. Cloned first for the same
	// reason as executions above: b.versions.Delete mutates the
	// versionsByStateMachine index this slice is backed by.
	versions := slices.Clone(b.versionsByStateMachine.Get(arn))
	for _, v := range versions {
		b.versions.Delete(v.StateMachineVersionArn)
	}

	// Remove all aliases for this state machine.
	for _, aARN := range b.smAliases[arn] {
		b.aliases.Delete(aARN)
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

	all := make([]StateMachine, 0, b.stateMachines.Len())
	for _, sm := range b.stateMachines.All() {
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

	sm, exists := b.stateMachines.Get(arn)
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

	sm, exists := b.stateMachines.Get(smARN)
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
func (b *InMemoryBackend) StartSyncExecution(
	stateMachineArn, name, input string,
) (*SyncExecutionResult, error) {
	if len(input) > maxExecutionInputBytes {
		return nil, fmt.Errorf(
			"%w: input exceeds %d bytes",
			ErrInvalidExecutionInput,
			maxExecutionInputBytes,
		)
	}

	b.mu.RLock("StartSyncExecution")
	sm, exists := b.stateMachines.Get(stateMachineArn)
	if !exists {
		b.mu.RUnlock()

		return nil, fmt.Errorf("%w: %s", ErrStateMachineDoesNotExist, stateMachineArn)
	}

	if sm.Type != "EXPRESS" {
		b.mu.RUnlock()

		// AWS: StartSyncExecution is only supported for EXPRESS state
		// machines and returns StateMachineTypeNotSupported for STANDARD.
		return nil, fmt.Errorf(
			"%w: StartSyncExecution requires an EXPRESS state machine",
			ErrStateMachineTypeNotSupported,
		)
	}

	smName := sm.Name
	definition := sm.Definition
	lambdaInvoker := b.lambdaInvoker
	sqsIntegration := b.sqsIntegration
	snsIntegration := b.snsIntegration
	ddbIntegration := b.ddbIntegration
	ecsIntegration := b.ecsIntegration
	glueIntegration := b.glueIntegration
	ebIntegration := b.ebIntegration
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
	executor.SetECSIntegration(ecsIntegration)
	executor.SetGlueIntegration(glueIntegration)
	executor.SetEventBridgeIntegration(ebIntegration)
	executor.SetActivityInvoker(b)
	executor.SetTaskTokenCallbackInvoker(b)
	executor.SetMapRunNotifier(
		&syncMapRunNotifier{backend: b, execARN: execARN, smARN: stateMachineArn},
	)
	executor.SetExecutionContext(
		execARN,
		name,
		sm.RoleArn,
		time.Unix(int64(startDate), 0).UTC().Format(time.RFC3339),
		stateMachineArn,
		sm.Name,
	)

	result, execErr := executor.Execute(syncCtx, execARN, input)

	return finalizeSyncExecutionResult(
		execARN,
		stateMachineArn,
		name,
		input,
		startDate,
		result,
		execErr,
	), nil
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

func (b *InMemoryBackend) initializeExecutionRecord(smArn, name, execArn, input, def string, now float64) *Execution {
	exec := &Execution{
		StartDate:       now,
		ExecutionArn:    execArn,
		StateMachineArn: smArn,
		Name:            name,
		Status:          statusRunning,
		Input:           input,
		history: []*HistoryEvent{
			{Timestamp: now, Type: "ExecutionStarted", ID: executionStartedEventID, PreviousEventID: 0},
		},
	}
	// Put also inserts exec into the executionsByStateMachine index, replacing
	// the former manual b.smExecutions[smArn] append.
	b.executions.Put(exec)
	b.executionDefinitions[execArn] = def
	b.addToStatusBucket(smArn, statusRunning, execArn)

	return exec
}

// StartExecution creates an execution and runs the ASL interpreter asynchronously.
func (b *InMemoryBackend) StartExecution(stateMachineArn, name, input string) (*Execution, error) {
	if len(input) > maxExecutionInputBytes {
		return nil, fmt.Errorf(
			"%w: input exceeds %d bytes",
			ErrInvalidExecutionInput,
			maxExecutionInputBytes,
		)
	}

	if name != "" {
		if err := validateName(name, maxExecutionNameLen); err != nil {
			return nil, err
		}
	}

	b.mu.Lock("StartExecution")

	// Opportunistically prune finished executions that have aged past the retention
	// period so the executions/history maps stay bounded when the janitor is off.
	if b.executions.Len() >= executionPruneSweepThreshold {
		retention := b.settings.ExecutionRetention
		if retention == 0 {
			retention = defaultExecutionRetention
		}

		b.pruneExecutionsLocked(float64(time.Now().Add(-retention).Unix()))
	}

	sm, exists := b.stateMachines.Get(stateMachineArn)
	if !exists {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: %s", ErrStateMachineDoesNotExist, stateMachineArn)
	}

	// AWS allows StartExecution (asynchronous execution) on EXPRESS state
	// machines too -- only StartSyncExecution is restricted to EXPRESS.
	// See "Asynchronous Express Workflows" in the AWS Step Functions docs.
	execArn := b.execARN(stateMachineArn, sm.Name, name)
	if b.executions.Has(execArn) {
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
	exec := b.initializeExecutionRecord(stateMachineArn, name, execArn, input, definition, now)

	lambdaInvoker := b.lambdaInvoker
	sqsIntegration := b.sqsIntegration
	snsIntegration := b.snsIntegration
	ddbIntegration := b.ddbIntegration
	ecsIntegration := b.ecsIntegration
	glueIntegration := b.glueIntegration
	ebIntegration := b.ebIntegration

	// Register the execution in the SM→executions index and store a cancel fn
	// so StopExecution and DeleteStateMachine can cancel the goroutine.
	// The context is derived from b.svcCtx so that all active executions are
	// also cancelled when the server shuts down.

	//nolint:gosec // cancel is stored in b.cancelFns for StopExecution/DeleteStateMachine
	ctx, cancel := context.WithCancel(b.svcCtx)
	b.cancelFns[execArn] = cancel

	var activityInvoker asl.ActivityInvoker = b

	b.mu.Unlock()

	// Run the ASL interpreter asynchronously.
	go b.runParsedExecution(
		ctx,
		execArn,
		parsedSM,
		input,
		lambdaInvoker,
		sqsIntegration,
		snsIntegration,
		ddbIntegration,
		ecsIntegration,
		glueIntegration,
		ebIntegration,
		activityInvoker,
	)

	return exec, nil
}

// applyExecutorContext populates the ASL executor's `$$` context object with
// data derived from the persisted execution and state machine records.
func (b *InMemoryBackend) applyExecutorContext(executor *asl.Executor, execARN string) {
	b.mu.RLock("applyExecutorContext")
	defer b.mu.RUnlock()

	exec, ok := b.executions.Get(execARN)
	if !ok {
		return
	}

	sm, smOK := b.stateMachines.Get(exec.StateMachineArn)
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
		return fmt.Errorf("%w: roleArn is required", ErrValidation)
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
	case "Fail":
		return "FailStateExited"
	case "Parallel":
		return "ParallelStateExited"
	case "Map":
		return "MapStateExited"
	default:
		return stateType + "StateExited"
	}
}

// appendHistory appends event to the per-execution history without the global
// write lock on the hot path. Lock order: b.mu (RLock) then b.historyMu (Lock).
// Holding b.mu.RLock for the entire call ensures b.mu write-lock holders (e.g.
// StartExecution, runParsedExecution) cannot race with concurrent map writes here.
func (b *InMemoryBackend) appendHistory(execARN string, event *HistoryEvent) {
	b.mu.RLock("appendHistory")
	defer b.mu.RUnlock()

	if b.deletedExecs[execARN] {
		return
	}

	exec, ok := b.executions.Get(execARN)
	if !ok {
		return
	}

	b.historyMu.Lock()
	defer b.historyMu.Unlock()

	if !b.checkHistoryCapacity(exec) {
		return
	}

	nextID := int64(len(exec.history) + 1)
	event.ID = nextID
	event.PreviousEventID = nextID - 1
	exec.history = append(exec.history, event)
}

func (r *historyRecorder) RecordStateEntered(execARN, stateName, stateType string, input any) {
	r.backend.appendHistory(execARN, &HistoryEvent{
		Timestamp: float64(time.Now().Unix()),
		Type:      stateEnteredEventType(stateType),
		StateEnteredEventDetails: &StateEnteredEventDetails{
			Name:  stateName,
			Input: historyValueToJSON(input),
		},
	})
}

func (r *historyRecorder) RecordStateExited(execARN, stateName, stateType string, output any) {
	r.backend.appendHistory(execARN, &HistoryEvent{
		Timestamp: float64(time.Now().Unix()),
		Type:      stateExitedEventType(stateType),
		StateExitedEventDetails: &StateExitedEventDetails{
			Name:   stateName,
			Output: historyValueToJSON(output),
		},
	})
}

func (r *historyRecorder) RecordTaskScheduled(execARN, _ /* stateName */, resource string) {
	r.backend.appendHistory(execARN, &HistoryEvent{
		Timestamp:                 float64(time.Now().Unix()),
		Type:                      "TaskScheduled",
		TaskScheduledEventDetails: &TaskScheduledEventDetails{Resource: resource},
	})
}

func (r *historyRecorder) RecordTaskSucceeded(execARN, _ /* stateName */ string, output any) {
	r.backend.appendHistory(execARN, &HistoryEvent{
		Timestamp:                 float64(time.Now().Unix()),
		Type:                      "TaskSucceeded",
		TaskSucceededEventDetails: &TaskSucceededEventDetails{Output: historyValueToJSON(output)},
	})
}

func (r *historyRecorder) RecordTaskFailed(
	execARN, _ /* stateName */, errCode, cause string,
) {
	r.backend.appendHistory(execARN, &HistoryEvent{
		Timestamp: float64(time.Now().Unix()),
		Type:      "TaskFailed",
		TaskFailedEventDetails: &TaskFailedEventDetails{
			Error: errCode,
			Cause: cause,
		},
	})
}

// historyValueToJSON marshals a state input/output value to its JSON string
// representation for execution-history detail fields, matching AWS's
// convention of embedding input/output as a JSON string rather than a nested
// object. Marshal failures (which should not occur for values that already
// round-tripped through the ASL interpreter) fall back to an empty string
// rather than corrupting the history event.
func historyValueToJSON(v any) string {
	if v == nil {
		return ""
	}

	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}

	return string(b)
}

// checkHistoryCapacity reports whether there is room to append another event
// to exec.history. On the first refusal per execution it logs a warning so
// that silent truncation is observable. Caller must hold b.historyMu write lock.
func (b *InMemoryBackend) checkHistoryCapacity(exec *Execution) bool {
	if len(exec.history) < maxHistoryEvents {
		return true
	}

	if !b.historyTruncated[exec.ExecutionArn] {
		b.historyTruncated[exec.ExecutionArn] = true
		logger.Load(b.svcCtx).Warn(
			"stepfunctions: execution history truncated at maxHistoryEvents",
			"executionArn", exec.ExecutionArn,
			"maxHistoryEvents", maxHistoryEvents,
		)
	}

	return false
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
	ecsIntegration asl.ECSIntegration,
	glueIntegration asl.GlueIntegration,
	ebIntegration asl.EventBridgeIntegration,
	activityInvoker asl.ActivityInvoker,
) {
	rec := &historyRecorder{backend: b}
	executor := asl.NewExecutor(sm, lambdaInvoker, rec)
	executor.SetSQSIntegration(sqsIntegration)
	executor.SetSNSIntegration(snsIntegration)
	executor.SetDynamoDBIntegration(ddbIntegration)
	executor.SetECSIntegration(ecsIntegration)
	executor.SetGlueIntegration(glueIntegration)
	executor.SetEventBridgeIntegration(ebIntegration)
	executor.SetActivityInvoker(activityInvoker)
	executor.SetTaskTokenCallbackInvoker(b)
	executor.SetMapRunNotifier(b)
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

	exec, ok := b.executions.Get(execARN)
	if !ok {
		return
	}

	// If the execution was already moved to a terminal state (e.g., ABORTED via
	// StopExecution) while the background runner was in flight, do not overwrite it.
	if exec.Status != statusRunning {
		return
	}

	now := float64(time.Now().Unix())
	exec.StopDate = &now
	nextID := int64(len(exec.history) + 1)

	if execErr != nil {
		exec.Status = statusFailed
		exec.Error = execErr.Error()
		b.removeFromStatusBucket(exec.StateMachineArn, statusRunning, execARN)
		b.addToStatusBucket(exec.StateMachineArn, exec.Status, execARN)
		exec.history = append(exec.history, &HistoryEvent{
			Timestamp: now, Type: "ExecutionFailed", ID: nextID, PreviousEventID: nextID - 1,
		})

		return
	}

	if result.Error != "" {
		exec.Status = statusFailed
		exec.Error = result.Error
		exec.Cause = result.Cause
		b.removeFromStatusBucket(exec.StateMachineArn, statusRunning, execARN)
		b.addToStatusBucket(exec.StateMachineArn, exec.Status, execARN)
		exec.history = append(exec.history, &HistoryEvent{
			Timestamp: now, Type: "ExecutionFailed", ID: nextID, PreviousEventID: nextID - 1,
		})

		return
	}

	outputBytes, _ := json.Marshal(result.Output)
	exec.Status = statusSucceeded
	exec.Output = string(outputBytes)
	b.removeFromStatusBucket(exec.StateMachineArn, statusRunning, execARN)
	b.addToStatusBucket(exec.StateMachineArn, exec.Status, execARN)
	exec.history = append(exec.history, &HistoryEvent{
		Timestamp: now, Type: "ExecutionSucceeded", ID: nextID, PreviousEventID: nextID - 1,
	})
}

// StopExecution marks a RUNNING execution as ABORTED.
// AWS behaviour: idempotent on already-terminal executions — returns success without mutation.
func (b *InMemoryBackend) StopExecution(executionArn, errCode, cause string) error {
	b.mu.Lock("StopExecution")
	defer b.mu.Unlock()

	exec, exists := b.executions.Get(executionArn)
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
	b.removeFromStatusBucket(exec.StateMachineArn, statusRunning, executionArn)
	b.addToStatusBucket(exec.StateMachineArn, statusAborted, executionArn)

	// Cancel the running goroutine for this execution.
	if cancelFn, ok := b.cancelFns[executionArn]; ok {
		cancelFn()
		delete(b.cancelFns, executionArn)
	}

	nextID := int64(len(exec.history) + 1)
	exec.history = append(exec.history, &HistoryEvent{
		Timestamp: now, Type: "ExecutionAborted", ID: nextID, PreviousEventID: nextID - 1,
	})

	return nil
}

// DescribeExecution returns details for a single execution.
func (b *InMemoryBackend) DescribeExecution(executionArn string) (*Execution, error) {
	b.mu.RLock("DescribeExecution")
	defer b.mu.RUnlock()

	exec, exists := b.executions.Get(executionArn)
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrExecutionDoesNotExist, executionArn)
	}

	// exec.history is written by appendHistory under only b.mu.RLock +
	// b.historyMu.Lock (a deliberate hot-path optimization, see appendHistory),
	// so a whole-struct copy here -- which touches every field, including
	// history -- must also take historyMu to avoid racing that write; b.mu's
	// RLock alone does not exclude it. Lock order: b.mu before historyMu.
	b.historyMu.RLock()
	cp := *exec
	b.historyMu.RUnlock()

	return &cp, nil
}

// ListExecutions returns executions for a state machine with optional pagination.
func (b *InMemoryBackend) ListExecutions(
	stateMachineArn, statusFilter, nextToken string, maxResults int,
) ([]Execution, string, error) {
	b.mu.RLock("ListExecutions")
	defer b.mu.RUnlock()

	// When a status filter is given, use the O(1) status bucket index instead
	// of scanning the full executionsByStateMachine index.
	var execs []*Execution

	if statusFilter != "" {
		if bucket := b.smExecsByStatus[stateMachineArn]; bucket != nil {
			for _, execARN := range bucket[statusFilter] {
				if exec, ok := b.executions.Get(execARN); ok {
					execs = append(execs, exec)
				}
				// Defensive guard: indexes should be consistent with
				// b.executions, but skip any stale references just in case.
			}
		}
	} else {
		execs = b.executionsByStateMachine.Get(stateMachineArn)
	}

	// See the comment in DescribeExecution: whole-struct copies of *Execution
	// touch history, which appendHistory writes under historyMu rather than
	// b.mu's write lock, so copying it here needs the same guard.
	all := make([]Execution, 0, len(execs))
	b.historyMu.RLock()
	for _, exec := range execs {
		all = append(all, *exec)
	}
	b.historyMu.RUnlock()

	sort.Slice(all, func(i, j int) bool { return all[i].StartDate > all[j].StartDate })

	page, token := paginate(all, nextToken, maxResults)

	return page, token, nil
}

// GetExecutionHistory returns history events for an execution.
func (b *InMemoryBackend) GetExecutionHistory(
	executionArn, nextToken string, maxResults int, reverseOrder bool,
) ([]HistoryEvent, string, error) {
	b.mu.RLock("GetExecutionHistory")
	defer b.mu.RUnlock()

	exec, exists := b.executions.Get(executionArn)
	if !exists {
		return nil, "", fmt.Errorf("%w: %s", ErrExecutionDoesNotExist, executionArn)
	}

	b.historyMu.RLock()
	defer b.historyMu.RUnlock()

	raw := exec.history
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
	defer b.mu.Unlock()

	b.resetLocked()
}

// resetLocked performs the actual reset. It is shared by Reset and by
// Restore's incompatible-snapshot-version path (see persistence.go), both of
// which already hold b.mu for writing when they call it.
func (b *InMemoryBackend) resetLocked() {
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
	newDeleted := make(map[string]bool, b.executions.Len())
	for _, exec := range b.executions.All() {
		newDeleted[exec.ExecutionArn] = true
	}

	// Clears stateMachines, executions (and their inline history),
	// activities, versions, aliases, and mapRuns in one call, including
	// their store.Index entries.
	b.registry.ResetAll()

	b.nameIndex = make(map[string]map[string]string)
	b.cancelFns = make(map[string]context.CancelFunc)
	b.deletedExecs = newDeleted
	b.activityNameIndex = make(map[string]map[string]string)
	b.pendingTaskQueues = make(map[string]chan *activityTaskEntry)
	b.tasksByToken = make(map[string]*activityTaskEntry)
	b.smAliases = make(map[string][]string)
	b.executionDefinitions = make(map[string]string)
	b.historyTruncated = make(map[string]bool)
	b.historyMu = sync.RWMutex{}
	b.smExecsByStatus = make(map[string]map[string][]string)
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
	b.activities.Put(a)
	actIdx[name] = actARN
	b.pendingTaskQueues[actARN] = make(chan *activityTaskEntry, maxPendingActivityTasks)

	cp := *a

	return &cp, nil
}

// DeleteActivity deletes an activity and closes its pending task queue.
func (b *InMemoryBackend) DeleteActivity(activityArn string) error {
	b.mu.Lock("DeleteActivity")
	defer b.mu.Unlock()

	a, exists := b.activities.Get(activityArn)
	if !exists {
		return fmt.Errorf("%w: %s", ErrActivityDoesNotExist, activityArn)
	}

	b.activities.Delete(activityArn)

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
		entry := b.tasksByToken[taskToken]
		delete(b.tasksByToken, taskToken)
		if entry.heartbeatTimer != nil {
			entry.heartbeatTimer.Stop()
		}
		// Unblock any InvokeActivity goroutine waiting on this task's resultCh.
		select {
		case entry.resultCh <- activityTaskResult{errCode: "ActivityDoesNotExist", cause: "activity deleted"}:
		default:
		}
	}

	return nil
}

// DescribeActivity returns activity details.
func (b *InMemoryBackend) DescribeActivity(activityArn string) (*Activity, error) {
	b.mu.RLock("DescribeActivity")
	defer b.mu.RUnlock()

	a, ok := b.activities.Get(activityArn)
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

	all := make([]Activity, 0, b.activities.Len())
	for _, a := range b.activities.All() {
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

	sm, exists := b.stateMachines.Get(smARN)
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrStateMachineDoesNotExist, smARN)
	}

	versionNum := len(b.versionsByStateMachine.Get(smARN)) + 1
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

	// Put also inserts v into the versionsByStateMachine index, replacing the
	// former manual b.smVersions[smARN] append.
	b.versions.Put(v)

	cp := *v

	return &cp, nil
}

// DescribeStateMachineVersion returns details for a specific version.
func (b *InMemoryBackend) DescribeStateMachineVersion(
	versionARN string,
) (*StateMachineVersion, error) {
	b.mu.RLock("DescribeStateMachineVersion")
	defer b.mu.RUnlock()

	v, exists := b.versions.Get(versionARN)
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

	if !b.versions.Has(versionARN) {
		return fmt.Errorf("%w: %s", ErrStateMachineVersionDoesNotExist, versionARN)
	}

	// Delete also removes v from the versionsByStateMachine index, replacing
	// the former manual b.smVersions[smARN] filter-and-rebuild.
	b.versions.Delete(versionARN)

	return nil
}

// ListStateMachineVersions returns all versions for a state machine.
func (b *InMemoryBackend) ListStateMachineVersions(
	smARN, nextToken string, maxResults int,
) ([]StateMachineVersion, string, error) {
	b.mu.RLock("ListStateMachineVersions")
	defer b.mu.RUnlock()

	if !b.stateMachines.Has(smARN) {
		return nil, "", fmt.Errorf("%w: %s", ErrStateMachineDoesNotExist, smARN)
	}

	vers := b.versionsByStateMachine.Get(smARN)
	all := make([]StateMachineVersion, 0, len(vers))
	for _, v := range vers {
		all = append(all, *v)
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
		return fmt.Errorf(
			"%w: routing configuration must have 1 or 2 entries",
			ErrInvalidRoutingConfiguration,
		)
	}

	total := 0

	for _, r := range routing {
		if r.Weight < 0 || r.Weight > 100 {
			return fmt.Errorf(
				"%w: each routing weight must be between 0 and 100",
				ErrInvalidRoutingConfiguration,
			)
		}

		total += r.Weight
	}

	const totalWeight = 100
	if total != totalWeight {
		return fmt.Errorf(
			"%w: routing weights must sum to 100, got %d",
			ErrInvalidRoutingConfiguration,
			total,
		)
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

	sm, exists := b.stateMachines.Get(smARN)
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrStateMachineDoesNotExist, smARN)
	}

	aARN := b.aliasARN(smARN, sm.Name, name)

	if b.aliases.Has(aARN) {
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

	b.aliases.Put(alias)
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

	alias, exists := b.aliases.Get(aliasARN)
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

	if !b.aliases.Has(aliasARN) {
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

	b.aliases.Delete(aliasARN)

	return nil
}

// DescribeStateMachineAlias returns details for a state machine alias.
func (b *InMemoryBackend) DescribeStateMachineAlias(aliasARN string) (*StateMachineAlias, error) {
	b.mu.RLock("DescribeStateMachineAlias")
	defer b.mu.RUnlock()

	alias, exists := b.aliases.Get(aliasARN)
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

	if !b.stateMachines.Has(smARN) {
		return nil, "", fmt.Errorf("%w: %s", ErrStateMachineDoesNotExist, smARN)
	}

	aARNs := b.smAliases[smARN]
	all := make([]StateMachineAlias, 0, len(aARNs))
	for _, aARN := range aARNs {
		if a, ok := b.aliases.Get(aARN); ok {
			all = append(all, *a)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	aliases, token := paginate(all, nextToken, maxResults)

	return aliases, token, nil
}

func (b *InMemoryBackend) resetExecutionForRedrive(exec *Execution, executionARN, smARN string, now float64) {
	oldStatus := exec.Status
	exec.Status = statusRunning
	exec.Output = ""
	exec.Error = ""
	exec.Cause = ""
	exec.StopDate = nil
	exec.StartDate = now
	exec.RedriveCount++
	exec.RedriveDate = &now
	b.removeFromStatusBucket(smARN, oldStatus, executionARN)
	b.addToStatusBucket(smARN, statusRunning, executionARN)
	exec.history = []*HistoryEvent{
		{Timestamp: now, Type: "ExecutionStarted", ID: executionStartedEventID, PreviousEventID: 0},
	}
}

// RedriveExecution re-runs a FAILED or ABORTED execution starting from its last known state.
// AWS Step Functions re-runs from the last state that was reached before failure.
// In this implementation we restart the entire execution with the original input (AWS parity for STANDARD executions).
func (b *InMemoryBackend) RedriveExecution(executionARN string) (*Execution, error) {
	b.mu.Lock("RedriveExecution")

	exec, exists := b.executions.Get(executionARN)
	if !exists {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: %s", ErrExecutionDoesNotExist, executionARN)
	}

	if exec.Status != statusFailed && exec.Status != statusAborted {
		b.mu.Unlock()

		return nil, fmt.Errorf(
			"%w: execution %s is in status %s; only FAILED or ABORTED executions can be redriven",
			ErrExecutionNotRedrivable,
			executionARN,
			exec.Status,
		)
	}

	smARN := exec.StateMachineArn
	originalInput := exec.Input

	sm, smExists := b.stateMachines.Get(smARN)
	if !smExists {
		b.mu.Unlock()

		return nil, fmt.Errorf(
			"%w: state machine %s no longer exists",
			ErrStateMachineDoesNotExist,
			smARN,
		)
	}

	definition := sm.Definition
	parsedSM, parseErr := asl.Parse(definition)
	if parseErr != nil {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: %w", ErrInvalidDefinition, parseErr)
	}

	// Reset the execution to RUNNING.
	now := float64(time.Now().Unix())
	b.resetExecutionForRedrive(exec, executionARN, smARN, now)

	// Snapshot the (possibly-updated) definition.
	b.executionDefinitions[executionARN] = definition

	lambdaInvoker := b.lambdaInvoker
	sqsIntegration := b.sqsIntegration
	snsIntegration := b.snsIntegration
	ddbIntegration := b.ddbIntegration
	ecsIntegration := b.ecsIntegration
	glueIntegration := b.glueIntegration
	ebIntegration := b.ebIntegration

	//nolint:gosec // cancel is stored in b.cancelFns for StopExecution/DeleteStateMachine
	ctx, cancel := context.WithCancel(b.svcCtx)
	b.cancelFns[executionARN] = cancel

	// No manual "ensure execution is tracked under the SM" step is needed here
	// (unlike the former smExecutions []string index): exec was already
	// inserted into the executions table -- and, via the immutable
	// StateMachineArn field, into the executionsByStateMachine index -- when
	// the execution was first created, and neither Redrive nor any other path
	// ever removes it from the table without also deleting it outright.
	var activityInvoker asl.ActivityInvoker = b

	b.mu.Unlock()

	go b.runParsedExecution(
		ctx,
		executionARN,
		parsedSM,
		originalInput,
		lambdaInvoker,
		sqsIntegration,
		snsIntegration,
		ddbIntegration,
		ecsIntegration,
		glueIntegration,
		ebIntegration,
		activityInvoker,
	)

	b.mu.RLock("RedriveExecution.result")
	execAfter, _ := b.executions.Get(executionARN)
	// See the comment in DescribeExecution: guard the whole-struct copy
	// against a concurrent appendHistory write, which b.mu's RLock alone
	// does not exclude.
	b.historyMu.RLock()
	cp := *execAfter
	b.historyMu.RUnlock()
	b.mu.RUnlock()

	return &cp, nil
}

// DescribeStateMachineForExecution returns the state machine definition that was active
// when the given execution was started.
func (b *InMemoryBackend) DescribeStateMachineForExecution(
	executionARN string,
) (*StateMachine, error) {
	b.mu.RLock("DescribeStateMachineForExecution")
	defer b.mu.RUnlock()

	exec, exists := b.executions.Get(executionARN)
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrExecutionDoesNotExist, executionARN)
	}

	definition, hasSnapshot := b.executionDefinitions[executionARN]
	if !hasSnapshot {
		// Fall back to the current definition if no snapshot was taken (pre-snapshot executions).
		sm, smExists := b.stateMachines.Get(exec.StateMachineArn)
		if !smExists {
			return nil, fmt.Errorf(
				"%w: state machine %s no longer exists",
				ErrStateMachineDoesNotExist,
				exec.StateMachineArn,
			)
		}

		cp := *sm

		return &cp, nil
	}

	sm, smExists := b.stateMachines.Get(exec.StateMachineArn)
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
		createdAt:   time.Now(),
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

// mapRunARNFor builds a Map Run ARN from an execution ARN and a state name.
// Exec ARN format: arn:aws:states:{region}:{account}:execution:{smName}:{execName}
// MapRun ARN format: arn:aws:states:{region}:{account}:mapRun:{smName}/{execName}/{stateName}.
func (b *InMemoryBackend) mapRunARNFor(execARN, stateName string) string {
	// Parse exec ARN: arn:aws:states:{region}:{acct}:execution:{smName}:{execName}
	parts := strings.Split(execARN, ":")
	const execARNMinParts = 8
	if len(parts) < execARNMinParts {
		return fmt.Sprintf(
			"arn:aws:states:%s:%s:mapRun:unknown/%s/%s",
			b.region,
			b.accountID,
			"unknown",
			stateName,
		)
	}
	// parts[3]=region, parts[4]=account, parts[6]=smName, parts[7]=execName
	region := parts[3]
	account := parts[4]
	smName := parts[6]
	execName := parts[7]

	return fmt.Sprintf(
		"arn:aws:states:%s:%s:mapRun:%s/%s/%s",
		region,
		account,
		smName,
		execName,
		stateName,
	)
}

// syncMapRunNotifier wraps InMemoryBackend to provide MapRunNotifier for
// sync executions which do not have entries in b.executions.
type syncMapRunNotifier struct {
	backend *InMemoryBackend
	execARN string
	smARN   string
}

func (s *syncMapRunNotifier) OnMapRunStart(
	executionARN, stateName string,
	maxConcurrency, itemCount int,
) string {
	return s.backend.storeMapRun(executionARN, stateName, s.smARN, maxConcurrency, itemCount)
}

func (s *syncMapRunNotifier) OnMapRunEnd(mapRunARN, status string, succeeded, failed, total int) {
	s.backend.OnMapRunEnd(mapRunARN, status, succeeded, failed, total)
}

// storeMapRun creates and persists a MapRun record. smARN may be empty if not known.
func (b *InMemoryBackend) storeMapRun(
	executionARN, stateName, smARN string,
	maxConcurrency, itemCount int,
) string {
	mapRunARN := b.mapRunARNFor(executionARN, stateName)
	const millisPerSecond = 1000.0
	now := float64(time.Now().UnixMilli()) / millisPerSecond

	mr := &MapRun{
		MapRunArn:       mapRunARN,
		ExecutionArn:    executionARN,
		StateMachineArn: smARN,
		StartDate:       now,
		Status:          "RUNNING",
		MaxConcurrency:  maxConcurrency,
		ItemCounts:      MapRunItemCounts{Total: itemCount, Pending: itemCount},
	}

	b.mu.Lock("storeMapRun")
	// Put also inserts mr into the mapRunsByExecution index, replacing the
	// former manual b.execMapRuns[executionARN] append.
	b.mapRuns.Put(mr)
	b.mu.Unlock()

	return mapRunARN
}

// OnMapRunStart implements asl.MapRunNotifier.
func (b *InMemoryBackend) OnMapRunStart(
	executionARN, stateName string,
	maxConcurrency, itemCount int,
) string {
	b.mu.RLock("OnMapRunStart.lookup")
	exec, _ := b.executions.Get(executionARN)
	var smARN string
	if exec != nil {
		smARN = exec.StateMachineArn
	}
	b.mu.RUnlock()

	return b.storeMapRun(executionARN, stateName, smARN, maxConcurrency, itemCount)
}

// OnMapRunEnd implements asl.MapRunNotifier.
func (b *InMemoryBackend) OnMapRunEnd(mapRunARN, status string, succeeded, failed, total int) {
	const millisPerSecond = 1000.0
	now := float64(time.Now().UnixMilli()) / millisPerSecond

	b.mu.Lock("OnMapRunEnd")
	defer b.mu.Unlock()

	mr, ok := b.mapRuns.Get(mapRunARN)
	if !ok {
		return
	}

	mr.Status = status
	mr.StopDate = &now
	mr.ItemCounts.Succeeded = succeeded
	mr.ItemCounts.Failed = failed
	mr.ItemCounts.Total = total
	mr.ItemCounts.Pending = 0
	mr.ItemCounts.Running = 0
	mr.ItemCounts.ResultsWritten = succeeded
}

// DescribeMapRun returns details for a Map Run.
func (b *InMemoryBackend) DescribeMapRun(mapRunARN string) (*MapRun, error) {
	b.mu.RLock("DescribeMapRun")
	defer b.mu.RUnlock()

	mr, ok := b.mapRuns.Get(mapRunARN)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrMapRunDoesNotExist, mapRunARN)
	}

	cp := *mr

	return &cp, nil
}

// UpdateMapRun updates concurrency/tolerated-failure settings for a Map Run.
func (b *InMemoryBackend) UpdateMapRun(
	mapRunARN string,
	maxConcurrency int,
	toleratedFailureCount int,
	toleratedFailurePercentage float64,
) (*MapRun, error) {
	b.mu.Lock("UpdateMapRun")
	defer b.mu.Unlock()

	mr, ok := b.mapRuns.Get(mapRunARN)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrMapRunDoesNotExist, mapRunARN)
	}

	if maxConcurrency >= 0 {
		mr.MaxConcurrency = maxConcurrency
	}

	mr.ToleratedFailureCount = toleratedFailureCount
	mr.ToleratedFailurePercentage = toleratedFailurePercentage

	cp := *mr

	return &cp, nil
}

// ListMapRuns returns all MapRuns for an execution.
func (b *InMemoryBackend) ListMapRuns(
	executionARN, nextToken string, maxResults int,
) ([]MapRun, string, error) {
	b.mu.RLock("ListMapRuns")
	defer b.mu.RUnlock()

	runs := b.mapRunsByExecution.Get(executionARN)
	all := make([]MapRun, 0, len(runs))

	for _, mr := range runs {
		all = append(all, *mr)
	}

	page, token := paginate(all, nextToken, maxResults)

	return page, token, nil
}

// removeFromStatusBucket removes execARN from the smExecsByStatus bucket for smARN/status.
// Must be called with b.mu write lock held.
func (b *InMemoryBackend) removeFromStatusBucket(smARN, status, execARN string) {
	bucket := b.smExecsByStatus[smARN]
	if bucket == nil {
		return
	}
	arns := bucket[status]
	for i, a := range arns {
		if a == execARN {
			bucket[status] = append(arns[:i], arns[i+1:]...)

			return
		}
	}
}

// addToStatusBucket adds execARN to the smExecsByStatus bucket for smARN/status.
// Must be called with b.mu write lock held.
func (b *InMemoryBackend) addToStatusBucket(smARN, status, execARN string) {
	if b.smExecsByStatus[smARN] == nil {
		b.smExecsByStatus[smARN] = make(map[string][]string)
	}
	b.smExecsByStatus[smARN][status] = append(b.smExecsByStatus[smARN][status], execARN)
}
