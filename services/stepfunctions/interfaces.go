package stepfunctions

import (
	"context"
)

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
	UpdateStateMachine(arn, definition, roleArn string) (updateDate float64, revisionID string, err error)
	PublishStateMachineVersion(smARN, description, revisionID string) (*StateMachineVersion, error)
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
	StartExecutionWithTrace(stateMachineArn, name, input, traceHeader string) (*Execution, error)
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
	SetActivityEncryptionConfiguration(activityArn string, encryption *EncryptionConfiguration) error
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

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
