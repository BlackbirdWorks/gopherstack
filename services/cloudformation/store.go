package cloudformation

import (
	"context"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

type StorageBackend interface {
	CreateStack(
		ctx context.Context,
		name, templateBody string,
		params []Parameter,
		opts StackOptions,
	) (*Stack, error)
	UpdateStack(
		ctx context.Context,
		nameOrID, templateBody string,
		params []Parameter,
		opts StackOptions,
	) (*Stack, error)
	DeleteStack(ctx context.Context, nameOrID string) error
	DescribeStack(nameOrID string) (*Stack, error)
	ListStacks(statusFilter []string, nextToken string) (page.Page[StackSummary], error)
	DescribeStackEvents(nameOrID, nextToken string) (page.Page[StackEvent], error)
	DescribeStackResource(nameOrID, logicalID string) (*StackResource, error)
	ListStackResources(nameOrID, nextToken string) (page.Page[StackResourceSummary], error)
	DescribeStackResources(nameOrID string) ([]StackResource, error)
	ListExports(nextToken string) (page.Page[Export], error)
	ListImports(exportName, nextToken string) (page.Page[string], error)
	CreateChangeSet(
		ctx context.Context,
		stackName, changeSetName, templateBody, description string,
		params []Parameter,
		capabilities []string,
		tags []Tag,
		opts CreateChangeSetOptions,
	) (*ChangeSet, error)
	DescribeChangeSet(stackName, changeSetName string) (*ChangeSet, error)
	GetChangeSetTemplate(stackName, changeSetName string) (string, error)
	ExecuteChangeSet(
		ctx context.Context, stackName, changeSetName string, disableRollback, retainExceptOnCreate bool,
	) error
	DeleteChangeSet(stackName, changeSetName string) error
	ListChangeSets(stackName, nextToken string) (page.Page[ChangeSetSummary], error)
	GetTemplate(nameOrID string) (string, error)
	ListAll() []*Stack
	// Drift detection
	DetectStackDrift(nameOrID string) (string, error)
	DetectStackResourceDrift(nameOrID, logicalID string) (*StackResourceDrift, error)
	DescribeStackDriftDetectionStatus(detectionID string) (*DriftDetectionStatus, error)
	DescribeStackResourceDrifts(nameOrID string) ([]StackResourceDrift, error)
	// Stack policy
	SetStackPolicy(nameOrID, policy string) error
	GetStackPolicy(nameOrID string) (string, error)
	// Template analysis
	GetTemplateSummary(templateBody, stackName string) (*TemplateSummary, error)
	EstimateTemplateCost(templateBody string, params []Parameter) (string, error)
	// Stack management
	ContinueUpdateRollback(ctx context.Context, nameOrID string) error
	CancelUpdateStack(ctx context.Context, nameOrID string) error
	DescribeAccountLimits() []AccountLimit
	// Stack Sets
	CreateStackSet(name, description, templateBody string, opts StackSetOptions) (*StackSet, error)
	UpdateStackSet(
		name, description, templateBody string, opts StackSetOptions, opOpts ...StackSetOpOption,
	) (*StackSet, string, error)
	DeleteStackSet(name string) error
	DescribeStackSet(name string) (*StackSet, error)
	StackSetRegions(name string) []string
	ListStackSets(maxResults int, nextToken, status string) (page.Page[StackSetSummary], error)
	CreateStackInstances(
		ctx context.Context,
		stackSetName string,
		accounts, ouIDs, regions []string,
		filterType string,
		opOpts ...StackSetOpOption,
	) (string, error)
	DeleteStackInstances(
		ctx context.Context,
		stackSetName string,
		accounts, ouIDs, regions []string,
		retainStacks bool,
		filterType string,
		opOpts ...StackSetOpOption,
	) (string, error)
	UpdateStackInstances(
		stackSetName string, accounts, ouIDs, regions []string, filterType string, opOpts ...StackSetOpOption,
	) (string, error)
	ListStackInstances(
		stackSetName string, maxResults int, nextToken string, filter ListStackInstancesFilter,
	) (page.Page[StackInstance], error)
	DescribeStackInstance(stackSetName, account, region string) (*StackInstance, error)
	DetectStackSetDrift(stackSetName string, opOpts ...StackSetOpOption) (string, error)
	ListStackSetOperations(
		stackSetName string, maxResults int, nextToken string,
	) (page.Page[StackSetOperationSummary], error)
	DescribeStackSetOperation(stackSetName, operationID string) (*StackSetOperation, error)
	StopStackSetOperation(stackSetName, operationID string) error
	ListStackSetOperationResults(
		stackSetName, operationID string, maxResults int, nextToken string, statuses []string,
	) (page.Page[StackSetOperationResult], error)
	ListStackSetAutoDeploymentTargets(
		stackSetName string, maxResults int, nextToken string,
	) (page.Page[AutoDeploymentTarget], error)
	ImportStacksToStackSet(
		stackSetName string, stackIDs, ouIDs []string, opOpts ...StackSetOpOption,
	) (string, error)
	ListStackInstanceResourceDrifts(
		stackSetName, operationID, account, region string,
	) ([]StackResourceDrift, error)
	// Generated templates
	CreateGeneratedTemplate(name string, resources []string) (*GeneratedTemplate, error)
	UpdateGeneratedTemplate(id, name string) (*GeneratedTemplate, error)
	DeleteGeneratedTemplate(id string) error
	DescribeGeneratedTemplate(id string) (*GeneratedTemplate, error)
	GetGeneratedTemplate(id string) (string, error)
	ListGeneratedTemplates(maxResults int, nextToken string) (page.Page[GeneratedTemplate], error)
	// Resource scans
	StartResourceScan(types []string) (string, error)
	DescribeResourceScan(scanID string) (*ResourceScan, error)
	ListResourceScans(maxResults int, nextToken, scanTypeFilter string) (page.Page[ResourceScan], error)
	ListResourceScanResources(scanID, nextToken string, maxResults int) (page.Page[ScannedResource], error)
	ListResourceScanRelatedResources(scanID string, resources []string) ([]string, error)
	// Type management
	ActivateType(typeName, typeArn string, opts ActivateTypeOptions) (string, error)
	DeactivateType(typeName, typeArn string) error
	RegisterType(typeName, schemaHandlerPackage string) (string, error)
	DeregisterType(typeName, typeArn, versionID string) error
	PublishType(typeName string) (string, error)
	SetTypeDefaultVersion(arn, typeName, version string) error
	SetTypeConfiguration(typeName, configuration string) (string, error)
	BatchDescribeTypeConfigurations(
		identifiers []TypeConfigurationIdentifier,
	) ([]TypeConfigurationDetail, []BatchDescribeTypeConfigurationsError, []TypeConfigurationIdentifier)
	ListTypes(
		visibilityFilter, provisioningTypeFilter, typeNamePrefix string, maxResults int, nextToken string,
	) (page.Page[TypeSummary], error)
	ListTypesFiltered(opts ListTypesOptions) (page.Page[TypeSummary], error)
	ListTypeVersions(
		typeName, deprecatedStatus string, maxResults int, nextToken string,
	) (page.Page[string], error)
	ListTypeRegistrations(
		typeName, typeFilter, registrationStatusFilter string, maxResults int, nextToken string,
	) (page.Page[string], error)
	DescribeTypeRegistration(registrationToken string) (status, typeArn, typeVersionArn string, err error)
	DescribeType(typeName, arn, versionID string) (*TypeDetails, error)
	TestType(typeName, arn, versionID string) (string, error)
	RegisterPublisher(connectionArn string) (string, error)
	DescribePublisher(publisherID string) (string, error)
	// Stack refactor
	CreateStackRefactor(
		description string,
		stackDefinitions []StackDefinition,
		resourceMappings []ResourceMapping,
		enableStackCreation bool,
	) (string, error)
	DescribeStackRefactor(stackRefactorID string) (*StackRefactor, error)
	ExecuteStackRefactor(ctx context.Context, stackRefactorID string) error
	ListStackRefactors(maxResults int, nextToken string) (page.Page[StackRefactorSummary], error)
	ListStackRefactorsFiltered(
		maxResults int, nextToken string, executionStatuses []string,
	) (page.Page[StackRefactorSummary], error)
	ListStackRefactorActions(
		stackRefactorID string, maxResults int, nextToken string,
	) (page.Page[StackRefactorAction], error)
	// Org access
	ActivateOrganizationsAccess() error
	DeactivateOrganizationsAccess() error
	DescribeOrganizationsAccess() (string, error)
	// Misc
	SignalResource(stackName, logicalID, uniqueID, status string) error
	RollbackStack(ctx context.Context, stackName string, retainExceptOnCreate bool) (*Stack, error)
	RecordHandlerProgress(bearerToken, operationStatus string) error
	GetHookResult(hookResultToken string) (string, error)
	ListHookResults(hookResultToken, nextToken string) ([]HookResult, error)
	DescribeChangeSetHooks(stackName, changeSetName string) ([]ChangeSetHook, error)
	DescribeEvents(stackName, nextToken string, failedOnly bool) (page.Page[StackEvent], error)
	UpdateTerminationProtection(stackName string, enable bool) error
	ValidateTemplate(templateBody string) (*TemplateSummary, error)
}

// InMemoryBackend is a concurrency-safe in-memory CloudFormation backend.
type InMemoryBackend struct {
	resolver            DynamicRefResolver
	orgDirectory        OrganizationsDirectory
	stackSetOperations  map[string]map[string]*StackSetOperation
	signals             map[string][]SignalRecord
	stackSets           *store.Table[StackSet]
	generatedTemplates  *store.Table[GeneratedTemplate]
	resourceScans       *store.Table[ResourceScan]
	stackSetOpResults   map[string]map[string][]StackSetOperationResult
	typeRegistrations   *store.Table[TypeRegistrationRecord]
	publishers          *store.Table[Publisher]
	stackRefactors      *store.Table[StackRefactor]
	hookResults         *store.Table[HookResult]
	stackIDIndex        map[string]string
	events              map[string][]StackEvent
	resources           map[string]map[string]*StackResource
	changeSets          map[string]map[string]*ChangeSet
	stackPolicies       map[string]string
	stackInstances      map[string][]StackInstance
	registry            *store.Registry
	typeConfigs         map[string]string
	driftDetections     *store.Table[DriftDetectionStatus]
	handlerProgress     map[string]string
	typeRegistry        *store.Table[RegisteredType]
	typeVersions        map[string][]*RegisteredTypeVersion
	resourceScanItems   map[string][]ScannedResource
	resourceDriftStatus map[string]map[string]string
	resourceDriftDetail map[string]map[string]StackResourceDrift
	driftByStackID      map[string][]string
	creator             *ResourceCreator
	exports             *store.Table[Export]
	stacks              *store.Table[Stack]
	mu                  *lockmetrics.RWMutex
	stackSetRuns        map[string]*stackSetOpRun
	stackSetQueues      map[string]*stackSetOpQueue
	region              string
	accountID           string
	opWG                sync.WaitGroup
	stackSetBatchDelay  time.Duration
	orgAccessEnabled    bool
}

const (
	MockAccountID = config.DefaultAccountID
	MockRegion    = config.DefaultRegion

	cfnStackType                   = "AWS::CloudFormation::Stack"
	statusCreateInProgress         = "CREATE_IN_PROGRESS"
	statusCreateComplete           = "CREATE_COMPLETE"
	statusReviewInProgress         = "REVIEW_IN_PROGRESS"
	executionStatusAvailable       = "AVAILABLE"
	statusCreateFailed             = "CREATE_FAILED"
	statusUpdateInProgress         = "UPDATE_IN_PROGRESS"
	statusUpdateComplete           = "UPDATE_COMPLETE"
	statusUpdateFailed             = "UPDATE_FAILED"
	statusUpdateRollbackInProgress = "UPDATE_ROLLBACK_IN_PROGRESS"
	statusUpdateRollbackComplete   = "UPDATE_ROLLBACK_COMPLETE"
	statusUpdateRollbackFailed     = "UPDATE_ROLLBACK_FAILED"
	statusDeleteInProgress         = "DELETE_IN_PROGRESS"
	statusDeleteComplete           = "DELETE_COMPLETE"
	statusDeleteFailed             = "DELETE_FAILED"
	statusRollbackInProgress       = "ROLLBACK_IN_PROGRESS"
	statusRollbackComplete         = "ROLLBACK_COMPLETE"
	statusRollbackFailed           = "ROLLBACK_FAILED"
	reasonUserInitiated            = "User Initiated"
	reasonRollbackDeleteFailed     = "rollback failed to delete one or more resources"
	deletionPolicyRetain           = "Retain"
	deletionPolicySnapshot         = "Snapshot"
)

// NewInMemoryBackend creates a new empty CloudFormation backend.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(MockAccountID, MockRegion, nil)
}

// NewInMemoryBackendWithConfig creates a new backend with the given config and resource creator.
func NewInMemoryBackendWithConfig(
	accountID, region string,
	creator *ResourceCreator,
) *InMemoryBackend {
	var resolver DynamicRefResolver
	if creator != nil {
		resolver = NewDynamicRefResolver(creator.backends)
	}

	b := &InMemoryBackend{
		registry:            store.NewRegistry(),
		stackIDIndex:        make(map[string]string),
		events:              make(map[string][]StackEvent),
		resources:           make(map[string]map[string]*StackResource),
		changeSets:          make(map[string]map[string]*ChangeSet),
		stackPolicies:       make(map[string]string),
		stackInstances:      make(map[string][]StackInstance),
		stackSetOperations:  make(map[string]map[string]*StackSetOperation),
		typeConfigs:         make(map[string]string),
		handlerProgress:     make(map[string]string),
		signals:             make(map[string][]SignalRecord),
		stackSetOpResults:   make(map[string]map[string][]StackSetOperationResult),
		typeVersions:        make(map[string][]*RegisteredTypeVersion),
		resourceScanItems:   make(map[string][]ScannedResource),
		resourceDriftStatus: make(map[string]map[string]string),
		resourceDriftDetail: make(map[string]map[string]StackResourceDrift),
		driftByStackID:      make(map[string][]string),
		creator:             creator,
		resolver:            resolver,
		accountID:           accountID,
		region:              region,
		mu:                  lockmetrics.New("cloudformation"),
	}

	registerAllTables(b)

	// Wire the backend as the NestedStackCreator so nested stacks can be provisioned.
	if creator != nil {
		creator.WithNestedStackCreator(b)
	}

	return b
}
