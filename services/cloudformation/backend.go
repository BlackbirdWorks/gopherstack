package cloudformation

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

var (
	ErrStackNotFound            = errors.New("stack with id does not exist")
	ErrStackAlreadyExists       = errors.New("stack already exists")
	ErrChangeSetNotFound        = errors.New("change set not found")
	ErrChangeSetExists          = errors.New("change set already exists")
	ErrChangeSetAlreadyExecuted = errors.New("change set has already been executed")
	ErrResourceNotFound         = errors.New("resource not found in stack")
	ErrExportNotFound           = errors.New("export with given name not found")
	ErrDuplicateExport          = errors.New("export already exists and is owned by another stack")
	ErrDriftDetectionNotFound   = errors.New("drift detection not found")
	ErrStackSetNotFound         = errors.New("stack set not found")
	ErrStackSetAlreadyExists    = errors.New("stack set already exists")
	ErrStackSetNotEmpty         = errors.New(
		"stack set is not empty: delete all stack instances before deleting the stack set",
	)
	ErrStackInstanceNotFound      = errors.New("stack instance not found")
	ErrStackInstanceAlreadyExists = errors.New(
		"stack instance already exists in this account/region",
	)
	ErrGeneratedTemplateNotFound = errors.New("generated template not found")
	ErrResourceScanNotFound      = errors.New("resource scan not found")
	ErrOperationNotFound         = errors.New("operation not found in stack set")
	ErrOperationNotRunning       = errors.New("operation is not in RUNNING state")
	ErrTypeNotFound              = errors.New("type not found")
	ErrTypeVersionNotFound       = errors.New("type version not found")
	ErrRegistrationTokenNotFound = errors.New("registration token not found")
	ErrPublisherNotFound         = errors.New("publisher not found")
	ErrInvalidRoleARN            = errors.New("invalid IAM role ARN format")
	ErrInsufficientCapabilities  = errors.New(
		"requires capabilities: CAPABILITY_IAM or CAPABILITY_NAMED_IAM",
	)
)

// StackOptions carries optional attributes for CreateStack and UpdateStack.
type StackOptions struct {
	RollbackConfiguration *RollbackConfiguration
	RoleARN               string
	OnFailure             string // DELETE | ROLLBACK | DO_NOTHING
	Capabilities          []string
	NotificationARNs      []string
	Tags                  []Tag
	TimeoutInMinutes      int
	DisableRollback       bool
}

// StorageBackend defines the interface for the CloudFormation in-memory backend.

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
	DescribeStackEvents(nameOrID string) ([]StackEvent, error)
	DescribeStackResource(nameOrID, logicalID string) (*StackResource, error)
	ListStackResources(nameOrID, nextToken string) (page.Page[StackResourceSummary], error)
	DescribeStackResources(nameOrID string) ([]StackResource, error)
	ListExports(nextToken string) (page.Page[Export], error)
	ListImports(exportName, nextToken string) (page.Page[string], error)
	CreateChangeSet(
		ctx context.Context,
		stackName, changeSetName, templateBody, description string,
		params []Parameter,
	) (*ChangeSet, error)
	DescribeChangeSet(stackName, changeSetName string) (*ChangeSet, error)
	ExecuteChangeSet(ctx context.Context, stackName, changeSetName string) error
	DeleteChangeSet(stackName, changeSetName string) error
	ListChangeSets(stackName, nextToken string) (page.Page[ChangeSetSummary], error)
	GetTemplate(nameOrID string) (string, error)
	ListAll() []*Stack
	// Drift detection
	DetectStackDrift(nameOrID string) (string, error)
	DetectStackResourceDrift(nameOrID, logicalID string) (string, error)
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
	CreateStackSet(name, description, templateBody string) (*StackSet, error)
	UpdateStackSet(name, description, templateBody string) (*StackSet, error)
	DeleteStackSet(name string) error
	DescribeStackSet(name string) (*StackSet, error)
	ListStackSets(nextToken string) ([]StackSetSummary, error)
	CreateStackInstances(stackSetName string, accounts, regions []string) (string, error)
	DeleteStackInstances(stackSetName string, accounts, regions []string) (string, error)
	UpdateStackInstances(stackSetName string, accounts, regions []string) (string, error)
	ListStackInstances(stackSetName, nextToken string) ([]StackInstance, error)
	DescribeStackInstance(stackSetName, account, region string) (*StackInstance, error)
	DetectStackSetDrift(stackSetName string) (string, error)
	ListStackSetOperations(stackSetName, nextToken string) ([]string, error)
	DescribeStackSetOperation(stackSetName, operationID string) (*StackSetOperation, error)
	StopStackSetOperation(stackSetName, operationID string) error
	ListStackSetOperationResults(
		stackSetName, operationID, nextToken string,
	) ([]StackSetOperationResult, error)
	ListStackSetAutoDeploymentTargets(stackSetName string) ([]string, error)
	ImportStacksToStackSet(stackSetName string, stackIDs []string) error
	ListStackInstanceResourceDrifts(
		stackSetName, operationID, account, region string,
	) ([]string, error)
	// Generated templates
	CreateGeneratedTemplate(name string, resources []string) (*GeneratedTemplate, error)
	UpdateGeneratedTemplate(id, name string) error
	DeleteGeneratedTemplate(id string) error
	DescribeGeneratedTemplate(id string) (*GeneratedTemplate, error)
	GetGeneratedTemplate(id string) (string, error)
	ListGeneratedTemplates(nextToken string) ([]GeneratedTemplate, error)
	// Resource scans
	StartResourceScan() (string, error)
	DescribeResourceScan(scanID string) (*ResourceScan, error)
	ListResourceScans(nextToken string) ([]ResourceScan, error)
	ListResourceScanResources(scanID, nextToken string) ([]ScannedResource, error)
	ListResourceScanRelatedResources(scanID string, resources []string) ([]string, error)
	// Type management
	ActivateType(typeName, typeArn string) error
	DeactivateType(typeName, typeArn string) error
	RegisterType(typeName, schemaHandlerPackage string) (string, error)
	DeregisterType(arn string) error
	PublishType(typeName string) error
	SetTypeDefaultVersion(arn, version string) error
	SetTypeConfiguration(typeName, configuration string) error
	BatchDescribeTypeConfigurations(typeConfigIdentifiers []string) ([]string, error)
	ListTypes(nextToken string) ([]TypeSummary, error)
	ListTypeVersions(typeName, nextToken string) ([]string, error)
	ListTypeRegistrations(typeName, nextToken string) ([]string, error)
	DescribeTypeRegistration(registrationToken string) (string, error)
	DescribeType(typeName, arn, versionID string) (*TypeDetails, error)
	TestType(typeName, arn string) (string, error)
	RegisterPublisher(connectionArn string) (string, error)
	DescribePublisher(publisherID string) (string, error)
	// Stack refactor
	CreateStackRefactor(description string, stackDefinitions []string) (string, error)
	DescribeStackRefactor(stackRefactorID string) (string, error)
	ExecuteStackRefactor(stackRefactorID string) error
	ListStackRefactors(nextToken string) ([]string, error)
	ListStackRefactorActions(stackRefactorID string) ([]string, error)
	// Org access
	ActivateOrganizationsAccess() error
	DeactivateOrganizationsAccess() error
	DescribeOrganizationsAccess() (string, error)
	// Misc
	SignalResource(stackName, logicalID, uniqueID, status string) error
	RollbackStack(ctx context.Context, stackName string) error
	RecordHandlerProgress(bearerToken, operationStatus string) error
	GetHookResult(hookResultToken string) (string, error)
	ListHookResults(hookResultToken, nextToken string) ([]string, error)
	DescribeChangeSetHooks(stackName, changeSetName string) ([]string, error)
	DescribeEvents(nextToken string) ([]StackEvent, error)
	UpdateTerminationProtection(stackName string, enable bool) error
	ValidateTemplate(templateBody string) (*TemplateSummary, error)
}

// InMemoryBackend is a concurrency-safe in-memory CloudFormation backend.
type InMemoryBackend struct {
	stacks              map[string]*Stack
	stackIDIndex        map[string]string // stackID (ARN) → stackName
	events              map[string][]StackEvent
	resources           map[string]map[string]*StackResource
	changeSets          map[string]map[string]*ChangeSet
	exports             map[string]*Export
	driftDetections     map[string]*DriftDetectionStatus
	stackPolicies       map[string]string
	stackSets           map[string]*StackSet
	stackInstances      map[string][]StackInstance               // stackSetName → instances
	stackSetOperations  map[string]map[string]*StackSetOperation // stackSetName → operationID → op
	generatedTemplates  map[string]*GeneratedTemplate
	resourceScans       map[string]*ResourceScan
	typeRegistry        map[string]*RegisteredType                      // typeArn → type
	typeRegistrations   map[string]*TypeRegistrationRecord              // token → record
	typeConfigs         map[string]string                               // typeName → config json
	publishers          map[string]*Publisher                           // publisherID → publisher
	stackRefactors      map[string]*StackRefactor                       // refactorID → refactor
	hookResults         map[string]*HookResult                          // token → result
	handlerProgress     map[string]string                               // bearerToken → status
	signals             map[string][]SignalRecord                       // stackName+logicalID → records
	stackSetOpResults   map[string]map[string][]StackSetOperationResult // stackSetName → opID → results
	typeVersions        map[string][]*RegisteredTypeVersion             // typeArn → versions
	resourceScanItems   map[string][]ScannedResource                    // scanID → scanned resources
	resourceDriftStatus map[string]map[string]string                    // stackID → logicalID → drift status
	creator             *ResourceCreator
	resolver            DynamicRefResolver
	mu                  *lockmetrics.RWMutex
	accountID           string
	region              string
	orgAccessEnabled    bool
}

const (
	MockAccountID = config.DefaultAccountID
	MockRegion    = config.DefaultRegion

	cfnStackType                   = "AWS::CloudFormation::Stack"
	statusCreateInProgress         = "CREATE_IN_PROGRESS"
	statusCreateComplete           = "CREATE_COMPLETE"
	statusCreateFailed             = "CREATE_FAILED"
	statusUpdateInProgress         = "UPDATE_IN_PROGRESS"
	statusUpdateComplete           = "UPDATE_COMPLETE"
	statusUpdateFailed             = "UPDATE_FAILED"
	statusUpdateRollbackInProgress = "UPDATE_ROLLBACK_IN_PROGRESS"
	statusUpdateRollbackComplete   = "UPDATE_ROLLBACK_COMPLETE"
	statusDeleteInProgress         = "DELETE_IN_PROGRESS"
	statusDeleteComplete           = "DELETE_COMPLETE"
	statusRollbackInProgress       = "ROLLBACK_IN_PROGRESS"
	statusRollbackComplete         = "ROLLBACK_COMPLETE"
	reasonUserInitiated            = "User Initiated"
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
		stacks:              make(map[string]*Stack),
		stackIDIndex:        make(map[string]string),
		events:              make(map[string][]StackEvent),
		resources:           make(map[string]map[string]*StackResource),
		changeSets:          make(map[string]map[string]*ChangeSet),
		exports:             make(map[string]*Export),
		driftDetections:     make(map[string]*DriftDetectionStatus),
		stackPolicies:       make(map[string]string),
		stackSets:           make(map[string]*StackSet),
		stackInstances:      make(map[string][]StackInstance),
		stackSetOperations:  make(map[string]map[string]*StackSetOperation),
		generatedTemplates:  make(map[string]*GeneratedTemplate),
		resourceScans:       make(map[string]*ResourceScan),
		typeRegistry:        make(map[string]*RegisteredType),
		typeRegistrations:   make(map[string]*TypeRegistrationRecord),
		typeConfigs:         make(map[string]string),
		publishers:          make(map[string]*Publisher),
		stackRefactors:      make(map[string]*StackRefactor),
		hookResults:         make(map[string]*HookResult),
		handlerProgress:     make(map[string]string),
		signals:             make(map[string][]SignalRecord),
		stackSetOpResults:   make(map[string]map[string][]StackSetOperationResult),
		typeVersions:        make(map[string][]*RegisteredTypeVersion),
		resourceScanItems:   make(map[string][]ScannedResource),
		resourceDriftStatus: make(map[string]map[string]string),
		creator:             creator,
		resolver:            resolver,
		accountID:           accountID,
		region:              region,
		mu:                  lockmetrics.New("cloudformation"),
	}

	// Wire the backend as the NestedStackCreator so nested stacks can be provisioned.
	if creator != nil {
		creator.WithNestedStackCreator(b)
	}

	return b
}

// CreateNestedStack implements NestedStackCreator. Must be called while b.mu is held by caller.
func (b *InMemoryBackend) CreateNestedStack(
	ctx context.Context,
	name, _ /* templateURL */, templateBody string,
	params []Parameter,
) (string, error) {
	// Lock already held by parent CreateStack — use the no-lock variant.
	stack, err := b.createStackLocked(ctx, name, templateBody, params, StackOptions{}, "")
	if err != nil {
		return "", err
	}

	return stack.StackID, nil
}

// DeleteNestedStack implements NestedStackCreator. Must be called while b.mu is held by caller.
func (b *InMemoryBackend) DeleteNestedStack(ctx context.Context, stackID string) error {
	return b.deleteStackLocked(ctx, stackID)
}

// deleteStackLocked deletes a stack without acquiring the mutex — callers must hold b.mu.
func (b *InMemoryBackend) deleteStackLocked(ctx context.Context, nameOrID string) error {
	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return ErrStackNotFound
	}

	if stack.EnableTerminationProtection {
		return fmt.Errorf("%w: %s", ErrTerminationProtectionEnabled, stack.StackName)
	}

	stack.StackStatus = statusDeleteInProgress
	b.addEvent(
		stack.StackID,
		stack.StackName,
		stack.StackName,
		stack.StackID,
		cfnStackType,
		statusDeleteInProgress,
		reasonUserInitiated,
	)

	for logicalID, res := range b.resources[stack.StackID] {
		b.addEvent(
			stack.StackID,
			stack.StackName,
			logicalID,
			res.PhysicalID,
			res.Type,
			statusDeleteInProgress,
			"",
		)
		_ = b.creator.Delete(ctx, res.Type, res.PhysicalID, res.Properties)
		b.addEvent(
			stack.StackID,
			stack.StackName,
			logicalID,
			res.PhysicalID,
			res.Type,
			statusDeleteComplete,
			"",
		)
	}

	now := time.Now()
	stack.DeletionTime = &now
	stack.StackStatus = statusDeleteComplete
	b.removeExports(stack.StackID)
	delete(b.stackPolicies, stack.StackID)
	delete(b.events, stack.StackID)
	delete(b.resources, stack.StackID)
	delete(b.changeSets, stack.StackName)
	b.pruneDriftDetections(stack.StackID)

	return nil
}

func (b *InMemoryBackend) buildStackARN(stackName, stackID string) string {
	return arn.Build("cloudformation", b.region, b.accountID, "stack/"+stackName+"/"+stackID)
}

func (b *InMemoryBackend) resolveStack(nameOrID string) (*Stack, bool) {
	if s, ok := b.stacks[nameOrID]; ok {
		return s, true
	}

	if name, ok := b.stackIDIndex[nameOrID]; ok {
		if s, found := b.stacks[name]; found {
			return s, true
		}
	}

	return nil, false
}

// addEvent appends an event to the stack's event history.
func (b *InMemoryBackend) addEvent(
	stackID, stackName, logicalID, physicalID, resourceType, status, reason string,
) {
	evt := StackEvent{
		EventID:              uuid.New().String(),
		StackID:              stackID,
		StackName:            stackName,
		LogicalResourceID:    logicalID,
		PhysicalResourceID:   physicalID,
		ResourceType:         resourceType,
		ResourceStatus:       status,
		ResourceStatusReason: reason,
		Timestamp:            time.Now(),
	}
	b.events[stackID] = append(b.events[stackID], evt)

	const maxEvents = 1000
	if len(b.events[stackID]) > maxEvents {
		b.events[stackID] = b.events[stackID][len(b.events[stackID])-maxEvents:]
	}
}

// CreateStack creates a new stack from a template.
func (b *InMemoryBackend) CreateStack(
	ctx context.Context,
	name, templateBody string,
	params []Parameter,
	opts StackOptions,
) (*Stack, error) {
	b.mu.Lock("CreateStack")
	defer b.mu.Unlock()

	return b.createStackLocked(ctx, name, templateBody, params, opts, "")
}

// createStackLocked is the lock-free body of CreateStack — callers must hold b.mu.
// parentID is set when provisioning a nested stack.
func (b *InMemoryBackend) createStackLocked(
	ctx context.Context,
	name, templateBody string,
	params []Parameter,
	opts StackOptions,
	parentID string,
) (*Stack, error) {
	// Validate RoleARN format and IAM capability requirements (top-level stacks only).
	if parentID == "" {
		if err := validateStackOptions(templateBody, opts); err != nil {
			return nil, err
		}
	}

	if existing, ok := b.stacks[name]; ok {
		if existing.StackStatus != statusDeleteComplete {
			return nil, ErrStackAlreadyExists
		}
		// Remove the old stack ID from the index before re-creating.
		delete(b.stackIDIndex, existing.StackID)
	}

	stackID := uuid.New().String()
	arn := b.buildStackARN(name, stackID)
	now := time.Now()

	stack := &Stack{
		StackID:               arn,
		StackName:             name,
		StackStatus:           statusCreateInProgress,
		CreationTime:          now,
		Parameters:            params,
		Tags:                  opts.Tags,
		TemplateBody:          templateBody,
		Capabilities:          opts.Capabilities,
		NotificationARNs:      opts.NotificationARNs,
		RoleARN:               opts.RoleARN,
		TimeoutInMinutes:      opts.TimeoutInMinutes,
		DisableRollback:       opts.DisableRollback,
		RollbackConfiguration: opts.RollbackConfiguration,
		ParentID:              parentID,
	}

	b.stacks[name] = stack
	b.stackIDIndex[arn] = name
	b.events[arn] = nil
	b.resources[arn] = make(map[string]*StackResource)

	b.addEvent(arn, name, name, arn, cfnStackType, statusCreateInProgress, reasonUserInitiated)

	// Parse and provision resources.
	if templateBody != "" {
		b.createStackFromTemplate(ctx, stack, params)
	}

	if stack.StackStatus != statusCreateFailed && stack.StackStatus != statusRollbackComplete {
		stack.StackStatus = statusCreateComplete
		b.addEvent(arn, name, name, arn, cfnStackType, statusCreateComplete, "")
	}

	// OnFailure=DELETE: remove the stack entirely when creation fails.
	if opts.OnFailure == "DELETE" &&
		(stack.StackStatus == statusCreateFailed || stack.StackStatus == statusRollbackComplete) {
		stack.StackStatus = statusDeleteInProgress
		b.addEvent(arn, name, name, arn, cfnStackType, statusDeleteInProgress, "")
		now2 := time.Now()
		stack.DeletionTime = &now2
		stack.StackStatus = statusDeleteComplete
		b.removeExports(arn)
		delete(b.events, arn)
		delete(b.resources, arn)
		delete(b.changeSets, name)
		b.pruneDriftDetections(arn)
	}

	return stack, nil
}

// createStackFromTemplate parses and applies a template during CreateStack.
// It updates stack.StackStatus on failure.
func (b *InMemoryBackend) createStackFromTemplate(
	ctx context.Context,
	stack *Stack,
	params []Parameter,
) {
	arn := stack.StackID
	name := stack.StackName

	tmpl, err := ParseTemplate(stack.TemplateBody)
	if err != nil {
		stack.StackStatus = statusCreateFailed
		stack.StackStatusReason = err.Error()
		b.addEvent(arn, name, name, arn, cfnStackType, statusCreateFailed, err.Error())

		return
	}
	stack.Description = tmpl.Description

	if dynErr := ResolveDynamicRefsInTemplate(tmpl, b.resolver); dynErr != nil {
		stack.StackStatus = statusCreateFailed
		stack.StackStatusReason = dynErr.Error()
		b.addEvent(arn, name, name, arn, cfnStackType, statusCreateFailed, dynErr.Error())

		return
	}

	resolvedParams := ResolveParameters(tmpl, params)

	if valErr := ValidateParameters(tmpl, resolvedParams); valErr != nil {
		b.failAndRollback(stack, valErr.Error())

		return
	}

	// Validate intrinsic references (Fn::GetAtt / Fn::Sub to undefined
	// resources, unsupported resource types) before provisioning anything.
	if intErr := validateIntrinsics(tmpl); intErr != nil {
		b.failAndRollback(stack, intErr.Error())

		return
	}

	// Validate that all Fn::ImportValue references can be satisfied before
	// creating any resources.
	if impErr := validateImportValues(tmpl, resolvedParams, b.buildExportsMap()); impErr != nil {
		b.failAndRollback(stack, impErr.Error())

		return
	}

	physicalIDs := b.provisionResources(ctx, stack, tmpl, resolvedParams)
	if stack.StackStatus == statusCreateFailed || stack.StackStatus == statusRollbackComplete {
		return
	}

	resourceTypes := make(map[string]string, len(tmpl.Resources))
	for logicalID, res := range tmpl.Resources {
		resourceTypes[logicalID] = res.Type
	}
	rctx := resolveCtx{
		params:        resolvedParams,
		physicalIDs:   physicalIDs,
		resourceTypes: resourceTypes,
		exports:       b.buildExportsMap(),
		conditions:    evaluateConditions(tmpl.Conditions, resolvedParams, physicalIDs),
		mappings:      tmpl.Mappings,
		accountID:     b.accountID,
		region:        b.region,
		stackName:     name,
	}
	var exportMap map[string]string
	stack.Outputs, exportMap = resolveOutputsWithContext(tmpl, rctx)

	if regErr := b.registerExports(stack.StackID, exportMap); regErr != nil {
		stack.StackStatus = statusCreateFailed
		stack.StackStatusReason = regErr.Error()
		b.addEvent(arn, name, name, arn, cfnStackType, statusCreateFailed, regErr.Error())
	}
}

// updateFailAndRollback records UPDATE_FAILED then emits UPDATE_ROLLBACK_IN_PROGRESS /
// UPDATE_ROLLBACK_COMPLETE for pre-flight update failures (no resources changed).
func (b *InMemoryBackend) updateFailAndRollback(stack *Stack, reason string) {
	arn := stack.StackID
	name := stack.StackName
	stack.StackStatusReason = reason
	b.addEvent(arn, name, name, arn, cfnStackType, statusUpdateFailed, reason)
	b.addEvent(arn, name, name, arn, cfnStackType, statusUpdateRollbackInProgress, reason)
	b.addEvent(arn, name, name, arn, cfnStackType, statusUpdateRollbackComplete, "")
	stack.StackStatus = statusUpdateRollbackComplete
}

// failAndRollback records a pre-flight CREATE_FAILED then immediately emits
// ROLLBACK_IN_PROGRESS / ROLLBACK_COMPLETE (no resources to undo).
func (b *InMemoryBackend) failAndRollback(stack *Stack, reason string) {
	arn := stack.StackID
	name := stack.StackName
	stack.StackStatusReason = reason
	b.addEvent(arn, name, name, arn, cfnStackType, statusCreateFailed, reason)
	b.addEvent(arn, name, name, arn, cfnStackType, statusRollbackInProgress, reason)
	b.addEvent(arn, name, name, arn, cfnStackType, statusRollbackComplete, "")
	stack.StackStatus = statusRollbackComplete
}

// provisionResources creates all resources defined in the template.
// Returns the physicalIDs map. On resource creation failure, rollback is
// performed in reverse order; stack.StackStatus is then set to
// statusRollbackComplete (matching real AWS behaviour). If the creation failure
// itself needs to be recorded separately, it is preserved in StackStatusReason.
func (b *InMemoryBackend) provisionResources(
	ctx context.Context,
	stack *Stack,
	tmpl *Template,
	resolvedParams map[string]string,
) map[string]string {
	arn := stack.StackID
	name := stack.StackName
	physicalIDs := make(map[string]string)

	// Inject stack metadata for custom resource event payloads.
	physicalIDs["_StackId"] = arn
	physicalIDs["_StackName"] = name

	ordered := topoSortResources(tmpl.Resources)

	created := make([]string, 0, len(ordered))

	for _, logicalID := range ordered {
		res := tmpl.Resources[logicalID]
		b.addEvent(arn, name, logicalID, "", res.Type, statusCreateInProgress, "")
		physicalID, cerr := b.creator.Create(
			ctx,
			logicalID,
			res.Type,
			res.Properties,
			resolvedParams,
			physicalIDs,
		)
		if cerr != nil {
			stack.StackStatusReason = fmt.Sprintf("resource %s: %v", logicalID, cerr)
			b.addEvent(arn, name, logicalID, "", res.Type, statusCreateFailed, cerr.Error())
			b.addEvent(arn, name, name, arn, cfnStackType, statusRollbackInProgress, cerr.Error())
			b.rollbackCreateResources(ctx, stack, created)
			b.addEvent(arn, name, name, arn, cfnStackType, statusRollbackComplete, "")
			stack.StackStatus = statusRollbackComplete

			return physicalIDs
		}
		physicalIDs[logicalID] = physicalID
		b.resources[arn][logicalID] = &StackResource{
			Timestamp:  time.Now(),
			LogicalID:  logicalID,
			PhysicalID: physicalID,
			Type:       res.Type,
			Status:     statusCreateComplete,
			Properties: res.Properties,
			StackID:    arn,
			StackName:  name,
		}
		b.addEvent(arn, name, logicalID, physicalID, res.Type, statusCreateComplete, "")
		created = append(created, logicalID)
	}

	return physicalIDs
}

// rollbackCreateResources deletes all resources that were created during a
// failed CreateStack provisioning pass, in reverse order.
func (b *InMemoryBackend) rollbackCreateResources(
	ctx context.Context,
	stack *Stack,
	created []string,
) {
	for _, v := range slices.Backward(created) {
		logicalID := v
		res, ok := b.resources[stack.StackID][logicalID]
		if !ok {
			continue
		}

		b.addEvent(
			stack.StackID,
			stack.StackName,
			logicalID,
			res.PhysicalID,
			res.Type,
			statusDeleteInProgress,
			"",
		)
		_ = b.creator.Delete(ctx, res.Type, res.PhysicalID, res.Properties)
		b.addEvent(
			stack.StackID,
			stack.StackName,
			logicalID,
			res.PhysicalID,
			res.Type,
			statusDeleteComplete,
			"",
		)
		delete(b.resources[stack.StackID], logicalID)
	}
}

// topoSortResources returns the logical resource IDs in an order that respects
// DependsOn declarations. Resources with no dependencies come first; within the
// same dependency level they are ordered alphabetically for determinism.
// If a cycle is detected the function falls back to plain alphabetical order.
func topoSortResources(resources map[string]TemplateResource) []string {
	// Collect all known IDs in alphabetical order for determinism.
	all := make([]string, 0, len(resources))
	for id := range resources {
		all = append(all, id)
	}

	sort.Strings(all)

	// Build forward-dependency map (id → ids it depends on) and
	// reverse-dependency map (id → ids that depend on it) simultaneously.
	deps := make(map[string][]string, len(resources))
	revDeps := make(map[string][]string, len(resources))

	for _, id := range all {
		res := resources[id]
		if len(res.DependsOn) > 0 {
			deps[id] = res.DependsOn
			for _, dep := range res.DependsOn {
				revDeps[dep] = append(revDeps[dep], id)
			}
		}
	}

	// Kahn's algorithm for topological sort.
	// inDegree counts how many declared dependencies are still unprocessed.
	inDegree := make(map[string]int, len(all))
	for _, id := range all {
		inDegree[id] = len(deps[id])
	}

	// Process nodes with zero in-degree first (alphabetical order for determinism).
	queue := make([]string, 0, len(all))
	for _, id := range all {
		if inDegree[id] == 0 {
			queue = append(queue, id)
		}
	}

	result := make([]string, 0, len(all))

	for len(queue) > 0 {
		// Pop first element (queue is kept sorted).
		cur := queue[0]
		queue = queue[1:]
		result = append(result, cur)

		// Use the reverse-dependency map for O(1) lookup of dependents.
		for _, dependent := range revDeps[cur] {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				insertSorted(&queue, dependent)
			}
		}
	}

	// Cycle detected: fall back to alphabetical order (best-effort).
	if len(result) < len(all) {
		return all
	}

	return result
}

// insertSorted inserts s into the sorted slice ss, maintaining ascending order.
func insertSorted(ss *[]string, s string) {
	i := sort.SearchStrings(*ss, s)
	*ss = append(*ss, "")
	copy((*ss)[i+1:], (*ss)[i:])
	(*ss)[i] = s
}

// UpdateStack updates an existing stack.
func (b *InMemoryBackend) UpdateStack(
	ctx context.Context,
	nameOrID, templateBody string,
	params []Parameter,
	opts StackOptions,
) (*Stack, error) {
	b.mu.Lock("UpdateStack")
	defer b.mu.Unlock()

	// Validate RoleARN format and IAM capability requirements.
	if err := validateStackOptions(templateBody, opts); err != nil {
		return nil, err
	}

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return nil, ErrStackNotFound
	}

	now := time.Now()
	stack.LastUpdatedTime = &now
	stack.StackStatus = statusUpdateInProgress

	if templateBody != "" {
		stack.TemplateBody = templateBody
	}
	if params != nil {
		stack.Parameters = params
	}
	if opts.RoleARN != "" {
		stack.RoleARN = opts.RoleARN
	}
	if len(opts.Capabilities) > 0 {
		stack.Capabilities = opts.Capabilities
	}
	if len(opts.Tags) > 0 {
		stack.Tags = opts.Tags
	}
	if opts.RollbackConfiguration != nil {
		stack.RollbackConfiguration = opts.RollbackConfiguration
	}

	b.addEvent(
		stack.StackID, stack.StackName, stack.StackName, stack.StackID,
		cfnStackType, statusUpdateInProgress, reasonUserInitiated,
	)

	if !b.applyTemplateToStack(ctx, stack) {
		return stack, nil
	}

	stack.StackStatus = statusUpdateComplete
	b.addEvent(
		stack.StackID, stack.StackName, stack.StackName, stack.StackID,
		cfnStackType, statusUpdateComplete, "",
	)

	return stack, nil
}

// applyTemplateToStack parses the stack's template and creates or updates resources.
// Returns true on success; on failure it sets the stack status and returns false.
func (b *InMemoryBackend) applyTemplateToStack(ctx context.Context, stack *Stack) bool {
	if stack.TemplateBody == "" {
		return true
	}

	tmpl, err := ParseTemplate(stack.TemplateBody)
	if err != nil {
		stack.StackStatus = statusUpdateFailed
		stack.StackStatusReason = err.Error()

		return false
	}

	stack.Description = tmpl.Description

	if dynErr := ResolveDynamicRefsInTemplate(tmpl, b.resolver); dynErr != nil {
		stack.StackStatus = statusUpdateFailed
		stack.StackStatusReason = dynErr.Error()
		b.addEvent(
			stack.StackID, stack.StackName, stack.StackName, stack.StackID,
			cfnStackType, statusUpdateFailed, dynErr.Error(),
		)

		return false
	}

	resolvedParams := ResolveParameters(tmpl, stack.Parameters)

	if valErr := ValidateParameters(tmpl, resolvedParams); valErr != nil {
		b.updateFailAndRollback(stack, valErr.Error())

		return false
	}

	// Validate intrinsic references before mutating any resource.
	if intErr := validateIntrinsics(tmpl); intErr != nil {
		b.updateFailAndRollback(stack, intErr.Error())

		return false
	}

	// Pre-populate physicalIDs from existing resources.
	physicalIDs := make(map[string]string, len(b.resources[stack.StackID]))
	for logicalID, res := range b.resources[stack.StackID] {
		physicalIDs[logicalID] = res.PhysicalID
	}

	// Validate that all Fn::ImportValue references can be satisfied before
	// updating any resources.
	if impErr := validateImportValues(tmpl, resolvedParams, b.buildExportsMap()); impErr != nil {
		b.updateFailAndRollback(stack, impErr.Error())

		return false
	}

	if !b.updateResources(ctx, stack, tmpl, resolvedParams, physicalIDs) {
		return false
	}

	b.removeExports(stack.StackID)

	updateResourceTypes := make(map[string]string, len(tmpl.Resources))
	for logicalID, res := range tmpl.Resources {
		updateResourceTypes[logicalID] = res.Type
	}
	rctx := resolveCtx{
		params:        resolvedParams,
		physicalIDs:   physicalIDs,
		resourceTypes: updateResourceTypes,
		exports:       b.buildExportsMap(),
		conditions:    evaluateConditions(tmpl.Conditions, resolvedParams, physicalIDs),
		mappings:      tmpl.Mappings,
		accountID:     b.accountID,
		region:        b.region,
		stackName:     stack.StackName,
	}

	var exportMap map[string]string
	stack.Outputs, exportMap = resolveOutputsWithContext(tmpl, rctx)

	if regErr := b.registerExports(stack.StackID, exportMap); regErr != nil {
		stack.StackStatus = statusUpdateFailed
		stack.StackStatusReason = regErr.Error()
		b.addEvent(
			stack.StackID, stack.StackName, stack.StackName, stack.StackID,
			cfnStackType, statusUpdateFailed, regErr.Error(),
		)

		return false
	}

	return true
}

// updateResources reconciles existing resources and creates newly declared ones.
// On creation failure it rolls back: newly-created resources are deleted and
// previously-existing resources are restored to their pre-update state.
// Stale resources (present in the stack but absent from the new template) are
// deleted after all new resources are created successfully.
// Returns true on success; on failure it sets stack.StackStatus to
// UPDATE_ROLLBACK_COMPLETE and returns false.
func (b *InMemoryBackend) updateResources(
	ctx context.Context,
	stack *Stack,
	tmpl *Template,
	resolvedParams map[string]string,
	physicalIDs map[string]string,
) bool {
	// Snapshot pre-update state for rollback.
	prevResources := make(map[string]*StackResource, len(b.resources[stack.StackID]))
	for k, v := range b.resources[stack.StackID] {
		cp := *v
		prevResources[k] = &cp
	}

	var created []string

	for logicalID, res := range tmpl.Resources {
		existing, exists := b.resources[stack.StackID][logicalID]
		if !exists {
			physicalID, cerr := b.createUpdateResource(ctx, stack, logicalID, res, resolvedParams, physicalIDs)
			if cerr != nil {
				b.rollbackUpdateResources(ctx, stack, prevResources, created)
				stack.StackStatusReason = fmt.Sprintf("resource %s: %v", logicalID, cerr)

				return false
			}

			physicalIDs[logicalID] = physicalID
			created = append(created, logicalID)

			continue
		}

		if uerr := b.updateExistingResource(ctx, stack, logicalID, res, existing); uerr != nil {
			b.rollbackUpdateResources(ctx, stack, prevResources, created)
			stack.StackStatusReason = fmt.Sprintf("resource %s update: %v", logicalID, uerr)

			return false
		}
	}

	b.deleteStaleResources(ctx, stack, tmpl)

	return true
}

// createUpdateResource creates a new resource during a stack update and registers it.
func (b *InMemoryBackend) createUpdateResource(
	ctx context.Context,
	stack *Stack,
	logicalID string,
	res TemplateResource,
	resolvedParams, physicalIDs map[string]string,
) (string, error) {
	b.addEvent(stack.StackID, stack.StackName, logicalID, "", res.Type, statusCreateInProgress, "")
	physicalID, cerr := b.creator.Create(ctx, logicalID, res.Type, res.Properties, resolvedParams, physicalIDs)
	if cerr != nil {
		b.addEvent(stack.StackID, stack.StackName, logicalID, "", res.Type, statusCreateFailed, cerr.Error())

		return "", cerr
	}

	b.resources[stack.StackID][logicalID] = &StackResource{
		Timestamp:  time.Now(),
		LogicalID:  logicalID,
		PhysicalID: physicalID,
		Type:       res.Type,
		Status:     statusCreateComplete,
		Properties: res.Properties,
		StackID:    stack.StackID,
		StackName:  stack.StackName,
	}
	b.addEvent(stack.StackID, stack.StackName, logicalID, physicalID, res.Type, statusCreateComplete, "")

	return physicalID, nil
}

// updateExistingResource processes an existing resource during a stack update.
// For CFN extensibility types it sends an Update event to the backing Lambda/SNS.
func (b *InMemoryBackend) updateExistingResource(
	ctx context.Context,
	stack *Stack,
	logicalID string,
	res TemplateResource,
	existing *StackResource,
) error {
	if isCFNExtensibilityType(res.Type) {
		b.addEvent(stack.StackID, stack.StackName, logicalID, existing.PhysicalID, res.Type, statusUpdateInProgress, "")
		uerr := b.creator.Update(ctx, logicalID, res.Type, existing.PhysicalID, res.Properties, existing.Properties)
		if uerr != nil {
			b.addEvent(
				stack.StackID, stack.StackName, logicalID,
				existing.PhysicalID, res.Type, statusUpdateFailed, uerr.Error(),
			)

			return uerr
		}

		existing.Properties = res.Properties
	}

	existing.Status = statusUpdateComplete
	existing.Timestamp = time.Now()
	b.addEvent(stack.StackID, stack.StackName, logicalID, existing.PhysicalID, res.Type, statusUpdateComplete, "")

	return nil
}

// deleteStaleResources removes logical IDs present in the stack but absent from the new template.
func (b *InMemoryBackend) deleteStaleResources(ctx context.Context, stack *Stack, tmpl *Template) {
	var stale []string
	for logicalID := range b.resources[stack.StackID] {
		if _, inTemplate := tmpl.Resources[logicalID]; !inTemplate {
			stale = append(stale, logicalID)
		}
	}

	sort.Strings(stale)

	for _, logicalID := range stale {
		res := b.resources[stack.StackID][logicalID]
		b.addEvent(stack.StackID, stack.StackName, logicalID, res.PhysicalID, res.Type, statusDeleteInProgress, "")
		_ = b.creator.Delete(ctx, res.Type, res.PhysicalID, res.Properties)
		b.addEvent(stack.StackID, stack.StackName, logicalID, res.PhysicalID, res.Type, statusDeleteComplete, "")
		delete(b.resources[stack.StackID], logicalID)
	}
}

// rollbackUpdateResources undoes a partially-applied update: it deletes every
// resource that was newly created in this update pass and restores resources that
// were modified to their pre-update snapshots, then sets the stack status to
// UPDATE_ROLLBACK_COMPLETE.
func (b *InMemoryBackend) rollbackUpdateResources(
	ctx context.Context,
	stack *Stack,
	prevResources map[string]*StackResource,
	created []string,
) {
	stack.StackStatus = statusUpdateRollbackInProgress
	b.addEvent(
		stack.StackID, stack.StackName, stack.StackName, stack.StackID,
		cfnStackType, statusUpdateRollbackInProgress, "",
	)

	for _, logicalID := range created {
		res, ok := b.resources[stack.StackID][logicalID]
		if !ok {
			continue
		}

		b.addEvent(
			stack.StackID,
			stack.StackName,
			logicalID,
			res.PhysicalID,
			res.Type,
			statusDeleteInProgress,
			"",
		)
		_ = b.creator.Delete(ctx, res.Type, res.PhysicalID, res.Properties)
		b.addEvent(
			stack.StackID,
			stack.StackName,
			logicalID,
			res.PhysicalID,
			res.Type,
			statusDeleteComplete,
			"",
		)
		delete(b.resources[stack.StackID], logicalID)
	}

	// Restore resources that existed before the update.
	maps.Copy(b.resources[stack.StackID], prevResources)

	stack.StackStatus = statusUpdateRollbackComplete
	b.addEvent(
		stack.StackID, stack.StackName, stack.StackName, stack.StackID,
		cfnStackType, statusUpdateRollbackComplete, "",
	)
}

// ErrTerminationProtectionEnabled is returned when deleting a termination-protected stack.
var ErrTerminationProtectionEnabled = errors.New("stack termination protection is enabled")

// DeleteStack marks a stack as deleted and deletes its resources.

func (b *InMemoryBackend) DeleteStack(ctx context.Context, nameOrID string) error {
	b.mu.Lock("DeleteStack")
	defer b.mu.Unlock()

	return b.deleteStackLocked(ctx, nameOrID)
}

// pruneDriftDetections removes all drift detection entries associated with a stack.
func (b *InMemoryBackend) pruneDriftDetections(stackID string) {
	for detectionID, status := range b.driftDetections {
		if status.StackID == stackID {
			delete(b.driftDetections, detectionID)
		}
	}
}

// DescribeStack returns details for a single stack.
func (b *InMemoryBackend) DescribeStack(nameOrID string) (*Stack, error) {
	b.mu.RLock("DescribeStack")
	defer b.mu.RUnlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return nil, ErrStackNotFound
	}

	return stack, nil
}

const cfnDefaultPageSize = 100

// ListStacks returns paginated stack summaries, optionally filtered by status.
func (b *InMemoryBackend) ListStacks(
	statusFilter []string,
	nextToken string,
) (page.Page[StackSummary], error) {
	b.mu.RLock("ListStacks")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(statusFilter))
	for _, s := range statusFilter {
		filter[s] = true
	}

	summaries := make([]StackSummary, 0, len(b.stacks))
	for _, stack := range b.stacks {
		if len(filter) > 0 && !filter[stack.StackStatus] {
			continue
		}
		summaries = append(summaries, StackSummary{
			StackID:      stack.StackID,
			StackName:    stack.StackName,
			StackStatus:  stack.StackStatus,
			CreationTime: stack.CreationTime,
			DeletionTime: stack.DeletionTime,
		})
	}

	sort.Slice(
		summaries,
		func(i, j int) bool { return summaries[i].StackName < summaries[j].StackName },
	)

	return page.New(summaries, nextToken, 0, cfnDefaultPageSize), nil
}

// DescribeStackEvents returns events for a stack.
func (b *InMemoryBackend) DescribeStackEvents(nameOrID string) ([]StackEvent, error) {
	b.mu.RLock("DescribeStackEvents")
	defer b.mu.RUnlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return nil, ErrStackNotFound
	}

	evts := b.events[stack.StackID]
	result := make([]StackEvent, len(evts))
	// Return in reverse chronological order (most recent first).
	for i, e := range evts {
		result[len(evts)-1-i] = e
	}

	return result, nil
}

// CreateChangeSet creates a change set for a stack.
func (b *InMemoryBackend) CreateChangeSet(
	_ context.Context,
	stackName, changeSetName, templateBody, description string,
	params []Parameter,
) (*ChangeSet, error) {
	b.mu.Lock("CreateChangeSet")
	defer b.mu.Unlock()

	if b.changeSets[stackName] == nil {
		b.changeSets[stackName] = make(map[string]*ChangeSet)
	}

	if _, exists := b.changeSets[stackName][changeSetName]; exists {
		return nil, ErrChangeSetExists
	}

	stack, _ := b.resolveStack(stackName)

	csID := uuid.New().String()
	stackID := ""
	changeSetType := "CREATE"
	if stack != nil {
		stackID = stack.StackID
		changeSetType = "UPDATE"
	}

	cs := &ChangeSet{
		ChangeSetID: arn.Build(
			"cloudformation",
			b.region,
			b.accountID,
			"changeSet/"+changeSetName+"/"+csID,
		),
		ChangeSetName:   changeSetName,
		StackID:         stackID,
		StackName:       stackName,
		Status:          statusCreateComplete,
		ExecutionStatus: "AVAILABLE",
		ChangeSetType:   changeSetType,
		Description:     description,
		CreationTime:    time.Now(),
		TemplateBody:    templateBody,
		Parameters:      params,
	}

	cs.Changes = b.computeChanges(templateBody, stack)

	// AWS marks a change set with no actual changes as FAILED / UNAVAILABLE so
	// it cannot be executed; only a change set that contains changes is
	// AVAILABLE for execution.
	if len(cs.Changes) == 0 {
		cs.Status = "FAILED"
		cs.StatusReason = "The submitted information didn't contain changes. " +
			"Submit different information to create a change set."
		cs.ExecutionStatus = "UNAVAILABLE"
	}

	b.changeSets[stackName][changeSetName] = cs

	return cs, nil
}

// computeChanges computes the change set diff from a template body against an existing stack.
func (b *InMemoryBackend) computeChanges(templateBody string, stack *Stack) []Change {
	if templateBody == "" {
		return nil
	}

	tmpl, err := ParseTemplate(templateBody)
	if err != nil {
		return nil
	}

	changes := make([]Change, 0, len(tmpl.Resources))

	for logicalID, res := range tmpl.Resources {
		action := "Add"
		if stack != nil && b.resources[stack.StackID] != nil {
			if _, exists := b.resources[stack.StackID][logicalID]; exists {
				action = "Modify"
			}
		}

		changes = append(changes, Change{
			Type: "Resource",
			ResourceChange: ResourceChange{
				Action:       action,
				LogicalID:    logicalID,
				ResourceType: res.Type,
			},
		})
	}

	return changes
}

// DescribeChangeSet returns details for a change set.
func (b *InMemoryBackend) DescribeChangeSet(stackName, changeSetName string) (*ChangeSet, error) {
	b.mu.RLock("DescribeChangeSet")
	defer b.mu.RUnlock()

	csMap, ok := b.changeSets[stackName]
	if !ok {
		return nil, ErrChangeSetNotFound
	}
	cs, ok := csMap[changeSetName]
	if !ok {
		return nil, ErrChangeSetNotFound
	}

	return cs, nil
}

// ExecuteChangeSet applies a change set to a stack.
func (b *InMemoryBackend) ExecuteChangeSet(
	ctx context.Context,
	stackName, changeSetName string,
) error {
	b.mu.Lock("ExecuteChangeSet")
	cs, ok := b.changeSets[stackName][changeSetName]
	if ok {
		cs.ExecutionStatus = "EXECUTE_IN_PROGRESS"
	}
	b.mu.Unlock()

	if !ok {
		return ErrChangeSetNotFound
	}

	var execErr error
	_, err := b.UpdateStack(ctx, stackName, cs.TemplateBody, cs.Parameters, StackOptions{})
	if err != nil {
		// Stack may not exist yet — create it.
		_, err = b.CreateStack(ctx, stackName, cs.TemplateBody, cs.Parameters, StackOptions{})
		if err != nil {
			execErr = err
		}
	}

	b.mu.Lock("ExecuteChangeSet")
	if execErr != nil {
		if cs2, ok2 := b.changeSets[stackName][changeSetName]; ok2 {
			cs2.ExecutionStatus = "EXECUTE_FAILED"
		}
	} else {
		delete(b.changeSets[stackName], changeSetName)
	}
	b.mu.Unlock()

	return execErr
}

// DeleteChangeSet removes a change set.
func (b *InMemoryBackend) DeleteChangeSet(stackName, changeSetName string) error {
	b.mu.Lock("DeleteChangeSet")
	defer b.mu.Unlock()

	if b.changeSets[stackName] == nil {
		return ErrChangeSetNotFound
	}
	if _, ok := b.changeSets[stackName][changeSetName]; !ok {
		return ErrChangeSetNotFound
	}
	delete(b.changeSets[stackName], changeSetName)

	return nil
}

// ListChangeSets returns paginated summaries of change sets for a stack.
func (b *InMemoryBackend) ListChangeSets(
	stackName, nextToken string,
) (page.Page[ChangeSetSummary], error) {
	b.mu.RLock("ListChangeSets")
	defer b.mu.RUnlock()

	csMap := b.changeSets[stackName]
	summaries := make([]ChangeSetSummary, 0, len(csMap))
	for _, cs := range csMap {
		summaries = append(summaries, ChangeSetSummary{
			ChangeSetID:   cs.ChangeSetID,
			ChangeSetName: cs.ChangeSetName,
			StackID:       cs.StackID,
			StackName:     cs.StackName,
			Status:        cs.Status,
			CreationTime:  cs.CreationTime,
			Description:   cs.Description,
		})
	}

	sort.Slice(
		summaries,
		func(i, j int) bool { return summaries[i].ChangeSetName < summaries[j].ChangeSetName },
	)

	return page.New(summaries, nextToken, 0, cfnDefaultPageSize), nil
}

// GetTemplate returns the template body for a stack.
func (b *InMemoryBackend) GetTemplate(nameOrID string) (string, error) {
	b.mu.RLock("GetTemplate")
	defer b.mu.RUnlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return "", ErrStackNotFound
	}

	return stack.TemplateBody, nil
}

// ListAll returns all stacks (for dashboard).
func (b *InMemoryBackend) ListAll() []*Stack {
	b.mu.RLock("ListAll")
	defer b.mu.RUnlock()

	stacks := make([]*Stack, 0, len(b.stacks))
	for _, s := range b.stacks {
		stacks = append(stacks, s)
	}

	return stacks
}

// DescribeStackResource returns details for a single resource in a stack.
func (b *InMemoryBackend) DescribeStackResource(
	nameOrID, logicalID string,
) (*StackResource, error) {
	b.mu.RLock("DescribeStackResource")
	defer b.mu.RUnlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return nil, ErrStackNotFound
	}

	res, ok := b.resources[stack.StackID][logicalID]
	if !ok {
		return nil, ErrResourceNotFound
	}

	return res, nil
}

// ListStackResources returns paginated summaries of all resources in a stack.
func (b *InMemoryBackend) ListStackResources(
	nameOrID, nextToken string,
) (page.Page[StackResourceSummary], error) {
	b.mu.RLock("ListStackResources")
	defer b.mu.RUnlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return page.Page[StackResourceSummary]{}, ErrStackNotFound
	}

	resMap := b.resources[stack.StackID]
	summaries := make([]StackResourceSummary, 0, len(resMap))

	for _, res := range resMap {
		summaries = append(summaries, StackResourceSummary{
			Timestamp:          res.Timestamp,
			LogicalResourceID:  res.LogicalID,
			PhysicalResourceID: res.PhysicalID,
			ResourceType:       res.Type,
			ResourceStatus:     res.Status,
		})
	}

	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].LogicalResourceID < summaries[j].LogicalResourceID
	})

	return page.New(summaries, nextToken, 0, cfnDefaultPageSize), nil
}

// DescribeStackResources returns all resources for a stack (or matching a physical resource ID).
func (b *InMemoryBackend) DescribeStackResources(nameOrID string) ([]StackResource, error) {
	b.mu.RLock("DescribeStackResources")
	defer b.mu.RUnlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return nil, ErrStackNotFound
	}

	resMap := b.resources[stack.StackID]
	resources := make([]StackResource, 0, len(resMap))

	for _, res := range resMap {
		resources = append(resources, *res)
	}

	sort.Slice(resources, func(i, j int) bool {
		return resources[i].LogicalID < resources[j].LogicalID
	})

	return resources, nil
}

// ListExports returns all exported output values across all stacks.
func (b *InMemoryBackend) ListExports(nextToken string) (page.Page[Export], error) {
	b.mu.RLock("ListExports")
	defer b.mu.RUnlock()

	exports := make([]Export, 0, len(b.exports))
	for _, exp := range b.exports {
		exports = append(exports, *exp)
	}

	sort.Slice(exports, func(i, j int) bool { return exports[i].Name < exports[j].Name })

	return page.New(exports, nextToken, 0, cfnDefaultPageSize), nil
}

// ListImports returns the names of stacks that import the given export.
func (b *InMemoryBackend) ListImports(exportName, nextToken string) (page.Page[string], error) {
	b.mu.RLock("ListImports")
	defer b.mu.RUnlock()

	if _, ok := b.exports[exportName]; !ok {
		return page.Page[string]{}, ErrExportNotFound
	}

	var stackNames []string

	for _, stack := range b.stacks {
		if stack.StackStatus == statusDeleteComplete {
			continue
		}

		// Build resolved params to handle non-literal import names like {"Ref": "Param"}.
		var resolvedParams map[string]string
		if tmpl, err := ParseTemplate(stack.TemplateBody); err == nil {
			resolvedParams = ResolveParameters(tmpl, stack.Parameters)
		}

		refs := collectImportValues(stack.TemplateBody, resolvedParams)
		if slices.Contains(refs, exportName) {
			stackNames = append(stackNames, stack.StackName)
		}
	}

	sort.Strings(stackNames)

	return page.New(stackNames, nextToken, 0, cfnDefaultPageSize), nil
}

// registerExports upserts exports for a stack from the given export map.
// It returns ErrDuplicateExport if an export name is already owned by a different stack.
func (b *InMemoryBackend) registerExports(stackID string, exportMap map[string]string) error {
	for name, value := range exportMap {
		if existing, ok := b.exports[name]; ok && existing.ExportingStackID != stackID {
			return fmt.Errorf("%w: %s", ErrDuplicateExport, name)
		}

		b.exports[name] = &Export{
			ExportingStackID: stackID,
			Name:             name,
			Value:            value,
		}
	}

	return nil
}

// removeExports removes all exports owned by the given stack.
func (b *InMemoryBackend) removeExports(stackID string) {
	for name, exp := range b.exports {
		if exp.ExportingStackID == stackID {
			delete(b.exports, name)
		}
	}
}

// buildExportsMap builds a name→value map of all current exports (for Fn::ImportValue resolution).
func (b *InMemoryBackend) buildExportsMap() map[string]string {
	m := make(map[string]string, len(b.exports))
	for name, exp := range b.exports {
		m[name] = exp.Value
	}

	return m
}
