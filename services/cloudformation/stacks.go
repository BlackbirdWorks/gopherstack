package cloudformation

import (
	"context"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// isFailedCreateStatus reports whether status is one of the terminal
// "CreateStack did not succeed" outcomes: the failure itself
// (statusCreateFailed) or either outcome of the automatic rollback that
// follows it.
const (
	onFailureDoNothing = "DO_NOTHING"
	onFailureRollback  = "ROLLBACK"
	onFailureDelete    = "DELETE"
)

// validateOnFailure checks CreateStack's OnFailure enum and its exclusivity
// with DisableRollback.
func validateOnFailure(opts StackOptions) error {
	switch opts.OnFailure {
	case "":
		return nil
	case onFailureDoNothing, onFailureRollback, onFailureDelete:
	default:
		return awsErrorf(
			ErrStackOptions,
			"1 validation error detected: Value '%s' at 'onFailure' failed to satisfy constraint: "+
				"Member must satisfy enum value set: [ROLLBACK, DELETE, DO_NOTHING]",
			opts.OnFailure,
		)
	}

	if opts.DisableRollback {
		return awsErrorf(ErrStackOptions, "You can specify either DisableRollback or OnFailure, but not both.")
	}

	return nil
}

func isFailedCreateStatus(status string) bool {
	return status == statusCreateFailed || status == statusRollbackComplete || status == statusRollbackFailed
}

type StackOptions struct {
	RollbackConfiguration *RollbackConfiguration
	RoleARN               string
	OnFailure             string // DELETE | ROLLBACK | DO_NOTHING
	// StackPolicyDuringUpdateBody, when non-empty, overrides the stack's
	// stored policy for this UpdateStack call only (UpdateStackInput's
	// StackPolicyDuringUpdateBody, api_op_UpdateStack.go:223) -- it is never
	// persisted to the stack's stored policy.
	StackPolicyDuringUpdateBody string
	// StackPolicyBody is stored as the stack's policy on Create/UpdateStack.
	StackPolicyBody  string
	Capabilities     []string
	NotificationARNs []string
	Tags             []Tag
	// ResourceTypes is the optional per-call allowlist of resource type
	// wildcard patterns (CreateStackInput.ResourceTypes,
	// api_op_CreateStack.go): when non-empty, every resource Type in the
	// template must match one of its patterns or the operation fails. Not
	// persisted on the stack -- each Create/UpdateStack call supplies its own.
	ResourceTypes               []string
	TimeoutInMinutes            int
	DisableRollback             bool
	EnableTerminationProtection bool
	// DisableValidation skips the pre-deployment intrinsics validation pass
	// (validateIntrinsics) this backend runs before provisioning/updating
	// resources (CreateStackInput.DisableValidation, api_op_CreateStack.go:
	// "Set to true to disable pre-deployment validations ... Default: false").
	DisableValidation bool
	// RetainExceptOnCreate, when true, forces rollbackCreateResources to
	// delete newly-created resources during a CreateStack rollback even when
	// they carry DeletionPolicy=Retain/Snapshot (CreateStackInput.
	// RetainExceptOnCreate, api_op_CreateStack.go: "When set to true, newly
	// created resources are deleted when the operation rolls back. This
	// includes newly created resources marked with a deletion policy of
	// Retain. Default: false" -- i.e. the default already honors
	// DeletionPolicy on rollback).
	RetainExceptOnCreate bool
}

// CreateNestedStack implements NestedStackCreator. Must be called while b.mu is held by caller.
// Inside an async parent step the child is provisioned at once but settles on
// its own later step, which the parent waits for.
func (b *InMemoryBackend) CreateNestedStack(
	ctx context.Context,
	name, templateURL, templateBody string,
	params []Parameter,
	parentID string,
) (string, error) {
	if templateBody == "" && templateURL != "" {
		fetched, fetchErr := b.FetchS3URL(ctx, templateURL)
		if fetchErr != nil {
			return "", fetchErr
		}

		templateBody = fetched
	}

	settleLater := b.stepAsync && b.provisionSchedule != nil
	stepAsync := b.stepAsync
	b.stepAsync = false

	defer func() { b.stepAsync = stepAsync }()

	// Lock already held by parent CreateStack — use the no-lock variant.
	stack, err := b.beginCreateLocked(name, templateBody, params, StackOptions{}, parentID)
	if err != nil {
		return "", err
	}

	b.finishCreateLocked(ctx, stack, params, createFinish{settleLater: settleLater})

	if isFailedCreateStatus(stack.StackStatus) {
		return "", awsErrorf(
			ErrNestedStackFailed, "Embedded stack %s was not successfully created: %s",
			stack.StackID, stack.StackStatusReason,
		)
	}

	return stack.StackID, nil
}

// DeleteNestedStack implements NestedStackCreator. Must be called while b.mu is held by caller.
// The child is deleted at once and settles on its own later step, which the parent waits for.
func (b *InMemoryBackend) DeleteNestedStack(ctx context.Context, stackID string) error {
	settleLater := b.stepAsync && b.provisionSchedule != nil
	stepAsync := b.stepAsync
	b.stepAsync = false

	defer func() { b.stepAsync = stepAsync }()

	return b.deleteStackWithLocked(ctx, stackID, deleteSettings{settleLater: settleLater}, false)
}

// evictDeletedStacks caps the number of DELETE_COMPLETE stacks at maxDeletedStacks.
// Caller must hold b.mu.Lock.
func (b *InMemoryBackend) evictDeletedStacks() {
	const maxDeletedStacks = 1000
	deleted := make([]*Stack, 0)
	for _, s := range b.stacks.All() {
		if s.StackStatus == statusDeleteComplete {
			deleted = append(deleted, s)
		}
	}
	if len(deleted) <= maxDeletedStacks {
		return
	}
	sort.Slice(deleted, func(i, j int) bool {
		if deleted[i].DeletionTime == nil {
			return true
		}

		if deleted[j].DeletionTime == nil {
			return false
		}

		return deleted[i].DeletionTime.Before(*deleted[j].DeletionTime)
	})
	for _, s := range deleted[:len(deleted)-maxDeletedStacks] {
		b.stacks.Delete(s.StackName)
		delete(b.stackIDIndex, s.StackID)
	}
}

func (b *InMemoryBackend) buildStackARN(stackName, stackID string) string {
	return arn.Build("cloudformation", b.region, b.accountID, "stack/"+stackName+"/"+stackID)
}

func (b *InMemoryBackend) resolveStack(nameOrID string) (*Stack, bool) {
	if s, ok := b.stacks.Get(nameOrID); ok {
		return s, true
	}

	if name, ok := b.stackIDIndex[nameOrID]; ok {
		if s, found := b.stacks.Get(name); found {
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

// CreateStack creates a new stack from a template. With async provisioning
// enabled (SetProvisioningDelay) it returns while the stack is still
// CREATE_IN_PROGRESS.
func (b *InMemoryBackend) CreateStack(
	ctx context.Context,
	name, templateBody string,
	params []Parameter,
	opts StackOptions,
) (*Stack, error) {
	return b.createStack(ctx, name, templateBody, params, opts, true)
}

func (b *InMemoryBackend) createStack(
	ctx context.Context,
	name, templateBody string,
	params []Parameter,
	opts StackOptions,
	async bool,
) (*Stack, error) {
	quota := b.quotaLimit()

	b.mu.Lock("CreateStack")
	defer b.mu.Unlock()

	if err := preflightTemplateErr(
		templateBody, params, b.typeCheckLocked(opts.DisableValidation), b.ruleSource(),
	); err != nil {
		return nil, err
	}

	if err := b.checkStackQuotaLocked(name, quota); err != nil {
		return nil, err
	}

	stack, err := b.beginCreateLocked(name, templateBody, params, opts, "")
	if err != nil {
		return nil, err
	}

	if async && templateBody != "" && b.provisionSchedule != nil {
		op := &pendingStackOp{
			Kind:                 pendingCreate,
			DisableValidation:    opts.DisableValidation,
			ResourceTypes:        opts.ResourceTypes,
			RetainExceptOnCreate: opts.RetainExceptOnCreate,
			OnFailure:            opts.OnFailure,
			InFlight:             b.announceCreateLocked(stack, params),
		}

		if b.scheduleLocked(ctx, stack.StackID, op) {
			return stack, nil
		}
	}

	b.finishCreateLocked(ctx, stack, params, createFinishOpts(opts))

	return stack, nil
}

// validateCreateStackPreflight runs CreateStack's synchronous, pre-mutation
// checks: RoleARN/IAM capability requirements (top-level stacks only) and the
// Fn::GetAtt attribute check (both nested and top-level, unless
// DisableValidation), split out of createStackLocked to keep its cognitive
// complexity down (no //nolint:gocognit, per repo convention).
func validateCreateStackPreflight(templateBody string, opts StackOptions, parentID string) error {
	if err := validateOnFailure(opts); err != nil {
		return err
	}

	if parentID == "" {
		if err := validateStackOptions(templateBody, opts); err != nil {
			return err
		}
	}

	if err := validateStackPolicyBody(opts.StackPolicyBody); err != nil {
		return err
	}

	if opts.DisableValidation {
		return nil
	}

	return preflightGetAttAttributeErr(templateBody)
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
	stack, err := b.beginCreateLocked(name, templateBody, params, opts, parentID)
	if err != nil {
		return nil, err
	}

	b.finishCreateLocked(ctx, stack, params, createFinishOpts(opts))

	return stack, nil
}

// beginCreateLocked registers the stack as CREATE_IN_PROGRESS without
// provisioning any resource.
func (b *InMemoryBackend) beginCreateLocked(
	name, templateBody string,
	params []Parameter,
	opts StackOptions,
	parentID string,
) (*Stack, error) {
	if err := validateCreateStackPreflight(templateBody, opts, parentID); err != nil {
		return nil, err
	}

	arn, err := b.stackARNForCreateLocked(name)
	if err != nil {
		return nil, err
	}
	now := time.Now()

	stack := &Stack{
		StackID:                     arn,
		StackName:                   name,
		StackStatus:                 statusCreateInProgress,
		CreationTime:                now,
		Parameters:                  params,
		Tags:                        opts.Tags,
		TemplateBody:                templateBody,
		Capabilities:                opts.Capabilities,
		NotificationARNs:            opts.NotificationARNs,
		RoleARN:                     opts.RoleARN,
		TimeoutInMinutes:            opts.TimeoutInMinutes,
		DisableRollback:             opts.DisableRollback || opts.OnFailure == onFailureDoNothing,
		EnableTerminationProtection: opts.EnableTerminationProtection,
		RollbackConfiguration:       opts.RollbackConfiguration,
		ParentID:                    parentID,
	}

	// RootId is the top of the parent chain, not the immediate parent: if the
	// parent is itself nested, inherit its RootID; otherwise the parent IS
	// the root.
	if parentID != "" {
		if parent, ok := b.resolveStack(parentID); ok {
			if parent.RootID != "" {
				stack.RootID = parent.RootID
			} else {
				stack.RootID = parent.StackID
			}
		}
	}

	b.stacks.Put(stack)
	b.stackIDIndex[arn] = name
	if opts.StackPolicyBody != "" {
		b.stackPolicies[arn] = opts.StackPolicyBody
	}
	if _, reused := b.events[arn]; !reused {
		b.events[arn] = nil
	}
	b.resources[arn] = make(map[string]*StackResource)

	b.addEvent(arn, name, name, arn, cfnStackType, statusCreateInProgress, reasonUserInitiated)

	return stack, nil
}

type createFinish struct {
	announced            map[string]string
	onFailure            string
	resourceTypes        []string
	disableValidation    bool
	retainExceptOnCreate bool
	settleLater          bool
}

func (f createFinish) withAnnounced(inFlight map[string]string) createFinish {
	f.announced = inFlight

	return f
}

func createFinishOpts(opts StackOptions) createFinish {
	return createFinish{
		onFailure:            opts.OnFailure,
		resourceTypes:        opts.ResourceTypes,
		disableValidation:    opts.DisableValidation,
		retainExceptOnCreate: opts.RetainExceptOnCreate,
	}
}

// finishCreateLocked provisions the stack's resources and settles its status.
func (b *InMemoryBackend) finishCreateLocked(
	ctx context.Context,
	stack *Stack,
	params []Parameter,
	opts createFinish,
) {
	if stack.TemplateBody != "" {
		b.createStackFromTemplate(ctx, stack, params, opts)

		if b.createWalkPending(stack) {
			return
		}
	}

	b.settleCreateLocked(ctx, stack, opts)
}

func (b *InMemoryBackend) settleCreateLocked(ctx context.Context, stack *Stack, opts createFinish) {
	switch {
	case stack.StackStatus == statusRollbackInProgress,
		stack.StackStatus == statusDeleteInProgress,
		stack.StackStatus == statusDeleteComplete:
		return
	case isFailedCreateStatus(stack.StackStatus):
		b.deleteAfterFailedCreateLocked(ctx, stack, opts)
	case opts.settleLater || b.stepAsync && b.childrenInProgressLocked(stack):
		b.stageSettleLocked(ctx, stack, opts)
	default:
		b.completeCreateLocked(stack)
	}
}

// stageSettleLocked leaves a provisioned stack CREATE_IN_PROGRESS until its
// own later step, which also waits for any nested stacks.
func (b *InMemoryBackend) stageSettleLocked(ctx context.Context, stack *Stack, opts createFinish) {
	if opts.settleLater {
		stepAsync := b.stepAsync
		b.stepAsync = true

		defer func() { b.stepAsync = stepAsync }()
	}

	b.stageLocked(ctx, stack.StackID, &pendingStackOp{Kind: pendingCreateSettle}, func() {
		b.completeCreateLocked(stack)
	})
}

func (b *InMemoryBackend) completeCreateLocked(stack *Stack) {
	stack.StackStatus = statusCreateComplete
	b.addEvent(
		stack.StackID, stack.StackName, stack.StackName, stack.StackID,
		cfnStackType, statusCreateComplete, "",
	)
}

// deleteAfterFailedCreateLocked implements OnFailure=DELETE: once creation has
// failed and rolled back, the stack is removed. ROLLBACK_FAILED is excluded:
// the failed rollback cannot honestly report DELETE_COMPLETE, so the stack
// stays for the caller to inspect and retry.
func (b *InMemoryBackend) deleteAfterFailedCreateLocked(ctx context.Context, stack *Stack, opts createFinish) {
	if opts.onFailure != onFailureDelete ||
		(stack.StackStatus != statusCreateFailed && stack.StackStatus != statusRollbackComplete) {
		return
	}

	stack.StackStatus = statusDeleteInProgress
	b.addEvent(
		stack.StackID, stack.StackName, stack.StackName, stack.StackID,
		cfnStackType, statusDeleteInProgress, "",
	)
	b.stageLocked(ctx, stack.StackID, &pendingStackOp{Kind: pendingDeleteFinalize}, func() {
		b.finishDeleteLocked(stack)
	})
}

// createStackFromTemplate parses and applies a template during CreateStack.
// It updates stack.StackStatus on failure. opts carries CreateStackInput's own
// DisableValidation/ResourceTypes/RetainExceptOnCreate/OnFailure (see StackOptions);
// stack.DisableRollback (already persisted on Stack) gates whether a
// provisioning failure rolls back at all.
func (b *InMemoryBackend) createStackFromTemplate(
	ctx context.Context,
	stack *Stack,
	params []Parameter,
	opts createFinish,
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

	if dynErr := ResolveDynamicRefsInTemplate(ctx, tmpl, b.resolver); dynErr != nil {
		stack.StackStatus = statusCreateFailed
		stack.StackStatusReason = dynErr.Error()
		b.addEvent(arn, name, name, arn, cfnStackType, statusCreateFailed, dynErr.Error())

		return
	}

	resolvedParams := ResolveParameters(tmpl, params)

	if valErr := ValidateParameters(tmpl, resolvedParams); valErr != nil {
		b.failAndRollback(ctx, stack, opts, valErr.Error())

		return
	}

	// Validate intrinsic references (Fn::GetAtt / Fn::Sub to undefined
	// resources, unsupported resource types) before provisioning anything --
	// skipped entirely when the caller set DisableValidation.
	if !opts.disableValidation {
		if intErr := validateIntrinsics(tmpl); intErr != nil {
			b.failAndRollback(ctx, stack, opts, intErr.Error())

			return
		}
	}

	pruneConditional(tmpl, resolvedParams)

	if !opts.disableValidation {
		if propErr := b.validateProperties(tmpl); propErr != nil {
			b.failAndRollback(ctx, stack, opts, propErr.Error())

			return
		}
	}

	if rtErr := validateResourceTypesAllowed(tmpl, opts.resourceTypes); rtErr != nil {
		b.failAndRollback(ctx, stack, opts, rtErr.Error())

		return
	}

	// Validate that all Fn::ImportValue references can be satisfied before
	// creating any resources.
	if impErr := validateImportValues(tmpl, resolvedParams, b.buildExportsMap()); impErr != nil {
		b.failAndRollback(ctx, stack, opts, impErr.Error())

		return
	}

	if _, ordErr := topoSortResources(tmpl.Resources); ordErr != nil {
		b.failAndRollback(ctx, stack, opts, ordErr.Error())

		return
	}

	op := b.newCreateWalk(stack, &walkRuntime{tmpl: tmpl, params: resolvedParams}, opts)
	b.walkCreateLocked(ctx, stack, op)
}

// failAndRollback records a pre-flight CREATE_FAILED then enters rollback (no
// resources to undo) -- unless stack.DisableRollback is set: real AWS's "Set to
// true to disable rollback of the stack if stack creation failed" applies to
// every CreateStack failure, so the stack is then left CREATE_FAILED.
func (b *InMemoryBackend) failAndRollback(ctx context.Context, stack *Stack, opts createFinish, reason string) {
	stack.StackStatusReason = reason
	b.addEvent(
		stack.StackID, stack.StackName, stack.StackName, stack.StackID,
		cfnStackType, statusCreateFailed, reason,
	)
	b.enterCreateRollbackLocked(ctx, stack, nil, opts, reason)
}

// UpdateStack updates an existing stack.
func (b *InMemoryBackend) UpdateStack(
	ctx context.Context,
	nameOrID, templateBody string,
	params []Parameter,
	opts StackOptions,
) (*Stack, error) {
	return b.updateStack(ctx, nameOrID, templateBody, params, opts, true)
}

func (b *InMemoryBackend) updateStack(
	ctx context.Context,
	nameOrID, templateBody string,
	params []Parameter,
	opts StackOptions,
	async bool,
) (*Stack, error) {
	b.mu.Lock("UpdateStack")
	defer b.mu.Unlock()

	return b.updateStackLocked(ctx, nameOrID, templateBody, params, opts, async)
}

// updateStackLocked is updateStack's body for callers already holding b.mu.
func (b *InMemoryBackend) updateStackLocked(
	ctx context.Context,
	nameOrID, templateBody string,
	params []Parameter,
	opts StackOptions,
	async bool,
) (*Stack, error) {
	stack, params, err := b.prepareUpdateLocked(nameOrID, templateBody, params, opts)
	if err != nil {
		return nil, err
	}

	// Enforce the stack policy before mutating any state: computeChanges
	// needs stack.TemplateBody as it stood before this update, and a denied
	// update must fail atomically rather than partially transitioning the
	// stack to UPDATE_IN_PROGRESS.
	if err = b.checkStackPolicy(stack, templateBody, opts); err != nil {
		return nil, err
	}

	if opts.StackPolicyBody != "" {
		b.stackPolicies[stack.StackID] = opts.StackPolicyBody
	}

	// Captured before stack.TemplateBody is overwritten below: deleteStaleResources
	// needs the OLD template's own dependency graph to delete resources dropped
	// from the new one in reverse dependency order.
	oldTemplateBody := stack.TemplateBody
	oldParams := slices.Clone(stack.Parameters)

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

	op := b.newUpdateOp(opts, oldTemplateBody, oldParams)

	if async && b.provisionSchedule != nil {
		b.announceUpdateLocked(ctx, stack, op)

		if b.scheduleLocked(ctx, stack.StackID, op) {
			return stack, nil
		}
	}

	b.runUpdateStepLocked(ctx, stack, op)

	return stack, nil
}

func (b *InMemoryBackend) completeUpdateLocked(stack *Stack) {
	stack.StackStatus = statusUpdateComplete
	b.addEvent(
		stack.StackID, stack.StackName, stack.StackName, stack.StackID,
		cfnStackType, statusUpdateComplete, "",
	)
}

// parseAndValidateUpdateTemplate parses stack.TemplateBody and runs every
// pre-provisioning validation UpdateStack's own path requires (dynamic refs,
// parameters, intrinsics unless disableValidation, ResourceTypes allowlist,
// Fn::ImportValue availability), split out of applyTemplateToStack to keep
// it under this project's funlen ceiling (no //nolint:funlen, per repo
// convention). On failure it marks the stack failed/rolled-back itself and
// returns a nil template.
func (b *InMemoryBackend) parseAndValidateUpdateTemplate(
	ctx context.Context, stack *Stack, op *pendingStackOp,
) (*Template, map[string]string) {
	disableValidation, resourceTypes := op.DisableValidation, op.ResourceTypes

	tmpl, err := ParseTemplate(stack.TemplateBody)
	if err != nil {
		stack.StackStatus = statusUpdateFailed
		stack.StackStatusReason = err.Error()
		b.addEvent(
			stack.StackID, stack.StackName, stack.StackName, stack.StackID,
			cfnStackType, statusUpdateFailed, err.Error(),
		)

		return nil, nil
	}

	stack.Description = tmpl.Description

	if dynErr := ResolveDynamicRefsInTemplate(ctx, tmpl, b.resolver); dynErr != nil {
		stack.StackStatus = statusUpdateFailed
		stack.StackStatusReason = dynErr.Error()
		b.addEvent(
			stack.StackID, stack.StackName, stack.StackName, stack.StackID,
			cfnStackType, statusUpdateFailed, dynErr.Error(),
		)

		return nil, nil
	}

	resolvedParams := ResolveParameters(tmpl, stack.Parameters)

	if valErr := ValidateParameters(tmpl, resolvedParams); valErr != nil {
		b.updateFailAndRollback(ctx, stack, op, valErr.Error())

		return nil, nil
	}

	// Validate intrinsic references before mutating any resource -- skipped
	// entirely when the caller set DisableValidation.
	if !disableValidation {
		if intErr := validateIntrinsics(tmpl); intErr != nil {
			b.updateFailAndRollback(ctx, stack, op, intErr.Error())

			return nil, nil
		}
	}

	pruneConditional(tmpl, resolvedParams)

	if !disableValidation {
		if propErr := b.validateProperties(tmpl); propErr != nil {
			b.updateFailAndRollback(ctx, stack, op, propErr.Error())

			return nil, nil
		}
	}

	if rtErr := validateResourceTypesAllowed(tmpl, resourceTypes); rtErr != nil {
		b.updateFailAndRollback(ctx, stack, op, rtErr.Error())

		return nil, nil
	}

	// Validate that all Fn::ImportValue references can be satisfied before
	// updating any resources.
	if impErr := validateImportValues(tmpl, resolvedParams, b.buildExportsMap()); impErr != nil {
		b.updateFailAndRollback(ctx, stack, op, impErr.Error())

		return nil, nil
	}

	return tmpl, resolvedParams
}

// createUpdateResource creates a new resource during a stack update and registers it.
func (b *InMemoryBackend) createUpdateResource(
	ctx context.Context,
	stack *Stack,
	logicalID string,
	res TemplateResource,
	resolvedParams, physicalIDs map[string]string,
) (string, error) {
	physicalID, cerr := b.creator.Create(
		ctx,
		logicalID,
		res.Type,
		res.Properties,
		resolvedParams,
		physicalIDs,
	)
	if cerr != nil {
		b.addEvent(
			stack.StackID,
			stack.StackName,
			logicalID,
			"",
			res.Type,
			statusCreateFailed,
			cerr.Error(),
		)

		return "", cerr
	}

	b.addEvent(
		stack.StackID, stack.StackName, logicalID, physicalID, res.Type,
		statusCreateInProgress, reasonCreationInitiated,
	)

	b.resources[stack.StackID][logicalID] = &StackResource{
		Timestamp:      time.Now(),
		LogicalID:      logicalID,
		PhysicalID:     physicalID,
		Type:           res.Type,
		Status:         statusCreateComplete,
		Properties:     res.Properties,
		StackID:        stack.StackID,
		StackName:      stack.StackName,
		DeletionPolicy: res.DeletionPolicy,
	}
	b.addEvent(
		stack.StackID,
		stack.StackName,
		logicalID,
		physicalID,
		res.Type,
		statusCreateComplete,
		"",
	)

	return physicalID, nil
}

// prepareUpdateLocked resolves the target stack and runs every synchronous
// UpdateStack check; the returned params have UsePreviousValue resolved.
func (b *InMemoryBackend) prepareUpdateLocked(
	nameOrID, templateBody string,
	params []Parameter,
	opts StackOptions,
) (*Stack, []Parameter, error) {
	if err := validateStackOptions(templateBody, opts); err != nil {
		return nil, nil, err
	}

	if err := validateStackPolicyBody(opts.StackPolicyBody); err != nil {
		return nil, nil, err
	}

	stack, ok := b.resolveStack(nameOrID)
	if !ok || stack.StackStatus == statusDeleteComplete {
		return nil, nil, ErrStackNotFound
	}

	if err := stackUpdatableErr(stack); err != nil {
		return nil, nil, err
	}

	merged, err := mergePreviousParameters(stack, params)
	if err != nil {
		return nil, nil, err
	}

	if err = preflightTemplateErr(
		templateBody,
		merged,
		b.typeCheckLocked(opts.DisableValidation),
		b.ruleSource(),
	); err != nil {
		return nil, nil, err
	}

	if !opts.DisableValidation {
		if err = preflightGetAttAttributeErr(templateBody); err != nil {
			return nil, nil, err
		}
	}

	if isNoOpUpdate(stack, templateBody, merged, opts) {
		return nil, nil, awsErrorf(ErrNoUpdates, "No updates are to be performed.")
	}

	return stack, merged, nil
}

// stackARNForCreateLocked returns the StackId a CreateStack of name uses: the
// REVIEW_IN_PROGRESS stack's own ARN when a CREATE change set made one,
// otherwise a fresh one.
func (b *InMemoryBackend) stackARNForCreateLocked(name string) (string, error) {
	if existing, ok := b.stacks.Get(name); ok {
		switch existing.StackStatus {
		case statusDeleteComplete:
			delete(b.stackIDIndex, existing.StackID)
		case statusReviewInProgress:
			return existing.StackID, nil
		default:
			return "", ErrStackAlreadyExists
		}
	}

	return b.buildStackARN(name, uuid.New().String()), nil
}
