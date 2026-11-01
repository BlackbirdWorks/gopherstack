package cloudformation

import (
	"context"
	"fmt"
	"maps"
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
	stepAsync, announced := b.stepAsync, b.announced
	b.stepAsync, b.announced = false, nil

	defer func() { b.stepAsync, b.announced = stepAsync, announced }()

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
func (b *InMemoryBackend) DeleteNestedStack(ctx context.Context, stackID string) error {
	announced := b.announced
	b.announced = nil

	defer func() { b.announced = announced }()

	return b.deleteStackLocked(ctx, stackID)
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
	b.mu.Lock("CreateStack")
	defer b.mu.Unlock()

	if err := preflightTemplateErr(
		templateBody, params, b.typeCheckLocked(opts.DisableValidation), b.ruleSource(),
	); err != nil {
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
			Announced:            b.announceCreateLocked(stack, params),
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
	onFailure            string
	resourceTypes        []string
	disableValidation    bool
	retainExceptOnCreate bool
	settleLater          bool
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
	}

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

// announceCreateLocked emits CREATE_IN_PROGRESS for the resources that start
// first, so the window shows them before the provisioning step runs.
func (b *InMemoryBackend) announceCreateLocked(stack *Stack, params []Parameter) []string {
	tmpl, err := ParseTemplate(stack.TemplateBody)
	if err != nil {
		return nil
	}

	pruneConditional(tmpl, ResolveParameters(tmpl, params))

	return b.announceLocked(
		stack, statusCreateInProgress, firstWave(tmpl.Resources, sortedKeys(tmpl.Resources), false), tmpl.Resources,
	)
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

	physicalIDs := b.provisionResources(ctx, stack, tmpl, resolvedParams, opts)
	if isFailedCreateStatus(stack.StackStatus) || stack.StackStatus == statusRollbackInProgress {
		return
	}

	resourceTypes := make(map[string]string, len(tmpl.Resources))
	for logicalID, res := range tmpl.Resources {
		resourceTypes[logicalID] = res.Type
	}
	stack.ResourceAttrs = extractAttrStash(physicalIDs, resourceTypes)
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

// updateFailAndRollback records UPDATE_FAILED then enters UPDATE_ROLLBACK_IN_PROGRESS
// for pre-flight update failures (no resources changed).
func (b *InMemoryBackend) updateFailAndRollback(ctx context.Context, stack *Stack, reason string) {
	arn := stack.StackID
	name := stack.StackName
	stack.StackStatusReason = reason
	b.addEvent(arn, name, name, arn, cfnStackType, statusUpdateFailed, reason)
	b.beginUpdateRollbackLocked(ctx, stack, nil, nil, reason)
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

// enterCreateRollbackLocked moves a failed create to ROLLBACK_IN_PROGRESS and
// undoes created in the same step, or in the next one when async.
func (b *InMemoryBackend) enterCreateRollbackLocked(
	ctx context.Context, stack *Stack, created []string, opts createFinish, reason string,
) {
	if stack.DisableRollback {
		stack.StackStatus = statusCreateFailed

		return
	}

	stack.StackStatus = statusRollbackInProgress
	b.addEvent(
		stack.StackID, stack.StackName, stack.StackName, stack.StackID,
		cfnStackType, statusRollbackInProgress, reason,
	)

	op := &pendingStackOp{
		Kind:                 pendingRollbackCreate,
		Created:              created,
		OnFailure:            opts.onFailure,
		RetainExceptOnCreate: opts.retainExceptOnCreate,
	}

	b.stageLocked(ctx, stack.StackID, op, func() { b.finishCreateRollbackLocked(ctx, stack, created, opts) })
}

// finishCreateRollbackLocked deletes the resources a failed create made and
// settles the stack ROLLBACK_COMPLETE or ROLLBACK_FAILED.
func (b *InMemoryBackend) finishCreateRollbackLocked(
	ctx context.Context, stack *Stack, created []string, opts createFinish,
) {
	arn, name := stack.StackID, stack.StackName

	if b.rollbackCreateResources(ctx, stack, created, opts.retainExceptOnCreate) {
		b.addEvent(arn, name, name, arn, cfnStackType, statusRollbackComplete, "")
		stack.StackStatus = statusRollbackComplete
		b.deleteAfterFailedCreateLocked(ctx, stack, opts)

		return
	}

	reason := reasonRollbackDeleteFailed
	b.addEvent(arn, name, name, arn, cfnStackType, statusRollbackFailed, reason)
	stack.StackStatus = statusRollbackFailed
	stack.StackStatusReason = reason
}

// provisionResources creates all resources defined in the template.
// Returns the physicalIDs map. On resource creation failure, rollback is
// performed in reverse order (unless stack.DisableRollback -- real AWS:
// "Set to true to disable rollback of the stack if stack creation failed";
// the stack is then left CREATE_FAILED with whatever it managed to create
// intact); stack.StackStatus is then set to statusRollbackComplete (matching
// real AWS behaviour). If the creation failure itself needs to be recorded
// separately, it is preserved in StackStatusReason. retainExceptOnCreate
// mirrors CreateStackInput.RetainExceptOnCreate (see StackOptions).
func (b *InMemoryBackend) provisionResources(
	ctx context.Context,
	stack *Stack,
	tmpl *Template,
	resolvedParams map[string]string,
	opts createFinish,
) map[string]string {
	arn := stack.StackID
	name := stack.StackName
	physicalIDs := make(map[string]string)

	// Inject stack metadata for custom resource event payloads.
	physicalIDs["_StackId"] = arn
	physicalIDs["_StackName"] = name

	// Side channel so property-time Fn::GetAtt (ResolveValue, called by
	// strProp/resolve while a resource's own Properties are resolved at
	// create time) can find a referenced resource's declared type and the
	// account/region, the same way Outputs-time resolution does via
	// resolveCtx.resourceTypes -- see physIDResourceTypeKey.
	physicalIDs[physIDAccountIDKey] = b.accountID
	physicalIDs[physIDRegionKey] = b.region
	for logicalID, res := range tmpl.Resources {
		physicalIDs[physIDResourceTypeKey(logicalID)] = res.Type
	}

	// Lets a nested AWS::CloudFormation::Stack resource created below learn
	// its ParentId (gopherstack-pbv1) without widening ResourceCreator.Create's
	// signature.
	ctx = parentStackIDKey.Set(ctx, stack.StackID)

	ordered, ordErr := topoSortResources(tmpl.Resources)
	if ordErr != nil {
		b.failAndRollback(ctx, stack, opts, ordErr.Error())

		return physicalIDs
	}

	created := make([]string, 0, len(ordered))

	for _, logicalID := range ordered {
		res := tmpl.Resources[logicalID]
		b.addProgressEvent(arn, name, logicalID, "", res.Type, statusCreateInProgress)
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
			b.enterCreateRollbackLocked(ctx, stack, created, opts, cerr.Error())

			return physicalIDs
		}

		b.addEvent(arn, name, logicalID, physicalID, res.Type, statusCreateInProgress, reasonCreationInitiated)
		physicalIDs[logicalID] = physicalID
		b.resources[arn][logicalID] = &StackResource{
			Timestamp:      time.Now(),
			LogicalID:      logicalID,
			PhysicalID:     physicalID,
			Type:           res.Type,
			Status:         statusCreateComplete,
			Properties:     res.Properties,
			StackID:        arn,
			StackName:      name,
			DeletionPolicy: res.DeletionPolicy,
		}
		b.addEvent(arn, name, logicalID, physicalID, res.Type, statusCreateComplete, "")
		created = append(created, logicalID)
	}

	return physicalIDs
}

// rollbackCreateResources deletes all resources that were created during a
// failed CreateStack provisioning pass, in reverse order. It reports whether
// every deletion succeeded; a resource that fails to delete is left in place
// (matching real AWS, which leaves a ROLLBACK_FAILED stack's undeleted
// resources describable for a retry) rather than being silently dropped.
// retainExceptOnCreate mirrors CreateStackInput.RetainExceptOnCreate (see
// StackOptions): by default a newly-created resource with
// DeletionPolicy=Retain/Snapshot is left in place (not deleted) during
// rollback, matching every other DeletionPolicy-honoring deletion path in
// this backend (DeleteStack, deleteStaleResources); when true, it is force-
// deleted anyway.
func (b *InMemoryBackend) rollbackCreateResources(
	ctx context.Context,
	stack *Stack,
	created []string,
	retainExceptOnCreate bool,
) bool {
	ok := true
	physIDs := b.deleteResolveContext(stack)

	for _, v := range slices.Backward(created) {
		logicalID := v
		res, exists := b.resources[stack.StackID][logicalID]
		if !exists {
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

		keepsPolicy := res.DeletionPolicy == deletionPolicyRetain || res.DeletionPolicy == deletionPolicySnapshot
		retained := keepsPolicy && !retainExceptOnCreate
		if !retained {
			if delErr := b.creator.Delete(ctx, res.Type, res.PhysicalID, res.Properties, physIDs); delErr != nil {
				ok = false
				b.addEvent(
					stack.StackID, stack.StackName, logicalID, res.PhysicalID, res.Type,
					statusDeleteFailed, delErr.Error(),
				)

				continue
			}
		}

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

	return ok
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

	changes := b.changesIfAsync(async, templateBody, stack, params)

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

	op := &pendingStackOp{
		Kind:              pendingUpdate,
		DisableValidation: opts.DisableValidation,
		ResourceTypes:     opts.ResourceTypes,
		OldTemplateBody:   oldTemplateBody,
		OldParams:         oldParams,
	}

	if async && b.scheduleUpdateLocked(ctx, stack, op, changes) {
		return stack, nil
	}

	if b.applyTemplateToStack(ctx, stack, op.DisableValidation, op.ResourceTypes, oldTemplateBody, oldParams) {
		b.completeUpdateLocked(stack)
	}

	return stack, nil
}

func (b *InMemoryBackend) changesIfAsync(
	async bool, templateBody string, stack *Stack, params []Parameter,
) []Change {
	if !async || b.provisionSchedule == nil {
		return nil
	}

	return b.computeChanges(templateBody, stack, params)
}

// scheduleUpdateLocked announces the update's first wave and defers the rest;
// false means async provisioning is off.
func (b *InMemoryBackend) scheduleUpdateLocked(
	ctx context.Context, stack *Stack, op *pendingStackOp, changes []Change,
) bool {
	if b.provisionSchedule == nil {
		return false
	}

	op.Announced = b.announceUpdateLocked(stack, changes)

	return b.scheduleLocked(ctx, stack.StackID, op)
}

// announceUpdateLocked emits the first wave of CREATE/UPDATE_IN_PROGRESS events
// for the resources an update adds or modifies in place.
func (b *InMemoryBackend) announceUpdateLocked(stack *Stack, changes []Change) []string {
	tmpl, err := ParseTemplate(stack.TemplateBody)
	if err != nil {
		return nil
	}

	statusOf := make(map[string]string, len(changes))
	want := make([]string, 0, len(changes))

	for _, c := range changes {
		rc := c.ResourceChange

		switch {
		case rc.Action == changeActionAdd:
			statusOf[rc.LogicalID] = statusCreateInProgress
		case rc.Action == changeActionModify && rc.Replacement != "True":
			statusOf[rc.LogicalID] = statusUpdateInProgress
		default:
			continue
		}

		want = append(want, rc.LogicalID)
	}

	var keys []string

	for _, id := range firstWave(tmpl.Resources, want, false) {
		keys = append(keys, b.announceLocked(stack, statusOf[id], []string{id}, tmpl.Resources)...)
	}

	return keys
}

func (b *InMemoryBackend) completeUpdateLocked(stack *Stack) {
	stack.StackStatus = statusUpdateComplete
	b.addEvent(
		stack.StackID, stack.StackName, stack.StackName, stack.StackID,
		cfnStackType, statusUpdateComplete, "",
	)
}

// applyTemplateToStack parses the stack's template and creates or updates
// resources. Returns true on success; on failure it sets the stack status
// and returns false. disableValidation/resourceTypes mirror UpdateStackInput's
// own DisableValidation/ResourceTypes fields (see StackOptions).
// parseAndValidateUpdateTemplate parses stack.TemplateBody and runs every
// pre-provisioning validation UpdateStack's own path requires (dynamic refs,
// parameters, intrinsics unless disableValidation, ResourceTypes allowlist,
// Fn::ImportValue availability), split out of applyTemplateToStack to keep
// it under this project's funlen ceiling (no //nolint:funlen, per repo
// convention). On failure it marks the stack failed/rolled-back itself and
// returns a nil template.
func (b *InMemoryBackend) parseAndValidateUpdateTemplate(
	ctx context.Context, stack *Stack, disableValidation bool, resourceTypes []string,
) (*Template, map[string]string) {
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
		b.updateFailAndRollback(ctx, stack, valErr.Error())

		return nil, nil
	}

	// Validate intrinsic references before mutating any resource -- skipped
	// entirely when the caller set DisableValidation.
	if !disableValidation {
		if intErr := validateIntrinsics(tmpl); intErr != nil {
			b.updateFailAndRollback(ctx, stack, intErr.Error())

			return nil, nil
		}
	}

	pruneConditional(tmpl, resolvedParams)

	if !disableValidation {
		if propErr := b.validateProperties(tmpl); propErr != nil {
			b.updateFailAndRollback(ctx, stack, propErr.Error())

			return nil, nil
		}
	}

	if rtErr := validateResourceTypesAllowed(tmpl, resourceTypes); rtErr != nil {
		b.updateFailAndRollback(ctx, stack, rtErr.Error())

		return nil, nil
	}

	// Validate that all Fn::ImportValue references can be satisfied before
	// updating any resources.
	if impErr := validateImportValues(tmpl, resolvedParams, b.buildExportsMap()); impErr != nil {
		b.updateFailAndRollback(ctx, stack, impErr.Error())

		return nil, nil
	}

	return tmpl, resolvedParams
}

func (b *InMemoryBackend) applyTemplateToStack(
	ctx context.Context, stack *Stack, disableValidation bool, resourceTypes []string,
	oldTemplateBody string, oldParams []Parameter,
) bool {
	if stack.TemplateBody == "" {
		return true
	}

	tmpl, resolvedParams := b.parseAndValidateUpdateTemplate(ctx, stack, disableValidation, resourceTypes)
	if tmpl == nil {
		return false
	}

	// Pre-populate physicalIDs from existing resources.
	physicalIDs := make(map[string]string, len(b.resources[stack.StackID]))
	for logicalID, res := range b.resources[stack.StackID] {
		physicalIDs[logicalID] = res.PhysicalID
	}

	uc := newUpdateContext(oldTemplateBody, oldParams, physicalIDs)

	// Side channel for property-time Fn::GetAtt during update -- see the
	// matching block in provisionResources and physIDResourceTypeKey.
	physicalIDs[physIDAccountIDKey] = b.accountID
	physicalIDs[physIDRegionKey] = b.region
	physicalIDs[physIDStackNameKey] = stack.StackName
	for logicalID, res := range tmpl.Resources {
		physicalIDs[physIDResourceTypeKey(logicalID)] = res.Type
	}

	// Validate that the update does not drop an export that another active
	// stack still imports via Fn::ImportValue (computed against pre-update
	// resource state, mirroring AWS's validate-before-apply semantics).
	if expErr := b.validateExportsStillInUse(stack, tmpl, resolvedParams, physicalIDs); expErr != nil {
		b.updateFailAndRollback(ctx, stack, expErr.Error())

		return false
	}

	if !b.updateResources(ctx, stack, tmpl, resolvedParams, physicalIDs, oldTemplateBody, uc) {
		return false
	}

	b.removeExports(stack.StackID)

	updateResourceTypes := make(map[string]string, len(tmpl.Resources))
	for logicalID, res := range tmpl.Resources {
		updateResourceTypes[logicalID] = res.Type
	}
	stack.ResourceAttrs = extractAttrStash(physicalIDs, updateResourceTypes)
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
	oldTemplateBody string,
	uc *updateContext,
) bool {
	// Snapshot pre-update state for rollback.
	prevResources := make(map[string]*StackResource, len(b.resources[stack.StackID]))
	for k, v := range b.resources[stack.StackID] {
		cp := *v
		prevResources[k] = &cp
	}

	ordered, ordErr := topoSortResources(tmpl.Resources)
	if ordErr != nil {
		b.updateFailAndRollback(ctx, stack, ordErr.Error())

		return false
	}

	pass := &updatePass{uc: uc, params: resolvedParams, phys: physicalIDs}

	for _, logicalID := range ordered {
		if err := b.applyResourceUpdate(ctx, stack, tmpl.Resources[logicalID], logicalID, pass); err != nil {
			stack.StackStatusReason = err.Error()
			b.rollbackUpdateResources(ctx, stack, prevResources, pass.created)

			return false
		}
	}

	if !b.deleteStaleResources(ctx, stack, tmpl, oldTemplateBody) {
		reason := "failed to delete one or more resources removed from the template"
		stack.StackStatus = statusUpdateFailed
		stack.StackStatusReason = reason
		b.addEvent(
			stack.StackID, stack.StackName, stack.StackName, stack.StackID,
			cfnStackType, statusUpdateFailed, reason,
		)

		return false
	}

	b.addEvent(
		stack.StackID, stack.StackName, stack.StackName, stack.StackID,
		cfnStackType, statusUpdateCleanupProgress, "",
	)
	b.cleanupReplacedResources(ctx, stack, pass.replaced)

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
	b.addProgressEvent(stack.StackID, stack.StackName, logicalID, "", res.Type, statusCreateInProgress)
	ctx = parentStackIDKey.Set(ctx, stack.StackID)
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

// deleteStaleResources removes logical IDs present in the stack but absent
// from the new template. It reports whether every stale resource was
// actually deleted; a resource that fails to delete is left registered
// rather than dropped, so it stays visible via DescribeStackResources.
func (b *InMemoryBackend) deleteStaleResources(
	ctx context.Context, stack *Stack, tmpl *Template, oldTemplateBody string,
) bool {
	var stale []string
	for logicalID := range b.resources[stack.StackID] {
		if _, inTemplate := tmpl.Resources[logicalID]; !inTemplate {
			stale = append(stale, logicalID)
		}
	}

	stale = reverseDependencyOrder(stale, oldTemplateBody)

	ok := true
	physIDs := b.deleteResolveContext(stack)

	for _, logicalID := range stale {
		res := b.resources[stack.StackID][logicalID]
		b.addEvent(
			stack.StackID,
			stack.StackName,
			logicalID,
			res.PhysicalID,
			res.Type,
			statusDeleteInProgress,
			"",
		)
		if res.DeletionPolicy != deletionPolicyRetain && res.DeletionPolicy != deletionPolicySnapshot {
			if delErr := b.creator.Delete(ctx, res.Type, res.PhysicalID, res.Properties, physIDs); delErr != nil {
				ok = false
				b.addEvent(
					stack.StackID, stack.StackName, logicalID, res.PhysicalID, res.Type,
					statusDeleteFailed, delErr.Error(),
				)

				continue
			}
		}
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

	return ok
}

// rollbackUpdateResources undoes a partially-applied update: the stack goes
// UPDATE_ROLLBACK_IN_PROGRESS and, in this step or the next one when async,
// every resource the update newly created is deleted and modified resources
// are restored to their pre-update snapshots.
func (b *InMemoryBackend) rollbackUpdateResources(
	ctx context.Context,
	stack *Stack,
	prevResources map[string]*StackResource,
	created []string,
) {
	b.beginUpdateRollbackLocked(ctx, stack, prevResources, created, stack.StackStatusReason)
}

func (b *InMemoryBackend) beginUpdateRollbackLocked(
	ctx context.Context,
	stack *Stack,
	prevResources map[string]*StackResource,
	created []string,
	reason string,
) {
	stack.StackStatus = statusUpdateRollbackInProgress
	b.addEvent(
		stack.StackID, stack.StackName, stack.StackName, stack.StackID,
		cfnStackType, statusUpdateRollbackInProgress, reason,
	)

	op := &pendingStackOp{Kind: pendingRollbackUpdate, PrevResources: prevResources, Created: created}

	b.stageLocked(ctx, stack.StackID, op, func() {
		b.runUpdateRollbackLocked(ctx, stack, prevResources, created)
	})
}

// runUpdateRollbackLocked ends in UPDATE_ROLLBACK_FAILED when a newly-created
// resource can't be deleted (it stays registered rather than being dropped),
// otherwise in UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS.
func (b *InMemoryBackend) runUpdateRollbackLocked(
	ctx context.Context,
	stack *Stack,
	prevResources map[string]*StackResource,
	created []string,
) {
	rollbackOK := true
	physIDs := b.deleteResolveContext(stack)

	for _, logicalID := range created {
		res, exists := b.resources[stack.StackID][logicalID]
		if !exists {
			continue
		}

		b.addEvent(
			stack.StackID, stack.StackName, logicalID, res.PhysicalID, res.Type, statusDeleteInProgress, "",
		)

		if delErr := b.creator.Delete(ctx, res.Type, res.PhysicalID, res.Properties, physIDs); delErr != nil {
			rollbackOK = false
			b.addEvent(
				stack.StackID, stack.StackName, logicalID, res.PhysicalID, res.Type,
				statusDeleteFailed, delErr.Error(),
			)

			continue
		}

		b.addEvent(
			stack.StackID, stack.StackName, logicalID, res.PhysicalID, res.Type, statusDeleteComplete, "",
		)
		delete(b.resources[stack.StackID], logicalID)
	}

	maps.Copy(b.resources[stack.StackID], prevResources)

	if !rollbackOK {
		reason := reasonRollbackDeleteFailed
		stack.StackStatus = statusUpdateRollbackFailed
		stack.StackStatusReason = reason
		b.addEvent(
			stack.StackID, stack.StackName, stack.StackName, stack.StackID,
			cfnStackType, statusUpdateRollbackFailed, reason,
		)

		return
	}

	stack.StackStatus = statusUpdateRollbackCleanup
	b.addEvent(
		stack.StackID, stack.StackName, stack.StackName, stack.StackID,
		cfnStackType, statusUpdateRollbackCleanup, "",
	)

	b.stageLocked(ctx, stack.StackID, &pendingStackOp{Kind: pendingRollbackUpdateCleanup}, func() {
		b.completeUpdateRollbackLocked(stack)
	})
}

func (b *InMemoryBackend) completeUpdateRollbackLocked(stack *Stack) {
	stack.StackStatus = statusUpdateRollbackComplete
	b.addEvent(
		stack.StackID, stack.StackName, stack.StackName, stack.StackID,
		cfnStackType, statusUpdateRollbackComplete, "",
	)
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
