package cloudformation

import (
	"context"
	"fmt"
	"slices"
	"time"
)

// announceCreateLocked emits CREATE_IN_PROGRESS for the resources that start
// first, so the window shows them before the first step runs.
func (b *InMemoryBackend) announceCreateLocked(stack *Stack, params []Parameter) map[string]string {
	tmpl, err := ParseTemplate(stack.TemplateBody)
	if err != nil {
		return nil
	}

	pruneConditional(tmpl, ResolveParameters(tmpl, params))

	wave := nextWave(tmpl.Resources, sortedKeys(tmpl.Resources), false)
	inFlight := make(map[string]string, len(wave))

	for _, id := range wave {
		inFlight[id] = statusCreateInProgress
		b.addResourceEvent(stack, id, "", tmpl.Resources[id].Type, statusCreateInProgress, "")
	}

	return inFlight
}

func seedCreatePhysicalIDs(stack *Stack, tmpl *Template, accountID, region string) map[string]string {
	phys := map[string]string{
		"_StackId":         stack.StackID,
		"_StackName":       stack.StackName,
		physIDAccountIDKey: accountID,
		physIDRegionKey:    region,
	}

	for id, res := range tmpl.Resources {
		phys[physIDResourceTypeKey(id)] = res.Type
	}

	return phys
}

func (b *InMemoryBackend) newCreateWalk(
	stack *Stack, rt *walkRuntime, opts createFinish,
) *pendingStackOp {
	return &pendingStackOp{
		Kind:                 pendingCreateWalk,
		OnFailure:            opts.onFailure,
		ResourceTypes:        opts.resourceTypes,
		DisableValidation:    opts.disableValidation,
		RetainExceptOnCreate: opts.retainExceptOnCreate,
		PhysicalIDs:          seedCreatePhysicalIDs(stack, rt.tmpl, b.accountID, b.region),
		InFlight:             opts.announced,
		rt:                   rt,
	}
}

func (op *pendingStackOp) createFinish() createFinish {
	return createFinish{
		onFailure:            op.OnFailure,
		resourceTypes:        op.ResourceTypes,
		disableValidation:    op.DisableValidation,
		retainExceptOnCreate: op.RetainExceptOnCreate,
	}
}

// templateRuntime re-derives a resumed walk's template and resolved parameters.
func (b *InMemoryBackend) templateRuntime(ctx context.Context, stack *Stack) (*walkRuntime, error) {
	tmpl, err := ParseTemplate(stack.TemplateBody)
	if err != nil {
		return nil, err
	}

	if dynErr := ResolveDynamicRefsInTemplate(ctx, tmpl, b.resolver); dynErr != nil {
		return nil, dynErr
	}

	params := ResolveParameters(tmpl, stack.Parameters)
	pruneConditional(tmpl, params)

	return &walkRuntime{tmpl: tmpl, params: params}, nil
}

// createWalkPending reports whether a create walk has a later step scheduled.
func (b *InMemoryBackend) createWalkPending(stack *Stack) bool {
	op := b.pendingOps[stack.StackID]

	return op != nil && op.Kind == pendingCreateWalk
}

// walkCreateLocked creates the template's resources one dependency wave at a
// time: each step finishes the announced wave and announces the next.
func (b *InMemoryBackend) walkCreateLocked(ctx context.Context, stack *Stack, op *pendingStackOp) {
	if op.rt == nil {
		rt, err := b.templateRuntime(ctx, stack)
		if err != nil {
			b.failAndRollback(ctx, stack, op.createFinish(), err.Error())

			return
		}

		op.rt = rt
	}

	ctx = parentStackIDKey.Set(ctx, stack.StackID)

	for {
		if len(op.InFlight) > 0 {
			if reason := b.executeCreateWave(ctx, stack, op); reason != "" {
				b.enterCreateRollbackLocked(ctx, stack, op.Created, op.createFinish(), reason)

				return
			}
		}

		remaining := b.createRemaining(op)
		if len(remaining) == 0 {
			b.finalizeCreateLocked(ctx, stack, op)

			return
		}

		wave := nextWave(op.rt.tmpl.Resources, remaining, false)

		op.InFlight = make(map[string]string, len(wave))

		for _, id := range wave {
			op.InFlight[id] = statusCreateInProgress
			b.addResourceEvent(stack, id, "", op.rt.tmpl.Resources[id].Type, statusCreateInProgress, "")
		}

		if b.stageWalk(ctx, stack, op) {
			return
		}
	}
}

func (b *InMemoryBackend) createRemaining(op *pendingStackOp) []string {
	remaining := make([]string, 0, len(op.rt.tmpl.Resources))

	for _, id := range sortedKeys(op.rt.tmpl.Resources) {
		if !slices.Contains(op.Created, id) {
			remaining = append(remaining, id)
		}
	}

	return remaining
}

// executeCreateWave creates the announced resources and returns the failure reason, if any.
func (b *InMemoryBackend) executeCreateWave(ctx context.Context, stack *Stack, op *pendingStackOp) string {
	ids := sortedKeys(op.InFlight)
	op.InFlight = nil
	reason := ""

	for _, id := range ids {
		res, ok := op.rt.tmpl.Resources[id]
		if !ok {
			continue
		}

		physicalID, err := b.creator.Create(ctx, id, res.Type, res.Properties, op.rt.params, op.PhysicalIDs)
		if err != nil {
			b.addResourceEvent(stack, id, "", res.Type, statusCreateFailed, err.Error())

			if reason == "" {
				reason = err.Error()
				stack.StackStatusReason = fmt.Sprintf("resource %s: %v", id, err)
			}

			continue
		}

		b.addResourceEvent(stack, id, physicalID, res.Type, statusCreateInProgress, reasonCreationInitiated)
		op.PhysicalIDs[id] = physicalID
		b.resources[stack.StackID][id] = &StackResource{
			Timestamp: time.Now(), LogicalID: id, PhysicalID: physicalID, Type: res.Type,
			Status: statusCreateComplete, Properties: res.Properties, StackID: stack.StackID,
			StackName: stack.StackName, DeletionPolicy: res.DeletionPolicy,
		}
		b.addResourceEvent(stack, id, physicalID, res.Type, statusCreateComplete, "")
		op.Created = append(op.Created, id)
	}

	return reason
}

// finalizeCreateLocked resolves outputs and registers exports once every resource exists.
func (b *InMemoryBackend) finalizeCreateLocked(_ context.Context, stack *Stack, op *pendingStackOp) {
	tmpl := op.rt.tmpl

	resourceTypes := make(map[string]string, len(tmpl.Resources))
	for logicalID, res := range tmpl.Resources {
		resourceTypes[logicalID] = res.Type
	}

	stack.ResourceAttrs = extractAttrStash(op.PhysicalIDs, resourceTypes)
	rctx := resolveCtx{
		params:        op.rt.params,
		physicalIDs:   op.PhysicalIDs,
		resourceTypes: resourceTypes,
		exports:       b.buildExportsMap(),
		conditions:    evaluateConditions(tmpl.Conditions, op.rt.params, op.PhysicalIDs),
		mappings:      tmpl.Mappings,
		accountID:     b.accountID,
		region:        b.region,
		stackName:     stack.StackName,
	}

	var exportMap map[string]string
	stack.Outputs, exportMap = resolveOutputsWithContext(tmpl, rctx)

	if regErr := b.registerExports(stack.StackID, exportMap); regErr != nil {
		stack.StackStatus = statusCreateFailed
		stack.StackStatusReason = regErr.Error()
		b.addStackEvent(stack, statusCreateFailed, regErr.Error())
	}
}

// enterCreateRollbackLocked moves a failed create to ROLLBACK_IN_PROGRESS and
// undoes created resource by resource, in the next steps when async.
func (b *InMemoryBackend) enterCreateRollbackLocked(
	ctx context.Context, stack *Stack, created []string, opts createFinish, reason string,
) {
	if stack.DisableRollback {
		stack.StackStatus = statusCreateFailed

		return
	}

	stack.StackStatus = statusRollbackInProgress
	b.addStackEvent(stack, statusRollbackInProgress, reason)

	op := &pendingStackOp{
		Kind:                 pendingRollbackCreate,
		OnFailure:            opts.onFailure,
		RetainExceptOnCreate: opts.retainExceptOnCreate,
		Created:              created,
		Remaining:            slices.Clone(created),
		TemplateBody:         stack.TemplateBody,
	}

	b.announceReverseWave(stack, op, b.deleteWalk(stack, op, createRollbackRules(op)))
	b.stageLocked(ctx, stack.StackID, op, func() { b.runCreateRollbackLocked(ctx, stack, op) })
}

func createRollbackRules(op *pendingStackOp) deleteRules {
	return deleteRules{
		retainStatus: statusDeleteComplete, failStatus: statusDeleteFailed, ignorePolicy: op.RetainExceptOnCreate,
	}
}

// runCreateRollbackLocked deletes the resources a failed create made and
// settles the stack ROLLBACK_COMPLETE or ROLLBACK_FAILED.
func (b *InMemoryBackend) runCreateRollbackLocked(ctx context.Context, stack *Stack, op *pendingStackOp) {
	if b.walkReverseLocked(ctx, stack, op, b.deleteWalk(stack, op, createRollbackRules(op))) {
		return
	}

	opts := op.createFinish()

	if len(op.FailedReasons) == 0 {
		b.addStackEvent(stack, statusRollbackComplete, "")
		stack.StackStatus = statusRollbackComplete
		b.deleteAfterFailedCreateLocked(ctx, stack, opts)

		return
	}

	b.addStackEvent(stack, statusRollbackFailed, reasonRollbackDeleteFailed)
	stack.StackStatus = statusRollbackFailed
	stack.StackStatusReason = reasonRollbackDeleteFailed
}

// deleteRules says how a reverse walk treats retention policies and failures.
type deleteRules struct {
	retainStatus string
	failStatus   string
	callerRetain []string
	ignorePolicy bool
	forceDrop    bool
}

// deleteWalk builds the reverse walk that deletes op.Remaining from the stack's live resources.
func (b *InMemoryBackend) deleteWalk(stack *Stack, op *pendingStackOp, rules deleteRules) reverseWalk {
	var deps map[string]TemplateResource

	if tmpl, err := ParseTemplate(op.TemplateBody); err == nil {
		deps = tmpl.Resources
	}

	live := func(id string) (*StackResource, bool) {
		res, ok := b.resources[stack.StackID][id]

		return res, ok
	}

	return b.deleteWalkOver(stack, deps, live, func(id string, _ *StackResource) {
		delete(b.resources[stack.StackID], id)
	}, rules)
}

// deleteWalkOver is deleteWalk for any source of records; drop runs for each
// record that is gone from the stack afterwards.
func (b *InMemoryBackend) deleteWalkOver(
	stack *Stack,
	deps map[string]TemplateResource,
	lookup func(id string) (*StackResource, bool),
	drop func(id string, res *StackResource),
	rules deleteRules,
) reverseWalk {
	physIDs := b.deleteResolveContext(stack)

	return reverseWalk{
		deps: deps,
		target: func(id string) (string, string, bool) {
			res, ok := lookup(id)
			if !ok {
				return "", "", false
			}

			return res.Type, res.PhysicalID, true
		},
		progress: func(string) string { return statusDeleteInProgress },
		apply: func(ctx context.Context, id string) (string, string) {
			res, _ := lookup(id)
			status, reason := b.deleteOneResource(ctx, res, physIDs, id, rules)

			if !isFailedResourceStatus(status) || rules.forceDrop {
				drop(id, res)
			} else {
				res.Status = status
			}

			return status, reason
		},
	}
}

// deleteOneResource reports the event status for removing res: the retain
// status for kept resources, the failure status when the backend cannot delete it.
func (b *InMemoryBackend) deleteOneResource(
	ctx context.Context, res *StackResource, physIDs map[string]string, id string, rules deleteRules,
) (string, string) {
	policy := res.DeletionPolicy

	switch {
	case slices.Contains(rules.callerRetain, id):
		return statusDeleteSkipped, ""
	case policy == deletionPolicyRetain && !rules.ignorePolicy:
		return rules.retainStatus, ""
	case policy == deletionPolicySnapshot && !rules.ignorePolicy:
		return statusDeleteComplete, ""
	}

	if err := b.creator.Delete(ctx, res.Type, res.PhysicalID, res.Properties, physIDs); err != nil {
		return rules.failStatus, err.Error()
	}

	return statusDeleteComplete, ""
}
