package cloudformation

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"
)

type updateAction int

const (
	actNoop updateAction = iota
	actCreate
	actInPlace
	actReplace
	actExtensibility
)

// updatePass carries the resolved inputs of an update walk.
type updatePass struct {
	uc     *updateContext
	params map[string]string
	phys   map[string]string
}

func (b *InMemoryBackend) newUpdateOp(
	opts StackOptions, oldTemplateBody string, oldParams []Parameter,
) *pendingStackOp {
	return &pendingStackOp{
		Kind:                 pendingUpdate,
		DisableValidation:    opts.DisableValidation,
		ResourceTypes:        opts.ResourceTypes,
		RetainExceptOnCreate: opts.RetainExceptOnCreate,
		OldTemplateBody:      oldTemplateBody,
		OldParams:            oldParams,
	}
}

func oldPhysicalIDs(prev map[string]*StackResource, attrs map[string]string) map[string]string {
	out := stackPhysicalIDsSnapshot(prev)
	maps.Copy(out, attrs)

	return out
}

func (b *InMemoryBackend) updateSideKeys(stack *Stack, tmpl *Template, phys map[string]string) {
	phys[physIDAccountIDKey] = b.accountID
	phys[physIDRegionKey] = b.region
	phys[physIDStackNameKey] = stack.StackName

	for logicalID, res := range tmpl.Resources {
		phys[physIDResourceTypeKey(logicalID)] = res.Type
	}
}

// announceUpdateLocked emits the first wave of in-progress events when UpdateStack returns.
func (b *InMemoryBackend) announceUpdateLocked(ctx context.Context, stack *Stack, op *pendingStackOp) {
	rt, err := b.templateRuntime(ctx, stack)
	if err != nil {
		return
	}

	phys := make(map[string]string, len(b.resources[stack.StackID]))
	for id, res := range b.resources[stack.StackID] {
		phys[id] = res.PhysicalID
	}

	maps.Copy(phys, stack.ResourceAttrs)
	rt.uc = newUpdateContext(op.OldTemplateBody, op.OldParams, phys)

	pass := &updatePass{uc: rt.uc, params: rt.params, phys: phys}
	_, _ = b.announceUpdateWave(stack, op, rt, pass)
}

// runUpdateStepLocked starts or resumes the apply phase of an update.
func (b *InMemoryBackend) runUpdateStepLocked(ctx context.Context, stack *Stack, op *pendingStackOp) {
	if stack.TemplateBody == "" {
		b.completeUpdateLocked(stack)

		return
	}

	if op.PhysicalIDs == nil && !b.beginUpdateWalkLocked(ctx, stack, op) {
		return
	}

	b.walkUpdateLocked(ctx, stack, op)
}

// beginUpdateWalkLocked validates the new template and snapshots the pre-update state;
// false means the update already failed.
func (b *InMemoryBackend) beginUpdateWalkLocked(ctx context.Context, stack *Stack, op *pendingStackOp) bool {
	tmpl, params := b.parseAndValidateUpdateTemplate(ctx, stack, op)
	if tmpl == nil {
		return false
	}

	phys := make(map[string]string, len(b.resources[stack.StackID]))
	for logicalID, res := range b.resources[stack.StackID] {
		phys[logicalID] = res.PhysicalID
	}

	maps.Copy(phys, stack.ResourceAttrs)

	uc := newUpdateContext(op.OldTemplateBody, op.OldParams, phys)
	uc.retainExceptOnCreate = op.RetainExceptOnCreate

	b.updateSideKeys(stack, tmpl, phys)

	if expErr := b.validateExportsStillInUse(stack, tmpl, params, phys); expErr != nil {
		b.updateFailAndRollback(ctx, stack, op, expErr.Error())

		return false
	}

	if _, ordErr := topoSortResources(tmpl.Resources); ordErr != nil {
		b.updateFailAndRollback(ctx, stack, op, ordErr.Error())

		return false
	}

	op.PrevResources = make(map[string]*StackResource, len(b.resources[stack.StackID]))
	for k, v := range b.resources[stack.StackID] {
		cp := *v
		op.PrevResources[k] = &cp
	}

	op.PhysicalIDs = phys
	op.rt = &walkRuntime{tmpl: tmpl, uc: uc, params: params}

	return true
}

// updateRuntimeLocked rebuilds a resumed update walk's runtime from the persisted op.
func (b *InMemoryBackend) updateRuntimeLocked(ctx context.Context, stack *Stack, op *pendingStackOp) bool {
	rt, err := b.templateRuntime(ctx, stack)
	if err != nil {
		b.updateFailAndRollback(ctx, stack, op, err.Error())

		return false
	}

	rt.uc = newUpdateContext(op.OldTemplateBody, op.OldParams, oldPhysicalIDs(op.PrevResources, stack.ResourceAttrs))
	rt.uc.retainExceptOnCreate = op.RetainExceptOnCreate
	op.rt = rt

	return true
}

// walkUpdateLocked applies the new template one dependency wave at a time and
// then moves to the cleanup phase.
func (b *InMemoryBackend) walkUpdateLocked(ctx context.Context, stack *Stack, op *pendingStackOp) {
	if op.rt == nil && !b.updateRuntimeLocked(ctx, stack, op) {
		return
	}

	ctx = parentStackIDKey.Set(ctx, stack.StackID)
	pass := &updatePass{uc: op.rt.uc, params: op.rt.params, phys: op.PhysicalIDs}

	for {
		if len(op.InFlight) > 0 {
			if reason := b.executeUpdateWave(ctx, stack, op, pass); reason != "" {
				stack.StackStatusReason = reason
				b.beginUpdateRollbackLocked(ctx, stack, op, reason)

				return
			}
		}

		wave, err := b.announceUpdateWave(stack, op, op.rt, pass)
		if err != nil {
			stack.StackStatusReason = err.Error()
			b.beginUpdateRollbackLocked(ctx, stack, op, err.Error())

			return
		}

		if len(wave) == 0 {
			b.startUpdateCleanupLocked(ctx, stack, op)

			return
		}

		if b.stageWalk(ctx, stack, op) {
			return
		}
	}
}

// classifyUpdate decides what the update does to one template resource; an error is a
// pre-flight failure (the failure event is already recorded).
func (b *InMemoryBackend) classifyUpdate(
	stack *Stack, id string, res TemplateResource, pass *updatePass,
) (updateAction, *StackResource, error) {
	existing, exists := b.resources[stack.StackID][id]

	switch {
	case !exists:
		return actCreate, nil, nil
	case isCFNExtensibilityType(res.Type):
		return actExtensibility, existing, nil
	}

	change := b.planResourceUpdate(id, res, existing, pass.uc, pass.params, pass.phys)
	if len(change.changed) == 0 {
		existing.DeletionPolicy = res.DeletionPolicy

		return actNoop, existing, nil
	}

	if !change.replace {
		return actInPlace, existing, nil
	}

	if name, same := customNameUnchanged(res, existing, pass.uc, pass.params, pass.phys); same {
		err := fmt.Errorf("%w. Rename %s and update the stack again", ErrCustomNamedReplacement, name)
		b.addResourceEvent(stack, id, existing.PhysicalID, res.Type, statusUpdateFailed, err.Error())

		return actReplace, existing, fmt.Errorf("resource %s update: %w", id, err)
	}

	return actReplace, existing, nil
}

// announceUpdateWave picks the next wave of resources that change and emits their
// in-progress events; resources the update leaves alone are skipped silently.
func (b *InMemoryBackend) announceUpdateWave(
	stack *Stack, op *pendingStackOp, rt *walkRuntime, pass *updatePass,
) ([]string, error) {
	remaining := without(sortedKeys(rt.tmpl.Resources), op.Done...)

	for len(remaining) > 0 {
		candidates := nextWave(rt.tmpl.Resources, remaining, false)
		remaining = without(remaining, candidates...)

		var wave []string

		for _, id := range candidates {
			res := rt.tmpl.Resources[id]

			act, existing, err := b.classifyUpdate(stack, id, res, pass)
			if err != nil {
				return nil, err
			}

			if act == actNoop {
				op.Done = append(op.Done, id)

				continue
			}

			b.announceUpdateResource(stack, op, id, res, act, existing)
			wave = append(wave, id)
		}

		if len(wave) > 0 {
			return wave, nil
		}
	}

	return nil, nil
}

func (b *InMemoryBackend) announceUpdateResource(
	stack *Stack, op *pendingStackOp, id string, res TemplateResource, act updateAction, existing *StackResource,
) {
	status, phys, reason := statusUpdateInProgress, "", ""

	if existing != nil {
		phys = existing.PhysicalID
	}

	switch act {
	case actCreate:
		status = statusCreateInProgress
	case actReplace:
		reason = reasonReplacementRequired
	case actNoop, actInPlace, actExtensibility:
	}

	if op.InFlight == nil {
		op.InFlight = make(map[string]string)
	}

	op.InFlight[id] = status
	b.addResourceEvent(stack, id, phys, res.Type, status, reason)
}

// executeUpdateWave applies the announced wave and returns the failure reason, if any.
func (b *InMemoryBackend) executeUpdateWave(
	ctx context.Context, stack *Stack, op *pendingStackOp, pass *updatePass,
) string {
	ids := sortedKeys(op.InFlight)
	announced := op.InFlight
	op.InFlight = nil

	for i, id := range ids {
		res, ok := op.rt.tmpl.Resources[id]
		if !ok {
			continue
		}

		act, existing, _ := b.classifyUpdate(stack, id, res, pass)
		op.Done = append(op.Done, id)

		if err := b.applyUpdateAction(ctx, stack, op, id, res, act, existing, pass); err != nil {
			op.InFlight = make(map[string]string, len(ids)-i-1)
			for _, rest := range ids[i+1:] {
				op.InFlight[rest] = announced[rest]
			}

			return err.Error()
		}
	}

	return ""
}

func (b *InMemoryBackend) applyUpdateAction(
	ctx context.Context, stack *Stack, op *pendingStackOp, id string, res TemplateResource, act updateAction,
	existing *StackResource, pass *updatePass,
) error {
	switch act {
	case actCreate:
		physicalID, err := b.createUpdateResource(ctx, stack, id, res, pass.params, pass.phys)
		if err != nil {
			return fmt.Errorf("resource %s: %w", id, err)
		}

		pass.phys[id] = physicalID
		op.Created = appendUnique(op.Created, id)
	case actExtensibility:
		if err := b.updateExtensibilityResource(ctx, stack, id, res, existing); err != nil {
			return fmt.Errorf("resource %s update: %w", id, err)
		}

		op.Updated = appendUnique(op.Updated, id)
	case actInPlace:
		if err := b.updateInPlaceResource(ctx, stack, id, res, existing, pass); err != nil {
			return fmt.Errorf("resource %s update: %w", id, err)
		}

		op.Updated = appendUnique(op.Updated, id)
	case actReplace:
		repl, err := b.replaceResource(ctx, stack, id, res, existing, pass)
		if err != nil {
			return fmt.Errorf("resource %s update: %w", id, err)
		}

		op.Replaced = append(op.Replaced, *repl)
		op.Created = appendUnique(op.Created, id)
	case actNoop:
	}

	return nil
}

func (b *InMemoryBackend) updateInPlaceResource(
	ctx context.Context, stack *Stack, id string, res TemplateResource, existing *StackResource, pass *updatePass,
) error {
	_, err := b.creator.UpdateInPlace(ctx, res.Type, existing.PhysicalID, res.Properties, pass.params, pass.phys)
	if err != nil {
		b.addResourceEvent(stack, id, existing.PhysicalID, res.Type, statusUpdateFailed, err.Error())

		return err
	}

	existing.Properties = res.Properties
	existing.DeletionPolicy = res.DeletionPolicy
	existing.Status = statusUpdateComplete
	existing.Timestamp = time.Now()
	b.addResourceEvent(stack, id, existing.PhysicalID, res.Type, statusUpdateComplete, "")

	return nil
}

// startUpdateCleanupLocked enters UPDATE_COMPLETE_CLEANUP_IN_PROGRESS, where replaced
// and removed resources are deleted one dependency wave per step.
func (b *InMemoryBackend) startUpdateCleanupLocked(ctx context.Context, stack *Stack, op *pendingStackOp) {
	stack.StackStatus = statusUpdateCleanupProgress
	b.addStackEvent(stack, statusUpdateCleanupProgress, "")

	op.Kind = pendingUpdateCleanup
	op.Phase = phaseReplaced
	op.Done = nil
	op.FailedReasons = nil

	for _, r := range slices.Backward(op.Replaced) {
		op.Remaining = append(op.Remaining, r.Old.LogicalID)
	}

	if b.stageWalk(ctx, stack, op) {
		return
	}

	b.walkUpdateCleanupLocked(ctx, stack, op)
}

func (b *InMemoryBackend) walkUpdateCleanupLocked(ctx context.Context, stack *Stack, op *pendingStackOp) {
	if op.rt == nil && !b.updateRuntimeLocked(ctx, stack, op) {
		return
	}

	for {
		if b.walkReverseLocked(ctx, stack, op, b.cleanupWalk(stack, op)) {
			return
		}

		if op.Phase == phaseReplaced {
			op.Phase = phaseStale
			op.Remaining = b.staleResources(stack, op)
			op.FailedReasons = nil

			continue
		}

		break
	}

	if len(op.FailedReasons) > 0 {
		reason := "failed to delete one or more resources removed from the template"
		stack.StackStatus = statusUpdateFailed
		stack.StackStatusReason = reason
		b.addStackEvent(stack, statusUpdateFailed, reason)

		return
	}

	b.finalizeUpdateLocked(ctx, stack, op)
}

func (b *InMemoryBackend) staleResources(stack *Stack, op *pendingStackOp) []string {
	var stale []string

	for id := range b.resources[stack.StackID] {
		if _, in := op.rt.tmpl.Resources[id]; !in {
			stale = append(stale, id)
		}
	}

	return reverseDependencyOrder(stale, op.OldTemplateBody)
}

// cleanupWalk deletes the old physical resources of replacements, then resources the new template dropped.
func (b *InMemoryBackend) cleanupWalk(stack *Stack, op *pendingStackOp) reverseWalk {
	if op.Phase == phaseReplaced {
		olds := make(map[string]*StackResource, len(op.Replaced))

		for i := range op.Replaced {
			r := &op.Replaced[i]
			olds[r.Old.LogicalID] = &StackResource{
				LogicalID: r.Old.LogicalID, PhysicalID: r.Old.PhysicalID, Type: r.Old.Type,
				Properties: r.Old.Properties, DeletionPolicy: r.Policy,
			}
		}

		return b.deleteWalkOver(stack, op.rt.tmpl.Resources, func(id string) (*StackResource, bool) {
			res, ok := olds[id]

			return res, ok
		}, func(string, *StackResource) {}, deleteRules{
			retainStatus: statusDeleteComplete, failStatus: statusDeleteFailed, forceDrop: true,
		})
	}

	var deps map[string]TemplateResource
	if op.rt.uc != nil && op.rt.uc.oldTmpl != nil {
		deps = op.rt.uc.oldTmpl.Resources
	}

	return b.deleteWalkOver(stack, deps, func(id string) (*StackResource, bool) {
		res, ok := b.resources[stack.StackID][id]

		return res, ok
	}, func(id string, _ *StackResource) { delete(b.resources[stack.StackID], id) }, deleteRules{
		retainStatus: statusDeleteComplete, failStatus: statusDeleteFailed,
	})
}

// finalizeUpdateLocked resolves outputs and exports after the last cleanup wave, then settles.
func (b *InMemoryBackend) finalizeUpdateLocked(ctx context.Context, stack *Stack, op *pendingStackOp) {
	tmpl := op.rt.tmpl

	b.removeExports(stack.StackID)

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
		stack.StackStatus = statusUpdateFailed
		stack.StackStatusReason = regErr.Error()
		b.addStackEvent(stack, statusUpdateFailed, regErr.Error())

		return
	}

	b.settleUpdateLocked(ctx, stack)
}

// settleUpdateLocked completes the update once nested stacks have settled.
func (b *InMemoryBackend) settleUpdateLocked(ctx context.Context, stack *Stack) {
	if b.stepAsync && b.childrenInProgressLocked(stack) {
		b.scheduleLocked(ctx, stack.StackID, &pendingStackOp{Kind: pendingUpdateCleanup, Phase: phaseSettle})

		return
	}

	b.completeUpdateLocked(stack)
}
