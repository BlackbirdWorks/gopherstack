package cloudformation

import (
	"context"
	"errors"
	"slices"
	"strings"
)

// ErrContinueRollback is returned for ContinueUpdateRollback requests the stack's state does not allow.
var ErrContinueRollback = errors.New("invalid ContinueUpdateRollback request")

// ContinueRollbackInput carries the optional members of ContinueUpdateRollback.
type ContinueRollbackInput struct {
	ClientRequestToken string
	RoleARN            string
	ResourcesToSkip    []string
	ForceRollback      bool
}

// updateFailAndRollback records UPDATE_FAILED then enters UPDATE_ROLLBACK_IN_PROGRESS
// for pre-flight update failures (no resources changed).
func (b *InMemoryBackend) updateFailAndRollback(ctx context.Context, stack *Stack, op *pendingStackOp, reason string) {
	stack.StackStatusReason = reason
	b.addStackEvent(stack, statusUpdateFailed, reason)
	b.beginUpdateRollbackLocked(ctx, stack, op, reason)
}

// cancelInFlightLocked fails the resources an update announced but did not get to.
func (b *InMemoryBackend) cancelInFlightLocked(stack *Stack, op *pendingStackOp) {
	if len(op.InFlight) == 0 {
		return
	}

	types := b.inFlightTypes(stack)

	for _, id := range sortedKeys(op.InFlight) {
		phys := ""
		if res, ok := b.resources[stack.StackID][id]; ok {
			phys = res.PhysicalID
		}

		status, reason := statusUpdateFailed, reasonUpdateCancelled
		if op.InFlight[id] == statusCreateInProgress {
			status, reason = statusCreateFailed, reasonCreateCancelled
		}

		b.addResourceEvent(stack, id, phys, types[id].Type, status, reason)
	}

	op.InFlight = nil
}

// beginUpdateRollbackLocked starts UPDATE_ROLLBACK_IN_PROGRESS for an update that
// applied the resources recorded on op (nil when none was touched).
func (b *InMemoryBackend) beginUpdateRollbackLocked(
	ctx context.Context, stack *Stack, op *pendingStackOp, reason string,
) {
	rb := &pendingStackOp{Kind: pendingRollbackUpdate, TemplateBody: stack.TemplateBody}

	if op != nil {
		b.cancelInFlightLocked(stack, op)

		rb.PrevResources = op.PrevResources
		rb.Created = op.Created
		rb.Updated = op.Updated
		rb.OldTemplateBody = op.OldTemplateBody
		rb.OldParams = op.OldParams
		rb.RetainExceptOnCreate = op.RetainExceptOnCreate

		for _, id := range slices.Concat(op.Created, op.Updated) {
			rb.Remaining = appendUnique(rb.Remaining, id)
		}

		b.restorePreviousTemplate(stack, op)
	}

	stack.StackStatus = statusUpdateRollbackInProgress
	b.addStackEvent(stack, statusUpdateRollbackInProgress, reason)

	b.startUpdateRollbackLocked(ctx, stack, rb)
}

func (b *InMemoryBackend) restorePreviousTemplate(stack *Stack, op *pendingStackOp) {
	if op.OldTemplateBody == "" {
		return
	}

	stack.TemplateBody = op.OldTemplateBody
	stack.Parameters = op.OldParams

	if tmpl, err := ParseTemplate(op.OldTemplateBody); err == nil {
		stack.Description = tmpl.Description
	}
}

// startUpdateRollbackLocked announces the first rollback wave and runs the rollback,
// in the following steps when async.
func (b *InMemoryBackend) startUpdateRollbackLocked(ctx context.Context, stack *Stack, op *pendingStackOp) {
	b.announceReverseWave(stack, op, b.updateRollbackWalk(stack, op))
	b.stageLocked(ctx, stack.StackID, op, func() { b.runUpdateRollbackLocked(ctx, stack, op) })
}

// updateRollbackWalk undoes the update: new resources are deleted, modified ones
// restored; a resource that cannot be rolled back ends UPDATE_FAILED.
func (b *InMemoryBackend) updateRollbackWalk(stack *Stack, op *pendingStackOp) reverseWalk {
	var deps map[string]TemplateResource

	if tmpl, err := ParseTemplate(op.TemplateBody); err == nil {
		deps = tmpl.Resources
	}

	uc := newUpdateContext(op.OldTemplateBody, op.OldParams, oldPhysicalIDs(op.PrevResources, stack.ResourceAttrs))
	physIDs := b.deleteResolveContext(stack)
	rules := deleteRules{
		retainStatus: statusDeleteComplete, failStatus: statusUpdateFailed, ignorePolicy: op.RetainExceptOnCreate,
	}

	return reverseWalk{
		deps: deps,
		target: func(id string) (string, string, bool) {
			res, ok := b.resources[stack.StackID][id]
			if !ok {
				return "", "", false
			}

			return res.Type, res.PhysicalID, true
		},
		progress: func(id string) string {
			if slices.Contains(op.Created, id) {
				return statusDeleteInProgress
			}

			return statusUpdateInProgress
		},
		apply: func(ctx context.Context, id string) (string, string) {
			cur := b.resources[stack.StackID][id]
			prev := op.PrevResources[id]

			status, reason := b.rollbackOne(ctx, cur, prev, id, op, uc, physIDs, rules)
			if isFailedResourceStatus(status) {
				cur.Status = statusUpdateFailed

				return statusUpdateFailed, reason
			}

			if prev != nil {
				restored := *prev
				b.resources[stack.StackID][id] = &restored
			} else {
				delete(b.resources[stack.StackID], id)
			}

			return status, ""
		},
	}
}

func (b *InMemoryBackend) rollbackOne(
	ctx context.Context, cur, prev *StackResource, id string, op *pendingStackOp, uc *updateContext,
	physIDs map[string]string, rules deleteRules,
) (string, string) {
	if slices.Contains(op.Created, id) {
		return b.deleteOneResource(ctx, cur, physIDs, id, rules)
	}

	if prev == nil {
		return statusUpdateFailed, "no previous state recorded for " + id
	}

	var err error

	if isCFNExtensibilityType(prev.Type) {
		err = b.creator.Update(ctx, id, prev.Type, prev.PhysicalID, prev.Properties, cur.Properties)
	} else {
		_, err = b.creator.UpdateInPlace(ctx, prev.Type, prev.PhysicalID, prev.Properties, uc.oldParams, uc.oldPhys)
	}

	if err != nil {
		return statusUpdateFailed, err.Error()
	}

	return statusUpdateComplete, ""
}

// runUpdateRollbackLocked ends in UPDATE_ROLLBACK_FAILED when a resource cannot be
// rolled back (kept for ContinueUpdateRollback), otherwise in
// UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS.
func (b *InMemoryBackend) runUpdateRollbackLocked(ctx context.Context, stack *Stack, op *pendingStackOp) {
	if b.walkReverseLocked(ctx, stack, op, b.updateRollbackWalk(stack, op)) {
		return
	}

	if len(op.FailedReasons) > 0 {
		reason := reasonRollbackDeleteFailed
		stack.StackStatus = statusUpdateRollbackFailed
		stack.StackStatusReason = reason
		op.Remaining = sortedKeys(op.FailedReasons)
		b.failedRollbacks[stack.StackID] = op
		b.addStackEvent(stack, statusUpdateRollbackFailed, reason)

		return
	}

	delete(b.failedRollbacks, stack.StackID)

	stack.StackStatus = statusUpdateRollbackCleanup
	b.addStackEvent(stack, statusUpdateRollbackCleanup, "")

	b.stageLocked(ctx, stack.StackID, &pendingStackOp{Kind: pendingRollbackUpdateCleanup}, func() {
		b.completeUpdateRollbackLocked(stack)
	})
}

func (b *InMemoryBackend) completeUpdateRollbackLocked(stack *Stack) {
	stack.StackStatus = statusUpdateRollbackComplete
	b.addStackEvent(stack, statusUpdateRollbackComplete, "")
}

// ContinueUpdateRollback retries the rollback of a stack in UPDATE_ROLLBACK_FAILED,
// skipping the resources the caller names (or all failed ones with ForceRollback).
func (b *InMemoryBackend) ContinueUpdateRollback(ctx context.Context, nameOrID string, in ContinueRollbackInput) error {
	if err := ValidateRoleARN(in.RoleARN); err != nil {
		return err
	}

	if in.ForceRollback && len(in.ResourcesToSkip) > 0 {
		return awsErrorf(ErrContinueRollback, "ForceRollback and ResourcesToSkip are mutually exclusive.")
	}

	b.mu.Lock("ContinueUpdateRollback")
	defer b.mu.Unlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return ErrStackNotFound
	}

	return b.continueRollbackLocked(ctx, stack, in)
}

func (b *InMemoryBackend) continueRollbackLocked(ctx context.Context, stack *Stack, in ContinueRollbackInput) error {
	if stack.StackStatus != statusUpdateRollbackFailed {
		return awsErrorf(
			ErrContinueRollback, "Stack:%s is in %s state and can not be continue rolled back.",
			stack.StackID, stack.StackStatus,
		)
	}

	op := b.failedRollbackOp(stack)

	skip, err := b.resolveSkips(ctx, stack, op, in)
	if err != nil {
		return err
	}

	if in.RoleARN != "" {
		stack.RoleARN = in.RoleARN
	}

	for _, id := range skip {
		cur := b.resources[stack.StackID][id]
		cur.Status = statusUpdateComplete
		b.addResourceEvent(stack, id, cur.PhysicalID, cur.Type, statusUpdateComplete, reasonUpdateSkipped)
		op.Remaining = without(op.Remaining, id)
	}

	delete(b.failedRollbacks, stack.StackID)

	op.FailedReasons = nil
	stack.StackStatus = statusUpdateRollbackInProgress
	stack.StackStatusReason = ""
	b.addStackEvent(stack, statusUpdateRollbackInProgress, reasonUserInitiated)

	stepAsync := b.stepAsync
	b.stepAsync = true

	defer func() { b.stepAsync = stepAsync }()

	b.startUpdateRollbackLocked(ctx, stack, op)

	return nil
}

// failedRollbackOp returns the saved rollback of the stack, or rebuilds the
// remaining work from the resources left in UPDATE_FAILED.
func (b *InMemoryBackend) failedRollbackOp(stack *Stack) *pendingStackOp {
	if op := b.failedRollbacks[stack.StackID]; op != nil {
		return op
	}

	op := &pendingStackOp{Kind: pendingRollbackUpdate, TemplateBody: stack.TemplateBody}

	for _, id := range sortedKeys(b.resources[stack.StackID]) {
		if b.resources[stack.StackID][id].Status == statusUpdateFailed {
			op.Remaining = append(op.Remaining, id)
		}
	}

	return op
}

// resolveSkips validates ResourcesToSkip against the resources that failed to roll back.
func (b *InMemoryBackend) resolveSkips(
	ctx context.Context, stack *Stack, op *pendingStackOp, in ContinueRollbackInput,
) ([]string, error) {
	if in.ForceRollback {
		return slices.Clone(op.Remaining), nil
	}

	var skip, invalid []string

	for _, id := range in.ResourcesToSkip {
		switch {
		case slices.Contains(op.Remaining, id):
			if b.embeddedStackBlocksSkip(stack, id) {
				invalid = append(invalid, id)

				continue
			}

			skip = appendUnique(skip, id)
		case strings.Contains(id, "."):
			if err := b.skipInNestedStack(ctx, stack, id, in); err != nil {
				return nil, err
			}
		default:
			invalid = append(invalid, id)
		}
	}

	if len(invalid) > 0 {
		return nil, awsErrorf(
			ErrContinueRollback,
			"Resources [%s] cannot be skipped: only resources in UPDATE_FAILED because a rollback failed can be skipped.",
			strings.Join(invalid, ", "),
		)
	}

	return skip, nil
}

// embeddedStackBlocksSkip reports whether id is a nested-stack resource whose
// embedded stack is not yet DELETE_IN_PROGRESS, DELETE_COMPLETE or DELETE_FAILED.
func (b *InMemoryBackend) embeddedStackBlocksSkip(stack *Stack, id string) bool {
	res := b.resources[stack.StackID][id]
	if res == nil || res.Type != cfnStackType {
		return false
	}

	child, ok := b.resolveStack(res.PhysicalID)
	if !ok {
		return false
	}

	switch child.StackStatus {
	case statusDeleteInProgress, statusDeleteComplete, statusDeleteFailed:
		return false
	}

	return true
}

// skipInNestedStack continues the rollback of the embedded stack named by the
// NestedStackName.ResourceLogicalID form of a skip entry.
func (b *InMemoryBackend) skipInNestedStack(
	ctx context.Context, stack *Stack, entry string, in ContinueRollbackInput,
) error {
	name, logicalID, _ := strings.Cut(entry, ".")

	child, ok := b.resolveStack(name)
	if !ok || child.ParentID != stack.StackID {
		return awsErrorf(
			ErrContinueRollback, "Stack %s is not a nested stack of %s, so %s cannot be skipped.",
			name, stack.StackName, entry,
		)
	}

	return b.continueRollbackLocked(ctx, child, ContinueRollbackInput{
		ResourcesToSkip: []string{logicalID}, RoleARN: in.RoleARN,
	})
}
