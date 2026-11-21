package cloudformation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrInvalidDeleteRequest is returned for DeleteStack options the stack's state does not allow.
var ErrInvalidDeleteRequest = errors.New("invalid DeleteStack request")

type deleteSettings struct {
	retain      []string
	force       bool
	settleLater bool
}

// DeleteStackOption customises DeleteStack.
type DeleteStackOption func(*deleteSettings)

// WithRetainResources keeps the named resources when deleting a DELETE_FAILED stack.
func WithRetainResources(logicalIDs []string) DeleteStackOption {
	return func(s *deleteSettings) { s.retain = logicalIDs }
}

// WithForceDelete is DeletionMode FORCE_DELETE_STACK: resources that fail to delete are dropped.
func WithForceDelete() DeleteStackOption {
	return func(s *deleteSettings) { s.force = true }
}

// DeleteStack moves the stack to DELETE_IN_PROGRESS and, with async
// provisioning on, finishes the deletion one step later.
func (b *InMemoryBackend) DeleteStack(ctx context.Context, nameOrID string, opts ...DeleteStackOption) error {
	var s deleteSettings

	for _, o := range opts {
		o(&s)
	}

	b.mu.Lock("DeleteStack")
	defer b.mu.Unlock()

	return b.deleteStackWithLocked(ctx, nameOrID, s, true)
}

// deleteStackLocked deletes a stack synchronously without taking the mutex.
func (b *InMemoryBackend) deleteStackLocked(ctx context.Context, nameOrID string) error {
	return b.deleteStackWithLocked(ctx, nameOrID, deleteSettings{}, false)
}

func (b *InMemoryBackend) deleteStackWithLocked(
	ctx context.Context, nameOrID string, s deleteSettings, top bool,
) error {
	stack, ok := b.resolveStack(nameOrID)
	if !ok || stack.StackStatus == statusDeleteComplete {
		// DeleteStack has no modeled "stack not found" error: deleting a stack
		// that never existed or is already deleted is a silent no-op.
		return nil
	}

	if stack.StackStatus == statusDeleteInProgress && b.pendingOps[stack.StackID] != nil {
		return nil
	}

	if err := b.checkDeletable(stack, s); err != nil {
		return err
	}

	delete(b.pendingOps, stack.StackID)
	delete(b.failedRollbacks, stack.StackID)

	stack.StackStatus = statusDeleteInProgress
	stack.StackStatusReason = ""
	b.addStackEvent(stack, statusDeleteInProgress, reasonUserInitiated)

	op := &pendingStackOp{
		Kind: pendingDelete, RetainResources: s.retain, ForceDelete: s.force, SettleLater: s.settleLater,
		Remaining: sortedKeys(b.resources[stack.StackID]), TemplateBody: stack.TemplateBody,
	}

	if top && b.provisionSchedule != nil {
		b.announceReverseWave(stack, op, b.stackDeleteWalk(stack, op))

		if b.scheduleLocked(ctx, stack.StackID, op) {
			return nil
		}
	}

	b.runDeleteLocked(ctx, stack, op)

	return nil
}

func (b *InMemoryBackend) checkDeletable(stack *Stack, s deleteSettings) error {
	if len(s.retain) > 0 || s.force {
		if stack.StackStatus != statusDeleteFailed {
			return awsErrorf(
				ErrInvalidDeleteRequest,
				"RetainResources and DeletionMode can only be specified for stacks in DELETE_FAILED state",
			)
		}

		if missing := b.missingResources(stack, s.retain); len(missing) > 0 {
			return awsErrorf(
				ErrInvalidDeleteRequest,
				"Specified retain resource(s) [%s] do not exist in stack %s",
				strings.Join(missing, ", "), stack.StackName,
			)
		}
	}

	if stack.EnableTerminationProtection {
		return fmt.Errorf("%w: %s", ErrTerminationProtectionEnabled, stack.StackName)
	}

	if name, importer, inUse := b.stackExportsInUse(stack.StackID, nil); inUse {
		return fmt.Errorf(
			"%w: Export %s cannot be deleted as it is in use by %s",
			ErrExportInUse, name, importer,
		)
	}

	return nil
}

func (b *InMemoryBackend) missingResources(stack *Stack, ids []string) []string {
	var missing []string

	for _, id := range ids {
		if _, ok := b.resources[stack.StackID][id]; !ok {
			missing = append(missing, id)
		}
	}

	return missing
}

func (b *InMemoryBackend) stackDeleteWalk(stack *Stack, op *pendingStackOp) reverseWalk {
	return b.deleteWalk(stack, op, deleteRules{
		retainStatus: statusDeleteSkipped, failStatus: statusDeleteFailed,
		callerRetain: op.RetainResources, forceDrop: op.ForceDelete,
	})
}

// runDeleteLocked deletes the stack's resources in reverse dependency order,
// then either settles the stack or leaves it DELETE_FAILED for a retry.
func (b *InMemoryBackend) runDeleteLocked(ctx context.Context, stack *Stack, op *pendingStackOp) {
	if b.walkReverseLocked(ctx, stack, op, b.stackDeleteWalk(stack, op)) {
		return
	}

	// AWS never rolls DELETE_FAILED back to DELETE_COMPLETE: the stack, its
	// remaining resources, and its events all stay describable so the caller
	// can retry DeleteStack after fixing the underlying resource.
	if len(op.FailedReasons) > 0 && !op.ForceDelete {
		stack.StackStatus = statusDeleteFailed
		stack.StackStatusReason = failedSummary(op.FailedReasons)
		b.addStackEvent(stack, statusDeleteFailed, stack.StackStatusReason)

		return
	}

	if op.SettleLater || b.stepAsync && (stack.ParentID != "" || b.childrenInProgressLocked(stack)) {
		stepAsync := b.stepAsync
		b.stepAsync = true

		defer func() { b.stepAsync = stepAsync }()

		b.stageLocked(ctx, stack.StackID, &pendingStackOp{Kind: pendingDeleteFinalize}, func() {
			b.finishDeleteLocked(stack)
		})

		return
	}

	b.finishDeleteLocked(stack)
}

// finalizeDeleteLocked waits for nested stacks to settle, then completes the delete.
func (b *InMemoryBackend) finalizeDeleteLocked(ctx context.Context, stack *Stack, op *pendingStackOp) {
	if b.childrenInProgressLocked(stack) {
		b.scheduleLocked(ctx, stack.StackID, op)

		return
	}

	b.finishDeleteLocked(stack)
}

// finishDeleteLocked marks the stack DELETE_COMPLETE. The stack stays
// describable by StackId; its events are kept like AWS keeps them.
func (b *InMemoryBackend) finishDeleteLocked(stack *Stack) {
	now := time.Now()
	stack.DeletionTime = &now
	stack.StackStatus = statusDeleteComplete
	stack.StackStatusReason = ""
	b.addEvent(
		stack.StackID, stack.StackName, stack.StackName, stack.StackID,
		cfnStackType, statusDeleteComplete, "",
	)
	b.removeExports(stack.StackID)
	delete(b.stackPolicies, stack.StackID)
	delete(b.resources, stack.StackID)
	delete(b.failedRollbacks, stack.StackID)
	delete(b.changeSets, stack.StackName)
	b.pruneDriftDetections(stack.StackID)
	b.evictDeletedStacks()
}
