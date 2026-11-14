package cloudformation

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// ErrInvalidDeleteRequest is returned for DeleteStack options the stack's state does not allow.
var ErrInvalidDeleteRequest = errors.New("invalid DeleteStack request")

type deleteSettings struct {
	retain []string
	force  bool
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

	stack.StackStatus = statusDeleteInProgress
	stack.StackStatusReason = ""
	b.addEvent(
		stack.StackID, stack.StackName, stack.StackName, stack.StackID,
		cfnStackType, statusDeleteInProgress, reasonUserInitiated,
	)

	if top && b.provisionSchedule != nil {
		op := &pendingStackOp{Kind: pendingDelete, RetainResources: s.retain, ForceDelete: s.force}
		op.Announced = b.announceDeleteLocked(stack)

		if b.scheduleLocked(ctx, stack.StackID, op) {
			return nil
		}
	}

	b.runDeleteLocked(ctx, stack, s)

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

// announceDeleteLocked emits DELETE_IN_PROGRESS for the resources deleted first.
func (b *InMemoryBackend) announceDeleteLocked(stack *Stack) []string {
	tmpl, err := ParseTemplate(stack.TemplateBody)
	if err != nil {
		return nil
	}

	live := sortedKeys(b.resources[stack.StackID])

	return b.announceLocked(stack, statusDeleteInProgress, firstWave(tmpl.Resources, live, true), tmpl.Resources)
}

// runDeleteLocked deletes the stack's resources in reverse dependency order,
// then either settles the stack or leaves it DELETE_FAILED for a retry.
func (b *InMemoryBackend) runDeleteLocked(ctx context.Context, stack *Stack, s deleteSettings) {
	live := sortedKeys(b.resources[stack.StackID])
	physIDs := b.deleteResolveContext(stack)

	var failed []string

	for _, logicalID := range reverseDependencyOrder(live, stack.TemplateBody) {
		res := b.resources[stack.StackID][logicalID]
		b.addProgressEvent(
			stack.StackID, stack.StackName, logicalID, res.PhysicalID, res.Type, statusDeleteInProgress,
		)

		status, reason := b.deleteOneResource(ctx, res, physIDs, slices.Contains(s.retain, logicalID))
		b.addEvent(stack.StackID, stack.StackName, logicalID, res.PhysicalID, res.Type, status, reason)

		if status == statusDeleteFailed {
			failed = append(failed, fmt.Sprintf("%s: %s", logicalID, reason))

			if !s.force {
				continue
			}
		}

		delete(b.resources[stack.StackID], logicalID)
	}

	// AWS never rolls DELETE_FAILED back to DELETE_COMPLETE: the stack, its
	// remaining resources, and its events all stay describable so the caller
	// can retry DeleteStack after fixing the underlying resource.
	if len(failed) > 0 && !s.force {
		stack.StackStatus = statusDeleteFailed
		stack.StackStatusReason = strings.Join(failed, "; ")
		b.addEvent(
			stack.StackID, stack.StackName, stack.StackName, stack.StackID,
			cfnStackType, statusDeleteFailed, stack.StackStatusReason,
		)

		return
	}

	if b.stepAsync && (stack.ParentID != "" || b.childrenInProgressLocked(stack)) {
		b.stageLocked(ctx, stack.StackID, &pendingStackOp{Kind: pendingDeleteFinalize}, func() {
			b.finishDeleteLocked(stack)
		})

		return
	}

	b.finishDeleteLocked(stack)
}

// deleteOneResource reports the resource event status for removing res from
// a stack: DELETE_SKIPPED for retained resources, DELETE_FAILED when the
// backend cannot delete it.
func (b *InMemoryBackend) deleteOneResource(
	ctx context.Context, res *StackResource, physIDs map[string]string, retainByCaller bool,
) (string, string) {
	switch {
	case retainByCaller || res.DeletionPolicy == deletionPolicyRetain:
		return statusDeleteSkipped, ""
	case res.DeletionPolicy == deletionPolicySnapshot:
		return statusDeleteComplete, ""
	}

	if err := b.creator.Delete(ctx, res.Type, res.PhysicalID, res.Properties, physIDs); err != nil {
		return statusDeleteFailed, err.Error()
	}

	return statusDeleteComplete, ""
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
	delete(b.changeSets, stack.StackName)
	b.pruneDriftDetections(stack.StackID)
	b.evictDeletedStacks()
}
