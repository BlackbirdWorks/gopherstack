package cloudformation

import (
	"context"
	"slices"
	"strings"
	"time"
)

const (
	pendingCreate                = "create"
	pendingCreateSettle          = "createSettle"
	pendingRollbackCreate        = "rollbackCreate"
	pendingUpdate                = "update"
	pendingUpdateCleanup         = "updateCleanup"
	pendingRollbackUpdate        = "rollbackUpdate"
	pendingRollbackUpdateCleanup = "rollbackUpdateCleanup"
	pendingDelete                = "delete"
	pendingDeleteFinalize        = "deleteFinalize"

	suffixInProgress = "_IN_PROGRESS"
)

// pendingStackOp is the deferred half of an async stack operation; persisted
// so a restored snapshot can finish it.
type pendingStackOp struct {
	PrevResources        map[string]*StackResource `json:"prevResources,omitempty"`
	Kind                 string                    `json:"kind"`
	OnFailure            string                    `json:"onFailure,omitempty"`
	OldTemplateBody      string                    `json:"oldTemplateBody,omitempty"`
	ResourceTypes        []string                  `json:"resourceTypes,omitempty"`
	OldParams            []Parameter               `json:"oldParams,omitempty"`
	Created              []string                  `json:"created,omitempty"`
	Announced            []string                  `json:"announced,omitempty"`
	RetainResources      []string                  `json:"retainResources,omitempty"`
	DisableValidation    bool                      `json:"disableValidation,omitempty"`
	RetainExceptOnCreate bool                      `json:"retainExceptOnCreate,omitempty"`
	ForceDelete          bool                      `json:"forceDelete,omitempty"`
}

type updateOptions struct {
	resourceTypes        []string
	disableValidation    bool
	retainExceptOnCreate bool
}

func (op *pendingStackOp) updateOptions() updateOptions {
	return updateOptions{
		resourceTypes: op.ResourceTypes, disableValidation: op.DisableValidation,
		retainExceptOnCreate: op.RetainExceptOnCreate,
	}
}

// SetProvisioningScheduler makes top-level stack operations return in their
// *_IN_PROGRESS status and run the rest when schedule calls run.
// A nil schedule restores synchronous provisioning (the default).
func (b *InMemoryBackend) SetProvisioningScheduler(schedule func(run func())) {
	b.mu.Lock("SetProvisioningScheduler")
	defer b.mu.Unlock()

	b.provisionSchedule = schedule
}

// SetProvisioningDelay enables async provisioning that finishes each step d
// after the previous one; d <= 0 disables it.
func (b *InMemoryBackend) SetProvisioningDelay(d time.Duration) {
	b.mu.Lock("SetProvisioningDelay")
	b.provisionDelay = max(d, 0)
	b.mu.Unlock()

	if d <= 0 {
		b.SetProvisioningScheduler(nil)

		return
	}

	b.SetProvisioningScheduler(func(run func()) { time.AfterFunc(d, run) })
}

// WaitForStackProvisioning blocks until every scheduled stack step has run.
func (b *InMemoryBackend) WaitForStackProvisioning() { b.provisionWG.Wait() }

// WaitForStack blocks until the stack has no scheduled step left, then returns
// its settled state; in-process callers use it instead of polling.
func (b *InMemoryBackend) WaitForStack(ctx context.Context, nameOrID string) (*Stack, error) {
	for {
		b.mu.RLock("WaitForStack")

		stack, ok := b.resolveStack(nameOrID)
		if !ok {
			b.mu.RUnlock()

			return nil, ErrStackNotFound
		}

		cp := *stack
		pending := b.pendingOps[stack.StackID] != nil
		wake := b.settled
		b.mu.RUnlock()

		if !pending {
			return &cp, nil
		}

		select {
		case <-wake:
		case <-ctx.Done():
			return &cp, ctx.Err()
		}
	}
}

// signalSettledLocked wakes every WaitForStack caller to re-check its stack.
func (b *InMemoryBackend) signalSettledLocked() {
	close(b.settled)
	b.settled = make(chan struct{})
}

// scheduleLocked records op for the stack and hands its execution to the
// scheduler; false means async provisioning is off and the caller runs inline.
func (b *InMemoryBackend) scheduleLocked(ctx context.Context, stackID string, op *pendingStackOp) bool {
	if b.provisionSchedule == nil {
		return false
	}

	b.pendingOps[stackID] = op
	b.provisionWG.Add(1)

	ctx = context.WithoutCancel(ctx)

	b.provisionSchedule(func() {
		defer b.provisionWG.Done()

		b.runPendingOp(ctx, stackID)
	})

	return true
}

// stageLocked defers op to a later step when called from inside an async
// step; otherwise (sync callers, scheduling off) inline runs at once.
func (b *InMemoryBackend) stageLocked(ctx context.Context, stackID string, op *pendingStackOp, inline func()) {
	if b.stepAsync && b.scheduleLocked(ctx, stackID, op) {
		return
	}

	inline()
}

func (b *InMemoryBackend) runPendingOp(ctx context.Context, stackID string) {
	b.mu.Lock("runPendingOp")
	defer b.mu.Unlock()

	b.runPendingOpLocked(ctx, stackID)
}

func (b *InMemoryBackend) runPendingOpLocked(ctx context.Context, stackID string) {
	op, ok := b.pendingOps[stackID]
	if !ok {
		return
	}

	delete(b.pendingOps, stackID)

	stack, found := b.resolveStack(stackID)
	if !found || stack.StackID != stackID {
		b.signalSettledLocked()

		return
	}

	b.stepAsync = true
	b.announced = make(map[string]struct{}, len(op.Announced))

	for _, key := range op.Announced {
		b.announced[key] = struct{}{}
	}

	defer func() {
		b.stepAsync = false
		b.announced = nil
		b.settleChangeSetsLocked(stack)
		b.signalSettledLocked()
	}()

	b.runOpLocked(ctx, stack, op)
}

func (b *InMemoryBackend) runOpLocked(ctx context.Context, stack *Stack, op *pendingStackOp) {
	switch op.Kind {
	case pendingCreate, pendingCreateSettle, pendingRollbackCreate:
		b.runCreateOpLocked(ctx, stack, op)
	case pendingUpdate, pendingUpdateCleanup, pendingRollbackUpdate, pendingRollbackUpdateCleanup:
		b.runUpdateOpLocked(ctx, stack, op)
	case pendingDelete:
		if stack.StackStatus == statusDeleteInProgress {
			b.runDeleteLocked(ctx, stack, deleteSettings{retain: op.RetainResources, force: op.ForceDelete})
		}
	case pendingDeleteFinalize:
		if stack.StackStatus == statusDeleteInProgress {
			b.finalizeDeleteLocked(ctx, stack, op)
		}
	}
}

func (b *InMemoryBackend) runCreateOpLocked(ctx context.Context, stack *Stack, op *pendingStackOp) {
	switch op.Kind {
	case pendingCreate:
		if stack.StackStatus == statusCreateInProgress {
			b.finishCreateLocked(ctx, stack, stack.Parameters, createFinish{
				onFailure:            op.OnFailure,
				resourceTypes:        op.ResourceTypes,
				disableValidation:    op.DisableValidation,
				retainExceptOnCreate: op.RetainExceptOnCreate,
			})
		}
	case pendingCreateSettle:
		if stack.StackStatus != statusCreateInProgress {
			return
		}

		if b.childrenInProgressLocked(stack) {
			b.scheduleLocked(ctx, stack.StackID, op)

			return
		}

		b.completeCreateLocked(stack)
	case pendingRollbackCreate:
		if stack.StackStatus == statusRollbackInProgress {
			b.finishCreateRollbackLocked(ctx, stack, op.Created, createFinish{
				onFailure:            op.OnFailure,
				retainExceptOnCreate: op.RetainExceptOnCreate,
			})
		}
	}
}

func (b *InMemoryBackend) runUpdateOpLocked(ctx context.Context, stack *Stack, op *pendingStackOp) {
	switch op.Kind {
	case pendingUpdate:
		if stack.StackStatus != statusUpdateInProgress {
			return
		}

		if !b.applyTemplateToStack(ctx, stack, op.updateOptions(), op.OldTemplateBody, op.OldParams) {
			return
		}

		stack.StackStatus = statusUpdateCleanupProgress

		b.stageLocked(ctx, stack.StackID, &pendingStackOp{Kind: pendingUpdateCleanup}, func() {
			b.completeUpdateLocked(stack)
		})
	case pendingUpdateCleanup:
		if stack.StackStatus != statusUpdateCleanupProgress {
			return
		}

		if b.childrenInProgressLocked(stack) {
			b.scheduleLocked(ctx, stack.StackID, op)

			return
		}

		b.completeUpdateLocked(stack)
	case pendingRollbackUpdate:
		if stack.StackStatus == statusUpdateRollbackInProgress {
			b.runUpdateRollbackLocked(ctx, stack, op.PrevResources, op.Created, op.RetainExceptOnCreate)
		}
	case pendingRollbackUpdateCleanup:
		if stack.StackStatus == statusUpdateRollbackCleanup {
			b.completeUpdateRollbackLocked(stack)
		}
	}
}

// childrenInProgressLocked reports whether a nested stack of stack still has
// a step scheduled: a parent only settles once its children have.
func (b *InMemoryBackend) childrenInProgressLocked(stack *Stack) bool {
	for _, s := range b.stacks.All() {
		if s.ParentID == stack.StackID && b.pendingOps[s.StackID] != nil {
			return true
		}
	}

	return false
}

// settleChangeSetsLocked closes the EXECUTE_IN_PROGRESS change sets of a stack
// whose operation has finished.
func (b *InMemoryBackend) settleChangeSetsLocked(stack *Stack) {
	sets := b.changeSets[stack.StackName]
	if len(sets) == 0 || b.pendingOps[stack.StackID] != nil {
		return
	}

	final := executionStatusFailed

	switch {
	case stack.StackStatus == statusCreateComplete || stack.StackStatus == statusUpdateComplete:
		final = executionStatusComplete
	case strings.HasSuffix(stack.StackStatus, suffixInProgress):
		return
	}

	for _, cs := range sets {
		if cs.ExecutionStatus == executionStatusInProgress {
			cs.ExecutionStatus = final
		}
	}
}

// addProgressEvent records a plain *_IN_PROGRESS resource event unless the
// operation already announced it when it started.
func (b *InMemoryBackend) addProgressEvent(stackID, stackName, logicalID, physicalID, resourceType, status string) {
	key := announceKey(logicalID, status)
	if _, ok := b.announced[key]; ok {
		delete(b.announced, key)

		return
	}

	b.addEvent(stackID, stackName, logicalID, physicalID, resourceType, status, "")
}

func announceKey(logicalID, status string) string { return logicalID + "|" + status }

// announceLocked emits the first wave of resource events at operation start
// and returns their keys so the later step does not repeat them.
func (b *InMemoryBackend) announceLocked(
	stack *Stack, status string, ids []string, resources map[string]TemplateResource,
) []string {
	keys := make([]string, 0, len(ids))

	for _, id := range ids {
		b.addEvent(stack.StackID, stack.StackName, id, "", resources[id].Type, status, "")
		keys = append(keys, announceKey(id, status))
	}

	return keys
}

// firstWave returns the sorted members of want that start first: those that
// depend on no other member (creates/updates), or that no other member
// depends on (leaves, for deletes).
func firstWave(resources map[string]TemplateResource, want []string, leaves bool) []string {
	all := make(map[string]struct{}, len(resources))
	for id := range resources {
		all[id] = struct{}{}
	}

	inWant := make(map[string]struct{}, len(want))
	for _, id := range want {
		inWant[id] = struct{}{}
	}

	blocked := make(map[string]bool, len(want))

	for _, id := range want {
		res, ok := resources[id]
		if !ok {
			continue
		}

		for _, dep := range resourceDependencies(id, res, all) {
			if _, ok = inWant[dep]; !ok {
				continue
			}

			if leaves {
				blocked[dep] = true
			} else {
				blocked[id] = true
			}
		}
	}

	out := make([]string, 0, len(want))

	for _, id := range want {
		if _, ok := resources[id]; ok && !blocked[id] {
			out = append(out, id)
		}
	}

	slices.Sort(out)

	return out
}

// resumePendingOpsLocked finishes operations a restored snapshot captured
// mid-flight; with no scheduler they run inline.
func (b *InMemoryBackend) resumePendingOpsLocked(ctx context.Context) {
	for _, id := range sortedKeys(b.pendingOps) {
		op := b.pendingOps[id]

		if b.provisionSchedule != nil {
			delete(b.pendingOps, id)
			b.scheduleLocked(ctx, id, op)

			continue
		}

		for b.pendingOps[id] != nil {
			b.runPendingOpLocked(ctx, id)
		}
	}
}
