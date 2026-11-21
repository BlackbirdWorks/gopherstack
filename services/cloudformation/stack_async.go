package cloudformation

import (
	"context"
	"strings"
	"time"
)

const (
	pendingCreate                = "create"
	pendingCreateWalk            = "createWalk"
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
	PhysicalIDs          map[string]string         `json:"physicalIds,omitempty"`
	InFlight             map[string]string         `json:"inFlight,omitempty"`
	FailedReasons        map[string]string         `json:"failedReasons,omitempty"`
	rt                   *walkRuntime
	Kind                 string           `json:"kind"`
	Phase                string           `json:"phase,omitempty"`
	OnFailure            string           `json:"onFailure,omitempty"`
	OldTemplateBody      string           `json:"oldTemplateBody,omitempty"`
	TemplateBody         string           `json:"templateBody,omitempty"`
	ResourceTypes        []string         `json:"resourceTypes,omitempty"`
	OldParams            []Parameter      `json:"oldParams,omitempty"`
	Created              []string         `json:"created,omitempty"`
	Updated              []string         `json:"updated,omitempty"`
	Done                 []string         `json:"done,omitempty"`
	Remaining            []string         `json:"remaining,omitempty"`
	Replaced             []replacedRecord `json:"replaced,omitempty"`
	Announced            []string         `json:"announced,omitempty"`
	RetainResources      []string         `json:"retainResources,omitempty"`
	DisableValidation    bool             `json:"disableValidation,omitempty"`
	RetainExceptOnCreate bool             `json:"retainExceptOnCreate,omitempty"`
	ForceDelete          bool             `json:"forceDelete,omitempty"`
	SettleLater          bool             `json:"settleLater,omitempty"`
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

	b.SetProvisioningScheduler(b.delayScheduler(d))
}

// delayScheduler runs each step d later on a timer the backend can cancel; it is
// only called from scheduleLocked, with b.mu held.
func (b *InMemoryBackend) delayScheduler(d time.Duration) func(run func()) {
	return func(run func()) {
		b.timerSeq++
		id := b.timerSeq

		b.delayTimers[id] = time.AfterFunc(d, func() {
			b.mu.Lock("provisioningTimer")
			delete(b.delayTimers, id)
			b.mu.Unlock()

			run()
		})
	}
}

// cancelProvisioningLocked drops every scheduled step without running it.
func (b *InMemoryBackend) cancelProvisioningLocked() {
	for id, t := range b.delayTimers {
		if t.Stop() {
			b.provisionWG.Done()
		}

		delete(b.delayTimers, id)
	}

	clear(b.pendingOps)
	b.signalSettledLocked()
}

// Close cancels every scheduled provisioning step; stacks still in flight stay in
// their *_IN_PROGRESS status and later operations run synchronously.
func (b *InMemoryBackend) Close() {
	b.mu.Lock("Close")
	defer b.mu.Unlock()

	b.cancelProvisioningLocked()
	b.provisionSchedule = nil
}

// Reset cancels scheduled provisioning and clears all stacks and their state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.cancelProvisioningLocked()
	b.registry.ResetAll()
	b.initState()
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

	op.legacyInFlight()

	b.stepAsync = true

	defer func() {
		b.stepAsync = false
		b.settleChangeSetsLocked(stack)
		b.signalSettledLocked()
	}()

	b.runOpLocked(ctx, stack, op)
}

func (b *InMemoryBackend) runOpLocked(ctx context.Context, stack *Stack, op *pendingStackOp) {
	switch op.Kind {
	case pendingCreate, pendingCreateWalk, pendingCreateSettle, pendingRollbackCreate:
		b.runCreateOpLocked(ctx, stack, op)
	case pendingUpdate, pendingUpdateCleanup, pendingRollbackUpdate, pendingRollbackUpdateCleanup:
		b.runUpdateOpLocked(ctx, stack, op)
	case pendingDelete:
		if stack.StackStatus == statusDeleteInProgress {
			b.runDeleteLocked(ctx, stack, op)
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
			b.finishCreateLocked(ctx, stack, stack.Parameters, op.createFinish().withAnnounced(op.InFlight))
		}
	case pendingCreateWalk:
		if stack.StackStatus != statusCreateInProgress {
			return
		}

		b.walkCreateLocked(ctx, stack, op)

		if !b.createWalkPending(stack) {
			b.settleCreateLocked(ctx, stack, op.createFinish())
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
			b.runCreateRollbackLocked(ctx, stack, op)
		}
	}
}

func (b *InMemoryBackend) runUpdateOpLocked(ctx context.Context, stack *Stack, op *pendingStackOp) {
	switch op.Kind {
	case pendingUpdate:
		if stack.StackStatus != statusUpdateInProgress {
			return
		}

		b.runUpdateStepLocked(ctx, stack, op)
	case pendingUpdateCleanup:
		if stack.StackStatus != statusUpdateCleanupProgress {
			return
		}

		if op.Phase == phaseSettle {
			b.settleUpdateLocked(ctx, stack)

			return
		}

		b.walkUpdateCleanupLocked(ctx, stack, op)
	case pendingRollbackUpdate:
		if stack.StackStatus == statusUpdateRollbackInProgress {
			b.runUpdateRollbackLocked(ctx, stack, op)
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
