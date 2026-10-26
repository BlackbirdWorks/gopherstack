package cloudformation

import (
	"context"
	"time"
)

const (
	pendingCreate        = "create"
	pendingUpdate        = "update"
	pendingUpdateCleanup = "updateCleanup"
)

// pendingStackOp is the deferred half of an async CreateStack/UpdateStack;
// persisted so a restored snapshot can finish it.
type pendingStackOp struct {
	Kind                 string      `json:"kind"`
	OnFailure            string      `json:"onFailure,omitempty"`
	OldTemplateBody      string      `json:"oldTemplateBody,omitempty"`
	ResourceTypes        []string    `json:"resourceTypes,omitempty"`
	OldParams            []Parameter `json:"oldParams,omitempty"`
	DisableValidation    bool        `json:"disableValidation,omitempty"`
	RetainExceptOnCreate bool        `json:"retainExceptOnCreate,omitempty"`
}

// SetProvisioningScheduler makes top-level CreateStack/UpdateStack return in
// *_IN_PROGRESS and run the rest of the operation when schedule calls run.
// A nil schedule restores synchronous provisioning (the default).
func (b *InMemoryBackend) SetProvisioningScheduler(schedule func(run func())) {
	b.mu.Lock("SetProvisioningScheduler")
	defer b.mu.Unlock()

	b.provisionSchedule = schedule
}

// SetProvisioningDelay enables async provisioning that finishes each step d
// after the previous one; d <= 0 disables it.
func (b *InMemoryBackend) SetProvisioningDelay(d time.Duration) {
	if d <= 0 {
		b.SetProvisioningScheduler(nil)

		return
	}

	b.SetProvisioningScheduler(func(run func()) { time.AfterFunc(d, run) })
}

// WaitForStackProvisioning blocks until every scheduled stack step has run.
func (b *InMemoryBackend) WaitForStackProvisioning() { b.provisionWG.Wait() }

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
		return
	}

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
	case pendingUpdate:
		if stack.StackStatus != statusUpdateInProgress {
			return
		}

		if !b.applyTemplateToStack(
			ctx, stack, op.DisableValidation, op.ResourceTypes, op.OldTemplateBody, op.OldParams,
		) {
			return
		}

		stack.StackStatus = statusUpdateCleanupProgress

		if !b.scheduleLocked(ctx, stackID, &pendingStackOp{Kind: pendingUpdateCleanup}) {
			b.completeUpdateLocked(stack)
		}
	case pendingUpdateCleanup:
		if stack.StackStatus == statusUpdateCleanupProgress {
			b.completeUpdateLocked(stack)
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
