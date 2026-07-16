package sagemaker

import (
	"context"
	"time"
)

// ---------------------------------------------------------------------------
// Lifecycle simulator delays
// ---------------------------------------------------------------------------

const (
	notebookPendingToInServiceDelay = 250 * time.Millisecond
	notebookStoppingToStoppedDelay  = 150 * time.Millisecond
	notebookUpdatingToInService     = 200 * time.Millisecond
	trainingInProgressToCompleted   = 300 * time.Millisecond
	trainingStoppingToStopped       = 150 * time.Millisecond
	endpointCreatingToInService     = 300 * time.Millisecond
	endpointUpdatingToInService     = 250 * time.Millisecond
	processingJobCompletionDelay    = 300 * time.Millisecond
	processingJobStopDelay          = 150 * time.Millisecond
)

// ---------------------------------------------------------------------------
// Status constants
// ---------------------------------------------------------------------------

const (
	notebookStatusStopped       = "Stopped"
	notebookStatusPending       = "Pending"
	notebookStatusStopping      = "Stopping"
	trainingJobStatusInProgress = "InProgress"
	keyNotebookInstanceArn      = "NotebookInstanceArn"
)

// ---------------------------------------------------------------------------
// Lifecycle context management — per-backend context cancelled by Reset()
// ---------------------------------------------------------------------------

// resetLifecycleContext replaces the backend's lifecycle context with a fresh one,
// cancelling all pending goroutines from the previous context.
// Must be called with no lock held.
func (b *InMemoryBackend) resetLifecycleContext() {
	if b.lifecycleCancel != nil {
		b.lifecycleCancel()
	}

	parent := b.lifecycleParent
	if parent == nil {
		parent = context.Background()
	}

	ctx, cancel := context.WithCancel(parent)
	b.lifecycleCtx = ctx
	b.lifecycleCancel = cancel
}

// runDelayed runs fn after delay unless ctx is cancelled first (via Reset,
// Restore, or Shutdown, all of which cancel the lifecycle context). The goroutine
// is tracked by b.wg so Shutdown can wait for in-flight transitions to drain. fn is
// responsible for taking any locks it needs and re-checking resource existence.
//
// ctx must be the backend's lifecycle context (b.lifecycleCtx) captured by the
// caller — typically while holding b.mu — so a concurrent Reset that swaps
// b.lifecycleCtx cannot race this goroutine's select.
func (b *InMemoryBackend) runDelayed(ctx context.Context, delay time.Duration, fn func()) {
	b.wg.Go(func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		fn()
	})
}

// Shutdown cancels all pending lifecycle-transition goroutines and waits for the
// in-flight ones to finish, bounded by ctx. It implements the shutdown half of the
// service.Shutdowner contract (wired through the Handler).
func (b *InMemoryBackend) Shutdown(ctx context.Context) {
	b.mu.Lock("Shutdown")
	if b.lifecycleCancel != nil {
		b.lifecycleCancel()
	}
	b.mu.Unlock()

	done := make(chan struct{})

	go func() {
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}
}
