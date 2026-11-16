package ocifilestorage

import (
	"context"
	"net/http"
	"sync"
)

// Lock order: the backend lock is outermost for control-plane state and is never held while a data-plane gate or
// quota mutex is taken; code inside the gate or quota mutex never takes the backend lock (writers read the quota
// rules first). Copies run with no backend lock held and re-lock only to finish.

// copyJob copies data outside the backend lock and then settles the resource it was queued for.
type copyJob func() error

// copyScope is the lifetime of the copies started since the last Reset or Restore.
type copyScope struct {
	ctx    context.Context
	cancel context.CancelFunc
}

type copies struct {
	scope  *copyScope
	busy   map[string]struct{}
	jobs   []copyJob
	wg     sync.WaitGroup
	closed bool
}

func errUnavailable() *APIError {
	return newErr(http.StatusServiceUnavailable, "ServiceUnavailable", "The service is shutting down or resetting")
}

// armCopies gives the backend a live copy context. Callers hold the write lock or own the backend exclusively.
func (b *InMemoryBackend) armCopies() {
	if b.cp.closed {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	b.cp.scope = &copyScope{ctx: ctx, cancel: cancel}
	b.cp.busy = map[string]struct{}{}
}

// reserveCopy registers one in-flight copy and returns the context it must honour. Callers hold the write lock.
func (b *InMemoryBackend) reserveCopy() (context.Context, *APIError) {
	if b.cp.scope.ctx.Err() != nil {
		return nil, errUnavailable()
	}

	b.cp.wg.Add(1)

	return b.cp.scope.ctx, nil
}

// queueCopy hands a job to the next Sweep tail. Callers hold the write lock.
func (b *InMemoryBackend) queueCopy(j copyJob) { b.cp.jobs = append(b.cp.jobs, j) }

// takeCopies drains the queued jobs. Callers hold the write lock.
func (b *InMemoryBackend) takeCopies() []copyJob {
	jobs := b.cp.jobs
	b.cp.jobs = nil

	return jobs
}

// quiesceCopies cancels every in-flight copy and waits for each to settle; no lock is held on return.
func (b *InMemoryBackend) quiesceCopies() {
	b.mu.Lock("quiesceCopies")
	b.cp.scope.cancel()
	b.mu.Unlock()

	b.cp.wg.Wait()
}

// stopCopies cancels in-flight copies without waiting.
func (b *InMemoryBackend) stopCopies() {
	b.mu.Lock("stopCopies")
	b.cp.scope.cancel()
	b.mu.Unlock()
}
