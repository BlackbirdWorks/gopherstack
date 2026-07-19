package worker

import (
	"context"
	"sync"
)

// Runner is a single blocking task that runs until its context is cancelled,
// such as a service janitor's Run method.
type Runner interface {
	Run(context.Context)
}

// SingleRun manages the start/stop lifecycle of one Runner — typically a
// service's optional background janitor — under a single lock, so every
// service's StartWorker/Shutdown pair shares the exact same panic-safe shape:
// Start is a no-op if a run is already active, and Stop cancels the active
// run (if any) and waits for it to exit, or for the caller's context to be
// done, whichever happens first. The zero value is ready to use.
type SingleRun struct {
	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
}

// Start runs r in its own goroutine, derived from ctx, unless a run started
// by an earlier Start is still active (in which case Start is a no-op). It
// returns immediately; the run happens asynchronously.
func (s *SingleRun) Start(ctx context.Context, r Runner) {
	runCtx, done, ready := s.startLocked(ctx)
	if !ready {
		return
	}

	go func() {
		defer close(done)
		r.Run(runCtx)
	}()
}

// startLocked registers a new run context and done channel under s.mu. It
// returns ready=false without side effects if a run is already active.
func (s *SingleRun) startLocked(ctx context.Context) (context.Context, chan struct{}, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.done != nil {
		return nil, nil, false
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	s.cancel = cancel
	s.done = done

	return runCtx, done, true
}

// Stop cancels the active run, if any, and waits for it to exit or for ctx
// to be done. It is a no-op if no run is active, and safe to call more than
// once (a subsequent call finds no active run and returns immediately).
func (s *SingleRun) Stop(ctx context.Context) {
	s.mu.Lock()
	cancel := s.cancel
	done := s.done
	s.cancel = nil
	s.done = nil
	s.mu.Unlock()

	if cancel == nil || done == nil {
		return
	}

	cancel()

	select {
	case <-done:
	case <-ctx.Done():
	}
}
