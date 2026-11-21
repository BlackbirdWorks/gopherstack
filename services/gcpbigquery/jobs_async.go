package gcpbigquery

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

const (
	statePending    = "PENDING"
	stateRunning    = "RUNNING"
	maxActiveJobs   = 1000
	defaultJobWait  = 10 * time.Second
	reasonStopped   = "stopped"
	reasonTimeout   = "timeout"
	litJobTimeoutMs = "jobTimeoutMs"
)

// jobRun is the runtime handle of a job that has not finished; it is never persisted.
type jobRun struct {
	cancel    context.CancelFunc
	done      chan struct{}
	cancelled atomic.Bool
}

// SetJobStartHook runs fn on the job's worker in phase PENDING (before) and RUNNING (after the state change);
// tests use it to hold a job in either state.
func (b *InMemoryBackend) SetJobStartHook(fn func(ctx context.Context, phase, project, id string)) {
	b.mu.Lock("SetJobStartHook")
	defer b.mu.Unlock()

	b.startHook = fn
}

// Close cancels every unfinished job and waits for the workers to stop.
func (b *InMemoryBackend) Close() {
	b.baseCancel()
	b.wg.Wait()
}

func (b *InMemoryBackend) waitFor(d time.Duration, run *jobRun) {
	if run == nil {
		return
	}

	select {
	case <-run.done:
		return
	default:
	}

	if d <= 0 {
		return
	}

	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-run.done:
	case <-t.C:
	case <-b.baseCtx.Done():
	}
}

// startJob stores job as PENDING and hands it to a worker; the caller holds the write lock.
func (b *InMemoryBackend) startJob(
	ctx context.Context, job *Job, kind string, cfg map[string]any, media []byte,
) error {
	if b.runs.Len() >= maxActiveJobs {
		return newErr(http.StatusTooManyRequests, "rateLimitExceeded", "Exceeded rate limits: too many concurrent jobs")
	}

	rctx, cancel := context.WithCancel(withPrincipal(b.baseCtx, principalFrom(ctx)))

	if ms, ok := intOf(cfg[litJobTimeoutMs]); ok && ms > 0 {
		var stop context.CancelFunc

		rctx, stop = context.WithTimeout(rctx, time.Duration(ms)*time.Millisecond)
		inner := cancel
		cancel = func() { stop(); inner() }
	}

	run := &jobRun{cancel: cancel, done: make(chan struct{})}
	key := jobKey(job.Project, job.ID)

	job.State = statePending
	job.Stats = map[string]any{litCreationtime: msString(job.Created)}
	b.storeJob(job)
	b.runs.Set(key, run)

	b.wg.Go(func() { b.work(rctx, run, job, kind, cfg, media) })

	return nil
}

func (b *InMemoryBackend) work(
	ctx context.Context, run *jobRun, job *Job, kind string, cfg map[string]any, media []byte,
) {
	defer func() {
		run.cancel()
		b.runs.Delete(jobKey(job.Project, job.ID))
		close(run.done)
	}()

	b.mu.RLock("jobHook")
	hook := b.startHook
	b.mu.RUnlock()

	if hook != nil {
		hook(ctx, statePending, job.Project, job.ID)
	}

	pre := b.preload(kind, cfg, media)

	b.mu.Lock("jobRunning")
	if b.jobs[jobKey(job.Project, job.ID)] != job {
		b.mu.Unlock()

		return
	}

	job.State, job.Started = stateRunning, nowMs()
	b.mu.Unlock()

	if hook != nil {
		hook(ctx, stateRunning, job.Project, job.ID)
	}

	out, post := b.runLocked(ctx, run, job, kind, cfg, pre)

	if post != nil {
		out = post()
	}

	b.mu.Lock("jobDone")
	defer b.mu.Unlock()

	if b.jobs[jobKey(job.Project, job.ID)] != job {
		return
	}

	b.finishJob(job, kind, b.interrupted(ctx, run, out))
}

func (b *InMemoryBackend) runLocked(
	ctx context.Context, run *jobRun, job *Job, kind string, cfg map[string]any, pre *loadPre,
) (*outcome, func() *outcome) {
	b.mu.Lock("jobRun")
	defer b.mu.Unlock()

	if ctx.Err() != nil || run.cancelled.Load() {
		return &outcome{Err: stoppedError(ctx, run)}, nil
	}

	var (
		out *outcome
		err error
	)

	switch kind {
	case jobLoad:
		out, err = b.execLoadPre(objectOf(cfg, jobLoad), pre)
	case jobExtract:
		var post func() *outcome

		out, post, err = b.execExtractPlan(objectOf(cfg, jobExtract), true)
		if err == nil && post != nil {
			return out, post
		}
	default:
		out, err = b.execute(ctx, job, kind, cfg, false)
	}

	if err != nil {
		return &outcome{Err: &jobError{Reason: apiReason(err), Message: apiMessage(err)}, Stats: map[string]any{}}, nil
	}

	return out, nil
}

// interrupted turns the result of a job whose context ended into the documented stopped or timeout error.
func (*InMemoryBackend) interrupted(ctx context.Context, run *jobRun, out *outcome) *outcome {
	if out.Err != nil && ctx.Err() != nil {
		out.Err = stoppedError(ctx, run)
	}

	return out
}

func stoppedError(ctx context.Context, run *jobRun) *jobError {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) && !run.cancelled.Load() {
		return &jobError{Reason: reasonTimeout, Message: "Job timed out"}
	}

	return &jobError{Reason: reasonStopped, Message: "Job execution was cancelled: User requested cancellation"}
}

func waitTimeout(timeoutMs *int) time.Duration {
	if timeoutMs == nil {
		return defaultJobWait
	}

	return time.Duration(*timeoutMs) * time.Millisecond
}

func (b *InMemoryBackend) markInterrupted() {
	for _, j := range b.jobs {
		if j.State == stateDone || j.State == "" {
			continue
		}

		j.State = stateDone
		j.Ended = nowMs()
		j.ErrorResult = &jobError{
			Reason: reasonStopped, Message: fmt.Sprintf("Job %s was interrupted by an emulator restart", j.ID),
		}
	}
}
