package gcpscheduler

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"time"

	"cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"google.golang.org/genproto/googleapis/rpc/code"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpdispatch"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// result is what came back from one attempt; sc is 0 when no HTTP response arrived.
type result struct {
	err error
	sc  int
}

func (r result) ok() bool {
	return r.err == nil && (r.sc == 0 || (r.sc >= http.StatusOK && r.sc < http.StatusMultipleChoices))
}

// rpcStatus maps an attempt outcome to the google.rpc.Status recorded on the job.
func (r result) rpcStatus() *rpcstatus.Status {
	switch {
	case r.err != nil && errors.Is(r.err, context.DeadlineExceeded):
		return &rpcstatus.Status{Code: int32(code.Code_DEADLINE_EXCEEDED), Message: r.err.Error()}
	case r.err != nil && status.Code(r.err) != codes.Unknown:
		return &rpcstatus.Status{Code: int32(status.Code(r.err)), //nolint:gosec // grpc codes fit int32
			Message: status.Convert(r.err).Message()}
	case r.err != nil:
		return &rpcstatus.Status{Code: int32(code.Code_UNAVAILABLE), Message: r.err.Error()}
	case r.ok():
		return &rpcstatus.Status{Code: int32(code.Code_OK)}
	}

	return &rpcstatus.Status{
		Code:    int32(gcpdispatch.HTTPToRPC(r.sc)),
		Message: "HTTP status code " + http.StatusText(r.sc),
	}
}

// RunDispatcher fires due jobs every interval until ctx ends.
func (b *InMemoryBackend) RunDispatcher(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.DispatchDue()
		}
	}
}

// DispatchDue starts every attempt that is due: cron fires first, then pending retries.
func (b *InMemoryBackend) DispatchDue() {
	b.mu.Lock("DispatchDue")
	defer b.mu.Unlock()

	now := b.now()

	for _, r := range b.jobs {
		if r.pb.GetState() != schedulerpb.Job_ENABLED || r.running > 0 {
			continue
		}

		switch st := r.pb.GetScheduleTime(); {
		case st != nil && !st.AsTime().After(now):
			r.pb.ScheduleTime = b.nextFireLocked(r, now)
			b.launchLocked(r, now, st.AsTime(), true)
		case !r.retryAt.IsZero() && !r.retryAt.After(now):
			b.launchLocked(r, now, r.fireAt, false)
		}
	}
}

// launchLocked records the attempt start and runs delivery in the background.
func (b *InMemoryBackend) launchLocked(r *jobRec, now, fireAt time.Time, fresh bool) *schedulerpb.Job {
	if fresh {
		r.failures, r.execStart, r.fireAt = 0, now, fireAt
	}

	r.retryAt = time.Time{}
	r.running++
	r.pb.LastAttemptTime = tsOf(now)

	a := b.buildAttemptLocked(r, now)
	ctx := b.runCtx

	b.wg.Go(func() { b.finish(r, b.deliver(ctx, a)) })

	return cloneJob(r.pb)
}

// deliver performs the attempt; it holds no lock.
func (b *InMemoryBackend) deliver(ctx context.Context, a attempt) result {
	if a.err != nil {
		return result{err: a.err}
	}

	ctx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()

	if a.publish != nil {
		_, err := a.publisher.Publish(a.publish)

		return result{err: err}
	}

	req, err := http.NewRequestWithContext(ctx, a.http.method, a.http.url, bytes.NewReader(a.http.body))
	if err != nil {
		return result{err: err}
	}

	req.Header = a.http.header.Clone()
	gcpserve.MarkInternal(req)

	resp, err := a.client.Do(req)
	if err != nil {
		return result{err: err}
	}

	_ = resp.Body.Close()

	return result{sc: resp.StatusCode}
}

// finish records an attempt's outcome and schedules a retry when the job's retry config allows one.
func (b *InMemoryBackend) finish(r *jobRec, res result) {
	b.mu.Lock("finish")
	defer b.mu.Unlock()

	if b.jobs[r.pb.GetName()] != r {
		return
	}

	r.running = max(r.running-1, 0)
	r.pb.Status = res.rpcStatus()

	if res.ok() {
		r.failures, r.retryAt = 0, time.Time{}

		return
	}

	r.failures++

	now := b.now()
	rc := r.pb.GetRetryConfig()

	if r.pb.GetState() != schedulerpb.Job_ENABLED || !retryAllowed(rc, r, now) {
		r.retryAt = time.Time{}
		logger.Load(b.runCtx).WarnContext(b.runCtx, "gcpscheduler: attempt failed, not retrying",
			"job", r.pb.GetName(), "failures", r.failures)

		return
	}

	r.retryAt = now.Add(gcpdispatch.Backoff(
		rc.GetMinBackoffDuration().AsDuration(), rc.GetMaxBackoffDuration().AsDuration(),
		rc.GetMaxDoublings(), r.failures))
}

// retryAllowed reports whether another attempt may follow the r.failures-th failure: retries continue
// until both retry_count and max_retry_duration are exhausted.
func retryAllowed(rc *schedulerpb.RetryConfig, r *jobRec, now time.Time) bool {
	if rc.GetRetryCount() > 0 && r.failures-1 < rc.GetRetryCount() {
		return true
	}

	d := rc.GetMaxRetryDuration().AsDuration()

	return d > 0 && now.Before(r.execStart.Add(d))
}

// RunJob forces an immediate attempt regardless of state or schedule; it returns before the attempt completes.
func (b *InMemoryBackend) RunJob(name string) (*schedulerpb.Job, error) {
	if _, err := parseJobName(name); err != nil {
		return nil, err
	}

	b.mu.Lock("RunJob")
	defer b.mu.Unlock()

	r, ok := b.jobs[name]
	if !ok {
		return nil, errJobMissing()
	}

	now := b.now()

	return b.launchLocked(r, now, now, true), nil
}
