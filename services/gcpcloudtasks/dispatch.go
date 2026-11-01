package gcpcloudtasks

import (
	"bytes"
	"context"
	"errors"
	"math"
	"net/http"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"google.golang.org/genproto/googleapis/rpc/code"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpdispatch"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// job is one started dispatch attempt.
type job struct {
	client *http.Client
	att    *cloudtaskspb.Attempt
	err    error
	queue  string
	task   string
	out    outbound
}

// result is what came back from one attempt; sc is 0 when no response arrived.
type result struct {
	err error
	sc  int
}

func (r result) ok() bool {
	return r.err == nil && r.sc >= http.StatusOK && r.sc < http.StatusMultipleChoices
}

func httpToRPC(sc int) code.Code { return gcpdispatch.HTTPToRPC(sc) }

// rpcStatus maps an HTTP outcome to the google.rpc.Status recorded on the attempt.
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

	return &rpcstatus.Status{Code: int32(httpToRPC(r.sc)), Message: "HTTP status code " + http.StatusText(r.sc)}
}

// RunDispatcher dispatches due tasks every interval until ctx ends.
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

// DispatchDue starts every attempt that is due and allowed by queue state, rate and concurrency limits.
func (b *InMemoryBackend) DispatchDue() {
	b.mu.Lock("DispatchDue")
	defer b.mu.Unlock()

	now := b.now()

	for n, exp := range b.tombstones {
		if !exp.After(now) {
			delete(b.tombstones, n)
		}
	}

	for _, q := range b.queues {
		for _, t := range b.pickLocked(q, now) {
			b.launchLocked(q, t, now)
		}
	}
}

// pickLocked returns the due tasks the queue's token bucket and concurrency cap admit.
func (b *InMemoryBackend) pickLocked(q *queueRec, now time.Time) []*taskRec {
	if q.pb.GetState() != cloudtaskspb.Queue_RUNNING {
		return nil
	}

	rl := q.pb.GetRateLimits()
	burst := float64(max(rl.GetMaxBurstSize(), 1))

	if q.lastFill.IsZero() {
		q.tokens = burst
	} else {
		q.tokens = min(burst, q.tokens+rl.GetMaxDispatchesPerSecond()*max(now.Sub(q.lastFill).Seconds(), 0))
	}

	q.lastFill = now

	var due []*taskRec

	for _, t := range sortedTasks(q) {
		if t.running == 0 && !t.pb.GetScheduleTime().AsTime().After(now) {
			due = append(due, t)
		}
	}

	room := int(rl.GetMaxConcurrentDispatches()) - q.inflight
	n := max(min(len(due), room, int(math.Floor(q.tokens))), 0)
	q.tokens -= float64(n)

	return due[:n]
}

// launchLocked records the attempt start and runs delivery in the background.
func (b *InMemoryBackend) launchLocked(q *queueRec, t *taskRec, now time.Time) *cloudtaskspb.Task {
	j := b.startLocked(q, t, now)
	ctx := b.runCtx

	b.wg.Go(func() { b.finish(j, b.deliver(ctx, j)) })

	return cloneTask(t.pb)
}

func (b *InMemoryBackend) startLocked(q *queueRec, t *taskRec, now time.Time) job {
	t.running++
	q.inflight++
	t.pb.DispatchCount++

	att := &cloudtaskspb.Attempt{ScheduleTime: t.pb.GetScheduleTime(), DispatchTime: tsOf(now)}
	if t.pb.GetFirstAttempt() == nil {
		t.pb.FirstAttempt = &cloudtaskspb.Attempt{DispatchTime: tsOf(now)}
	}

	t.pb.LastAttempt = att

	out, err := b.buildOutboundLocked(q, t, now)

	return job{client: b.client, att: att, err: err, queue: q.pb.GetName(), task: t.pb.GetName(), out: out}
}

// deliver sends the request; it holds no lock.
func (b *InMemoryBackend) deliver(ctx context.Context, j job) result {
	if j.err != nil {
		return result{err: j.err}
	}

	ctx, cancel := context.WithTimeout(ctx, j.out.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, j.out.method, j.out.url, bytes.NewReader(j.out.body))
	if err != nil {
		return result{err: err}
	}

	req.Header = j.out.header.Clone()
	gcpserve.MarkInternal(req)

	resp, err := j.client.Do(req)
	if err != nil {
		return result{err: err}
	}

	_ = resp.Body.Close()

	return result{sc: resp.StatusCode}
}

// finish records an attempt's outcome: delete on success, reschedule or drop on failure.
func (b *InMemoryBackend) finish(j job, res result) {
	b.mu.Lock("finish")
	defer b.mu.Unlock()

	q, ok := b.queues[j.queue]
	if !ok {
		return
	}

	q.inflight = max(q.inflight-1, 0)

	t, ok := q.tasks[j.task]
	if !ok {
		return
	}

	t.running = max(t.running-1, 0)
	now := b.now()
	log := logger.Load(b.runCtx)

	j.att.ResponseTime = tsOf(now)
	j.att.ResponseStatus = res.rpcStatus()

	if res.sc != 0 {
		t.pb.ResponseCount++
	}

	if res.ok() {
		delete(q.tasks, j.task)
		b.tombstones[j.task] = now.Add(taskTombstoneTTL)

		return
	}

	rc := effectiveRetry(q.pb.GetRetryConfig(), t.pb.GetRetryConfig())
	if exhausted(rc, t.pb, now) {
		delete(q.tasks, j.task)
		b.tombstones[j.task] = now.Add(taskTombstoneTTL)
		log.WarnContext(b.runCtx, "gcpcloudtasks: task dropped, retries exhausted",
			"task", j.task, "attempts", t.pb.GetDispatchCount())

		return
	}

	t.pb.ScheduleTime = tsOf(now.Add(backoff(rc, t.pb.GetDispatchCount())))
}

func exhausted(rc *cloudtaskspb.RetryConfig, t *cloudtaskspb.Task, now time.Time) bool {
	if rc.GetMaxAttempts() != -1 && t.GetDispatchCount() >= rc.GetMaxAttempts() {
		return true
	}

	d := rc.GetMaxRetryDuration().AsDuration()

	return d > 0 && !now.Before(t.GetFirstAttempt().GetDispatchTime().AsTime().Add(d))
}

// effectiveRetry overlays a task's non-zero retry fields onto the queue's.
func effectiveRetry(queue, task *cloudtaskspb.RetryConfig) *cloudtaskspb.RetryConfig {
	if task == nil {
		return queue
	}

	out, _ := proto.Clone(queue).(*cloudtaskspb.RetryConfig)

	if v := task.GetMaxAttempts(); v != 0 {
		out.MaxAttempts = v
	}

	if v := task.GetMaxDoublings(); v != 0 {
		out.MaxDoublings = v
	}

	for _, f := range []struct {
		src *durationpb.Duration
		dst **durationpb.Duration
	}{
		{task.GetMaxRetryDuration(), &out.MaxRetryDuration},
		{task.GetMinBackoff(), &out.MinBackoff},
		{task.GetMaxBackoff(), &out.MaxBackoff},
	} {
		if f.src.AsDuration() != 0 {
			*f.dst = f.src
		}
	}

	return out
}

// backoff is the delay after the failures-th failed attempt: min doubles max_doublings times, then grows
// linearly, capped at max.
func backoff(rc *cloudtaskspb.RetryConfig, failures int32) time.Duration {
	return gcpdispatch.Backoff(
		rc.GetMinBackoff().AsDuration(), rc.GetMaxBackoff().AsDuration(), rc.GetMaxDoublings(), failures)
}

// RunTask forces an immediate attempt regardless of queue state, schedule or rate limits.
func (b *InMemoryBackend) RunTask(req *cloudtaskspb.RunTaskRequest) (*cloudtaskspb.Task, error) {
	queue, err := parseTaskName(req.GetName())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("RunTask")
	defer b.mu.Unlock()

	q, ok := b.queues[queue]
	if !ok {
		return nil, errQueueMissing()
	}

	t, ok := q.tasks[req.GetName()]
	if !ok {
		return nil, errTaskMissing()
	}

	return viewOf(b.launchLocked(q, t, b.now()), req.GetResponseView()), nil
}
