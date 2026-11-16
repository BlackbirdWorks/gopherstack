package gcpworkflows

import (
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/workflows/apiv1/workflowspb"
	executionspb "cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/wfinterp"
)

// Quotas from docs.cloud.google.com/workflows/quotas.
const (
	maxActivePerRegion = 10000
	maxQueuedPerRegion = 100000
	executionLifetime  = 365 * 24 * time.Hour
	executionRetention = 90 * 24 * time.Hour
	sweepEvery         = time.Hour
)

type regionLoad struct {
	queue  []string
	active int
}

type pendingRun struct {
	wf   *workflowspb.Workflow
	prog *wfinterp.Program
	arg  any
}

// afterClock is implemented by clocks that can schedule a callback on their own timeline.
type afterClock interface {
	AfterFunc(d time.Duration, f func()) func() bool
}

// SetExecutionLimits replaces the per-region concurrent and queued execution quotas.
func (b *InMemoryBackend) SetExecutionLimits(active, queued int) {
	b.mu.Lock("SetExecutionLimits")
	defer b.mu.Unlock()

	b.maxActive, b.maxQueued = active, queued
}

func regionOf(execName string) string {
	project, rest, _ := strings.Cut(strings.TrimPrefix(execName, "projects/"), "/locations/")
	location, _, _ := strings.Cut(rest, "/")

	return strconv.Itoa(len(project)) + ":" + project + location
}

func (b *InMemoryBackend) loadLocked(key string) *regionLoad {
	ld, ok := b.load[key]
	if !ok {
		ld = &regionLoad{}
		b.load[key] = ld
	}

	return ld
}

// admitLocked starts the execution, or queues it when the region is at its concurrency quota.
func (b *InMemoryBackend) admitLocked(
	er *execRec, wf *workflowspb.Workflow, prog *wfinterp.Program, arg any,
) error {
	name := er.pb.GetName()
	ld := b.loadLocked(regionOf(name))

	if ld.active < b.maxActive {
		ld.active++
		b.launchLocked(er, wf, prog, arg)

		return nil
	}

	if len(ld.queue) >= b.maxQueued {
		return status.Error(codes.ResourceExhausted,
			"Too many requests: the backlog of executions in this region is full.")
	}

	er.pb.State = executionspb.Execution_QUEUED
	er.pending = &pendingRun{wf: wf, prog: prog, arg: arg}
	ld.queue = append(ld.queue, name)

	return nil
}

// promoteLocked starts queued executions while the region has free capacity.
func (b *InMemoryBackend) promoteLocked(key string) {
	ld := b.loadLocked(key)

	for ld.active < b.maxActive && len(ld.queue) > 0 {
		name := ld.queue[0]
		ld.queue = ld.queue[1:]

		rec, ok := b.execs[name]
		if !ok || rec.pending == nil || rec.pb.GetState() != executionspb.Execution_QUEUED {
			continue
		}

		p := rec.pending
		rec.pending = nil
		rec.pb.State = executionspb.Execution_ACTIVE
		ld.active++
		b.launchLocked(rec, p.wf, p.prog, p.arg)
	}

	if ld.active == 0 && len(ld.queue) == 0 {
		delete(b.load, key)
	}
}

func (b *InMemoryBackend) scheduleExpiryLocked(rec *execRec) {
	name := rec.pb.GetName()
	fire := func() { b.expire(name) }

	if ac, ok := b.clock.(afterClock); ok {
		rec.stopTimer = ac.AfterFunc(executionLifetime, fire)

		return
	}

	t := time.AfterFunc(executionLifetime, fire)
	rec.stopTimer = t.Stop
}

func (b *InMemoryBackend) stopTimersLocked() {
	for _, rec := range b.execs {
		if rec.stopTimer != nil {
			rec.stopTimer()
			rec.stopTimer = nil
		}
	}
}

func (b *InMemoryBackend) expire(name string) {
	b.mu.Lock("expire")
	defer b.mu.Unlock()

	rec, ok := b.execs[name]
	if !ok || rec.pb.GetState() != executionspb.Execution_ACTIVE {
		return
	}

	if rec.cancel != nil {
		rec.cancel()
	}

	b.endLocked(name, rec, executionspb.Execution_FAILED, "", &executionspb.Execution_Error{
		Payload: `{"message":"The execution exceeded the maximum duration of 1 year.","tags":["TimeoutError"]}`,
		Context: "The execution exceeded the maximum duration of 1 year.",
	})
}

// sweepExecutions drops finished executions older than the retention period.
func (b *InMemoryBackend) sweepExecutions() {
	now := b.now()

	b.mu.Lock("sweepExecutions")
	defer b.mu.Unlock()

	if now.UnixNano()-b.lastSweep < int64(sweepEvery) {
		return
	}

	b.lastSweep = now.UnixNano()

	for name, rec := range b.execs {
		end := rec.pb.GetEndTime()
		if end != nil && now.Sub(end.AsTime()) >= executionRetention {
			delete(b.execs, name)
		}
	}
}
