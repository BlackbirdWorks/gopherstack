package gcpbatch

import (
	"context"
	"sync"
	"sync/atomic"

	batchpb "cloud.google.com/go/batch/apiv1/batchpb"
)

// barrier releases its waiters once every task of the group has reached it or has finished.
type barrier struct {
	release chan struct{}
	arrived []atomic.Bool
	once    sync.Once
	count   atomic.Int32
}

func newBarriers(g *batchpb.TaskGroup, tasks int) map[int]*barrier {
	out := map[int]*barrier{}

	for n, r := range g.GetTaskSpec().GetRunnables() {
		if _, ok := r.GetExecutable().(*batchpb.Runnable_Barrier_); ok {
			out[n] = &barrier{release: make(chan struct{}), arrived: make([]atomic.Bool, tasks)}
		}
	}

	return out
}

func (br *barrier) arrive(task int) {
	if !br.arrived[task].CompareAndSwap(false, true) {
		return
	}

	if int(br.count.Add(1)) == len(br.arrived) {
		br.once.Do(func() { close(br.release) })
	}
}

// reachBarrier marks task as arrived at the barrier runnable n and, unless the task already failed, waits for the
// other tasks.
func (ex *execEnv) reachBarrier(ctx context.Context, n, task int, failed bool) {
	br := ex.barriers[n]
	if br == nil {
		return
	}

	br.arrive(task)

	if failed {
		return
	}

	select {
	case <-br.release:
	case <-ctx.Done():
	}
}

// leaveBarriers releases every barrier task has not reached, so a finished or failed task never blocks the others.
func (ex *execEnv) leaveBarriers(task int) {
	for _, br := range ex.barriers {
		br.arrive(task)
	}
}
