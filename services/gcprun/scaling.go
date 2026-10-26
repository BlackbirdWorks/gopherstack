package gcprun

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

const (
	defaultMaxInstances = 100
	// DefaultIdleTimeout is how long an idle instance above the minimum is kept; Cloud Run documents "up to 15 minutes".
	DefaultIdleTimeout = 15 * time.Minute
	minQueueTimeout    = 10 * time.Second
	queueStartupFactor = 3.5
)

var (
	errQueueTimeout  = errors.New("no instance became available: the request timed out in the pending queue")
	errRevisionGone  = errors.New("revision was removed")
	errRetired       = errors.New("instance was retired while starting")
	errInstanceStart = errors.New("instance failed to start")
)

// inst is one running copy of a revision: every container of the revision sharing a network namespace.
type inst struct {
	ready     chan struct{}
	err       error
	idle      Timer
	mctx      context.Context //nolint:containedctx // lifetime of the instance's containers and probes
	cancel    context.CancelFunc
	cleaned   atomic.Bool
	seq       uint64
	ports     map[int]int
	ids       []string
	hostPorts []int
	dirs      []string
	hostPort  int
	inflight  int
	dead      bool
}

func (in *inst) id() string {
	if len(in.ids) == 0 {
		return ""
	}

	return in.ids[0]
}

// SetIdleTimeout sets how long an idle instance above the revision's minimum is kept before it scales to zero.
func (b *InMemoryBackend) SetIdleTimeout(d time.Duration) {
	b.mu.Lock("SetIdleTimeout")
	defer b.mu.Unlock()

	if d > 0 {
		b.idleTimeout = d
	}
}

func (b *InMemoryBackend) scalingLimits(r *revRec) (minN, maxN int) {
	minN, maxN = int(r.pb.GetScaling().GetMinInstanceCount()), int(r.pb.GetScaling().GetMaxInstanceCount())

	if svc := b.services[r.pb.GetService()]; svc != nil {
		if minN == 0 {
			minN = int(svc.pb.GetScaling().GetMinInstanceCount())
		}

		if maxN == 0 {
			maxN = int(svc.pb.GetScaling().GetMaxInstanceCount())
		}
	}

	if maxN <= 0 {
		maxN = defaultMaxInstances
	}

	return min(minN, maxN), maxN
}

func (r *revRec) liveCount() int {
	n := 0

	for _, in := range r.insts {
		if !in.dead {
			n++
		}
	}

	return n
}

func (r *revRec) notifyLocked() {
	close(r.changed)
	r.changed = make(chan struct{})
}

// pickLocked returns the first instance with a free request slot, preferring ones that already serve.
func pickLocked(r *revRec, conc int) *inst {
	var starting *inst

	for _, in := range r.insts {
		if in.dead || in.inflight >= conc {
			continue
		}

		select {
		case <-in.ready:
			return in
		default:
			if starting == nil {
				starting = in
			}
		}
	}

	return starting
}

// growLocked adds a starting instance and brings it up in the background.
func (b *InMemoryBackend) growLocked(name string, r *revRec) *inst {
	in := &inst{ready: make(chan struct{}), seq: b.instSeq.Add(1)}
	in.mctx, in.cancel = context.WithCancel(b.runCtx)
	r.insts = append(r.insts, in)
	pb := clone(r.pb)

	b.wg.Go(func() { b.bringUp(name, in, pb) })

	return in
}

func (b *InMemoryBackend) queueTimeout(r *revRec) time.Duration {
	return max(minQueueTimeout, time.Duration(queueStartupFactor*float64(r.avgStart)))
}

// acquire reserves a request slot on an instance of the revision, starting one when every slot is taken and the
// revision is below max instances, and queueing the request otherwise until it times out (HTTP 429).
func (b *InMemoryBackend) acquire(ctx context.Context, name string) (*inst, func(), error) {
	var deadline Timer

	defer func() {
		if deadline != nil {
			deadline.Stop()
		}
	}()

	for {
		b.mu.Lock("acquire")

		r := b.revisions[name]
		if r == nil {
			b.mu.Unlock()

			return nil, nil, errRevisionGone
		}

		conc := max(int(r.pb.GetMaxInstanceRequestConcurrency()), 1)

		in := pickLocked(r, conc)
		if _, maxN := b.scalingLimits(r); in == nil && r.liveCount() < maxN {
			in = b.growLocked(name, r)
		}

		if in != nil {
			in.inflight++
			stopTimer(in)
			b.mu.Unlock()

			return b.await(ctx, name, in)
		}

		changed, wait := r.changed, b.queueTimeout(r)
		b.mu.Unlock()

		if deadline == nil {
			deadline = b.clk().NewTimer(wait)
		}

		select {
		case <-changed:
		case <-deadline.C():
			return nil, nil, errQueueTimeout
		case <-ctx.Done():
			return nil, nil, fmt.Errorf("request cancelled while queued: %w", ctx.Err())
		}
	}
}

func stopTimer(in *inst) {
	if in.idle != nil {
		in.idle.Stop()
		in.idle = nil
	}
}

func (b *InMemoryBackend) await(ctx context.Context, name string, in *inst) (*inst, func(), error) {
	select {
	case <-in.ready:
	case <-ctx.Done():
		b.release(name, in)

		return nil, nil, fmt.Errorf("request cancelled while the instance started: %w", ctx.Err())
	}

	if in.err != nil {
		b.release(name, in)

		return nil, nil, in.err
	}

	return in, func() { b.release(name, in) }, nil
}

// release frees a request slot and arms the idle timer when the instance has gone quiet.
func (b *InMemoryBackend) release(name string, in *inst) {
	b.mu.Lock("release")
	defer b.mu.Unlock()

	in.inflight--

	if r := b.revisions[name]; r != nil {
		r.notifyLocked()
		b.armIdleLocked(name, r, in)
	}
}

func (b *InMemoryBackend) armIdleLocked(name string, r *revRec, in *inst) {
	if in.dead || in.inflight > 0 || in.idle != nil || in.err != nil {
		return
	}

	select {
	case <-in.ready:
	default:
		return
	}

	in.idle = b.clk().AfterFunc(b.idleTimeout, func() { b.idleExpired(name, r, in) })
}

// idleExpired scales an instance to zero unless it is needed to keep the revision at its minimum.
func (b *InMemoryBackend) idleExpired(name string, r *revRec, in *inst) {
	b.mu.Lock("idleExpired")

	minN, _ := b.scalingLimits(r)
	if in.dead || in.inflight > 0 || r.liveCount() <= minN || b.revisions[name] != r {
		in.idle = nil
		b.mu.Unlock()

		return
	}

	b.detachLocked(r, in)
	r.notifyLocked()
	b.mu.Unlock()

	b.teardownInst(in)
}

// detachLocked marks in dead and removes it from the revision's pool.
func (b *InMemoryBackend) detachLocked(r *revRec, in *inst) {
	in.dead = true
	stopTimer(in)

	for i, x := range r.insts {
		if x == in {
			r.insts = append(r.insts[:i], r.insts[i+1:]...)

			break
		}
	}
}

// ensure brings the revision to at least n instances and waits until they are ready.
func (b *InMemoryBackend) ensure(ctx context.Context, name string, n int) error {
	b.mu.Lock("ensure")

	r := b.revisions[name]
	if r == nil {
		b.mu.Unlock()

		return errRevisionGone
	}

	_, maxN := b.scalingLimits(r)
	for r.liveCount() < min(n, maxN) {
		b.growLocked(name, r)
	}

	waits := make([]*inst, 0, n)

	for _, in := range r.insts {
		if !in.dead && len(waits) < n {
			waits = append(waits, in)
		}
	}
	b.mu.Unlock()

	for _, in := range waits {
		select {
		case <-in.ready:
		case <-ctx.Done():
			return fmt.Errorf("waiting for instances: %w", ctx.Err())
		}

		if in.err != nil {
			return in.err
		}
	}

	b.mu.Lock("ensure.idle")
	for _, in := range waits {
		b.armIdleLocked(name, r, in)
	}
	b.mu.Unlock()

	return nil
}

// startRevision makes sure the revision has the instances its scaling settings call for (at least one).
func (b *InMemoryBackend) startRevision(ctx context.Context, name string) error {
	b.mu.RLock("startRevision")

	r := b.revisions[name]
	if r == nil {
		b.mu.RUnlock()

		return errStartFailed
	}

	minN, _ := b.scalingLimits(r)
	b.mu.RUnlock()

	return b.ensure(ctx, name, max(minN, 1))
}

// warmServing starts the minimum instances of every revision of rec that receives traffic.
func (b *InMemoryBackend) warmServing(svcName string) {
	b.mu.RLock("warmServing")

	var names []string

	for name, r := range b.revisions {
		if r.pb.GetService() != svcName || !b.servingLocked(r) {
			continue
		}

		if minN, _ := b.scalingLimits(r); minN > 0 && len(r.insts) < minN {
			names = append(names, name)
		}
	}
	b.mu.RUnlock()

	for _, name := range names {
		b.wg.Go(func() { _ = b.ensure(b.baseCtx(), name, b.minFor(name)) })
	}
}

func (b *InMemoryBackend) minFor(name string) int {
	b.mu.RLock("minFor")
	defer b.mu.RUnlock()

	if r := b.revisions[name]; r != nil {
		minN, _ := b.scalingLimits(r)

		return minN
	}

	return 0
}

// killInstance retires an instance whose liveness probe failed; Cloud Run starts a replacement on demand.
func (b *InMemoryBackend) killInstance(name string, in *inst) {
	b.mu.Lock("killInstance")

	r := b.revisions[name]
	if r == nil || in.dead {
		b.mu.Unlock()

		return
	}

	b.detachLocked(r, in)
	r.notifyLocked()
	svcName := r.pb.GetService()
	b.mu.Unlock()

	b.teardownInst(in)
	b.warmServing(svcName)
}
