package gcprun

import (
	"context"
	"errors"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpscale"
)

var (
	errRevisionGone  = errors.New("revision was removed")
	errInstanceStart = errors.New("instance failed to start")
)

// DefaultIdleTimeout is how long an idle instance above the minimum is kept; Cloud Run documents "up to 15 minutes".
const DefaultIdleTimeout = gcpscale.DefaultIdleTimeout

// SetIdleTimeout sets how long an idle instance above the revision's minimum is kept before it scales to zero.
func (b *InMemoryBackend) SetIdleTimeout(d time.Duration) {
	b.mu.Lock("SetIdleTimeout")
	defer b.mu.Unlock()

	if d > 0 {
		b.idleTimeout = d
	}
}

func (b *InMemoryBackend) idleFor() time.Duration {
	b.mu.RLock("idleFor")
	defer b.mu.RUnlock()

	return b.idleTimeout
}

// backendClock follows SetClock for pools created before the swap.
type backendClock struct{ b *InMemoryBackend }

func (c backendClock) Now() time.Time { return c.b.clk().Now() }

func (c backendClock) NewTimer(d time.Duration) Timer { return c.b.clk().NewTimer(d) }

func (c backendClock) AfterFunc(d time.Duration, f func()) Timer { return c.b.clk().AfterFunc(d, f) }

func (b *InMemoryBackend) scalingLimits(r *revRec) (int, int) {
	minN, maxN := int(r.pb.GetScaling().GetMinInstanceCount()), int(r.pb.GetScaling().GetMaxInstanceCount())

	if svc := b.services[r.pb.GetService()]; svc != nil {
		if minN == 0 {
			minN = int(svc.pb.GetScaling().GetMinInstanceCount())
		}

		if maxN == 0 {
			maxN = int(svc.pb.GetScaling().GetMaxInstanceCount())
		}
	}

	if maxN <= 0 {
		maxN = gcpscale.DefaultMaxInstances
	}

	return min(minN, maxN), maxN
}

func (b *InMemoryBackend) newPoolLocked(name string, r *revRec) *gcpscale.Pool[*inst] {
	return gcpscale.New(lastSegment(name), gcpscale.Config[*inst]{
		Start: func(ctx context.Context) (*inst, error) { return b.startInst(ctx, name) },
		Stop:  b.teardownInst,
		Limits: func() gcpscale.Limits {
			b.mu.RLock("limits")
			defer b.mu.RUnlock()

			minN, maxN := b.scalingLimits(r)

			return gcpscale.Limits{
				Min: minN, Max: maxN, Concurrency: int(r.pb.GetMaxInstanceRequestConcurrency()),
			}
		},
		Idle:  b.idleFor,
		Clock: backendClock{b: b},
		Base:  b.runCtx,
	})
}

// poolFor returns the revision's instance pool, creating it on first use.
func (b *InMemoryBackend) poolFor(name string) (*gcpscale.Pool[*inst], string, error) {
	b.mu.Lock("poolFor")
	defer b.mu.Unlock()

	r := b.revisions[name]
	if r == nil {
		return nil, "", errRevisionGone
	}

	if r.pool == nil {
		r.pool = b.newPoolLocked(name, r)
	}

	return r.pool, r.pb.GetService(), nil
}

func goneIfClosed(err error) error {
	if errors.Is(err, gcpscale.ErrClosed) {
		return errRevisionGone
	}

	return err
}

// acquire reserves a request slot on an instance of the revision (see gcpscale.Pool.Acquire).
func (b *InMemoryBackend) acquire(ctx context.Context, name string) (*inst, func(), error) {
	pool, _, err := b.poolFor(name)
	if err != nil {
		return nil, nil, err
	}

	in, release, err := pool.Acquire(ctx)

	return in, release, goneIfClosed(err)
}

// ensure brings the revision to at least n instances and waits until they are ready.
func (b *InMemoryBackend) ensure(ctx context.Context, name string, n int) error {
	pool, _, err := b.poolFor(name)
	if err != nil {
		return err
	}

	return goneIfClosed(pool.Ensure(ctx, n))
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

	mins := map[string]int{}

	for name, r := range b.revisions {
		if r.pb.GetService() != svcName || !b.servingLocked(r) {
			continue
		}

		if minN, _ := b.scalingLimits(r); minN > 0 {
			mins[name] = minN
		}
	}
	b.mu.RUnlock()

	for name, minN := range mins {
		pool, _, err := b.poolFor(name)
		if err != nil || pool.Live() >= minN {
			continue
		}

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
	pool, svcName, err := b.poolFor(name)
	if err != nil {
		return
	}

	if pool.Remove(func(x *inst) bool { return x == in }) {
		b.warmServing(svcName)
	}
}
