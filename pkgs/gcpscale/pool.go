package gcpscale

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	// DefaultIdleTimeout is how long an idle instance above the minimum is kept; Cloud Run documents "up to 15 minutes".
	DefaultIdleTimeout = 15 * time.Minute
	// DefaultMaxInstances is Cloud Run's default instance cap.
	DefaultMaxInstances = 100
	minQueueTimeout     = 10 * time.Second
	queueStartupFactor  = 3.5
	avgDivisor          = 2
)

// ErrSaturated means every instance slot stayed busy until the pending-queue timeout (HTTP 429 on Cloud Run).
var ErrSaturated = errors.New("no instance became available: the request timed out in the pending queue")

// ErrClosed means the pool was shut down.
var ErrClosed = errors.New("instance pool is closed")

// ErrRetired means the instance was drained or removed before it finished starting.
var ErrRetired = errors.New("instance was retired while starting")

// Limits are the scaling settings read afresh on every decision. Zero Max means DefaultMaxInstances, zero
// Concurrency means 1.
type Limits struct {
	Min         int
	Max         int
	Concurrency int
}

// Config wires a Pool to the thing it scales. Start gets a context cancelled once its instance is retired (after
// Stop for a started one). Limits and Idle run under the pool lock and must not call back into the pool.
type Config[T any] struct {
	Start       func(ctx context.Context) (T, error)
	Stop        func(T)
	Limits      func() Limits
	Idle        func() time.Duration
	Clock       Clock
	Base        context.Context //nolint:containedctx // lifetime of the instances, set by the worker
	IdleTimeout time.Duration
}

type slot[T any] struct {
	err      error
	idle     Timer
	cancel   context.CancelFunc
	ready    chan struct{}
	val      T
	inflight int
	dead     bool
}

// Pool keeps instances of one revision: requests take a concurrency slot, new instances start while below the
// maximum, and idle ones above the minimum are removed.
type Pool[T any] struct {
	cfg      Config[T]
	mu       *lockmetrics.RWMutex
	changed  chan struct{}
	slots    []*slot[T]
	wg       sync.WaitGroup
	avgStart time.Duration
	closed   bool
}

// New creates an empty pool.
func New[T any](name string, cfg Config[T]) *Pool[T] {
	if cfg.Clock == nil {
		cfg.Clock = RealClock{}
	}

	if cfg.Base == nil {
		cfg.Base = context.Background()
	}

	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = DefaultIdleTimeout
	}

	return &Pool[T]{cfg: cfg, mu: lockmetrics.New("gcpscale." + name), changed: make(chan struct{})}
}

func (p *Pool[T]) limits() (int, int, int) {
	l := p.cfg.Limits()
	if l.Max <= 0 {
		l.Max = DefaultMaxInstances
	}

	return min(l.Min, l.Max), l.Max, max(l.Concurrency, 1)
}

func (p *Pool[T]) notifyLocked() {
	close(p.changed)
	p.changed = make(chan struct{})
}

func (p *Pool[T]) liveLocked() int {
	n := 0

	for _, s := range p.slots {
		if !s.dead {
			n++
		}
	}

	return n
}

// Live returns the number of instances started or starting.
func (p *Pool[T]) Live() int {
	p.mu.RLock("Live")
	defer p.mu.RUnlock()

	return p.liveLocked()
}

func (p *Pool[T]) growLocked() *slot[T] {
	ctx, cancel := context.WithCancel(p.cfg.Base)
	s := &slot[T]{ready: make(chan struct{}), cancel: cancel}
	p.slots = append(p.slots, s)

	p.wg.Go(func() {
		began := p.cfg.Clock.Now()
		val, err := p.cfg.Start(ctx)

		p.mu.Lock("started")

		discard := err == nil && s.dead

		switch {
		case s.dead && p.closed:
			err = errors.Join(ErrClosed, err)
		case s.dead:
			err = errors.Join(ErrRetired, err)
		}

		s.val, s.err = val, err

		if err != nil {
			s.dead = true
			p.removeLocked(s)
		} else {
			d := p.cfg.Clock.Now().Sub(began)
			if p.avgStart == 0 {
				p.avgStart = d
			} else {
				p.avgStart = (p.avgStart + d) / avgDivisor
			}
		}

		close(s.ready)
		p.notifyLocked()
		p.mu.Unlock()

		if discard {
			p.cfg.Stop(val)
		}

		if err != nil {
			cancel()
		}
	})

	return s
}

func (p *Pool[T]) removeLocked(s *slot[T]) {
	for i, x := range p.slots {
		if x == s {
			p.slots = append(p.slots[:i], p.slots[i+1:]...)

			return
		}
	}
}

func (p *Pool[T]) pickLocked(conc int) *slot[T] {
	var starting *slot[T]

	for _, s := range p.slots {
		if s.dead || s.inflight >= conc {
			continue
		}

		select {
		case <-s.ready:
			return s
		default:
			if starting == nil {
				starting = s
			}
		}
	}

	return starting
}

func (p *Pool[T]) idleFor() time.Duration {
	if p.cfg.Idle != nil {
		if d := p.cfg.Idle(); d > 0 {
			return d
		}
	}

	return p.cfg.IdleTimeout
}

func (p *Pool[T]) queueTimeout() time.Duration {
	return max(minQueueTimeout, time.Duration(queueStartupFactor*float64(p.avgStart)))
}

func stopIdle[T any](s *slot[T]) {
	if s.idle != nil {
		s.idle.Stop()
		s.idle = nil
	}
}

// Acquire reserves a request slot, starting an instance when all slots are taken and the pool is below its
// maximum, and queueing for max(10s, 3.5 x observed start time) otherwise (then ErrSaturated).
func (p *Pool[T]) Acquire(ctx context.Context) (T, func(), error) {
	var (
		zero     T
		deadline Timer
	)

	defer func() {
		if deadline != nil {
			deadline.Stop()
		}
	}()

	for {
		p.mu.Lock("Acquire")

		if p.closed {
			p.mu.Unlock()

			return zero, nil, ErrClosed
		}

		_, maxN, conc := p.limits()

		s := p.pickLocked(conc)
		if s == nil && p.liveLocked() < maxN {
			s = p.growLocked()
		}

		if s != nil {
			s.inflight++
			stopIdle(s)
			p.mu.Unlock()

			return p.await(ctx, s)
		}

		changed, wait := p.changed, p.queueTimeout()
		p.mu.Unlock()

		if deadline == nil {
			deadline = p.cfg.Clock.NewTimer(wait)
		}

		select {
		case <-changed:
		case <-deadline.C():
			return zero, nil, ErrSaturated
		case <-ctx.Done():
			return zero, nil, fmt.Errorf("request cancelled while queued: %w", ctx.Err())
		}
	}
}

func (p *Pool[T]) await(ctx context.Context, s *slot[T]) (T, func(), error) {
	var zero T

	select {
	case <-s.ready:
	case <-ctx.Done():
		p.release(s)

		return zero, nil, fmt.Errorf("request cancelled while the instance started: %w", ctx.Err())
	}

	if s.err != nil {
		p.release(s)

		return zero, nil, s.err
	}

	return s.val, func() { p.release(s) }, nil
}

func (p *Pool[T]) release(s *slot[T]) {
	p.mu.Lock("release")
	defer p.mu.Unlock()

	s.inflight--
	p.notifyLocked()
	p.armIdleLocked(s)
}

func (p *Pool[T]) armIdleLocked(s *slot[T]) {
	if s.dead || s.inflight > 0 || s.idle != nil || s.err != nil || p.closed {
		return
	}

	select {
	case <-s.ready:
	default:
		return
	}

	s.idle = p.cfg.Clock.AfterFunc(p.idleFor(), func() { p.idleExpired(s) })
}

func (p *Pool[T]) idleExpired(s *slot[T]) {
	p.mu.Lock("idleExpired")

	minN, _, _ := p.limits()
	if s.dead || s.inflight > 0 || p.liveLocked() <= minN {
		s.idle = nil
		p.mu.Unlock()

		return
	}

	s.dead = true
	p.removeLocked(s)
	p.notifyLocked()
	p.mu.Unlock()

	p.stop(s)
}

func (p *Pool[T]) stop(s *slot[T]) {
	p.cfg.Stop(s.val)
	s.cancel()
}

// retireLocked marks s dead and unlinks it; a slot still starting has its start cancelled. It returns whether s
// is a started instance the caller must stop.
func (p *Pool[T]) retireLocked(s *slot[T]) bool {
	started := false

	select {
	case <-s.ready:
		started = !s.dead && s.err == nil
	default:
		s.cancel()
	}

	s.dead = true
	stopIdle(s)

	return started
}

// Drain retires every instance but keeps the pool open: later requests start fresh ones.
func (p *Pool[T]) Drain() {
	p.mu.Lock("Drain")

	var live []*slot[T]

	for _, s := range p.slots {
		if p.retireLocked(s) {
			live = append(live, s)
		}
	}

	p.slots = nil
	p.notifyLocked()
	p.mu.Unlock()

	for _, s := range live {
		p.stop(s)
	}
}

// Remove retires and stops the started instances match selects (a failed liveness probe, say); the pool starts a
// replacement on demand. It reports whether any instance was removed.
func (p *Pool[T]) Remove(match func(T) bool) bool {
	p.mu.Lock("Remove")

	var gone []*slot[T]

	for _, s := range slices.Clone(p.slots) {
		select {
		case <-s.ready:
		default:
			continue
		}

		if s.dead || s.err != nil || !match(s.val) {
			continue
		}

		p.retireLocked(s)
		p.removeLocked(s)
		gone = append(gone, s)
	}

	if len(gone) > 0 {
		p.notifyLocked()
	}
	p.mu.Unlock()

	for _, s := range gone {
		p.stop(s)
	}

	return len(gone) > 0
}

// Ensure brings the pool to at least n instances (capped at the maximum) and waits until they are ready.
func (p *Pool[T]) Ensure(ctx context.Context, n int) error {
	p.mu.Lock("Ensure")

	if p.closed {
		p.mu.Unlock()

		return ErrClosed
	}

	_, maxN, _ := p.limits()
	for p.liveLocked() < min(n, maxN) {
		p.growLocked()
	}

	waits := make([]*slot[T], 0, n)

	for _, s := range p.slots {
		if !s.dead && len(waits) < n {
			waits = append(waits, s)
		}
	}
	p.mu.Unlock()

	for _, s := range waits {
		select {
		case <-s.ready:
		case <-ctx.Done():
			return fmt.Errorf("waiting for instances: %w", ctx.Err())
		}

		if s.err != nil {
			return s.err
		}
	}

	p.mu.Lock("Ensure.idle")
	for _, s := range waits {
		p.armIdleLocked(s)
	}
	p.mu.Unlock()

	return nil
}

// Close stops every instance and rejects further requests. Starts in flight are cancelled and stopped as they finish.
func (p *Pool[T]) Close() {
	p.mu.Lock("Close")
	p.closed = true

	var live []*slot[T]

	for _, s := range p.slots {
		if p.retireLocked(s) {
			live = append(live, s)
		}
	}

	p.slots = nil
	p.notifyLocked()
	p.mu.Unlock()

	for _, s := range live {
		p.stop(s)
	}

	p.wg.Wait()
}
