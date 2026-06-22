package worker

import (
	"context"
	"runtime/debug"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// Group supervises all of a backend's background work — periodic sweeps,
// one-shot delayed callbacks, and tracked goroutines — under a single
// cancellable lifecycle rooted at the service context. It owns the cross-cutting
// concerns so sweeps only do work: every callback is panic-recovered (logged via
// the injected logger and recorded as a "panic" task), each periodic sweep's
// task status is emitted to the injected Metrics, and Stop deterministically
// cancels everything, stops pending timers, and joins the goroutines it owns —
// so nothing outlives the service.
//
// Construct one per service from the service/lifecycle context (never
// context.Background) and call Stop from the service's Shutdown/Close.
type Group struct {
	ctx     context.Context
	cancel  context.CancelFunc
	metrics Metrics
	timers  map[uint64]*time.Timer
	service string
	nextID  uint64
	wg      sync.WaitGroup
	mu      sync.Mutex
	stopped bool
}

// NewGroup returns a Group whose work derives from ctx (the service/lifecycle
// context) and is attributed to service. It always logs via the logger carried
// by ctx (logger.Load) and records telemetry via pkgs/telemetry by default;
// override the telemetry sink with WithMetrics.
func NewGroup(ctx context.Context, service string, opts ...GroupOption) *Group {
	cfg := groupConfig{metrics: telemetryMetrics{}}
	for _, opt := range opts {
		opt(&cfg)
	}

	gctx, cancel := context.WithCancel(ctx)

	return &Group{
		ctx:     gctx,
		cancel:  cancel,
		metrics: cfg.metrics,
		timers:  make(map[uint64]*time.Timer),
		service: service,
	}
}

// Go runs fn in a tracked, panic-recovered goroutine that Stop joins. fn should
// return when its context is cancelled (Stop cancels it). No-op after Stop.
func (g *Group) Go(component string, fn func(context.Context)) {
	g.mu.Lock()
	if g.stopped {
		g.mu.Unlock()

		return
	}
	g.wg.Add(1)
	g.mu.Unlock()

	ctx := logger.WithWorker(g.ctx, g.service, component)

	go func() {
		defer g.wg.Done()
		defer g.recoverPanic(ctx, component)

		fn(ctx)
	}()
}

// Ticker runs sweep once per interval until Stop, as a tracked goroutine. Each
// sweep is panic-recovered and its task status ("success"/"panic") is recorded
// to the group's Metrics; when timeout > 0 each sweep is bounded by that
// deadline. Pass WithImmediate to sweep once before the first tick.
func (g *Group) Ticker(
	component string,
	interval, timeout time.Duration,
	sweep func(context.Context),
	opts ...Option,
) {
	cfg := newConfig(opts)

	g.Go(component, func(ctx context.Context) {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		if cfg.immediate {
			g.sweepOnce(ctx, component, timeout, sweep, cfg.onPanic)
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				g.sweepOnce(ctx, component, timeout, sweep, cfg.onPanic)
			}
		}
	})
}

// sweepOnce invokes sweep (optionally under a per-sweep timeout), recovering
// panics and recording the task status to Metrics. ctx is already worker-tagged.
func (g *Group) sweepOnce(
	ctx context.Context,
	component string,
	timeout time.Duration,
	sweep func(context.Context),
	onPanic func(any),
) {
	sctx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		sctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	status := "success"

	func() {
		defer func() {
			if r := recover(); r != nil {
				status = "panic"
				g.logPanic(sctx, component, r)

				if onPanic != nil {
					onPanic(r)
				}
			}
		}()

		sweep(sctx)
	}()

	g.metrics.RecordTask(g.service, component, status)
}

// After schedules fn to run once after d. It is the resilient, stoppable
// replacement for raw time.AfterFunc: the timer is tracked so Stop cancels it,
// and fn is panic-recovered. No-op after Stop.
func (g *Group) After(component string, d time.Duration, fn func()) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.stopped {
		return
	}

	id := g.nextID
	g.nextID++

	ctx := logger.WithWorker(g.ctx, g.service, component)
	g.timers[id] = time.AfterFunc(d, func() {
		g.removeTimer(id)
		defer g.recoverPanic(ctx, component)
		fn()
	})
}

// recoverPanic recovers and logs a panic from a goroutine/timer (not a periodic
// sweep, which records its own task status). Used as a deferred backstop.
func (g *Group) recoverPanic(ctx context.Context, component string) {
	if r := recover(); r != nil {
		g.logPanic(ctx, component, r)
		g.metrics.RecordTask(g.service, component, "panic")
	}
}

func (g *Group) logPanic(ctx context.Context, component string, r any) {
	logger.Load(ctx).ErrorContext(ctx,
		"worker: recovered from panic in background task",
		"component", component,
		"panic", r,
		"stack", string(debug.Stack()),
	)
}

func (g *Group) removeTimer(id uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()

	delete(g.timers, id)
}

// Stop cancels the group context, stops every pending one-shot timer, and waits
// for all Go/Ticker goroutines to return. Safe to call multiple times.
func (g *Group) Stop() {
	g.mu.Lock()
	if g.stopped {
		g.mu.Unlock()

		return
	}
	g.stopped = true

	timers := make([]*time.Timer, 0, len(g.timers))
	for _, t := range g.timers {
		timers = append(timers, t)
	}
	g.timers = nil
	g.mu.Unlock()

	g.cancel()
	for _, t := range timers {
		t.Stop()
	}
	g.wg.Wait()
}
