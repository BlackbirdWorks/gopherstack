package worker

import (
	"context"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// Group supervises all of a backend's background work — periodic sweeps,
// one-shot delayed callbacks, and tracked goroutines — under a single
// cancellable lifecycle with built-in panic recovery. It is the standard
// start/stop/timer handle a backend holds: construct one in the backend
// constructor and call [Group.Stop] from the backend's Close. Every registered
// callback is panic-recovered (logged via the worker-tagged logger) so one
// failure never tears down the goroutine or the process, and Stop deterministically
// cancels everything, stops pending timers, and joins the goroutines it owns —
// so nothing outlives the backend (the raw time.AfterFunc / go func leak class).
type Group struct {
	ctx     context.Context
	cancel  context.CancelFunc
	timers  map[uint64]*time.Timer
	service string
	nextID  uint64
	wg      sync.WaitGroup
	mu      sync.Mutex
	stopped bool
}

// NewGroup returns a Group whose work is attributed to the given service name.
func NewGroup(service string) *Group {
	ctx, cancel := context.WithCancel(context.Background())

	return &Group{
		ctx:     ctx,
		cancel:  cancel,
		timers:  make(map[uint64]*time.Timer),
		service: service,
	}
}

// Go runs fn in a tracked, panic-recovered goroutine that Stop joins. fn should
// return when its context is cancelled (Stop cancels it). It is a no-op after Stop.
func (g *Group) Go(component string, fn func(context.Context)) {
	g.mu.Lock()
	if g.stopped {
		g.mu.Unlock()

		return
	}
	g.wg.Add(1)
	g.mu.Unlock()

	go func() {
		defer g.wg.Done()

		safely(logger.WithWorker(g.ctx, g.service, component), fn, nil)
	}()
}

// Ticker runs sweep once per interval (optional per-sweep timeout) until Stop,
// as a tracked goroutine. It is the [Group] form of [RunTicker].
func (g *Group) Ticker(component string, interval, timeout time.Duration, sweep func(context.Context)) {
	g.Go(component, func(ctx context.Context) {
		RunTicker(ctx, g.service, component, interval, timeout, sweep)
	})
}

// After schedules fn to run once after d. It is the resilient, stoppable
// replacement for raw time.AfterFunc: the timer is tracked so Stop cancels it,
// and fn is panic-recovered. It is a no-op after Stop.
func (g *Group) After(component string, d time.Duration, fn func()) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.stopped {
		return
	}

	id := g.nextID
	g.nextID++
	g.timers[id] = time.AfterFunc(d, func() {
		g.removeTimer(id)
		safely(logger.WithWorker(g.ctx, g.service, component), func(context.Context) { fn() }, nil)
	})
}

func (g *Group) removeTimer(id uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()

	delete(g.timers, id)
}

// Stop cancels the group context, stops every pending one-shot timer, and waits
// for all Go/Ticker goroutines to return. It is safe to call multiple times.
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
