package azurefunctions

import (
	"context"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// Hosting is the plan family an app runs on; it decides when an idle host is unloaded.
type Hosting string

// Hosting values; the zero value means no idle policy (the host is kept until stopped or replaced).
const (
	HostingNone        Hosting = ""
	HostingConsumption Hosting = "Consumption"
	HostingFlex        Hosting = "Flex"
	HostingPremium     Hosting = "Premium"
	HostingDedicated   Hosting = "Dedicated"
)

const (
	// Learn (App Service "Configure an App Service app", Always On): an app without Always On is unloaded after
	// 20 minutes without an incoming request.
	dedicatedIdleLimit = 20 * time.Minute
	// Learn (Event-driven scaling, Cold start) says only that an idle Consumption or Flex app can scale to zero
	// after "a few minutes"; the emulator picks five.
	elasticIdleLimit = 5 * time.Minute
	reapInterval     = 30 * time.Second
)

// SetClock replaces the clock that idle reaping measures against.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// idleLimit is how long a running host may go without a request before it is unloaded; false means never.
func (a *appState) idleLimit() (time.Duration, bool) {
	switch a.Hosting {
	case HostingConsumption:
		return elasticIdleLimit, true
	case HostingFlex:
		return elasticIdleLimit, !a.AlwaysReady
	case HostingDedicated:
		return dedicatedIdleLimit, !a.AlwaysOn
	case HostingPremium, HostingNone:
	}

	return 0, false
}

// beginRequest marks an app request in flight so its host is not reaped, and returns the matching end call.
func (b *InMemoryBackend) beginRequest(name string) func() { return b.hold(name, true) }

// hold keeps the app's host from being reaped until the returned call; counts is true for a request to the app
// itself, which also restarts its idle clock (Kudu traffic only holds the host).
func (b *InMemoryBackend) hold(name string, counts bool) func() {
	b.mu.Lock("hold")

	a, ok := b.apps[strs.Fold(name)]
	if ok {
		a.inflight++

		if counts {
			a.lastActive = b.now()
		}
	}

	b.mu.Unlock()

	return func() {
		b.mu.Lock("release")
		defer b.mu.Unlock()

		if cur, found := b.apps[strs.Fold(name)]; found && cur == a {
			cur.inflight--

			if counts {
				cur.lastActive = b.now()
			}
		}
	}
}

// needsHostAlive reports whether something besides requests depends on the running host: a background trigger
// the emulator cannot wake without the host, or a continuous WebJob.
func (a *app) needsHostAlive() bool {
	for _, j := range a.cont {
		if j.status != jobStatusStopped {
			return true
		}
	}

	return !a.isWeb() && hasBackgroundTrigger(a.Zip)
}

// ReapIdle unloads the host of every app that has gone idle past its plan's limit and returns how many it took
// down. The next request starts the host again (a cold start). Apps with in-flight requests, with a stopped
// state, or whose triggers need the host are left alone.
func (b *InMemoryBackend) ReapIdle() int {
	b.mu.Lock("ReapIdle")

	now := b.now()

	var olds []box

	for _, a := range b.apps {
		limit, reapable := a.idleLimit()
		if !reapable || a.containerID == "" || a.starting != nil || a.inflight > 0 || now.Sub(a.lastActive) < limit {
			continue
		}

		if a.needsHostAlive() {
			continue
		}

		olds = append(olds, b.takeBoxLocked(a))
	}

	b.mu.Unlock()

	for _, x := range olds {
		b.teardown(x)
	}

	return len(olds)
}

// startReaper runs ReapIdle every reapInterval until stopReaper; calling it again is a no-op while it runs.
func (b *InMemoryBackend) startReaper(ctx context.Context) {
	b.mu.Lock("startReaper")
	defer b.mu.Unlock()

	if b.stopReap != nil {
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	done := &sync.WaitGroup{}
	b.stopReap, b.reapDone = cancel, done

	done.Go(func() {
		tick := time.NewTicker(reapInterval)
		defer tick.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				b.ReapIdle()
			}
		}
	})
}

func (b *InMemoryBackend) stopReaper() {
	b.mu.Lock("stopReaper")
	cancel, done := b.stopReap, b.reapDone
	b.stopReap, b.reapDone = nil, nil
	b.mu.Unlock()

	if cancel != nil {
		cancel()
		done.Wait()
	}
}
