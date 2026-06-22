package worker

import (
	"context"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// Sweep describes one periodic task for [RunTickerN]: a component name (for log
// attribution), the tick interval, an optional per-sweep timeout (<=0 disables),
// and the function to run on each tick.
type Sweep struct {
	Fn        func(context.Context)
	Component string
	Interval  time.Duration
	Timeout   time.Duration
}

// RunTickerN runs each sweep on its own independent ticker until ctx is
// cancelled, then returns once all tickers have stopped. It is the multi-ticker
// generalisation of [RunTicker] for janitors that sweep several resources at
// different cadences (e.g. a main pass plus a TTL pass). Every sweep is
// panic-recovered and tagged via [logger.WithWorker] with its own component, so
// one wedged or panicking sweep can neither stall nor crash the others.
func RunTickerN(ctx context.Context, service string, sweeps ...Sweep) {
	cfg := newConfig(nil)

	var wg sync.WaitGroup
	for _, s := range sweeps {
		if s.Fn == nil || s.Interval <= 0 {
			continue
		}

		wg.Add(1)

		go func(s Sweep) {
			defer wg.Done()

			sweepCtx := logger.WithWorker(ctx, service, s.Component)

			ticker := time.NewTicker(s.Interval)
			defer ticker.Stop()

			for {
				select {
				case <-sweepCtx.Done():
					return
				case <-ticker.C:
					runSweep(sweepCtx, s.Timeout, s.Fn, cfg.onPanic)
				}
			}
		}(s)
	}

	wg.Wait()
}
